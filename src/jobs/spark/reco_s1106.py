# -*- coding: utf-8 -*-
"""
Created on Mon Mar 27 17:50:00 2023

@author: wy
"""
import decimal
import json
from datetime import datetime, timedelta

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window

from client.redis import RedisClientRegistry
from configs import Context
from configs.model.base import BaseConfig
from core.job.registry import JobRegistry
from core.spark.table import TableInserter
from core.utils import DateHelper, Datetime, df_view, prt_table_view, rdd_iterate, tr_logger
from jobs.base import SparkRecoJob


@JobRegistry.register(JobRegistry.T.INFER)
class RecoS1106(SparkRecoJob):

    _PROD_INFO_KEY_TMP = 'tr|s1106|prod_info|{prod_id}'
    _CUST_PRODS_KEY_TMP = 'tr|s1106|cust_prods|{cust_id}'
    _SCENE_ID = 'S1106'

    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self.spark.sql('set spark.sql.hive.convertMetastoreParquet=false')

        self._date_helper = DateHelper(self.batch_id, self._base.react_delay)
        self._table_inserter = TableInserter(self.output_table_info_list(self._base)[0])
        self._redis = RedisClientRegistry.get(self._base.redis)
        assert self._scene.adhoc, 'config not set, check your dag id or dag config'
        self._redis_ttl = self._scene.adhoc.redis_key_ttl_ex
        self._default_prod = self._scene.adhoc.s1106_base_product
        self._max_prod_num = self._scene.adhoc.s1106_max_prod_num

    def run(self):
        # 这个表可能有200w容量
        with prt_table_view(
            self, self._base.external_table.user_feat_ls_crd, 'user_feat', self.batch_id
        ), prt_table_view(
            self, self._base.external_table.rbw_06_06, 'rbw_06_06', self.batch_id, 'partitionbid'
        ) as login_dt, prt_table_view(
            # 短信营销授权
            self,
            self._base.external_table.user_stat_mt,
            'user_stat_mt',
            self.batch_id,
        ):
            data_date_start = (Datetime.from_str(login_dt) + timedelta(days=-7)).str
            cust = self.run_spark_sql(
                f"""
                SELECT DISTINCT a.cust_id
                FROM
                  (SELECT cust_id
                   FROM user_feat
                   WHERE debcard_ind = '1'
                     AND pmbs_cust_ind = '1'
                     AND tot_asset_n_zhixiao_month_avg_bal > 10000
                   GROUP BY cust_id) a
                JOIN
                  (SELECT cust_id
                   FROM rbw_06_06
                   WHERE lbl_06_06_007 >= '{data_date_start}' -- lbl_06_06_007：最近一次登陆手机银行时间
                   GROUP BY cust_id) b
                ON a.cust_id = b.cust_id
                LEFT JOIN
                  (SELECT cust_id,
                          1 AS tmp
                   FROM user_stat_mt
                   WHERE camp_stat='1' ) AS c ON a.cust_id = c.cust_id
                WHERE c.tmp=1
                """
            )

        # TODO 这里的prod的risk_level、invest_term这些是全null，反正测试逻辑就这样
        # 在售未售罄产品逻辑
        #     注：下面不一定要加1天，因为手机银行是提前一天计算结果次日推送，所以要+1天，短信当天投放的话可能不需要
        #     TODO：需要跟竹间确认一下他们上传fin_tbproduct这个表格的时间
        start_date = self._date_helper.add_day(1).str
        with prt_table_view(
            self, self._base.external_table.fin_tbproduct, 'fin_tbproduct', self.batch_id
        ):
            prod = self.run_spark_sql(
                f"""
                SELECT prd_code as prod_id, list_type, prd_time_limit
                FROM fin_tbproduct
                WHERE channels like '%7%'
                  AND status in ('0', '1', '6')
                  AND (reserve1 not in ('1301', 'D321')
                       OR (reserve1 in ('1301', 'D321') AND cycle_tradable = '1'))
                  AND per_allow = '1'
                  AND (ipo_end_date_outer = 0
                       OR ipo_end_date_outer IS NULL
                       OR ipo_end_date_outer >= {start_date})
                """
            ).dropDuplicates(['prod_id'])

        # 客户产品交叉
        cust_prod = cust.crossJoin(prod)
        # 风险等级过滤
        cust_prod = self._filter_risk(cust_prod).cache()
        # 投资期限 过滤
        cust_prod = self._filter_prd_time_limit(cust_prod, prod)
        # 获取产品信息，给下游BI
        prod_info = self._get_prod_info(prod)
        # 聚合产品列表
        result = self._agg_prod_list(
            cust_prod.join(prod_info, ['prod_id'], 'left'),
            F.desc('benchmark_f'),  # 按起购业绩比较基准降序
            self._max_prod_num,
        )
        # 填充没有匹配到的客户，这些客户推送默认产品
        result = cust.join(result, ['cust_id'], how='left')
        # 填充客户手机号
        result = self._with_cust_info(result)
        # 保存结果至hive
        self._table_inserter.insert_df(result, dt=self.batch_id, dag_id=self._SCENE_ID)
        # 保存结果至redis
        self._save_cust_to_redis()
        self._save_prod_to_redis(prod_info)

    def _filter_prd_time_limit(self, cust_prod_df, prod_df):
        with prt_table_view(
            self, self._base.external_table.cstpro_ind_chrem, 'cstpro_ind_chrem', self.batch_id
        ), df_view(self, prod_df, 'prod',), df_view(
            self, cust_prod_df, 'cust_prod',
        ):
            df = self.run_spark_sql(
                """

            SELECT a.cust_id,
                   a.prod_id,
                   b.prd_time_limit,
                   nvl(c.avg_prd_time_limit, 1) as avg_prd_time_limit
            FROM
              (SELECT *
               FROM cust_prod) AS a
            LEFT JOIN
              (SELECT prod_id,
                      prd_time_limit
               FROM prod) AS b ON a.prod_id = b.prod_id
            LEFT JOIN
              (SELECT cust_id,
                      avg(prd_time_limit) AS avg_prd_time_limit
               FROM
                 (SELECT a.cust_id,
                         a.prod_id,
                         b.prd_time_limit
                  FROM
                    (SELECT cust_id,
                            prod_id
                     FROM cstpro_ind_chrem
                     WHERE rct1y_subsc_amt > 0 ) AS a
                  LEFT JOIN
                    (SELECT prod_id,
                            nvl(prd_time_limit, 0) AS prd_time_limit
                     FROM prod) AS b ON a.prod_id = b.prod_id) ALL
               GROUP BY cust_id) AS c ON a.cust_id = c.cust_id
            """
            )
            return df.filter('prd_time_limit <= avg_prd_time_limit')

    def _filter_risk(self, cust_prod_df):

        with prt_table_view(
            self, self._base.external_table.user_feat_ls_crd, 'user_feat_ls_crd', self.batch_id
        ), prt_table_view(
            self, self._base.external_table.rbw_08_hd, 'rbw_08_hd', self.batch_id
        ), prt_table_view(
            self, self._base.external_table.rbw_04_27, 'rbw_04_27', self.batch_id, 'partitionbid'
        ), prt_table_view(
            self, self._base.external_table.rbw_07, 'rbw_07', self.batch_id, 'partitionbid'
        ), prt_table_view(
            self, self._base.external_table.rbw_04_10, 'rbw_04_10', self.batch_id, 'partitionbid'
        ):
            cust_white_list = self.run_spark_sql(
                f"""
                select
                b1.cust_id,  -- 客户号
                risk_fin_valid_ind,  -- 是否有效风评
                is_new_client,  -- 是否理财新户
                over_star_card_ind,  -- 是否持有星钻卡的客户
                salary_ind,  -- 是否代发工资客户
                pension_ind,  -- 是否代发养老金客户
                service_level_cust,  -- 客户等级（判断53或54）
                auth_type,  -- 合格投资者认证
                risk_level_cust  -- 理财风险等级
                from
                    (select distinct cust_id, risk_fin_valid_ind, over_star_card_ind, salary_ind, pension_ind
                    from user_feat_ls_crd
                    where debcard_ind = '1' and pmbs_cust_ind = '1'
                    ) b1

                left join
                    (select distinct cust_id, lbl_04_27_006 is_new_client
                    from rbw_04_27
                    where lbl_04_27_006 = '1'
                    ) b3
                on b1.cust_id = b3.cust_id

                left join
                    (select distinct cust_id, lbl_07_00_002 service_level_cust
                    from rbw_07
                    ) b4
                on b1.cust_id = b4.cust_id

                left join
                    (select distinct cust_id, '1' auth_type
                    from rbw_04_10
                    where (lbl_04_10_054 = '1' or lbl_04_10_055 = '1')  -- lbl_04_10_054：渠道认证，lbl_04_10_055：柜面认证
                    ) b5
                on b1.cust_id = b5.cust_id

                left join
                    (select distinct cust_id, lbl_08_00_001 risk_level_cust
                    from rbw_08_hd
                    where lbl_08_00_001 in ('1', '2', '3', '4', '5')  -- 理财风险等级
                    ) b6
                on b1.cust_id = b6.cust_id

                """
            ).dropDuplicates(['cust_id'])

        with prt_table_view(
            self, self._base.external_table.fin_tbproduct, 'fin_tbproduct', self.batch_id
        ):
            prod_white_list = self.run_spark_sql(
                f"""
                select prd_code,
                if(is_new_client_prd = '1', '1', '0') is_new_client_prd,  -- 是否理财新户产品
                if(diamond_card_flag = '1', '1', '0') diamond_card_flag,  -- 星钻卡产品
                if(salary_card_flags = '1', '1', '0') salary_card_flags,  -- 代发工资产品
                if(pension_funds_flag = '1', '1', '0') pension_funds_flag,  -- 代发养老金产品
                if(service_level is null, '0', service_level) service_level,  -- 产品等级
                if(ipo_type = '2', '2', '1null9') ipo_type,  -- 是不是私募产品 TODO(shenxin2@bosc.cn)：这个1null9没懂
                risk_level
                from fin_tbproduct
                """
            ).dropDuplicates(['prd_code'])

        cust_prod_df.createOrReplaceTempView('table_t1')
        cust_white_list.createOrReplaceTempView('table_t2')
        prod_white_list.createOrReplaceTempView('table_t3')

        with prt_table_view(
            self, self._base.external_table.fin_whiteblacklist, 'fin_whiteblacklist', self.batch_id
        ):

            risk = self.run_spark_sql(
                """
                select
                cust_id,
                prod_id,

                -- 有白名单/标签的产品，要与客户进行匹配
                case
                    -- 1、白名单理财产品、客户在白名单内
                    when list_type = '1' then if(in_whitelist = 1, 1 ,0)
                    -- 2、理财新户产品、客户有理财新户标签
                    when is_new_client_prd = '1' then if(is_new_client = is_new_client_prd, 1, 0)
                    -- 3、星钻卡、客户等级是53或54
                    when diamond_card_flag = '1' then if(diamond_card_flag = '1' and service_level_cust in ('53', '54'), 1, 0)
                    -- 4、代发工资、客户有代发工资标签
                    when salary_card_flags = '1' then if(salary_ind = salary_card_flags, 1, 0)
                    -- 5、代发养老金、客户有代发养老金标签
                    when pension_funds_flag = '1' then if(pension_ind = pension_funds_flag, 1, 0)
                    -- 客户理财等级≥理财产品发售对象等级
                    else if(service_level_cust >= service_level, 1, 0)
                end as service_match,  -- service_match: 白名单/标签的产品是否满足

                -- 产品是私募的 则客户合格投资者认证=渠道认证 or 合格投资者认证=柜面认证
                if(ipo_type = '2', if(auth_type = '1', 1, 0), 1) auth_match,  -- auth_match: 合格投资者认证是否满足

                -- 客户没有做过理财风险评估的，或者做了理财预风评的、或者做了理财风险评估但是过期的，这些人不纳入匹配（也就是所有产品都可以推送）；
                -- 仅针对做了风险评估，且在有效期内的客户进行风险承受能力匹配（产品风险等级≥客户的），匹配符合条件的可以看到反之则不能。不纳入匹配的可以看到产品。
                if(risk_fin_valid_ind = '1', if(risk_level_cust >= risk_level, 1, 0), 1) risk_match  -- auth_match: 合格投资者认证是否满足

                from
                    (
                    select
                        a.cust_id,
                        a.prod_id,
                        a.list_type,

                        -- 客户的标签
                        risk_fin_valid_ind,
                        if(is_new_client = '1', '1', '0') is_new_client,
                        if(over_star_card_ind = '1', '1', '0') over_star_card_ind,
                        if(salary_ind = '1', '1', '0') salary_ind,
                        if(pension_ind = '1', '1', '0') pension_ind,
                        if(service_level_cust is null, '0', service_level_cust) service_level_cust,
                        if(auth_type = '1', '1', '0') auth_type,
                        if(risk_level_cust is null, '0', risk_level_cust) risk_level_cust,

                        -- 产品的标签
                        coalesce(is_new_client_prd, '0') is_new_client_prd,
                        coalesce(diamond_card_flag, '0') diamond_card_flag,
                        coalesce(salary_card_flags, '0') salary_card_flags,
                        coalesce(pension_funds_flag, '0') pension_funds_flag,
                        if(service_level is null or service_level = ' ', '0', service_level) service_level,
                        ipo_type,
                        cast(risk_level as string) risk_level,

                        -- 客户产品白名单
                        whitelist.tmp as in_whitelist

                    from table_t1 as a
                    left join
                        table_t2 as cust
                        on a.cust_id = cust.cust_id
                    left join
                        table_t3 as prod
                        on a.prod_id = prod.prd_code
                    left join (
                        select prd_code, cust_id, 1 as tmp from fin_whiteblacklist
                    ) as whitelist
                        on a.prod_id = whitelist.prd_code and a.cust_id = whitelist.cust_id
                    ) rule
                """
            )

        return risk.filter('service_match = 1 and auth_match = 1 and risk_match = 1 ')

    def _save_cust_to_redis(self):
        df = self.run_spark_sql(
            f"""
            SELECT *
            FROM {self._table_inserter.table_name}
            WHERE dt='{self.batch_id}'
              AND dag_id='{self._SCENE_ID}'
        """
        )

        def _save_row(row):
            prod_list = row['prod_list'] or []
            data = {'prod_list': prod_list, 'default': self._default_prod}
            data['cust_name'] = row['cust_name']
            data['phone_number'] = row['phone_number']

            key = self._CUST_PRODS_KEY_TMP.format(cust_id=row['cust_id'])
            value = json.dumps(data, ensure_ascii=False)
            self._redis.set(key, value, ex=self._redis_ttl)

        for row in rdd_iterate(df.rdd):
            _save_row(row)

    def _save_prod_to_redis(self, df):
        # fill required fields
        df = df.dropDuplicates(['prod_id'])

        def _save_row(row):
            key = self._PROD_INFO_KEY_TMP.format(prod_id=row['prod_id'])

            output_info = get_prod_output_info(row.asDict(), self._SCENE_ID)
            filter_info = get_prod_filter_info(row.asDict(), 'chrem')

            value = json.dumps(
                {'output_info': output_info, 'filter_info': filter_info,}, ensure_ascii=False,
            )
            print(key, value)
            self._redis.set(key, value, ex=self._redis_ttl)

        for row in rdd_iterate(df.rdd):
            _save_row(row)

    def _agg_prod_list(self, df, order_by, max_len: int = 10):
        group_by: str = 'cust_id'
        agg: str = 'prod_id'
        output: str = 'prod_list'
        # group prod list
        tmp_list_field = '_tmp_list'
        window = Window.partitionBy(group_by).orderBy(order_by)
        result = (
            df.withColumn(tmp_list_field, F.collect_list(agg).over(window))
            .groupBy(group_by)
            .agg(F.max(tmp_list_field).alias(tmp_list_field))
        )

        # truncate prod list
        @F.udf(returnType=T.ArrayType(T.StringType()))
        def _truncate_prod_list(all_prod_list):
            if not all_prod_list:
                return None
            return all_prod_list[:max_len]

        result = result.withColumn(output, _truncate_prod_list(tmp_list_field))
        result = result.drop(tmp_list_field)
        return result

    def _with_cust_info(self, df):
        # DONE: fill cust name and cust phone number
        with prt_table_view(
            self, self._base.external_table.rbw_01, 'rbw_01', self.batch_id, 'partitionbid'
        ), df_view(
            self, df, 'table_without_cust_info',
        ):
            result = self.run_spark_sql(
                """
            SELECT a.cust_id,
                   a.prod_list,
                   b.lbl_01_00_001 AS cust_name,
                   b.lbl_01_00_023 AS phone_number
            FROM
              (SELECT *
               FROM table_without_cust_info) AS a
            LEFT JOIN
              (SELECT cust_id,
                      lbl_01_00_001,
                      lbl_01_00_023
               FROM rbw_01
               WHERE lbl_01_00_023 IS NOT NULL
                  AND lbl_01_00_001 IS NOT NULL
                  AND LENGTH(lbl_01_00_023)=11) AS b ON a.cust_id=b.cust_id
            """
            )
        return result

    def _get_prod_info(self, prod_df):
        assert self._scene.adhoc
        default_prod_df = self.spark.createDataFrame(
            [{'prod_id': self._scene.adhoc.s1106_base_product}]
        )

        prod_df_with_default = prod_df.select('prod_id').union(default_prod_df)
        prod_df_with_default.cache()
        self.logger.info('prod_df_with_default:')
        prod_df_with_default.show(200, False)
        return self._with_prod_info(self, prod_df_with_default)

    @staticmethod
    def _with_prod_info(job: SparkRecoJob, prod_df):
        with prt_table_view(
            job, job._base.external_table.fin_tbproduct, 'fin_tbproduct', job.batch_id
        ), df_view(job, prod_df, 'prod_df'):
            prod_info = job.run_spark_sql(
                f"""
                select a.prod_id, b.*
                from
                (
                select prod_id from prod_df
                ) as a
                left join
                (
                select prd_code,
                       ta_code,
                       prd_name,
                       prd_name2,
                       pfirst_amt, -- 起购金额
                       prd_invest_type, -- 投资类型
                       prd_time_limit, -- 产品期限
                       nav,
                       nav_date,
                       ipo_start_date,
                       ipo_end_date,
                       estab_date,
                       income_date,
                       end_date,
                       risk_level, -- 风险等级
                       reserve1,
                       ta_name,
                       ta_shortname,
                       benchmark,
                       modelcomment_desc,
                       nav_ratio,
                       cycle_days,
                       guest_type,
                       estab_ratio,
                       day_yearrate,
                       week_yearrate,
                       two_week_yearrate,
                       month_yearrate,
                       two_month_yearrate,
                       three_month_yearrate,
                       half_year_yearrate,
                       year_yearrate,
                       rate1,
                       twoyear_yearrate,
                       threeyear_yearrate,
                       benchmark_show,
                       lock_days,
                       term_enjoy_type,
                       CASE WHEN (guest_type = '0') THEN estab_ratio
                           WHEN (guest_type = '1') THEN day_yearrate
                           WHEN (guest_type = '2') THEN week_yearrate
                           WHEN (guest_type = '3') THEN two_week_yearrate
                           WHEN (guest_type = '4') THEN month_yearrate
                           WHEN (guest_type = '5') THEN two_month_yearrate
                           WHEN (guest_type = '6') THEN three_month_yearrate
                           WHEN (guest_type = '7') THEN half_year_yearrate
                           WHEN (guest_type = '8') THEN year_yearrate
                           WHEN (guest_type = '9') THEN rate1
                           WHEN (guest_type = 'a') THEN twoyear_yearrate
                           WHEN (guest_type = 'b') THEN threeyear_yearrate
                           ELSE '' END as tb_reco_1
                      from fin_tbproduct
                ) as b
                on a.prod_id = b.prd_code
                """
            ).dropDuplicates(['prod_id'])

        # prod_info = prod_info.withColumn(
        #     'tb_reco_1', F.concat(F.format_number(F.col('tb_reco_1_raw') * 100, 3), F.lit('%'))
        # )

        @F.udf(returnType=T.FloatType())
        def _process_benchmark(inds_comp_base):

            if type(inds_comp_base) != str:
                return 0

            if '~' in inds_comp_base:
                inds_comp_base = inds_comp_base.split('~')[-1]

            try:
                res = float(inds_comp_base) * 100
            except:
                if inds_comp_base[-1] == '%':
                    try:
                        res = float(inds_comp_base[:-1])
                    except:
                        res = 0
                else:
                    res = 0
            return res

        # extract 业绩基准，example：1.0%~2.0%
        prod_info = prod_info.withColumn('benchmark_f', _process_benchmark('benchmark'))
        prod_info.cache()
        return prod_info

    @staticmethod
    def output_table_info_list(cfg: BaseConfig):
        return [
            {
                'name': cfg.output_table.s1106_results,
                'comment': '人群包',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': ''},
                    {'name': 'cust_name', 'type': 'string', 'comment': ''},
                    {'name': 'phone_number', 'type': 'string', 'comment': ''},
                    {'name': 'prod_list', 'type': 'array<string>', 'comment': ''},
                ],
                'partition_field_list': [
                    {'name': 'dt', 'type': 'string', 'comment': '日期分区'},
                    {'name': 'dag_id', 'type': 'string', 'comment': '一个dag的唯一标识'},
                ],
            },
        ]


RENAME_MAP = {
    'prd_name2': 'prd_nametwo',
    'reserve1': 'reserveone',
    'rate1': 'rateone',
    'tb_reco_1': 'tb_reco_one',
    'tb_reco_2': 'tb_reco_two',
    'tb_reco_3': 'tb_reco_three',
    'tb_reco_4': 'tb_reco_four',
    'tb_reco_5': 'tb_reco_five',
    'tb_reco_6': 'tb_reco_six',
    'tb_reco_7': 'tb_reco_seven',
    'tb_reco_8': 'tb_reco_eight',
    'tb_reco_9': 'tb_reco_nine',
    'tb_reco_10': 'tb_reco_ten',
}

REQUIRED_KEYS = [
    # cust
    'kehuhao',
    'kehuxingming',
    'phoneNumber',
    'scene_id',
    'msgmodel_id',
    # prod
    'prd_code',
    'ta_code',
    'prd_name',
    'prd_name2',
    'nav',
    'nav_date',
    'ipo_start_date',
    'ipo_end_date',
    'estab_date',
    'income_date',
    'end_date',
    'risk_level',
    'reserve1',
    'ta_name',
    'ta_shortname',
    'benchmark',
    'modelcomment_desc',
    'nav_ratio',
    'cycle_days',
    'min_hold_days',  # TODO: 持有其天数
    'term_enjoy_type',  # TODO: 期限享分类
    'guest_type',
    'estab_ratio',
    'day_yearrate',
    'week_yearrate',
    'two_week_yearrate',
    'month_yearrate',
    'two_month_yearrate',
    'three_month_yearrate',
    'half_year_yearrate',
    'year_yearrate',
    'rate1',
    'twoyear_yearrate',
    'threeyear_yearrate',
    'benchmark_show',
    'tb_reco_1',
    'tb_reco_2',
    'tb_reco_3',
    'tb_reco_4',
    'tb_reco_5',
    'tb_reco_6',
    'tb_reco_7',
    'tb_reco_8',
    'tb_reco_9',
    'tb_reco_10',
]


def get_prod_output_info(prod_info: dict, scene_id: str):
    # 按业务要求组装指定格式的数据，填充用户信息后，直接传给BI
    data = {}
    prod_info = {k: '' if v is None else v for k, v in prod_info.items()}

    # replace none to empty str
    prod_info['scene_id'] = scene_id.upper()
    prod_info['prd_code'] = prod_info['prod_id']  # 防止部分 prd_code 为空值
    prod_info['msgmodel_id'] = _gen_msgmodel_id(prod_info)

    prod_info = convert_prod_info(prod_info)

    for k in REQUIRED_KEYS:
        new_k = RENAME_MAP.get(k, k)
        data[new_k] = prod_info.get(k, None)
        if isinstance(data[new_k], decimal.Decimal):
            data[new_k] = float(data[new_k])

    return data


def get_prod_filter_info(prod_info: dict, type='chrem'):
    # 仅包含用于产品过滤的数据
    data = {}
    if type == 'chrem':
        data['pfirst_amt'] = _convert(prod_info['pfirst_amt'], float)
        data['prd_time_limit'] = _convert(prod_info['prd_time_limit'], float)
        data['risk_level'] = _convert(prod_info['risk_level'], int)
        data['prd_invest_type'] = _convert(prod_info['prd_invest_type'], str)

    return data


def _convert(value, dtype: type = str):
    if isinstance(value, str):
        value = value.strip()

    try:
        return dtype(value)
    except:
        return None


def _gen_msgmodel_id(data):
    prd_code = data['prd_code'].upper()
    if prd_code in ['J172411SA799', '5811221014', '5811222044', '9B310115']:
        return f'chrem_{prd_code}'
    reserve1 = data['reserve1'].upper()
    if reserve1 in ['1301', 'D309', 'D321', 'D311', '1303']:
        return f'chrem_{reserve1}'

    tr_logger.warning(f'msgmodel_id gen failed, prd_code: {prd_code}, reserve1: {reserve1}')
    return ''


_RISK_MAP = {
    '1': '风险等级极低风险',
    '2': '风险等级低风险',
    '3': '风险等级中等风险',
    '4': '风险等级较高风险 ',
    '5': '风险等级高风险',
}

# TODO: 这个字段怎么来的
_TERM_ENJOY_MAP = {
    '0': '每日',
    '1': '每周',
    '2': '每月',
    '3': '每季',
    '4': '每半年',
    '5': '每年',
    '6': '每二年',
    '7': '每三年',
}

_GUEST_TYPE_MAP = {
    '0': '成立以来年化收益率',
    '1': '上日年化收益率',
    '2': '近七日年化收益率',
    '3': '近14日年化收益率',
    '4': '近一月年化收益率',
    '5': '近两月年化收益率',
    '6': '近三月年化收益率',
    '7': '近半年年化收益率',
    '8': '上一年年化收益率',
    '9': '上期年化收益率',
    'a': '二年化收益率',
    'b': '三年化收益率',
}

_PROD_TYPE_MAP = {
    0: '精选固收',
    1: '精选权益',
    2: '精选商品及金融衍生品',
    3: '精选混合',
    4: '精选货币',
    99: '精选其他',
}


def convert_prod_info(data):
    data['nav'] = _convert_float(data['nav'], prefix='产品净值')
    data['nav_date'] = _convert_date(data['nav_date'], prefix='截至')
    data['ipo_start_date'] = _convert_date(data['ipo_start_date'])
    data['ipo_end_date'] = _convert_date(data['ipo_end_date'])
    data['estab_date'] = _convert_date(data['estab_date'])
    data['income_date'] = _convert_date(data['income_date'])
    data['end_date'] = _convert_date(data['end_date'])
    data['risk_level'] = _RISK_MAP.get(str(data['risk_level']).strip(), '')

    data['benchmark'] = data['benchmark'].strip()
    if data['benchmark']:
        data['benchmark'] = f'业绩比较基准：{data["benchmark"].replace("~","-")}'
    data['nav_ratio'] = _convert_ratio(data['nav_ratio'], prefix='上一投资周期年化收益率', suffix='、')

    data['min_hold_days'] = ''
    data['term_enjoy_type'] = _TERM_ENJOY_MAP.get(str(data['term_enjoy_type']).strip(), '')
    if str(data['guest_type']).strip().lower() == 'c':
        data['guest_type'] = data['benchmark_show']
    else:
        data['guest_type'] = _GUEST_TYPE_MAP.get(str(data['guest_type']).strip().lower(), '')

    for ratio_key in [
        'estab_ratio',
        'day_yearrate',
        'week_yearrate',
        'two_week_yearrate',
        'month_yearrate',
        'two_month_yearrate',
        'three_month_yearrate',
        'half_year_yearrate',
        'year_yearrate',
        'rate1',
        'twoyear_yearrate',
        'threeyear_yearrate',
        'tb_reco_1',
    ]:
        data[ratio_key] = _convert_ratio(str(data[ratio_key]))

    prd_invest_type = str(data['prd_invest_type']).strip()
    try:
        data['tb_reco_2'] = _PROD_TYPE_MAP.get(int(prd_invest_type), '')
    except:
        data['tb_reco_2'] = ''

    data['tb_reco_3'] = data['lock_days']
    if str(data['lock_days']).strip() in ['0', '']:
        data['tb_reco_3'] = ''

    return data


def _convert_float(fstr: str, prefix: str = '') -> str:
    if isinstance(fstr, (float, decimal.Decimal)):
        return str(fstr)
    fstr = fstr.strip()
    if not fstr:
        return ''
    try:
        f = decimal.Decimal(fstr)
        s = f'{f:.4}'.rstrip('0')
        if s.endswith('.'):
            s += '0'
        return f'{prefix}{s}'
    except:
        return ''


def _convert_ratio(ratio: str, prefix: str = '', suffix: str = '') -> str:
    ratio = str(ratio).strip()
    if not ratio:
        return ''
    try:
        ratio_f = decimal.Decimal(ratio) * 100
        # equal to zero
        if abs(ratio_f - decimal.Decimal(0.0)) < 1e-6:
            return ''
        ratio_s = f'{ratio_f:.4}'.rstrip('0')
        if ratio_s.endswith('.'):
            ratio_s += '0'

        return f'{prefix}{ratio_s}%{suffix}'
    except:
        return ''


def _convert_date(date_str, src_fmt='%Y%m%d', tgt_fmt='%Y/%m/%d', prefix: str = '') -> str:

    try:
        date = datetime.strptime(str(date_str).strip(), src_fmt)
        return prefix + date.strftime(tgt_fmt)
    except:
        return ''
