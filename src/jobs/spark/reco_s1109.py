# -*- coding: utf-8 -*-
"""
Created on Mon Mar 27 17:50:00 2023

@author: wy
"""
import decimal
import json

import pyspark.sql.functions as F
import pyspark.sql.types as T

from client.redis import RedisClientRegistry
from configs import Context
from configs.model.base import BaseConfig
from core.job.registry import JobRegistry
from core.spark.table import TableInserter
from core.utils import DateHelper, df_view, prt_table_view, rdd_iterate
from jobs.base import SparkRecoJob
from jobs.spark.reco_s1106 import RENAME_MAP, REQUIRED_KEYS, RecoS1106


@JobRegistry.register(JobRegistry.T.INFER)
class RecoS1109(SparkRecoJob):

    _PROD_INFO_KEY_TMP = 'tr|s1109|prod_info|{prod_id}'
    _CUST_PRODS_KEY_TMP = 'tr|s1109|cust_prods|{cust_id}'
    _SCENE_ID = 'S1109'

    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self.spark.sql('set spark.sql.hive.convertMetastoreParquet=false')
        assert self._scene.adhoc, 'config not set, check your dag id or dag config'

        self._date_helper = DateHelper(self.batch_id, self._base.react_delay)
        self._table_inserter = TableInserter(self.output_table_info_list(self._base)[0])
        self._redis = RedisClientRegistry.get(self._base.redis)
        self._redis_ttl = self._scene.adhoc.redis_key_ttl_ex
        self._max_prod_num = self._scene.adhoc.s1109_max_prod_num

    def run(self):
        cust = self._get_cust_df()
        prod = self._get_prod_df()

        prod_infos = {i.prod_id: i.asDict() for i in prod.collect()}
        # 用户产品交叉
        cust_prod = cust.crossJoin(prod.select('prod_id'))
        cust_prod.cache()
        # 风险等级过滤
        cust_prod = self._filter_risk(cust_prod)
        # group agg
        result = cust_prod.groupby('cust_id').agg(F.collect_set('prod_id').alias('prod_list'))
        result.cache()
        self.logger.info(f'cust count: result.count()')
        # padding cust
        result = cust.join(result, ['cust_id'], how='left')
        # truncate
        result = self._truncate_prod_list(result, self._max_prod_num, prod_infos)
        # fill cust name and phone number
        result = self._with_cust_info(result)

        self._table_inserter.insert_df(result, dt=self.batch_id, dag_id=self._SCENE_ID)
        self._save_cust_to_redis()
        self._save_prod_to_redis(prod)

    def _get_cust_df(self):
        with prt_table_view(
            # 短信营销授权
            self,
            self._base.external_table.user_stat_mt,
            'user_stat_mt',
            self.batch_id,
        ), prt_table_view(
            # 近7日有赎回的
            self,
            self._base.external_table.cstpro_ind_fund,
            'cstpro_ind_fund',
            self.batch_id,
        ):
            cust = self.run_spark_sql(
                f"""
                SELECT a.cust_id
                FROM
                (
                  SELECT DISTINCT cust_id
                  FROM cstpro_ind_fund
                  WHERE rct7d_rdmpt_amt >0
                ) AS a
                LEFT JOIN
                  (SELECT cust_id,
                          1 AS tmp
                   FROM user_stat_mt
                   WHERE camp_stat='1' ) AS b ON a.cust_id = b.cust_id
                WHERE b.tmp=1
                """
            )

        # force with test custs
        test_cust = self.spark.createDataFrame(
            [
                {'cust_id': '3095681436'},
                {'cust_id': '3127199832'},
                {'cust_id': '3032284975'},
                {'cust_id': '3126953062'},
            ]
        )
        cust = cust.union(test_cust)
        cust_cnt = cust.count()
        self.logger.info(f'custom count: {cust_cnt}')
        return cust

    def _get_prod_df(self):

        with prt_table_view(
            self, self._base.external_table.s14_tbproduct_h, 'fin_tbproduct', self.batch_id
        ), prt_table_view(
            # 近一年收益率为正
            self,
            self._base.external_table.pro_ind_fund,
            'pro_ind_fund',
            self.batch_id,
        ), prt_table_view(
            # 过滤热销主题基金
            self,
            self._base.external_table.s14_tbthemeprd_h,
            's14_tbthemeprd_h',
            self.batch_id,
        ):
            prod = self.run_spark_sql(
                f"""
                SELECT a.*,
                       b.rct1y_profit_rate
                FROM
                  (SELECT prd_code AS prod_id,
                          pfirst_amt, -- 起购金额
                          risk_level -- 投资类型
                   FROM fin_tbproduct
                   WHERE channels like '%7%'
                ) AS a
                LEFT JOIN
                  (SELECT prod_id,
                          rct1y_profit_rate
                   FROM pro_ind_fund
                   WHERE rct1y_profit_rate IS NOT NULL
                ) AS b ON a.prod_id = b.prod_id
                LEFT JOIN
                  (SELECT prd_code, 1 as tmp
                  FROM s14_tbthemeprd_h
                  WHERE theme_id='id2021052800000006'
                ) AS c ON a.prod_id = c.prd_code
                WHERE c.tmp=1
                AND b.rct1y_profit_rate > 0.0
                """
            ).dropDuplicates(['prod_id'])

        return prod

    def _filter_risk(self, cust_prod_df):
        with prt_table_view(
            # 产品风险等级
            self,
            self._base.external_table.s14_tbproduct_h,
            's14_tbproduct_h',
            self.batch_id,
        ), prt_table_view(
            # 客户风险等级
            self,
            self._base.external_table.rbw_08,
            'rbw_08',
            self.batch_id,
            'partitionbid',
        ), prt_table_view(
            # 客户年龄
            self,
            self._base.external_table.user_feat_ls_crd,
            'user_feat_ls_crd',
            self.batch_id,
        ), df_view(
            self, cust_prod_df, 'cust_prod',
        ):
            return self.run_spark_sql(
                f"""
            SELECT a.cust_id,
                   a.prod_id
            FROM
              (SELECT cust_id,
                      prod_id
               FROM cust_prod) AS a
            LEFT JOIN
              (SELECT prd_code,
                      risk_level
               FROM s14_tbproduct_h) AS b ON a.prod_id = b.prd_code
            LEFT JOIN
              (SELECT cust_id,
                      lbl_08_00_038
               FROM rbw_08
               WHERE lbl_08_00_038 <> '' ) AS c ON a.cust_id = c.cust_id
            LEFT JOIN
              (SELECT cust_id,
                      age
               FROM user_feat_ls_crd) AS d ON a.cust_id=d.cust_id
            WHERE c.lbl_08_00_038 IS NOT NULL
              AND b.risk_level IS NOT NULL
              AND b.risk_level <= cast(c.lbl_08_00_038 AS int)
              AND (d.age < 75
                   OR (d.age >= 75
                       AND b.risk_level<4))
            """
            )

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
            data = {'prod_list': prod_list}
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
            prod_id = row['prod_id']
            key = self._PROD_INFO_KEY_TMP.format(prod_id=prod_id)

            prod_info = row.asDict()
            prod_info['scene_id'] = self._SCENE_ID
            prod_info['prd_code'] = prod_id
            prod_info['msgmodel_id'] = f'fund_{prod_id}'

            output_info = {}
            for k in REQUIRED_KEYS:
                new_k = RENAME_MAP.get(k, k)
                output_info[new_k] = prod_info.get(k, None)
                if isinstance(output_info[new_k], decimal.Decimal):
                    output_info[new_k] = float(output_info[new_k])

            filter_info = {'pfirst_amt': prod_info['pfirst_amt']}
            if isinstance(filter_info['pfirst_amt'], decimal.Decimal):
                filter_info['pfirst_amt'] = float(filter_info['pfirst_amt'])

            value = json.dumps(
                {'output_info': output_info, 'filter_info': filter_info,}, ensure_ascii=False,
            )
            print(key, value)
            self._redis.set(key, value, ex=self._redis_ttl)

        for row in rdd_iterate(df.rdd):
            _save_row(row)

    @staticmethod
    def _truncate_prod_list(df, max_len: int, prod_info: dict):
        # truncate prod list
        @F.udf(returnType=T.ArrayType(T.StringType()))
        def _truncate(all_prod_list):
            if not all_prod_list:
                return None

            # make prod & profit_rate pair
            prod_rate_list = []
            for prod_id in all_prod_list:
                if prod_id not in prod_info or not prod_info[prod_id]['rct1y_profit_rate']:
                    continue
                rate = prod_info[prod_id]['rct1y_profit_rate']
                prod_rate_list.append((prod_id, rate))

            if not prod_rate_list:
                return None

            sorted_prods = sorted(prod_rate_list, key=lambda x: x[1], reverse=True)
            return [i[0] for i in sorted_prods][:max_len]

        result = df.withColumn('prod_list', _truncate('prod_list'))
        return result

    def _with_cust_info(self, df):
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

    @staticmethod
    def output_table_info_list(cfg: BaseConfig):
        return RecoS1106.output_table_info_list(cfg)
