# -*- coding: utf-8 -*-

import pyspark.sql.functions as F

from algorithm.classifer import ClassifierModelFactory
from configs import Context
from configs.model.base import BaseConfig
from configs.model.config import UserPoolConfig
from core.job.registry import JobRegistry
from core.spark.table import TableInserter
from core.utils import DateHelper, prt_table_view, timeit
from jobs.base import SparkRecoJob
from jobs.spark.feature import UserFeatures


@JobRegistry.register(JobRegistry.T.INFER)
class CancelUser(SparkRecoJob):

    _cancel_user: UserPoolConfig

    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self._date_helper = DateHelper(self.batch_id, self._base.react_delay)
        self._table_inserter = TableInserter(self.output_table_info_list(self._base)[0])

    @timeit()
    def run(self):
        # 待预测样本
        self.logger.info('开始预测销户用户')
        last_half_year = self._date_helper.to_str(self._date_helper.last_n_year(0.5))
        last_1_year = self._date_helper.to_str(self._date_helper.last_n_year(1))
        last_2_year = self._date_helper.to_str(self._date_helper.last_n_year(2))
        self.logger.info(
            f'当前时间 {self.batch_id}, 过去半年 {last_half_year}, 过去一年 {last_1_year}, 过去两年 {last_2_year}'
        )
        with prt_table_view(
            self, self._base.external_table.hx_cancel_user, 'hx_cancel_user', self.batch_id
        ), prt_table_view(
            self, self._base.external_table.mt_cancel_user, 'mt_cancel_user', self.batch_id
        ), prt_table_view(
            self, self._base.external_table.can_acct_type, 'can_acct_type', self.batch_id
        ), prt_table_view(
            self, self._base.external_table.dim_jydm, 'dim_jydm', self.batch_id
        ), prt_table_view(
            self, self._base.external_table.cstbsc_info_mt, 'cstbsc_info_mt', self.batch_id
        ), prt_table_view(
            self, self._base.external_table.ath_txn_info_mt, 'ath_txn_info_mt', self.batch_id
        ):
            user_df = self.run_spark_sql(
                f"""
                SELECT m.*, if(m.last_acct_id_dt is null or m.last_acct_id_dt<'{last_half_year}', 0, 1
                                ) as half_year_active_ind,
                     if(m.last_acct_id_dt is null, 0, 1) as year_active_ind,
                     n.if_logout_online_value_cust, n.if_logout_online_nvalue_cust
                FROM ((
                        select aa.cust_id as user_id, aa.user_type, aa.cust_mobile_no,
                            aa.cust_anniv_dt, nvl(bb.year_trx_amt, 0) as year_trx_amt, bb.last_acct_id_dt
                        from (
                            select cust_id, 'hx' as user_type, max(cust_mobile_no) as cust_mobile_no,
                                min(cust_anniv_dt) as cust_anniv_dt
                            from hx_cancel_user
                            where cust_cancel_ind='0'
                            group by cust_id
                        ) as aa
                        left join (
                            select a.cust_id, max(a.acct_id_dt) as last_acct_id_dt,
                                sum(case when a.in_acct_amt<>0
                                    then (case when a.in_acct_ccy_cd='CNY' then a.in_acct_amt
                                            else a.in_acct_amt*6.7 end)
                                    else 0 end) as year_trx_amt
                            from (select * from {self._base.external_table.ath_txn_info}
                                 where dt between '{last_1_year}' and '{self.batch_id}'
                            ) as a
                            join (select * from dim_jydm
                                 where if_use='1' and in_acct_type='01主账户' and trx_type1 in ('消费','取现')
                            ) c on a.crd_card_trans_cd=c.id_jydm
                            group by a.cust_id
                        ) as bb ON aa.cust_id=bb.cust_id
                    )
                    UNION ALL
                    (
                        select aa.cust_id as user_id, aa.user_type, bb.cust_mobile_no, bb.cust_anniv_dt,
                             nvl(cc.year_trx_amt, 0) as year_trx_amt, cc.last_acct_id_dt
                        from (
                            select cust_id, 'mt' as user_type
                            from mt_cancel_user
                            where card_ccl_ind='0'
                            group by cust_id
                        ) as aa
                        left join (
                            select cust_id, max(cust_mobile_no) as cust_mobile_no,
                                 min(cust_anniv_dt) as cust_anniv_dt
                            from cstbsc_info_mt
                            group by cust_id
                        ) as bb ON aa.cust_id=bb.cust_id
                        left join (
                            select a.cust_id, max(a.acct_id_dt) as last_acct_id_dt,
                                sum(case when a.in_acct_amt<>0
                                    then (case when a.in_acct_ccy_cd='CNY' then a.in_acct_amt
                                            else a.in_acct_amt*6.7 end)
                                    else 0 end) as year_trx_amt
                            from (select * from ath_txn_info_mt
                                 where acct_id_dt between '{last_1_year}' and '{self.batch_id}'
                            ) as a
                            join (select * from dim_jydm
                                 where if_use='1' and in_acct_type='01主账户' and trx_type1 in ('消费','取现')
                            ) c on a.crd_card_trans_cd=c.id_jydm
                            group by a.cust_id
                        ) as cc ON aa.cust_id=cc.cust_id
                    )
                ) AS m
                JOIN (
                    SELECT cust_id as user_id,
                        max(if_logout_online_value_cust) AS if_logout_online_value_cust,
                        max(if_logout_online_nvalue_cust) AS if_logout_online_nvalue_cust
                    FROM can_acct_type
                    WHERE if_logout_online_value_cust=1 OR if_logout_online_nvalue_cust=1
                    GROUP BY cust_id
                ) AS n ON m.user_id=n.user_id
                    """
            )

        model = ClassifierModelFactory.create(self._cancel_user.model)
        user_df = UserFeatures.with_user_feat(self, user_df, model.cfg.feat_version)
        user_df = model.transform(user_df, feature_col=UserFeatures.FEAT_COL)
        user_df = user_df.withColumn('score', F.element_at(user_df.probability, 2)).withColumn(
            'batch_id', F.lit(self.batch_id)
        )

        with prt_table_view(
            self, self._base.external_table.flag_month, 'flag_month', self.batch_id
        ), prt_table_view(
            self, self._base.external_table.user_feat_ls_crd, 'user_feat_ls_crd', self.batch_id
        ):
            user_df.createOrReplaceTempView('cancel_user_tmp')
            self.run_spark_sql(
                f"""INSERT OVERWRITE TABLE {self._table_inserter.table_name}
                    PARTITION(dt='{self.batch_id}', dag_id='{self.dag_id}')
                SELECT  a.user_id,
                        a.score,
                        a.if_logout_online_value_cust,
                        a.if_logout_online_nvalue_cust,
                        (a.half_year_active_ind +
                            nvl(d.dsc_ind, 0) +
                            if(substring(cust_anniv_dt, 1, 4)='{self.batch_id[:4]}', 1, 0) +
                            if(a.year_trx_amt>10000, 1, 0)
                        ) as high_value_ind,
                        a.cust_mobile_no,
                        a.cust_anniv_dt,
                        b.age,
                        b.activate_card_flag,
                        c.auto_repay_cust_ind,
                        b.first_bin_wx,
                        b.first_bin_zfb,
                        b.first_bin_unionpay,
                        b.first_bin_mt,
                        b.first_bin_jd,
                        b.first_bin_sn,
                        b.first_bin_wp,
                        a.half_year_active_ind,
                        a.year_active_ind,
                        a.user_type,
                        a.year_trx_amt,
                        a.last_acct_id_dt,
                        nvl(d.dsc_ind, 0) as dsc_ind,
                        a.batch_id
                FROM cancel_user_tmp AS a
                left join (
                    select cust_id, max(age) as age, max(activate_card_flag) as activate_card_flag,
                        max(first_bin_wx) as first_bin_wx, max(first_bin_zfb) as first_bin_zfb,
                        max(first_bin_unionpay) as first_bin_unionpay, max(first_bin_mt) as first_bin_mt,
                        max(first_bin_jd) as first_bin_jd, max(first_bin_sn) as first_bin_sn,
                        max(first_bin_wp) as first_bin_wp
                    from flag_month
                    group by cust_id
                ) as b on a.user_id=b.cust_id
                left join (
                    select cust_id, max(auto_repay_cust_ind) as auto_repay_cust_ind
                    from user_feat_ls_crd
                    group by cust_id
                ) as c ON a.user_id=c.cust_id
                left join (
                    select distinct cust_id, 1 as dsc_ind
                    from {self._base.external_table.card_pim_info}
                    where dt between '{last_2_year}' and '{self.batch_id}'
                        and (card_pim_dsc like '%交易分期%'
                            or card_pim_dsc like '%账单分期%'
                            or card_pim_dsc like '%转账分期%')
                ) as d ON a.user_id=d.cust_id
            """
            )

    def show(self):
        self.run_spark_sql(
            f"""
        SELECT count(*)
        FROM {self._table_inserter.table_name}
        WHERE dag_id='{self.dag_id}'
          AND dt='{self.batch_id}'
        """
        ).show()

    @staticmethod
    def output_table_info_list(cfg: BaseConfig):
        return [
            {
                'name': cfg.output_table.cancel_user,
                'comment': '销户用户',
                'field_list': [
                    {'name': 'user_id', 'type': 'string', 'comment': '用户'},
                    {'name': 'score', 'type': 'float', 'comment': '用户分数'},
                    {'name': 'if_logout_online_value_cust', 'type': 'string', 'comment': '是否价值客户'},
                    {'name': 'if_logout_online_nvalue_cust', 'type': 'string', 'comment': '是否风险客户'},
                    {'name': 'high_value_ind', 'type': 'int', 'comment': '高价值标志'},
                    {'name': 'cust_mobile_no', 'type': 'string', 'comment': '手机号'},
                    {'name': 'cust_anniv_dt', 'type': 'string', 'comment': '创户时间'},
                    {'name': 'age', 'type': 'int', 'comment': '年龄'},
                    {'name': 'activate_card_flag', 'type': 'int', 'comment': '是否有激活卡'},
                    {'name': 'auto_repay_cust_ind', 'type': 'string', 'comment': '是否绑定借记卡主动还款'},
                    {'name': 'first_bin_wx', 'type': 'string', 'comment': '微信首次绑定日期'},
                    {'name': 'first_bin_zfb', 'type': 'string', 'comment': '支付宝首次绑定日期'},
                    {'name': 'first_bin_unionpay', 'type': 'string', 'comment': '云闪付首次绑定日期'},
                    {'name': 'first_bin_mt', 'type': 'string', 'comment': '美团首次绑定日期'},
                    {'name': 'first_bin_jd', 'type': 'string', 'comment': '京东首次绑定日期'},
                    {'name': 'first_bin_sn', 'type': 'string', 'comment': '苏宁首次绑定日期'},
                    {'name': 'first_bin_wp', 'type': 'string', 'comment': '唯品首次绑定日期'},
                    {'name': 'half_year_active_ind', 'type': 'int', 'comment': '是否近半年活跃户'},
                    {'name': 'year_active_ind', 'type': 'int', 'comment': '是否近一年活跃户'},
                    {'name': 'user_type', 'type': 'string', 'comment': '用户类型(卡核心/美团)'},
                    {'name': 'year_trx_amt', 'type': 'float', 'comment': '近一年累计交易额'},
                    {'name': 'last_acct_id_dt', 'type': 'string', 'comment': '最近一次交易时间'},
                    {'name': 'dsc_ind', 'type': 'int', 'comment': '近两年分期标识'},
                    {'name': 'batch_id', 'type': 'string', 'comment': '实际跑批时间'},
                ],
                'partition_field_list': [
                    {'name': 'dt', 'type': 'string', 'comment': '日期分区'},
                    {'name': 'dag_id', 'type': 'string', 'comment': '一个dag的唯一标识'},
                ],
            }
        ]
