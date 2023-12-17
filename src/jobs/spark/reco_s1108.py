# -*- coding: utf-8 -*-
"""
Created on Mon Mar 27 17:50:00 2023

@author: wy
"""

import pyspark.sql.functions as F

from configs import Context
from configs.model.base import BaseConfig
from core.job.registry import JobRegistry
from core.spark.table import TableInserter
from core.utils import prt_table_view
from jobs.spark.reco_s1106 import RecoS1106


@JobRegistry.register(JobRegistry.T.INFER)
class RecoS1108(RecoS1106):

    _PROD_INFO_KEY_TMP = 'tr|s1108|prod_info|{prod_id}'
    _CUST_PRODS_KEY_TMP = 'tr|s1108|cust_prods|{cust_id}'
    _SCENE_ID = 'S1108'

    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self._table_inserter = TableInserter(self.output_table_info_list(self._base)[0])
        assert self._scene.adhoc, 'config not set, check your dag id or dag config'
        self._default_prod = self._scene.adhoc.s1108_base_product
        self._max_prod_num = self._scene.adhoc.s1108_max_prod_num

    def run(self):
        cust = self._get_cust_df()
        prod = self._get_prod_df()

        # 客户产品交叉
        cust_prod = cust.crossJoin(prod)
        # 风险等级过滤
        cust_prod = self._filter_risk(cust_prod).cache()
        # 投资期限 过滤
        cust_prod = self._filter_prd_time_limit(cust_prod, prod)
        # 获取产品信息
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
            self._base.external_table.cstpro_ind_chrem,
            'cstpro_ind_chrem',
            self.batch_id,
        ):
            cust = self.run_spark_sql(
                f"""
                SELECT a.cust_id
                FROM
                (
                  SELECT DISTINCT cust_id
                  FROM cstpro_ind_chrem
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
            self, self._base.external_table.fin_tbproduct, 'fin_tbproduct', self.batch_id
        ):
            start_date = self._date_helper.add_day(1).str
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
        return prod

    @staticmethod
    def output_table_info_list(cfg: BaseConfig):
        return RecoS1106.output_table_info_list(cfg)
