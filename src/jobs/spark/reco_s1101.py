# -*- coding: utf-8 -*-
# @File : reco_s1101.py
# @Author : r.yang
# @Date : Mon May 29 13:49:00 2023
# @Description :

import decimal
import json

import pyspark.sql.functions as F

from client.redis import RedisClientRegistry
from configs import Context
from configs.model.base import BaseConfig
from core.job.registry import JobRegistry
from core.spark.table import TableInserter
from core.utils import DateHelper, df_view, prt_table_view, rdd_iterate
from jobs.base import SparkRecoJob
from jobs.spark.reco_s1106 import RENAME_MAP, REQUIRED_KEYS, RecoS1106, get_prod_output_info


@JobRegistry.register(JobRegistry.T.INFER)
class RecoS1101(SparkRecoJob):

    _PROD_INFO_KEY_TMP = 'tr|s1101|prod_info|{prod_id}'
    _CUST_PRODS_KEY_TMP = 'tr|s1101|cust_prods|{cust_id}'
    _SCENE_ID = 'S1101'

    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self.spark.sql('set spark.sql.hive.convertMetastoreParquet=false')
        assert self._scene.adhoc, 'config not set, check your dag id or dag config'

        self._date_helper = DateHelper(self.batch_id, self._base.react_delay)
        self._table_inserter = TableInserter(self.output_table_info_list(self._base)[0])
        self._redis = RedisClientRegistry.get(self._base.redis)
        self._redis_ttl = self._scene.adhoc.redis_key_ttl_ex

    def run(self):
        cust_prod = self._get_cust_prod_df()
        prod_df = cust_prod.select('prod_id', 'item_type').dropDuplicates(['prod_id', 'item_type'])
        result = cust_prod.withColumn('prod_list', F.array(F.col('prod_id')))
        result = self._with_cust_info(result)

        self._table_inserter.insert_df(result, dt=self.batch_id, dag_id=self._SCENE_ID)
        self._save_cust_to_redis()
        self._save_prod_to_redis(prod_df)

    def _get_cust_prod_df(self):
        with prt_table_view(
            self, self._base.external_table.zj_reco_result, 'zj_reco_result', self.batch_id,
        ):
            cust_prod = self.run_spark_sql(
                f"""
                SELECT
                  user_id as cust_id,
                  item_id as prod_id,
                  item_type
                FROM zj_reco_result
                WHERE scene_id='{self._SCENE_ID}'
                  AND item_type in ('04', '05')
                """
            )

        cust_prod.cache()
        cust_prod = cust_prod.dropDuplicates(['cust_id'])
        cnt = cust_prod.count()
        self.logger.info(f'custom count: {cnt}')
        return cust_prod

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

    def _save_prod_to_redis(self, prod_df):
        self._save_chrem_prod_to_redis(prod_df.where("item_type = '05'"))
        self._save_fund_prod_to_redis(prod_df.where("item_type = '04'"))

    def _save_chrem_prod_to_redis(self, df):
        df = df.dropDuplicates(['prod_id'])
        df = RecoS1106._with_prod_info(self, df)

        def _save_row(row):
            key = self._PROD_INFO_KEY_TMP.format(prod_id=row['prod_id'])
            output_info = get_prod_output_info(row.asDict(), self._SCENE_ID)
            value = json.dumps(
                {'output_info': output_info, 'filter_info': {},}, ensure_ascii=False,
            )
            print(key, value)
            self._redis.set(key, value, ex=self._redis_ttl)

        for row in rdd_iterate(df.rdd):
            _save_row(row)

    def _save_fund_prod_to_redis(self, df):
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

            value = json.dumps(
                {'output_info': output_info, 'filter_info': {},}, ensure_ascii=False,
            )
            print(key, value)
            self._redis.set(key, value, ex=self._redis_ttl)

        for row in rdd_iterate(df.rdd):
            _save_row(row)

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
