#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'lingyv'

import pyspark.sql.functions as F
from pyspark.sql.window import Window

from configs import Context
from configs.model.base import BaseConfig
from core.job.registry import JobRegistry
from core.spark.table import TableInserter
from core.utils import timeit
from jobs.base import SparkRecoJob


@JobRegistry.register(JobRegistry.T.INFER)
class UserPercentile(SparkRecoJob):
    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self._table_inserter = TableInserter(self.output_table_info_list(self._base)[0])

    @timeit()
    def run(self):
        self.logger.info('开始处理用户评分、排名、分位数')
        user_df = self.run_spark_sql(
            f"""
            SELECT *
            FROM {self._base.output_table.user_pool}
            WHERE dag_id='{self.dag_id}' AND dt='{self.batch_id}'
            """
        )
        num = user_df.count()
        w = Window.orderBy('score')
        user_df = (
            user_df.withColumn('rank', F.row_number().over(w))
            .withColumn('user_num', F.lit(num))
            .withColumn('percentile', F.col('rank') / F.col('user_num'))
        )
        self._table_inserter.insert_df(
            user_df, overwrite=True, dt=self.batch_id, dag_id=self.dag_id
        )

    @staticmethod
    def output_table_info_list(cfg: BaseConfig):
        return [
            {
                'name': cfg.output_table.user_percentile,
                'comment': '用户评分分位数',
                'field_list': [
                    {'name': 'user_id', 'type': 'string', 'comment': '用户名'},
                    {'name': 'user_type', 'type': 'string', 'comment': '用户类型（实验组or对照组）'},
                    {'name': 'score', 'type': 'float', 'comment': '用户分数'},
                    {'name': 'rank', 'type': 'int', 'comment': '用户排名'},
                    {'name': 'user_num', 'type': 'int', 'comment': '用户排名'},
                    {'name': 'percentile', 'type': 'float', 'comment': '用户排名分位数'},
                    {'name': 'batch_id', 'type': 'string', 'comment': '实际跑批时间'},
                ],
                'partition_field_list': [
                    {'name': 'dt', 'type': 'string', 'comment': '日期分区'},
                    {'name': 'dag_id', 'type': 'string', 'comment': '一个dag的唯一标识'},
                ],
            }
        ]
