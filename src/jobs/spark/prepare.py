# -*- coding: utf-8 -*-


import pyspark.sql.functions as F

from configs import Context
from configs.init.common import get_reco_db
from configs.utils import Env
from core.job.base import IOutputTable
from core.job.registry import JobRegistry
from core.spark import WithSpark
from core.spark.table import SchemaCreator
from core.tr_logging.hive import HiveLoggerHandler
from core.utils import DateHelper
from jobs.base import BaseJob, HiveDagRuntimeConfigDAO, IDestructHook
from jobs.spark.feature import (
    PrepareCancelModelSample,
    PrepareCreditModelSample,
    PrepareDebitModelSample,
)


@JobRegistry.register(JobRegistry.T.BASE)
class PrepareTable(BaseJob, IDestructHook):
    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self._schema_creator = SchemaCreator()
        self.dbs = []
        self._base = self._ctx.base

    def run(self):
        self.dbs = set(
            i.split('.', 1)[0]
            for i in list(self._base.output_table.dict().values())
            + list(self._base.external_table.dict().values())
        )
        self.logger.info(f'dbs to create: {self.dbs}')
        for db in self.dbs:
            # 外部库不创建
            if (Env.is_stage or Env.is_prod) and db != get_reco_db():
                continue
            self._schema_creator.create_database(db)
            self.logger.info(f'db {db} created!')

        jobs = [i[1].clz for i in JobRegistry.iter_all_jobs()]
        for outputTableJob in [HiveDagRuntimeConfigDAO, HiveLoggerHandler,] + jobs:
            if not issubclass(outputTableJob, IOutputTable):
                continue
            for table_info in outputTableJob.output_table_info_list(self._ctx.base):
                # 外部表不创建
                if (Env.is_stage or Env.is_prod) and outputTableJob.__name__ == 'LoadData':
                    continue
                self._schema_creator.create_table(table_info=table_info)
                self.logger.info(f"table {table_info['name']} created!")

    def _drop_all_dbs(self):
        for db in self.dbs:
            self._schema_creator.drop_database(db)

    @property
    def fixture_destruct_hook(self):
        return self._drop_all_dbs


@JobRegistry.register(JobRegistry.T.BASE)
class CleanTable(BaseJob, WithSpark):
    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self._date_helper = DateHelper(self.batch_id, self._base.react_delay)
        self.tables_cleaned = []

    _jobs_except = [
        PrepareDebitModelSample,
        PrepareCreditModelSample,
        PrepareCancelModelSample,
    ]

    def run(self):
        jobs = [i[1].clz for i in JobRegistry.iter_all_jobs()]
        self.tables_cleaned.clear()
        for job in [HiveDagRuntimeConfigDAO, HiveLoggerHandler,] + jobs:
            if (
                not issubclass(job, IOutputTable)
                or not job.output_table_info_list(self._ctx.base)
                or job in self._jobs_except
            ):
                continue

            job_name = job.__name__
            output_table = job.output_table_info_list(self._ctx.base)[0]['name']
            if not output_table.startswith(get_reco_db()) or output_table in self.tables_cleaned:
                continue
            self.logger.info(f'正在清理任务 {job_name} 的输出表格：{output_table}')

            desc_df = self.run_spark_sql('desc {}'.format(output_table)).filter(
                "col_name like '%Partition%'"
            )
            if desc_df.count():
                expires_date = self._date_helper.add_day(-self._base.partition_keep_day).str
                self.logger.info(
                    '删除任务 {} 输出表 {} 过期分区，仅保存 {} 以后的数据。'.format(job_name, output_table, expires_date)
                )
                partition_df = self.run_spark_sql('show partitions {}'.format(output_table))
                regexp = '(\w+_?\w+)=(\d{4}\d{2}\\d{2})'
                date_df = (
                    partition_df.withColumn('name', F.regexp_extract('partition', regexp, 1))
                    .withColumn('date', F.regexp_extract('partition', regexp, 2))
                    .groupBy('name', 'date')
                    .agg(F.count('*').alias('num'))
                    .filter("date!='' and date<'{}'".format(expires_date))
                    .orderBy('date')
                )
                date_rows = date_df.collect()
                self.logger.info('删除任务 {} 输出表 {} 分区 {}'.format(job_name, output_table, date_rows))
                for row in date_rows:
                    self.run_spark_sql(
                        "ALTER TABLE {} DROP PARTITION({}='{}')".format(
                            output_table, row.name, row.date
                        )
                    )

                self.tables_cleaned.append(output_table)
            else:
                self.logger.warning('表 {} 不是分区表'.format(output_table))
