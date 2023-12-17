#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'lingyv'

import os
import shutil

import pyspark.sql.functions as F

from configs import Context
from configs.model.base import BaseConfig
from configs.utils import Env
from core.job.registry import JobRegistry
from core.spark.table import TableInserter, dump_df_with_header
from core.utils import run_cmd, run_cmd_rtn, timeit
from jobs.base import PostProcJob, SparkPostProcJob


@JobRegistry.register(JobRegistry.T.INFER)
class UserPredResultsToDW(SparkPostProcJob):
    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self._table_inserter = TableInserter(self.output_table_info_list(self._base)[0])

    @timeit()
    def run(self):
        user_df = self.run_spark_sql(
            f"""
            select nvl(a.cust_id, b.cust_id) as cust_id,
                nvl(a.user_type, b.user_type) as user_type,
                b.debit_score, b.debit_rank, b.debit_percentile, b.debit_date,
                a.credit_score, a.credit_rank, a.credit_percentile, a.credit_date,
                '{self.batch_id}' as model_dt, '{self.batch_id}' as data_dt
            from (
                select user_id as cust_id, 'credit_card' as user_type,
                    score as credit_score, rank as credit_rank,
                    percentile as credit_percentile,
                    batch_id as credit_date
                from {self._base.output_table.user_percentile}
                where dt='{self.batch_id}' and dag_id like '%credit%'
            ) as a
            full join (
                select user_id as cust_id, 'debit_card' as user_type,
                    score as debit_score, rank as debit_rank,
                    percentile as debit_percentile,
                    batch_id as debit_date
                from {self._base.output_table.user_percentile}
                where dt='{self.batch_id}' and dag_id like '%debit%'
            ) as b ON a.cust_id=b.cust_id
            """
        )
        for rev in ['rev' + str(x) for x in list(range(1, 11))]:
            user_df = user_df.withColumn(rev, F.lit('NULL_VALUE'))
        user_df.cache()
        self._table_inserter.insert_df(user_df, overwrite=True, dt=self.batch_id)

        user_df_fill = user_df.fillna(value="NULL_VALUE", subset=['debit_date', 'credit_date'])
        filename = f'BDAS_IRE_USER_PRED_RESULT_{self.batch_id}'
        hdfs_csv_path = os.path.join(self._base.hdfs_output_dir, self.batch_id, filename)
        dump_df_with_header(self.spark, user_df_fill, hdfs_csv_path)
        self.logger.info(f'write to hdfs csv path success: {hdfs_csv_path}')
        user_df.unpersist()

    @staticmethod
    def output_table_info_list(cfg: BaseConfig):
        return [
            {
                'name': cfg.output_table.ire_user_pred_result,
                'comment': '用户分位数结果入仓表',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': '用户编码'},
                    {'name': 'user_type', 'type': 'string', 'comment': '用户分组类型'},
                    {'name': 'debit_score', 'type': 'float', 'comment': '借记卡意愿模型预测分数'},
                    {'name': 'debit_rank', 'type': 'int', 'comment': '借记卡意愿模型预测分数排名'},
                    {'name': 'debit_percentile', 'type': 'float', 'comment': '借记卡意愿模型预测分数排名分位数'},
                    {'name': 'debit_date', 'type': 'string', 'comment': '借记卡意愿模型预测时间'},
                    {'name': 'credit_score', 'type': 'float', 'comment': '信用卡意愿模型预测分数'},
                    {'name': 'credit_rank', 'type': 'int', 'comment': '信用卡意愿模型预测分数排名'},
                    {'name': 'credit_percentile', 'type': 'float', 'comment': '信用卡意愿模型预测分数排名分位数'},
                    {'name': 'credit_date', 'type': 'string', 'comment': '信用卡意愿模型预测时间'},
                    {'name': 'model_dt', 'type': 'string', 'comment': '原表数据日期'},
                    {'name': 'data_dt', 'type': 'string', 'comment': '跑批日期'},
                    {'name': 'rev1', 'type': 'string', 'comment': '保留字段1'},
                    {'name': 'rev2', 'type': 'string', 'comment': '保留字段2'},
                    {'name': 'rev3', 'type': 'string', 'comment': '保留字段3'},
                    {'name': 'rev4', 'type': 'string', 'comment': '保留字段4'},
                    {'name': 'rev5', 'type': 'string', 'comment': '保留字段5'},
                    {'name': 'rev6', 'type': 'string', 'comment': '保留字段6'},
                    {'name': 'rev7', 'type': 'string', 'comment': '保留字段7'},
                    {'name': 'rev8', 'type': 'string', 'comment': '保留字段8'},
                    {'name': 'rev9', 'type': 'string', 'comment': '保留字段9'},
                    {'name': 'rev10', 'type': 'string', 'comment': '保留字段10'},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            }
        ]


@JobRegistry.register(JobRegistry.T.INFER)
class CollectIREUserPredResults(PostProcJob):
    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self.filename = f'BDAS_IRE_USER_PRED_RESULT_{self.batch_id}'
        self.hdfs_csv_path = os.path.join(self._base.hdfs_output_dir, self.batch_id, self.filename)

    def run(self):
        # 获取入仓的基础目录 /share_${Namespace}/${Environment}/result_rucang
        nas_base_dir = os.path.abspath(os.path.join(self._base.nas_output_dir, os.pardir))
        result_rucang_dir = os.path.join(nas_base_dir, 'result_rucang')

        # 实际入仓还有加output和日期目录
        dt_path_dir = os.path.join(result_rucang_dir, 'card_model_output', self.batch_id)
        os.makedirs(dt_path_dir, exist_ok=True)

        self._transfer_hdfs_file_to_nas(dt_path_dir)

    def _write_flg_file(self, flg_file_path, filename, filesize, rowcount):
        # 写 flg 文件标记已经传输完成
        with open(flg_file_path, 'w') as f:
            f.write(f'{filename} {filesize} {rowcount}')

    def _write_empty_dat_file(self, dat_file_path):
        # 写 空 dat文件
        with open(dat_file_path, mode='w', encoding='utf8') as f:
            ...

    def _transfer_hdfs_file_to_nas(self, dt_path_dir):
        dat_file_path = os.path.join(dt_path_dir, self.filename + '.dat')
        # 直接写入nas文件太大会失败，先写到/tmp目录下，再移动到nas目录下
        bak_dat_file_path = os.path.join('/tmp', self.filename + '.dat')
        flg_file_path = os.path.join(dt_path_dir, self.filename + '.flg')
        cat = 'cat' if Env.is_local else 'hadoop fs -cat'
        cat_cmd = f"{cat} {self.hdfs_csv_path}/*.csv | sed s@$'\001'@'\@\!\@'@g | sed 's/NULL_VALUE//g' > {bak_dat_file_path}"

        filesize = 0
        rowcount = 0
        try:
            run_cmd(cat_cmd, self.logger)
            shutil.move(bak_dat_file_path, dat_file_path)
            filesize = os.path.getsize(dat_file_path)
            rowcount = int(run_cmd_rtn(f'wc -l {dat_file_path}', self.logger).split()[0])
            self.logger.info(f'file size: {filesize}, row count: {rowcount}')
        except Exception as e:
            self.logger.error(f'run_cmd error: {e}')
            # 避免卡批，插入空dat文件
            filesize = 0
            rowcount = 0
            self._write_empty_dat_file(dat_file_path)
        finally:
            # 避免卡批，插入flg文件
            self._write_flg_file(flg_file_path, self.filename + '.dat', filesize, rowcount)
