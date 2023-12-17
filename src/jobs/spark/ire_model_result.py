# -*- coding: utf-8 -*-
# @File : ire_model_result.py
# @Author : sb.huang
# @Date : Tue July  17 09:38:31 2023
# @Description : model results to data warehouse
import os
import shutil
from datetime import timedelta

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from configs import Context
from configs.init.prod import _prod_app_post_proc, _prod_sms_post_proc
from configs.model.base import BaseConfig
from configs.utils import Env
from core.job.registry import JobRegistry
from core.spark.table import TableInserter, dump_df_with_header
from core.utils import Datetime, run_cmd, run_cmd_rtn, timeit
from jobs.base import PostProcJob, SparkPostProcJob


def collect_zj_reco_result(spark_func, dt, tbl_name, logger) -> DataFrame:
    try:
        return spark_func(
            f"""
            WITH tbl AS (
                SELECT
                    user_id AS cust_id
                    , 'exp' AS user_type
                    , null AS cust_name
                    , null AS phone_number
                    , (
                        CASE
                            WHEN item_type = '04' THEN 'fund'
                            WHEN item_type = '05' THEN 'finc'
                            ELSE null
                        END
                    ) AS prod_type
                    , item_id AS prod_id
                    , channel_id
                    , banner_id
                    , CAST(prediction AS STRING) AS score
                    , CAST(score AS STRING) AS rank
                    , scene_id
                    , pipeline_id
                    , version
                    , dt AS model_dt
                    , '{tbl_name}' AS src_tbl
                FROM {tbl_name}
                WHERE dt = '{dt}' AND scene_id = 'S1101'
            )

            SELECT *
            FROM (
                SELECT
                    * , ROW_NUMBER() OVER (PARTITION BY cust_id, prod_id, scene_id ORDER BY rank DESC) rn
                FROM tbl
            )
            WHERE rn = 1
        """
        ).drop('rn')
    except Exception as e:
        logger.error(f'collect_zj_reco_result error: {e}')
        return None


def collect_zj_rc_res_dlvy(spark_func, dt, tbl_name, logger) -> DataFrame:
    try:
        return spark_func(
            f"""
            WITH tbl AS (
                SELECT
                    user_id AS cust_id
                    , (
                        CASE
                            WHEN delivery_status = 0 THEN 'ctl'
                            WHEN delivery_status = 1 THEN 'exp'
                            ELSE null
                        END
                    ) AS user_type
                    , null AS cust_name
                    , null AS phone_number
                    , (
                        CASE
                            WHEN item_type = '04' THEN 'fund'
                            WHEN item_type = '05' THEN 'finc'
                            ELSE null
                        END
                    ) AS prod_type
                    , item_id AS prod_id
                    , channel_id
                    , banner_id
                    , CAST(prediction AS STRING) AS score
                    , CAST(score AS STRING) AS rank
                    , scene_id
                    , pipeline_id
                    , version
                    , dt AS model_dt
                    , '{tbl_name}' AS src_tbl
                FROM {tbl_name}
                WHERE dt = '{dt}' AND scene_id != 'S1101'
            )

            SELECT *
            FROM (
                SELECT
                    * , ROW_NUMBER() OVER (PARTITION BY cust_id, prod_id, scene_id ORDER BY rank DESC) rn
                FROM tbl
            )
            WHERE rn = 1
        """
        ).drop('rn')
    except Exception as e:
        logger.error(f'collect_zj_reco_result_delivery error: {e}')
        return None


def collect_crowd_package(spark_func, dt, tbl_name, logger) -> DataFrame:
    # 映射下游以S开头的场景ID
    actvs = _prod_app_post_proc().actvs + _prod_sms_post_proc().actvs
    actvs = [actv for actv in actvs if actv.test_user_item is None]
    case_when = '\n'.join(
        [
            f"WHEN NVL(banner_id, '') = '{actv.banner_id}' AND scene_id = '{actv.scene_id}' THEN '{actv.actv_id}'"
            for actv in actvs
        ]
    )
    scene_id = f"""(
        CASE
            {case_when}
            ELSE NULL
        END
    )"""
    try:
        return spark_func(
            f"""
            WITH tbl AS (
                SELECT
                    user_id AS cust_id
                    , user_type
                    , null AS cust_name
                    , null AS phone_number
                    , (
                        CASE
                            WHEN dag_id like '%debit%' THEN 'debitCard'
                            WHEN dag_id like '%credit%' THEN 'creditCard'
                            ELSE null
                        END
                    ) AS prod_type
                    , item_id AS prod_id
                    , channel_id
                    , banner_id
                    , CAST(score AS STRING) AS score
                    , CAST(rank AS STRING) AS rank
                    , {scene_id} AS scene_id
                    , dag_id AS pipeline_id
                    , model_version AS version
                    , dt AS model_dt
                    , '{tbl_name}' AS src_tbl
                FROM {tbl_name}
                WHERE dt = '{dt}'
            )

            SELECT *
            FROM (
                SELECT
                    * , ROW_NUMBER() OVER (PARTITION BY cust_id, prod_id, scene_id ORDER BY rank DESC) rn
                FROM tbl
                WHERE scene_id not like 'T%' -- 不需要测试场景
            )
            WHERE rn = 1
        """
        ).drop('rn')
    except Exception as e:
        logger.error(f'collect_bdasire_card_crowd_package error: {e}')
        return None


def collect_s1106_results(spark_func, dt, tbl_name, logger) -> DataFrame:
    try:
        return spark_func(
            f"""
            WITH tbl AS (
                SELECT
                    cust_id
                    , 'exp' AS user_type
                    , cust_name
                    , phone_number
                    , (
                        CASE
                            WHEN dag_id in ('S1106', 'S1108') THEN 'finc'
                            WHEN dag_id in ('S1109') THEN 'fund'
                            ELSE null
                        END
                    ) AS prod_type
                    , prod_id
                    , 'PSMS_IRE' AS channel_id
                    , null AS banner_id
                    , null AS score
                    , CAST(rank + 1 AS STRING) AS rank
                    , dag_id AS scene_id
                    , dag_id AS pipeline_id
                    , null AS version
                    , dt AS model_dt
                    , '{tbl_name}' AS src_tbl
                FROM {tbl_name}
                LATERAL VIEW posexplode(prod_list) AS rank, prod_id
                WHERE dt = '{dt}' and dag_id in ('S1106', 'S1108', 'S1109')
            )

            SELECT *
            FROM (
                SELECT
                    * , ROW_NUMBER() OVER (PARTITION BY cust_id, prod_id, scene_id ORDER BY rank DESC) rn
                FROM tbl
            )
            WHERE rn = 1
        """
        ).drop('rn')
    except Exception as e:
        logger.error(f'collect_bdasire_card_s1106_results error: {e}')
        return None


@JobRegistry.register(JobRegistry.T.INFER)
class ModelResultsToDW(SparkPostProcJob):
    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self._table_inserter = TableInserter(self.output_table_info_list(self._base)[0])

    @timeit()
    def run(self):
        # 此作业是T+1日执行，如20230720凌晨01:00执行，传递进来的batch_id为20230719
        # 对于竹间结果表，需要取20230719的数据
        # 对于天壤结果表，需要取20230718的数据
        # 插入result表，dt=T-1 日期，为20230719
        try:
            batch_dt = self.batch_id
            zj_dt = batch_dt
            tr_dt = (Datetime.from_str(batch_dt) - timedelta(days=1)).str
            dt = batch_dt
            zj_rc_res_tbl = self._base.external_table.zj_reco_result
            zj_rc_res_dlvy_tbl = self._base.external_table.zj_reco_result_delivery
            crowd_package_tbl = self._base.output_table.crowd_package
            s1106_results_tbl = self._base.output_table.s1106_results
            dfs = [
                (
                    collect_zj_reco_result(self.run_spark_sql, zj_dt, zj_rc_res_tbl, self.logger),
                    zj_rc_res_tbl,
                ),
                (
                    collect_zj_rc_res_dlvy(
                        self.run_spark_sql, zj_dt, zj_rc_res_dlvy_tbl, self.logger
                    ),
                    zj_rc_res_dlvy_tbl,
                ),
                (
                    collect_crowd_package(
                        self.run_spark_sql, tr_dt, crowd_package_tbl, self.logger
                    ),
                    crowd_package_tbl,
                ),
                (
                    collect_s1106_results(
                        self.run_spark_sql, tr_dt, s1106_results_tbl, self.logger
                    ),
                    s1106_results_tbl,
                ),
            ]
            new_dfs = []
            for df, name in dfs:
                if df is None or df.count() == 0:
                    self.logger.warning(f'Not found any result data in {name}, skip')
                else:
                    new_dfs.append(df)

            if len(new_dfs) == 0:
                self.logger.error('!!!!!!! Not found any result data, skip')
                return

            after_cnts = len(new_dfs)
            for i in range(after_cnts):
                if i == 0:
                    self.logger.info(f'union {i}th df, after union cnt: {new_dfs[0].count()}')
                    continue
                new_dfs[0] = new_dfs[0].union(new_dfs[i])
                self.logger.info(f'union {i}th df, after union cnt: {new_dfs[0].count()}')

            filename = f'BDAS_IRE_MODEL_RESULT_{dt}'
            hdfs_csv_path = os.path.join(self._base.hdfs_output_dir, dt, filename)
            subset = [
                'cust_id',
                'user_type',
                'cust_name',
                'phone_number',
                'prod_type',
                'prod_id',
                'channel_id',
                'banner_id',
                'score',
                'rank',
                'scene_id',
                'pipeline_id',
                'version',
                'model_dt',
            ]
            df = new_dfs[0].fillna('NULL_VALUE', subset=subset)
            revs = ['rev1', 'rev2', 'rev3', 'rev4', 'rev5', 'rev6', 'rev7', 'rev8', 'rev9', 'rev10']
            for rev in revs:
                df = df.withColumn(rev, F.lit('NULL_VALUE'))

            self._table_inserter.insert_df(df, overwrite=True, dt=dt)
            self.logger.info(f'write to dw table success: {self._table_inserter.table_name}')

            df = self.run_spark_sql(
                f"SELECT * FROM {self._table_inserter.table_name} WHERE dt = '{dt}'"
            )
            df = df.fillna('NULL_VALUE', subset=subset + revs)

            df = df.drop('src_tbl')
            data_dt = 'data_dt'
            df = df.withColumn(data_dt, F.lit(dt))
            df = df.select(subset + [data_dt] + revs)
            df.show(10, 0)
            dump_df_with_header(self.spark, df, hdfs_csv_path)
            self.logger.info(f'write to hdfs csv path success: {hdfs_csv_path}')
        except Exception as e:
            import traceback

            traceback.print_exc()
            # 错误不传递给下游，让下游可以继续产出空文件
            self.logger.error(f'run catch error: {e}')

    def show(self):
        dt = self.batch_id
        self.run_spark_sql(
            f"SELECT * FROM {self._table_inserter.table_name} WHERE dt = '{dt}'"
        ).show(10, 0)

    @staticmethod
    def output_table_info_list(cfg: BaseConfig):
        return [
            {
                'name': cfg.output_table.ire_model_result,
                'comment': '模型结果入仓表',
                'field_list': [
                    {'name': 'cust_id', 'type': 'string', 'comment': '用户编码'},
                    {'name': 'user_type', 'type': 'string', 'comment': '用户分组类型'},
                    {'name': 'cust_name', 'type': 'string', 'comment': '用户名称'},
                    {'name': 'phone_number', 'type': 'string', 'comment': '用户手机号'},
                    {'name': 'prod_type', 'type': 'string', 'comment': '产品类型'},
                    {'name': 'prod_id', 'type': 'string', 'comment': '产品编码'},
                    {'name': 'channel_id', 'type': 'string', 'comment': '渠道编码'},
                    {'name': 'banner_id', 'type': 'string', 'comment': '栏位编码'},
                    {'name': 'score', 'type': 'string', 'comment': '客户-产品得分'},
                    {'name': 'rank', 'type': 'string', 'comment': '客户-产品得分排名'},
                    {'name': 'scene_id', 'type': 'string', 'comment': '场景编号'},
                    {'name': 'pipeline_id', 'type': 'string', 'comment': '工作流编码'},
                    {'name': 'version', 'type': 'string', 'comment': '模型版本'},
                    {'name': 'model_dt', 'type': 'string', 'comment': '原表数据日期'},
                    {'name': 'src_tbl', 'type': 'string', 'comment': '原表'},
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
class CollectIREModelResults(PostProcJob):
    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self.dt = self.batch_id
        self.filename = f'BDAS_IRE_MODEL_RESULT_{self.dt}'
        self.hdfs_csv_path = os.path.join(self._base.hdfs_output_dir, self.dt, self.filename)

    def run(self):
        # 获取入仓的基础目录 /share_${Namespace}/${Environment}/result_rucang
        nas_base_dir = os.path.abspath(os.path.join(self._base.nas_output_dir, os.pardir))
        result_rucang_dir = os.path.join(nas_base_dir, 'result_rucang')

        # 实际入仓还有加output和日期目录
        dt_path_dir = os.path.join(result_rucang_dir, 'output', self.dt)
        os.makedirs(dt_path_dir, exist_ok=True)

        self._transfer_hdfs_file_to_nas(dt_path_dir)

    def _write_flg_file(self, flg_file_path, filename, filesize, rowcount):
        # 写 flg 文件标记已经传输完成
        with open(flg_file_path, 'w') as f:
            f.write(f'{filename} {filesize} {rowcount}')

    def _write_empty_dat_file(self, dat_file_path):
        # 写 空 dat文件
        with open(dat_file_path, mode='w', encoding='gbk') as f:
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
