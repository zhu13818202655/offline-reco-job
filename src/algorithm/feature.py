# -*- coding: utf-8 -*-
# @File : feature.py
# @Author : r.yang
# @Date : Thu Mar 17 14:28:51 2022
# @Description : format string

import os
from datetime import datetime

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import (
    Bucketizer,
    OneHotEncoder,
    QuantileDiscretizer,
    SQLTransformer,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.sql.dataframe import DataFrame

from configs.consts import FEAT_COL
from configs.model.base import FeatureEngineConfig
from core.logger import tr_logger
from core.spark import WithSpark
from core.spark.hdfs import HDFileSystem
from core.spark.table import TableUtils
from core.utils import timeit

logger = tr_logger.getChild('feature')


class FeatureEngine(WithSpark):

    COLUMNS_FOLDER = 'columns'
    PIPELINE_FOLDER = 'pipeline'
    NUM_COLUMNS_FOLDER = 'num_columns'
    STR_COLUMNS_FOLDER = 'str_columns'

    def __init__(self, config: FeatureEngineConfig):
        self.config = config
        self.feat_sub_dir = os.path.join(config.feat_dir, config.feat_version)
        self.feature_loader = FeatureLoader(config)

    @timeit()
    def transform(self, user_df, batch_id):
        """
        加载指定日期的特征数据，使用特定版本的 Pipeline 处理特征数据
        :param user_df: DataFrame, columns: user_id
        :return: DataFrame, columns: user_id, features

        # XXX(ryang): 模型加载一次即可
        """

        pipeline_folder_path = os.path.join(self.feat_sub_dir, FeatureEngine.PIPELINE_FOLDER)
        feature_columns_path = os.path.join(self.feat_sub_dir, FeatureEngine.COLUMNS_FOLDER)
        feature_num_columns_path = os.path.join(self.feat_sub_dir, FeatureEngine.NUM_COLUMNS_FOLDER)
        feature_str_columns_path = os.path.join(self.feat_sub_dir, FeatureEngine.STR_COLUMNS_FOLDER)
        # 判断路径是否存在
        if not HDFileSystem.exists(self.feat_sub_dir):
            msg = '特征工程 {} 版本 Pipeline 不存在'.format(self.config.feat_version)
            logger.exception(msg)
            raise FileNotFoundError(msg)

        logger.info('从 {} 加载需要读取的特征列'.format(feature_columns_path))
        origin_data = self.spark.read.parquet(feature_columns_path)
        feature_df = self.feature_loader.load_user_feature(
            user_df, set(origin_data.columns), batch_id
        )

        logger.info('从 {} 加载需要用 0 补全缺失值的特征列'.format(feature_num_columns_path))
        num_cols = self.spark.read.parquet(feature_num_columns_path).columns
        for num_col in num_cols:
            feature_df = feature_df.withColumn(num_col, F.col(num_col).cast('double'))
        feature_df = feature_df.fillna(0, subset=num_cols)

        logger.info('从 {} 加载需要用 NA  补全缺失值的特征列'.format(feature_str_columns_path))
        str_cols = self.spark.read.parquet(feature_str_columns_path).columns
        feature_df = feature_df.fillna('NA', subset=str_cols)
        feature_df = feature_df.na.replace('', 'NA', subset=str_cols)

        logger.info('从 {} 加载 Pipeline'.format(pipeline_folder_path))
        model = PipelineModel.load(pipeline_folder_path)
        vec2array = F.udf(
            lambda x: [0.0 if i is None else i for i in x.toArray().tolist()],
            returnType=T.ArrayType(T.DoubleType()),
        )
        return (
            model.transform(feature_df)
            .select('user_id', FEAT_COL)
            .withColumn(FEAT_COL, vec2array(FEAT_COL))
        )


class FeatureLoader(WithSpark):
    __doc__ = """特征数据加载

    - 会自动加载 configs.model.table.ExternalTable 下面所有 user_feat/item_feat 打头的表名
    - 具体字段和处理方式申明见 configs.model.base.FeatureEngineConfig.feat_map，只要往里
      添加上述 user_feat/item_feat 表中的字段即可

    """

    def __init__(self, config: FeatureEngineConfig) -> None:
        self.config = config

    @timeit()
    def load_user_feature(self, user_df: DataFrame, feature_columns, batch_id):
        logger.info(f'需要提取的特征列为 {feature_columns}')
        logger.info(f'从分区 {batch_id} 提取特征')

        # 生成 sql
        select_col = ['a.user_id']
        columns_loaded = set()
        for i, table in enumerate(self.config.user_feat_tables):
            alias = chr(i + 98)  # 0 -> b, 1 -> c, ...
            columns = TableUtils.get_columns(self.spark, table)
            columns_to_load = (feature_columns - columns_loaded) & set(columns)
            if not columns_to_load:
                logger.warning(f'特征工程无可加载特征列, table={table}')
                continue

            logger.info(f'{table} 表需要加载特征 {columns_to_load}')
            select_col += [f'{alias}.{x}' for x in columns_to_load]
            # XXX(ryang): 潜规则，用户id列必须是 cust_id

            dts = (
                self.run_spark_sql('show partitions {}'.format(table))
                .rdd.flatMap(lambda x: x)
                .collect()
            )
            dts = [i.split('/')[0].split('=')[1] for i in dts]
            dt = batch_id if batch_id in dts or not dts else max(dts)

            feature_df = self.run_spark_sql(
                f"""
            SELECT cust_id, {','.join(columns_to_load)}
            FROM {table}
            WHERE dt='{dt}'
            """
            ).dropDuplicates(['cust_id'])
            user_df = user_df.join(feature_df, user_df.user_id == feature_df.cust_id, how='left')
            columns_loaded.update(columns_to_load)

        return user_df


class FeatureEngineFit(WithSpark):
    def __init__(self, config: FeatureEngineConfig):
        self.config = config
        self.feat_sub_dir = os.path.join(config.feat_dir, config.feat_version)
        self.feature_loader = FeatureLoader(config)
        self.version = self.config.feat_version
        self.features = self.config.feat_map

        self.pipeline_folder_path = os.path.join(self.feat_sub_dir, FeatureEngine.PIPELINE_FOLDER)
        self.feature_columns_path = os.path.join(self.feat_sub_dir, FeatureEngine.COLUMNS_FOLDER)
        self.feature_num_columns_path = os.path.join(
            self.feat_sub_dir, FeatureEngine.NUM_COLUMNS_FOLDER
        )
        self.feature_str_columns_path = os.path.join(
            self.feat_sub_dir, FeatureEngine.STR_COLUMNS_FOLDER
        )

    @timeit()
    def fit(self, user_df, batch_id):
        feature_columns = []
        for cols in self.features.values():
            for col in cols:
                if isinstance(col, str):
                    feature_columns.append(col)
                else:
                    feature_columns.append(col[0])
        feature_df = self.feature_loader.load_user_feature(user_df, set(feature_columns), batch_id)
        feature_df.cache()
        logger.info('使用 {} 样本训练特征工程 Pipline'.format(feature_df.count()))

        # 保存 columns 信息
        logger.info(
            '保存特征工程版本 {} 需要加载的特征列至 {}'.format(self.config.feat_version, self.feature_columns_path)
        )
        feature_df.limit(1).write.parquet(self.feature_columns_path, mode='overwrite')
        # ************* 特征工程 ************
        logger.info('开始训练版本 {}  的特征工程Pipline'.format(self.version))
        stages = []
        vec_cols = self.features.get('default', [])
        idx_cols = self.features.get('stringIndex', [])
        onehot_cols = self.features.get('onehot', [])
        std_cols = self.features.get('standardScaler', [])
        qd_cols = self.features.get('quantileDiscretizer', [])
        buck_cols = self.features.get('bucketizer', [])
        log_cols = self.features.get('log1p', [])
        diff_cols = self.features.get('dateDiff', [])
        # 补全缺失值
        logger.info('补全缺失值')
        num_cols = vec_cols + std_cols + qd_cols + [i[0] for i in buck_cols] + log_cols
        for num_col in num_cols:
            feature_df = feature_df.withColumn(num_col, F.col(num_col).cast('double'))
        feature_df.limit(1).select(num_cols).write.parquet(
            self.feature_num_columns_path, mode='overwrite'
        )
        logger.info(
            '保存特征工程版本 {} 需要用 0 补全缺失值的特征列至 {}'.format(self.version, self.feature_num_columns_path)
        )
        str_cols = idx_cols + onehot_cols
        feature_df.limit(1).select(str_cols).write.parquet(
            self.feature_str_columns_path, mode='overwrite'
        )
        logger.info(
            '保存特征工程版本 {} 需要用 NA 补全缺失值的特征列至 {}'.format(self.version, self.feature_str_columns_path)
        )
        feature_df = feature_df.fillna(0, subset=num_cols)
        feature_df = feature_df.fillna('NA', subset=str_cols)
        feature_df = feature_df.na.replace('', 'NA', subset=str_cols)
        # StringIndex
        for c in idx_cols + onehot_cols:
            logger.info('对列 {} 执行 StringIndex 操作'.format(c))
            stages.append(
                StringIndexer(inputCol=c, outputCol='{}_idx'.format(c), handleInvalid='keep')
            )
        vec_cols += ['{}_idx'.format(c) for c in idx_cols]
        # OneHot
        if len(onehot_cols) > 0:
            logger.info('对列 {} 执行 OneHot 操作'.format(str(onehot_cols)))
            stages.append(
                OneHotEncoder(
                    inputCols=['{}_idx'.format(c) for c in onehot_cols],
                    outputCols=['{}_oh'.format(c) for c in onehot_cols],
                )
            )
            vec_cols += ['{}_oh'.format(c) for c in onehot_cols]
        # 标准化
        if len(std_cols) > 0:
            logger.info('对列 {} 执行 StandardScaler 操作'.format(std_cols))
            stages.append(VectorAssembler(inputCols=std_cols, outputCol='std_vec_org'))
            stages.append(
                StandardScaler(
                    inputCol='std_vec_org', outputCol='std_vec_col', withStd=True, withMean=False
                )
            )
            vec_cols.append('std_vec_col')
        # 自动分箱
        for c in qd_cols:
            logger.info('对列 {} 执行 QuantileDiscretizer 操作'.format(c))
            stages.append(
                QuantileDiscretizer(
                    numBuckets=10, inputCol=c, outputCol='{}_qd'.format(c), handleInvalid='keep'
                )
            )
        vec_cols += ['{}_qd'.format(c) for c in qd_cols]
        # 手动分箱
        for c in buck_cols:
            logger.info('对列 {} 执行 Bucketizer 操作'.format(c))
            if c[1][0] != -float('inf'):
                c[1].insert(0, -float('inf'))

            if c[1][-1] != float('inf'):
                c[1].append(float('inf'))

            stages.append(
                Bucketizer(
                    splits=c[1],
                    inputCol=c[0],
                    outputCol='{}_buck'.format(c[0]),
                    handleInvalid='keep',
                )
            )
        vec_cols += ['{}_buck'.format(c[0]) for c in buck_cols]
        # log
        if len(log_cols) > 0:
            log_col_str = [f'log1p(if({x}<0 OR {x} IS NULL, 0, {x})) AS {x}_lg' for x in log_cols]
            log_sql = 'SELECT *, {} FROM __THIS__'.format(', '.join(log_col_str))
            logger.info('对列 {} 执行 log1p 操作, SQL: {}'.format(str(log_cols), log_sql))
            stages.append(SQLTransformer(statement=log_sql))
            vec_cols += ['{}_lg'.format(c) for c in log_cols]
        # 日期差
        if len(diff_cols) > 0:
            batch_id_str = datetime.strftime(datetime.strptime(batch_id, '%Y%m%d'), '%Y-%m-%d')
            diff_col_str = [
                (
                    f"if({x} IS NULL OR {x}='19000101', 0, datediff(from_unixtime(unix_timestamp({x}, "
                    f"'yyyyMMdd'),'yyyy-MM-dd'), '{batch_id_str}')) AS {x}_diff"
                )
                for x in diff_cols
            ]
            diff_sql = 'SELECT *, {} FROM __THIS__'.format(', '.join(diff_col_str))
            logger.info('对列 {} 执行日期差操作, SQL: {}'.format(str(diff_cols), diff_sql))
            stages.append(SQLTransformer(statement=diff_sql))
            vec_cols += ['{}_diff'.format(c) for c in diff_cols]

        # 转成向量
        logger.info('将所有特征列合成特征向量')
        stages.append(VectorAssembler(inputCols=vec_cols, outputCol=FEAT_COL, handleInvalid='keep'))
        # 生成 Pipeline
        pipeline = Pipeline(stages=stages)
        model = pipeline.fit(feature_df)
        logger.info('保存特征工程版本 {} Pipeline 至 {}'.format(self.version, self.pipeline_folder_path))
        model.write().overwrite().save(self.pipeline_folder_path)
        vec2array = F.udf(
            lambda x: [0.0 if i is None else i for i in x.toArray().tolist()],
            returnType=T.ArrayType(T.DoubleType()),
        )
        return (
            model,
            model.transform(feature_df)
            .select('user_id', FEAT_COL)
            .withColumn(FEAT_COL, vec2array(FEAT_COL)),
        )
