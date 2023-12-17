# -*- coding: utf-8 -*-
# @File : classifer.py
# @Author : r.yang
# @Date : Wed Mar 16 16:47:36 2022
# @Description : classifer model


import datetime
import os
import shutil
from typing import Callable, Optional

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
import xgboost as xgb
from pyspark.ml.classification import GBTClassificationModel, GBTClassifier
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.dataframe import DataFrame

from configs.consts import FEAT_COL, LABEL_COL, PREDICT_COL, PROBA_COL
from configs.model.base import ClassifierConfig
from configs.utils import ClassifierRegistry, Env
from core.logger import tr_logger
from core.spark import WithSpark
from core.utils import new_tmp_file, numpy_feature_to_df, run_cmd, timeit

logger = tr_logger.getChild('algorithm')

__all__ = [
    'XGBClassifierModel',
    'LGBClassifierModel',
    'LGBIncrClassifierModel',
    'SparkGBTClassifierModel',
]


def save_model(local_save_func: Callable[[str], None], hdfs_model_path: str) -> str:
    tmp_model_file = new_tmp_file(suffix='.' + hdfs_model_path.split('.')[-1])
    local_save_func(tmp_model_file)

    hdfs_model_dirname = os.path.dirname(hdfs_model_path)
    hdfs_model_basename = os.path.basename(hdfs_model_path)
    hdfs_model_path_bak = os.path.join(
        hdfs_model_dirname, f'{datetime.datetime.today().strftime("%Y%m%d")}-{hdfs_model_basename}'
    )

    if Env.is_local:
        os.makedirs(os.path.dirname(hdfs_model_path), exist_ok=True)
        shutil.copyfile(tmp_model_file, hdfs_model_path)
        shutil.copyfile(tmp_model_file, hdfs_model_path_bak)
    else:
        # 上传到 hdfs 上
        run_cmd(f'hadoop fs -mkdir -p {os.path.dirname(hdfs_model_path)}', logger)
        run_cmd(
            'hadoop fs -put -f {} {}'.format(tmp_model_file, hdfs_model_path),
            logger,
            raise_now=True,
        )
        run_cmd(
            'hadoop fs -put -f {} {}'.format(tmp_model_file, hdfs_model_path_bak),
            logger,
            raise_now=True,
        )

    logger.info(f'model is saved to {hdfs_model_path}')
    return tmp_model_file


def fetch_model(hdfs_model_path: str) -> str:
    tmp_model_file = new_tmp_file(suffix='.' + hdfs_model_path.split('.')[-1])
    if os.path.exists(tmp_model_file):
        os.remove(tmp_model_file)

    if Env.is_local:
        shutil.copyfile(hdfs_model_path, tmp_model_file)
    else:
        cmd = 'hadoop fs -get {} {}'.format(hdfs_model_path, tmp_model_file)
        run_cmd(cmd, logger, raise_now=True)

    logger.info(f'fetch model from {hdfs_model_path} to local fs: {tmp_model_file}')
    return tmp_model_file


class IClassifier:
    @property
    def model(self):
        raise NotImplementedError

    @property
    def n_features(self) -> int:
        raise NotImplementedError

    @property
    def n_classes(self) -> int:
        raise NotImplementedError

    @classmethod
    def default_train_args(cls) -> dict:
        raise NotImplementedError

    @classmethod
    def model_file_name(cls) -> str:
        raise NotImplementedError

    def fit(
        self, X: np.ndarray, y: np.array, X_eval: np.array = None, y_eval: np.array = None, **kwargs
    ):
        raise NotImplementedError

    def fit_df(self, df: DataFrame, eval_df: Optional[DataFrame] = None):
        raise NotImplementedError

    def transform(
        self,
        df: DataFrame,
        feature_col: str,
        prediction_col: str = PREDICT_COL,
        probability_col: str = PROBA_COL,
    ):
        raise NotImplementedError


class BaseClassifierModel(WithSpark, IClassifier):
    def __init__(self, cfg: ClassifierConfig):
        logger.info(f'init {self.__class__} model from {cfg}, train_args: {cfg.train_args}')
        self.cfg = cfg
        self._model = None
        self.hdfs_model_path = os.path.join(cfg.model_dir, cfg.model_version, cfg.model_file)
        logger.info(f'hdfs model path: {self.hdfs_model_path}')

    def _load_model(self):
        raise NotImplementedError

    @classmethod
    def default_train_args(cls) -> dict:
        return {}

    @property
    def model(self):
        if self._model is None:
            self._load_model()
        return self._model

    def fit_df(self, df: DataFrame, eval_df: Optional[DataFrame] = None):
        df_pd = df.toPandas()
        X = np.array(df_pd[FEAT_COL].to_list())
        y = df_pd[LABEL_COL]
        if X.shape[0] == 0:
            logger.error('empty train data')
            raise ValueError('empty train data')
        logger.info(f'X.shape: {X.shape}, y len: {len(y)}')

        X_eval, y_eval = None, None
        if eval_df is not None:
            df_eval_pd = eval_df.toPandas()
            if df_eval_pd.shape[0] > 0:
                X_eval = np.array(df_eval_pd[FEAT_COL].to_list())
                y_eval = df_eval_pd[LABEL_COL]
                logger.info(f'X_eval.shape: {X_eval.shape}, y_eval len: {len(y_eval)}')

        self.fit(X, y, X_eval, y_eval)


@ClassifierRegistry.register()
class XGBClassifierModel(BaseClassifierModel):

    # _model: xgb.XGBClassifier
    model: xgb.XGBClassifier

    @property
    def n_features(self) -> int:
        return self.model.n_features_in_

    @property
    def n_classes(self) -> int:
        return self.model.n_classes_

    @classmethod
    def default_train_args(cls) -> dict:
        args = {
            'max_depth': 6,
            'learning_rate': 0.1,
            'n_estimators': 100,
            'use_label_encoder': False,
        }
        if Env.is_local:
            args['max_depth'] = 2
            args['n_estimators'] = 4
        return args

    @classmethod
    def model_file_name(cls) -> str:
        return 'xgb.json'

    def _load_model(self):
        self._model = xgb.XGBClassifier()
        self._model.load_model(fetch_model(self.hdfs_model_path))
        logger.info('xgboost model loaded!')

    def fit(
        self,
        X: np.ndarray,
        y: np.array,
        X_eval: np.ndarray = None,
        y_eval: np.array = None,
        **kwargs,
    ):
        if min(y) == max(y):
            self.cfg.custom_train_args['use_label_encoder'] = True
        self._model = xgb.XGBClassifier(**self.cfg.train_args).fit(X, y, **kwargs)
        save_model(self._model.save_model, self.hdfs_model_path)

    @timeit()
    def transform(
        self,
        df: DataFrame,
        feature_col: str,
        prediction_col: str = PREDICT_COL,
        probability_col: str = PROBA_COL,
    ):

        model = self.model
        if model is None:
            raise RuntimeError('model loaded failed')

        @F.pandas_udf(returnType=T.ArrayType(T.FloatType()))
        def predict(features):
            features = features.values.tolist()
            mask = [i for i, f in enumerate(features) if f is not None]
            feat_arr = np.array([i for i in features if i is not None])
            scores = np.repeat([[0.0] * max(2, model.n_classes_)], len(features), axis=0)
            if feat_arr.shape[0]:
                scores[mask] = model.predict_proba(feat_arr)
            return pd.Series(scores.tolist())

        argmax = F.udf(lambda x: int(np.argmax(x)), returnType=T.IntegerType())
        return df.withColumn(probability_col, predict(feature_col)).withColumn(
            prediction_col, argmax(probability_col)
        )


@ClassifierRegistry.register()
class LGBClassifierModel(BaseClassifierModel):

    model: lgb.LGBMClassifier

    @property
    def n_features(self) -> int:
        return self.model.n_features_in_

    @property
    def n_classes(self) -> int:
        return self.model.n_classes_

    @classmethod
    def default_train_args(cls) -> dict:

        if Env.is_local:
            return {'max_depth': 2, 'learning_rate': 0.1, 'n_estimators': 4}
        return {
            'max_depth': 6,
            'learning_rate': 0.1,
            'n_estimators': 200,
            'num_iterations': 2000,
            'early_stopping_round': 100,
            'num_leaves': 31,
            'subsample': 0.9,
            'colsample_bytree': 0.9,
            'reg_alpha': 0.01,
            'reg_lambda': 0.01,
            'min_split_gain': 0.01,
            'metric': 'auc',
            'n_jobs': 4,
        }

    @classmethod
    def model_file_name(cls) -> str:
        return 'lgb.pkl'

    def _load_model(self):
        self._model = joblib.load(fetch_model(self.hdfs_model_path))
        logger.info('lightgbm model loaded!')

    def fit(
        self,
        X: np.ndarray,
        y: np.array,
        X_eval: np.ndarray = None,
        y_eval: np.array = None,
        **kwargs,
    ):
        if 'early_stopping_round' in self.cfg.train_args:
            kwargs['eval_set'] = [
                (X, y),
            ]
            kwargs['verbose'] = 20
            if X_eval is not None and y_eval is not None:
                kwargs['eval_set'].append((X_eval, y_eval))

        self._model = lgb.LGBMClassifier(**self.cfg.train_args).fit(X, y, **kwargs)
        save_model(lambda p: joblib.dump(self._model, p), self.hdfs_model_path)

    @timeit()
    def transform(
        self,
        df: DataFrame,
        feature_col: str,
        prediction_col: str = PREDICT_COL,
        probability_col: str = PROBA_COL,
    ):

        model = self.model
        if model is None:
            raise RuntimeError('model loaded failed')

        @F.pandas_udf(returnType=T.ArrayType(T.FloatType()))
        def predict(features):
            features = features.values.tolist()
            mask = [i for i, f in enumerate(features) if f is not None]
            feat_arr = np.array([i for i in features if i is not None])
            scores = np.repeat([[0.0] * max(2, model.n_classes_)], len(features), axis=0)
            if feat_arr.shape[0]:
                scores[mask] = model.predict_proba(feat_arr)
            return pd.Series(scores.tolist())

        argmax = F.udf(lambda x: int(np.argmax(x)), returnType=T.IntegerType())
        return df.withColumn(probability_col, predict(feature_col)).withColumn(
            prediction_col, argmax(probability_col)
        )


@ClassifierRegistry.register()
class LGBIncrClassifierModel(LGBClassifierModel):
    @classmethod
    def default_train_args(cls) -> dict:
        args = LGBClassifierModel.default_train_args()
        args['batch_num'] = 4
        return args

    # tested by mem profiler
    # Line #    Mem usage    Increment  Occurrences   Line Contents
    # =============================================================
    #    238  166.793 MiB  166.793 MiB           1       @profile
    #    239                                             def fit_df(self, df: DataFrame):
    #    240  166.793 MiB    0.000 MiB           1           df = df.withColumn('rand', F.rand())
    #    241  166.793 MiB    0.000 MiB           1           batch_num = self.cfg.train_args.get('batch_num', 10)
    #    242  166.793 MiB    0.000 MiB           1           logger.info(f'batch num: {batch_num}')
    #    243  288.586 MiB   -0.582 MiB          11           for i in range(batch_num):
    #    244  287.973 MiB   -0.582 MiB          10               logger.info(f'batch training progress: {i}/{batch_num}')
    #    245  287.973 MiB   -1.082 MiB          10               tmp_df = df.filter((i / batch_num <= df.rand) & (df.rand < (i + 1) / batch_num))
    #    246  393.281 MiB 1091.602 MiB          10               df_pd = tmp_df.toPandas()
    #    247  393.281 MiB   15.535 MiB          10               X = np.array(df_pd[FEAT_COL].to_list())
    #    248  393.281 MiB   -0.414 MiB          10               y = df_pd[LABEL_COL]
    #    249  288.281 MiB-1073.125 MiB          10               del df_pd
    #    250  288.281 MiB    0.000 MiB          10               logger.info(f'X.shape: {X.shape}, y len: {len(y)}')
    #    251  288.281 MiB    0.000 MiB          10               if X.shape[0] == 0:
    #    252                                                         continue
    #    253  288.586 MiB  102.156 MiB          10               self.fit(X, y, init_model=self._model)
    #    254  288.586 MiB  -15.867 MiB          10               del X, y
    # @profile
    def fit_df(self, df: DataFrame, eval_df: Optional[DataFrame] = None):
        df = df.withColumn('rand', F.rand())
        batch_num = self.cfg.train_args.get('batch_num', 10)
        logger.info(f'batch num: {batch_num}')

        X_eval, y_eval = None, None
        if eval_df is not None:
            eval_df_pd = eval_df.toPandas()
            if eval_df_pd.shape[0] > 0:
                X_eval = np.array(eval_df_pd[FEAT_COL].to_list())
                y_eval = eval_df_pd[LABEL_COL]

        for i in range(batch_num):
            logger.info(f'batch training progress: {i+1}/{batch_num}')
            tmp_df = df.filter((i / batch_num <= df.rand) & (df.rand < (i + 1) / batch_num))
            df_pd = tmp_df.toPandas()
            X = np.array(df_pd[FEAT_COL].to_list())
            y = df_pd[LABEL_COL]
            del df_pd
            logger.info(f'X.shape: {X.shape}, y len: {len(y)}')
            if X.shape[0] == 0:
                continue
            self.fit(X, y, X_eval, y_eval, init_model=self._model)
            del X, y


@ClassifierRegistry.register()
class SparkGBTClassifierModel(BaseClassifierModel):
    model: GBTClassificationModel
    _FEATURES_COL = 'featureVec'

    @property
    def n_features(self) -> int:
        return self.model.numFeatures

    @property
    def n_classes(self) -> int:
        return self.model.numClasses

    @classmethod
    def default_train_args(cls) -> dict:
        return {}

    @classmethod
    def model_file_name(cls) -> str:
        return 'gbt'

    def fit(
        self,
        X: np.ndarray,
        y: np.array,
        X_eval: np.ndarray = None,
        y_eval: np.array = None,
        **kwargs,
    ):
        df = numpy_feature_to_df(self.spark, X, y)
        return self.fit_df(df)

    def _load_model(self):
        self._model = GBTClassificationModel.load(self.hdfs_model_path)
        logger.info('spark gbt model loaded!')

    def fit_df(self, df: DataFrame, eval_df: Optional[DataFrame] = None):
        to_vector = F.udf(lambda x: Vectors.dense(x), VectorUDT())

        self._model = GBTClassifier(
            labelCol=LABEL_COL, featuresCol=self._FEATURES_COL, **self.cfg.train_args
        ).fit(df.withColumn(self._FEATURES_COL, to_vector(FEAT_COL)))
        self._model.write().overwrite().save(self.hdfs_model_path)

    @timeit()
    def transform(
        self,
        df: DataFrame,
        feature_col: str,
        prediction_col: str = PREDICT_COL,
        probability_col: str = PROBA_COL,
    ):
        to_vector = F.udf(lambda x: Vectors.dense(x), VectorUDT())
        to_array = F.udf(
            lambda x: [0.0 if i is None else i for i in x.toArray().tolist()],
            returnType=T.ArrayType(T.DoubleType()),
        )
        return (
            self.model.transform(df.withColumn(self._FEATURES_COL, to_vector(feature_col)))
            .withColumn(probability_col, to_array(PROBA_COL))
            .withColumnRenamed(PREDICT_COL, prediction_col)
        )


class ClassifierModelFactory:
    @staticmethod
    def create(cfg: ClassifierConfig) -> BaseClassifierModel:
        return ClassifierRegistry.lookup(cfg.model_clz)(cfg)
