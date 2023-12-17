# -*- coding: utf-8 -*-
# @File : model.py
# @Author : r.yang
# @Date : Fri Mar 18 15:14:05 2022
# @Description :

import os
from typing import List

import pyspark.sql.functions as F
import pyspark.sql.types as T
from numpy.random import randint, randn
from pyspark.sql.dataframe import DataFrame
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score

from algorithm.classifer import ClassifierModelFactory
from configs import Context
from configs.consts import FEAT_COL, LABEL_COL, PREDICT_COL, PROBA_COL
from configs.init.common import S_CREDIT
from configs.model.base import ClassifierConfig, ItemInfo
from configs.model.config import RankingConfig, UserPoolConfig
from configs.utils import Env, TagSelector
from core.job.registry import JobRegistry
from core.spark import WithSpark
from core.utils import DateHelper, numpy_feature_to_df, run_cmd, timeit
from jobs.base import RecoJob, SparkRecoJob
from jobs.spark.feature import UserFeatures
from jobs.spark.user_pool import CreOnlyUserPool, DebOnlyUserPool

__MAGIC_FUNC0 = randn  # ADHOC for pseudo-random check
__MAGIC_FUNC1 = randint  # ADHOC for pseudo-random check


class FitUserModelBase(RecoJob, WithSpark):
    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self._date_helper = DateHelper(self.batch_id, self._base.react_delay)

    @property
    def model_config(self) -> ClassifierConfig:
        raise NotImplementedError

    def sample_prts(self, months) -> list:
        prts = []
        for month in months:
            feature_prt = self._date_helper.get_before_date(
                self._date_helper.from_str('{}01'.format(month)), 1
            ).str
            prts.append(feature_prt)
        return prts

    @timeit()
    def _sample(self, months: List[str]) -> DataFrame:
        prts = self.sample_prts(months)

        prt_sql = ', '.join([f"'{i}'" for i in prts])
        return self.run_spark_sql(
            f"""
        SELECT a.user_id,
               a.features,
               b.label
        FROM
          (SELECT user_id,
                  features,
                  dt
           FROM {self._base.output_table.model_user_feature}
           WHERE tag='{self.model_config.feat_version}_{self._scene.scene_id}'
             AND dt IN ({prt_sql})) AS a
        JOIN
          (SELECT user_id,
                  cast(label AS int) AS label,
                  dt
           FROM {self._base.output_table.model_sample}
           WHERE tag='{self.model_config.sample_version}'
             AND dt IN ({prt_sql}) ) AS b ON a.user_id=b.user_id
        AND a.dt=b.dt
        """
        )

    @timeit()
    def run(self):
        model_cfg = self.model_config
        model = ClassifierModelFactory.create(model_cfg)
        # hdfs_model_path = os.path.join(
        #     model_cfg.model_dir, model_cfg.model_version, model_cfg.model_file
        # )
        # if Env.is_local and os.path.exists(hdfs_model_path):
        #     self.logger.warning(f'model {hdfs_model_path} exist, skip ')
        #     return

        # train xgb model & save
        months = self._scene.train.train_sample_month
        if Env.is_local:
            # ensure local test have enough data
            months += self._scene.train.eval_sample_month

        self.logger.info(f'train months: {months}')
        df = self._sample(months)
        eval_df = self._sample(self._scene.train.eval_sample_month)
        model.fit_df(df, eval_df)


@JobRegistry.register(JobRegistry.T.BASE)
class FitUserModel(FitUserModelBase):
    _user_pool: UserPoolConfig

    @property
    def model_config(self):
        return self._user_pool.model


@JobRegistry.register(JobRegistry.T.BASE)
class FitCancelUserModel(FitUserModelBase):
    _cancel_user: UserPoolConfig

    @property
    def model_config(self):
        return self._cancel_user.model

    def sample_prts(self, months) -> list:
        prts = []
        for month in months:
            feature_partition_tmp = self._date_helper.get_before_date(
                self._date_helper.from_str('{}01'.format(month)), 1
            ).str
            feature_prt = self._date_helper.get_before_date(
                self._date_helper.from_str('{}01'.format(feature_partition_tmp[:6])), 1
            ).str
            prts.append(feature_prt)
        return prts


class EvalUserModelBase(RecoJob, WithSpark):
    def __init__(self, ctx: Context):
        super().__init__(ctx)

    def _get_data(self) -> DataFrame:
        df = self.model_fit_clz(self.ctx)._sample(self._scene.train.eval_sample_month)
        return df

    def _mock_data(self, feature_shape) -> DataFrame:
        X = eval('__MAGIC_FUNC0(100, feature_shape)')
        y = eval('__MAGIC_FUNC1(2, size=100)')
        return numpy_feature_to_df(self.spark, X, y)

    @property
    def model_config(self) -> ClassifierConfig:
        raise NotImplementedError

    @property
    def model_fit_clz(self):
        raise NotImplementedError

    @timeit()
    def run(self):
        model = ClassifierModelFactory.create(self.model_config)
        if Env.is_local:
            df = self._mock_data(model.n_features)
        else:
            df = self._get_data()

        df = model.transform(df, FEAT_COL).withColumn('score', F.element_at(PROBA_COL, 2))
        rows = df.select('score', LABEL_COL, PREDICT_COL).collect()
        proba = [r.score for r in rows]
        predict = [r.prediction for r in rows]
        y = [r.label for r in rows]
        acc = accuracy_score(y, predict)
        f1 = f1_score(y, predict)
        auc = roc_auc_score(y, proba)
        prec = precision_score(y, predict)
        recall = recall_score(y, predict)

        self.logger.info(f'auc: {auc}')
        self.logger.info(f'f1: {f1}')
        self.logger.info(f'acc: {acc}')
        self.logger.info(f'prec: {prec}')
        self.logger.info(f'recall: {recall}')


@JobRegistry.register(JobRegistry.T.BASE)
class EvalUserModel(EvalUserModelBase):
    _user_pool: UserPoolConfig

    @property
    def model_config(self) -> ClassifierConfig:
        return self._user_pool.model

    @property
    def model_fit_clz(self):
        return FitUserModel


@JobRegistry.register(JobRegistry.T.BASE)
class EvalCancelUserModel(EvalUserModelBase):
    _cancel_user: UserPoolConfig

    @property
    def model_config(self) -> ClassifierConfig:
        return self._cancel_user.model

    @property
    def model_fit_clz(self):
        return FitCancelUserModel


@JobRegistry.register(JobRegistry.T.BASE)
class FitRecoModel(RecoJob, WithSpark):
    _ranking: RankingConfig

    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self._sample_version = self._ranking.model.sample_version
        self.items: List[ItemInfo] = TagSelector(self._base.items).query(
            **self._scene.item_selector
        )
        self._date_helper = DateHelper(self.batch_id, self._base.react_delay)

    @timeit()
    def _sample(self, months: List[str]):
        # label < 0 表示不训练
        prod_ids = sum([i.item_ids for i in self.items if i.label >= 0], [])
        prod_ids_sql = ', '.join([f"'{i}'" for i in prod_ids])

        prts = []
        for month in months:
            feature_prt = self._date_helper.get_before_date(
                self._date_helper.from_str('{}01'.format(month)), 1
            ).str
            prts.append(feature_prt)

        prt_sql = ', '.join([f"'{i}'" for i in prts])
        return self.run_spark_sql(
            f"""
        SELECT a.user_id,
               a.features,
               label AS prod_id
        FROM
          (SELECT user_id,
                  features,
                  dt
           FROM {self._base.output_table.model_user_feature}
           WHERE tag='{self._ranking.model.feat_version}_{self._scene.scene_id}'
             AND dt IN ({prt_sql})) AS a
        JOIN
          (SELECT user_id,
                  label,
                  dt
           FROM {self._base.output_table.model_sample}
           WHERE tag='{self._sample_version}'
             AND label IN ({prod_ids_sql})
             AND dt IN ({prt_sql})) AS b ON a.user_id=b.user_id
        AND a.dt=b.dt
        """
        )

    @timeit()
    def run(self):
        model_cfg = self._ranking.model
        model = ClassifierModelFactory.create(model_cfg)
        # hdfs_model_path = os.path.join(
        #     model_cfg.model_dir, model_cfg.model_version, model_cfg.model_file
        # )
        # if Env.is_local and os.path.exists(hdfs_model_path):
        #     self.logger.warning(f'model {hdfs_model_path} exist, skip ')
        #     return

        # train xgb model & save
        months = self._scene.train.train_sample_month
        if Env.is_local:
            # ensure local test have enough data
            months += self._scene.train.eval_sample_month
        self.logger.info(f'train months: {months}')

        prod_to_label = {
            item_id: item.label
            for item in self.items
            for item_id in item.item_ids
            if item.label >= 0
        }
        self.logger.info(f'prod to label: {prod_to_label}')
        prod_to_label_udf = F.udf(lambda x: prod_to_label.get(x, 0), returnType=T.IntegerType())
        df = self._sample(months).withColumn(LABEL_COL, prod_to_label_udf('prod_id'))
        self._fit(model, df)

    def _fit(self, model, df):
        label_unique = set(r.label for r in df.select(LABEL_COL).distinct().collect())
        feature_len = len(df.select(FEAT_COL).first().features)
        self.logger.info(f'unique labels: {label_unique}, feature length: {feature_len}')

        padding = []
        for i in range(len(self.items)):
            if i not in label_unique:
                padding.append(([0.0] * feature_len, i))
        if padding:
            padding_df = self.spark.createDataFrame(padding, [FEAT_COL, LABEL_COL])
            df = df.select(FEAT_COL, LABEL_COL).union(padding_df)
        model.fit_df(df)


@JobRegistry.register(JobRegistry.T.BASE)
class EvalRecoModel(RecoJob, WithSpark):
    _ranking: RankingConfig

    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self.items: List[ItemInfo] = TagSelector(self._base.items).query(
            **self._scene.item_selector
        )

    @timeit()
    def run(self):
        model = ClassifierModelFactory.create(self._ranking.model)
        if Env.is_local:
            X = eval('__MAGIC_FUNC0(1000, model.n_features)')
            y = eval('__MAGIC_FUNC1(model.n_classes, size=1000)')
            df = numpy_feature_to_df(self.spark, X, y)
        else:
            df = FitRecoModel(self.ctx)._sample(self._scene.train.eval_sample_month)
            prod_to_label = {
                item_id: item.label
                for item in self.items
                for item_id in item.item_ids
                if item.label >= 0
            }
            prod_to_label_udf = F.udf(lambda x: prod_to_label.get(x, 0), returnType=T.IntegerType())
            df = df.withColumn(LABEL_COL, prod_to_label_udf('prod_id'))

        df = model.transform(df, FEAT_COL)
        rows = df.select(LABEL_COL, PROBA_COL, PREDICT_COL).collect()
        proba = [r[PROBA_COL] for r in rows]
        y = [r.label for r in rows]

        feature_len = len(proba[0])
        y_unique = set(y)
        for i in range(len(self.items)):
            if i not in y_unique:
                y.append(i)
                proba.append([0.0] * feature_len)
                proba[-1][i] = 1.0

        auc_ovo = roc_auc_score(y, proba, multi_class='ovo')
        auc_ovr = roc_auc_score(y, proba, multi_class='ovr')

        self.logger.info(f'One-vs-One auc: {auc_ovo}')
        self.logger.info(f'One-vs-Rest auc: {auc_ovr}')


@JobRegistry.register(JobRegistry.T.INFER)
class Inference(SparkRecoJob):

    _user_pool: UserPoolConfig
    _ranking: RankingConfig

    def __init__(self, ctx: Context):
        super().__init__(ctx)

    @timeit()
    def run(self):
        user_model = ClassifierModelFactory.create(self._user_pool.model)
        if self._config.scene.scene_id == S_CREDIT.scene_id:
            user_df = DebOnlyUserPool.get_users(self, self._base, self.batch_id)
        else:
            user_df = CreOnlyUserPool.get_users(self, self._base, self.batch_id)

        infer_dir = os.path.join(self._ctx.base.hdfs_output_dir, '../inference')
        if Env.is_local:
            os.makedirs(infer_dir, exist_ok=True)
        else:
            run_cmd(f'hadoop fs -mkdir -p {infer_dir}', self.logger)
        user_df = UserFeatures.with_user_feat(self, user_df, user_model.cfg.feat_version)
        dump_path = os.path.join(infer_dir, f'{self.dag_id}_user')
        user_model.transform(user_df, feature_col=UserFeatures.FEAT_COL).select(
            'user_id', PREDICT_COL, PROBA_COL
        ).write.parquet(dump_path, mode='overwrite')
        self.logger.info(f'user model inference results dumped to: {dump_path}')

        reco_model = ClassifierModelFactory.create(self._ranking.model)
        dump_path = os.path.join(infer_dir, f'{self.dag_id}_reco')
        reco_model.transform(user_df, feature_col=UserFeatures.FEAT_COL).select(
            'user_id', PREDICT_COL, PROBA_COL
        ).write.parquet(dump_path, mode='overwrite')
        self.logger.info(f'reco model inference results dumped to: {dump_path}')
