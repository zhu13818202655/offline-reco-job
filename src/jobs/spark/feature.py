# -*- coding: utf-8 -*-
# @File : feature.py
# @Author : r.yang
# @Date : Thu Mar 17 16:25:23 2022
# @Description :

import traceback

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from algorithm.feature import FeatureEngine, FeatureEngineFit
from configs import Context
from configs.consts import FEAT_COL, LABEL_COL
from configs.model.base import BaseConfig, FeatureEngineConfig
from configs.model.config import RankingConfig, UserPoolConfig
from core.job.base import IOutputTable
from core.job.registry import JobRegistry
from core.spark import WithSpark
from core.spark.hdfs import HDFileSystem
from core.spark.table import TableInserter
from core.utils import DateHelper, df_view, prt_table_view, timeit
from jobs.base import BaseJob, RecoJob, SparkRecoJob


@JobRegistry.register(JobRegistry.T.BASE)
class UserFeatures(BaseJob, IOutputTable, WithSpark):
    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self._date_helper = DateHelper(self.batch_id, self._base.react_delay)

    def run(self):
        for feat_engine in self._base.feat_engines:
            try:
                self._run(feat_engine)
            except BaseException as e:
                err_str = traceback.format_exc()
                self.logger.error(f'uesr feature error: {e}, traceback: {err_str}')

    @timeit()
    def _run(self, feat_engine: FeatureEngineConfig):
        feature_engine = FeatureEngine(feat_engine)
        with prt_table_view(
            self, self._base.external_table.cre_crd_user, 'cre_crd_user', self.batch_id
        ), prt_table_view(
            self, self._base.external_table.deb_crd_user, 'deb_crd_user', self.batch_id
        ), prt_table_view(
            self,
            self._base.external_table.cust_act_label,
            'cust_act_label',
            self.batch_id,
            'data_dt',
        ), prt_table_view(
            self, self._base.external_table.hx_cancel_user, 'hx_cancel_user', self.batch_id
        ), prt_table_view(
            self, self._base.external_table.mt_cancel_user, 'mt_cancel_user', self.batch_id
        ), prt_table_view(
            self, self._base.external_table.can_acct_type, 'can_acct_type', self.batch_id
        ):
            # 信用卡、借记卡推荐用户
            card_user_df = self.run_spark_sql(
                f"""
                SELECT DISTINCT a.cust_id as user_id
                FROM
                  (SELECT cust_id
                      FROM cre_crd_user
                      WHERE if_valid_hold_crd='1'
                   UNION ALL
                     SELECT cust_id
                      FROM deb_crd_user
                      WHERE if_hold_card='1') AS a
                LEFT JOIN
                  (SELECT cust_id,
                          1 AS pmbs_ind
                   FROM cust_act_label
                   WHERE pmbs_cust_ind='1'
                   GROUP BY cust_id) AS b ON a.cust_id=b.cust_id
                WHERE b.pmbs_ind=1
             """
            )
            # 销户用户
            cancel_user_df = self.run_spark_sql(
                f"""
                SELECT DISTINCT m.cust_id as user_id
                FROM (
                        SELECT cust_id
                        FROM hx_cancel_user
                        WHERE cust_cancel_ind='0'
                    UNION ALL
                        SELECT cust_id
                        FROM mt_cancel_user
                        WHERE card_ccl_ind='0'
                ) AS m
                JOIN (
                    SELECT DISTINCT cust_id as user_id
                    FROM can_acct_type
                    WHERE if_logout_online_value_cust=1 OR if_logout_online_nvalue_cust=1
                ) AS n ON m.cust_id=n.user_id
                    """
            )
            user_df = card_user_df.union(cancel_user_df).distinct()
            if not user_df.count():
                self.logger.warning('无用户可供生成特征')
                return

            # TODO(yang): 为啥这里要往前两天？
            feature_partition = self._date_helper.date_before_react_delay.str
            user_feature = feature_engine.transform(user_df, feature_partition)
            self.logger.info('{} 生成特征版本 {}'.format(self.batch_id, feat_engine.feat_version))

            with df_view(self, user_feature, 'user_features_tmp_table'):
                self.run_spark_sql(
                    """
                INSERT OVERWRITE TABLE {output_table} PARTITION(dt='{batch_id}', tag='{tag}')
                SELECT user_id,
                       features
                FROM user_features_tmp_table
                """.format(
                        output_table=self._base.output_table.user_feature,
                        tag=feat_engine.feat_version,
                        batch_id=self.batch_id,
                    )
                )
                self.logger.info('{} 生成特征版本 {} 结束'.format(self.batch_id, feat_engine.feat_version))

    FEAT_COL = 'user_features'

    @staticmethod
    def with_user_feat(job: SparkRecoJob, df: DataFrame, version: str) -> DataFrame:
        if 'user_id' not in df.columns:
            raise ValueError('df has no user_id column')

        with prt_table_view(job, job._base.output_table.user_feature, 'user_feature', job.batch_id):
            user_feature = job.run_spark_sql(
                f"""
            SELECT user_id, features as {UserFeatures.FEAT_COL}
            FROM user_feature
            WHERE tag='{version}'
            """
            ).dropDuplicates(['user_id'])
            return (
                df.alias('a')
                .join(user_feature, df.user_id == user_feature.user_id, how='left')
                .select([f'a.{i}' for i in df.columns] + [UserFeatures.FEAT_COL])
            )

    def show(self):
        self.run_spark_sql(
            f"""
        SELECT tag,
               count(*)
        FROM {self.output_table_info_list(self._base)[0]['name']}
        WHERE dt='{self.batch_id}'
        GROUP BY tag
        """
        ).show()

    @staticmethod
    def output_table_info_list(cfg: BaseConfig):
        return [
            {
                'name': cfg.output_table.user_feature,
                'comment': '用户特征表',
                'field_list': [
                    {'name': 'user_id', 'type': 'string', 'comment': '用户id'},
                    {'name': FEAT_COL, 'type': 'array<double>', 'comment': '打开次数'},
                ],
                'partition_field_list': [
                    {'name': 'dt', 'type': 'string', 'comment': '日分区表'},
                    {'name': 'tag', 'type': 'string', 'comment': '版本'},
                ],
            }
        ]


@JobRegistry.register(JobRegistry.T.BASE)
class FitFeatureEngine(BaseJob, WithSpark):
    __doc__ = """训练特征pipeline

    - FeatureEngineFit、FeatureEngine 同活动推荐V2

    """

    def __init__(self, ctx: Context):
        super().__init__(ctx)

    def run(self):
        for fe in self._base.feat_engines:
            self._run(fe)

    @timeit()
    def _run(self, feat_engine: FeatureEngineConfig):
        engine = FeatureEngineFit(feat_engine)
        if HDFileSystem.exists(engine.pipeline_folder_path):
            self.logger.info(f'model path: {engine.pipeline_folder_path} exists, skip')
            return

        train_batch = feat_engine.train_dt

        # 获取所有持卡用户
        user_df = self.run_spark_sql(
            f"""
            SELECT cust_id AS user_id
            FROM
              (SELECT cust_id
               FROM {self._base.external_table.cre_crd_user}
               WHERE dt='{train_batch}'
               UNION SELECT cust_id
               FROM {self._base.external_table.deb_crd_user}
               WHERE dt='{train_batch}' )
            GROUP BY cust_id
         """
        )

        # 训练 pipeline
        engine.fit(user_df, train_batch)


class PrepareModelSample(RecoJob, IOutputTable, WithSpark):
    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self._date_helper = DateHelper(self.batch_id, self._base.react_delay)
        self._sample_table = TableInserter(self.output_table_info_list(self._base)[0])
        self._feature_table = TableInserter(self.output_table_info_list(self._base)[1])
        self._month_processed = []

    def run(self):
        raise NotImplementedError

    def show(self):
        self.run_spark_sql(
            f"""
        SELECT tag,
               dt,
               count(*)
        FROM {self.output_table_info_list(self._base)[0]['name']}
        GROUP BY tag,
                 dt
        """
        ).show()

    @staticmethod
    def output_table_info_list(cfg: BaseConfig):
        return [
            {
                'name': cfg.output_table.model_sample,
                'comment': '用户样本表',
                'field_list': [
                    {'name': 'user_id', 'type': 'string', 'comment': '用户id'},
                    {'name': 'item_id', 'type': 'string', 'comment': '物品id'},
                    {'name': LABEL_COL, 'type': 'string', 'comment': '标签'},
                    {'name': 'sample_dt', 'type': 'string', 'comment': '标签日期'},
                ],
                'partition_field_list': [
                    {'name': 'dt', 'type': 'string', 'comment': '日分区表'},
                    {'name': 'tag', 'type': 'string', 'comment': '版本'},
                ],
            },
            {
                'name': cfg.output_table.model_user_feature,
                'comment': '训练用户特征表',
                'field_list': [
                    {'name': 'user_id', 'type': 'string', 'comment': '用户id'},
                    {'name': FEAT_COL, 'type': 'array<double>', 'comment': '打开次数'},
                ],
                'partition_field_list': [
                    {'name': 'dt', 'type': 'string', 'comment': '日分区表'},
                    {'name': 'tag', 'type': 'string', 'comment': '版本'},
                ],
            },
        ]


@JobRegistry.register(JobRegistry.T.INFER)
class PrepareDebitModelSample(PrepareModelSample):

    _user_pool: UserPoolConfig
    _ranking: RankingConfig

    def run(self):
        for fe in self._base.feat_engines:
            self._run(fe)

    @timeit()
    def _run(self, feat_engine):
        feature_engine = FeatureEngine(feat_engine)
        self.logger.info('生成训练模型需要的样本集及其特征')

        train_months = self._scene.train.train_sample_month
        eval_months = self._scene.train.eval_sample_month
        self.logger.info(f'train_months: {train_months}, eval_months: {eval_months}')
        for month in train_months + eval_months:
            self.logger.info('生成借记卡 {} 的样本'.format(month))

            feature_partition = self._date_helper.get_before_date(
                self._date_helper.from_str('{}01'.format(month)), 1
            ).str
            if (
                self._sample_table.has_parition(
                    dt=feature_partition, tag=self._user_pool.model.sample_version
                )
                and self._sample_table.has_parition(
                    dt=feature_partition, tag=self._ranking.model.sample_version
                )
                and self._feature_table.has_parition(
                    dt=feature_partition, tag=f'{feat_engine.feat_version}_{self._scene.scene_id}',
                )
            ):
                self.logger.info('借记卡 {} 的样本已存在，跳过'.format(month))
                continue
            self._month_processed.append(month)

            # ************************** 借记卡样本**************************
            tmp_debit_sample_df = self.run_spark_sql(
                """
            select a.cust_id, a.cred_apply_dt, b.deb_apply_dt,
                cast(a.cred_hold_crd as int) as cred_hold_crd,
                nvl(c.pmbs_ind, 0) as pmbs_ind,
                nvl(d.has_deb_card, 0) as has_deb_card
            from (
                select cust_id, min(last_apply_dt) as cred_apply_dt,
                max(if_valid_hold_crd) as cred_hold_crd
                from {cre_crd_user}
                where dt='{sample_partition}' and last_apply_dt<'{month}01'
                group by cust_id
            ) as a
            left join (
                select cust_id, firsr_apply_dt as deb_apply_dt
                from {deb_crd_user}
                where dt='{sample_partition}' and firsr_apply_dt like '{month}%'
            ) as b ON a.cust_id=b.cust_id
            left join (
                select cust_id, 1 as pmbs_ind
                from {cust_act_label}
                where data_dt='{sample_partition}' and pmbs_cust_ind='1'
                group by cust_id
            ) as c ON a.cust_id=c.cust_id
            left join (
                select cust_id, 1 as has_deb_card
                from {deb_crd_user}
                where dt='{sample_partition}' and if_hold_card='1' and firsr_apply_dt<'{month}01'
                group by cust_id
            ) as d ON a.cust_id=d.cust_id
            """.format(
                    month=month,
                    cre_crd_user=self._base.external_table.cre_crd_user,
                    deb_crd_user=self._base.external_table.deb_crd_user,
                    cust_act_label=self._base.external_table.cust_act_label,
                    sample_partition=self._scene.train.sample_prt,
                )
            )
            tmp_debit_sample_df.cache()
            self.logger.info(f'{month} 借记卡用户召回总样本数：{tmp_debit_sample_df.count()}')
            p_debit_sample_df = (
                tmp_debit_sample_df.filter(
                    'deb_apply_dt is not null and cred_hold_crd=1 and has_deb_card=0 and pmbs_ind=1'
                )
                .select('cust_id', 'deb_apply_dt')
                .withColumn(LABEL_COL, F.lit('1'))
                .withColumnRenamed('deb_apply_dt', 'sample_dt')
                .withColumn('tag', F.lit(self._user_pool.model.sample_version))
                .select('cust_id', LABEL_COL, 'sample_dt', 'tag')
            )
            self.logger.info(f'{month} 借记卡用户召回正样本数：{p_debit_sample_df.count()}')
            n_debit_sample_df = (
                tmp_debit_sample_df.filter(
                    "deb_apply_dt is null and pmbs_ind=1 and has_deb_card=0 and cred_hold_crd=1 and cred_apply_dt not like '1900%' and cred_apply_dt<'{}01'".format(
                        month
                    )
                )
                .sample(0.035)
                .withColumn(LABEL_COL, F.lit('0'))
                .withColumn('sample_dt', F.lit(month))
                .withColumn('tag', F.lit(self._user_pool.model.sample_version))
                .select('cust_id', LABEL_COL, 'sample_dt', 'tag')
            )
            self.logger.info(f'{month} 借记卡用户召回负样本数：{n_debit_sample_df.count()}')
            self.logger.info('获取借记卡推荐模型样本')
            debit_rec_all_sample_df = self.run_spark_sql(
                """select cust_id, prod_id, firsr_apply_dt
                    from {deb_crd_user}
                    where dt='{sample_partition}' and firsr_apply_dt like '{month}%'
                """.format(
                    sample_partition=self._scene.train.sample_prt,
                    month=month,
                    deb_crd_user=self._base.external_table.deb_crd_user,
                )
            )
            debit_rec_sample_df = (
                debit_rec_all_sample_df.join(p_debit_sample_df, on='cust_id')
                .select('cust_id', 'prod_id', 'firsr_apply_dt')
                .withColumnRenamed('firsr_apply_dt', 'sample_dt')
                .withColumnRenamed('prod_id', LABEL_COL)
            )
            debit_rec_sample_df = debit_rec_sample_df.withColumn(
                'tag', F.lit(self._ranking.model.sample_version)
            ).select('cust_id', LABEL_COL, 'sample_dt', 'tag')

            self.logger.info('合并样本集')
            user_df = (
                p_debit_sample_df.union(n_debit_sample_df)
                .union(debit_rec_sample_df)
                .withColumnRenamed('cust_id', 'user_id')
            )
            user_df.cache()

            if not user_df.count():
                self.logger.warning('无用户可供生成特征')
                continue

            tmp_debit_sample_df.unpersist()

            # ************************** 生成特征 **************************
            self.logger.info('生成特征')

            user_feature = feature_engine.transform(
                user_df.groupBy('user_id').agg(F.count('*').alias('num')), feature_partition
            )
            self.logger.info('{} 生成特征版本 {}'.format(feature_partition, feat_engine.feat_version))

            with df_view(self, user_feature, 'user_features_tmp_table'):
                self.run_spark_sql(
                    """
                INSERT OVERWRITE TABLE {output_table} PARTITION(dt='{feature_partition}', tag='{tag}_{scene_id}')
                SELECT user_id,
                       features
                FROM user_features_tmp_table
                """.format(
                        output_table=self._base.output_table.model_user_feature,
                        tag=feat_engine.feat_version,
                        feature_partition=feature_partition,
                        scene_id=self._scene.scene_id,
                    )
                )
                self.logger.info('{} 生成特征版本 {} 结束'.format(self.batch_id, feat_engine.feat_version))

            with df_view(self, user_df, 'user_tmp_table'):
                self.run_spark_sql(
                    """
                INSERT OVERWRITE TABLE {output_table} PARTITION(dt='{feature_partition}', tag)
                SELECT user_id, NULL AS item_id, label, sample_dt, tag
                FROM user_tmp_table
                """.format(
                        output_table=self._base.output_table.model_sample,
                        feature_partition=feature_partition,
                    )
                )

            user_df.unpersist()


@JobRegistry.register(JobRegistry.T.INFER)
class PrepareCreditModelSample(PrepareModelSample):
    _user_pool: UserPoolConfig
    _ranking: RankingConfig

    def run(self):
        for fe in self._base.feat_engines:
            self._run(fe)

    @timeit()
    def _run(self, feat_engine: FeatureEngineConfig):
        feature_engine = FeatureEngine(feat_engine)
        self.logger.info('生成训练模型需要的样本集及其特征')

        train_months = self._scene.train.train_sample_month
        eval_months = self._scene.train.eval_sample_month
        self.logger.info(f'train_months: {train_months}, eval_months: {eval_months}')
        for month in train_months + eval_months:
            self.logger.info('生成信用卡 {} 的样本'.format(month))

            feature_partition = self._date_helper.get_before_date(
                self._date_helper.from_str('{}01'.format(month)), 1
            ).str
            if (
                self._sample_table.has_parition(
                    dt=feature_partition, tag=self._user_pool.model.sample_version
                )
                and self._sample_table.has_parition(
                    dt=feature_partition, tag=self._ranking.model.sample_version
                )
                and self._feature_table.has_parition(
                    dt=feature_partition, tag=f'{feat_engine.feat_version}_{self._scene.scene_id}',
                )
            ):
                self.logger.info('信用卡 {} 的样本已存在，跳过'.format(month))
                continue

            self._month_processed.append(month)
            # ************************** 信用卡样本**************************
            tmp_credit_sample_df = self.run_spark_sql(
                """
                select a.cust_id, a.deb_apply_dt, b.cred_apply_dt,
                    cast(a.deb_hold_card as int) as deb_hold_card,
                    nvl(c.pmbs_ind, 0) as pmbs_ind,
                    nvl(d.has_cred_card, 0) as has_cred_card
                from (
                    select cust_id, min(firsr_apply_dt) as deb_apply_dt,
                    max(if_hold_card) as deb_hold_card
                    from {deb_crd_user}
                    where dt='{sample_partition}' and firsr_apply_dt<'{month}01'
                    group by cust_id
                ) as a
                left join (
                    select cust_id, last_apply_dt as cred_apply_dt
                    from {cre_crd_user}
                    where dt='{sample_partition}' and last_apply_dt like '{month}%'
                ) as b ON a.cust_id=b.cust_id
                left join (
                    select cust_id, 1 as pmbs_ind
                    from {cust_act_label}
                    where data_dt='{sample_partition}' and pmbs_cust_ind='1'
                    group by cust_id
                ) as c ON a.cust_id=c.cust_id
                left join (
                    select cust_id, 1 as has_cred_card
                    from {cre_crd_user}
                    where dt='{sample_partition}' and if_valid_hold_crd='1' and last_apply_dt<'{month}01'
                    group by cust_id
                ) as d ON a.cust_id=d.cust_id
            """.format(
                    month=month,
                    cre_crd_user=self._base.external_table.cre_crd_user,
                    deb_crd_user=self._base.external_table.deb_crd_user,
                    cust_act_label=self._base.external_table.cust_act_label,
                    sample_partition=self._scene.train.sample_prt,
                )
            )
            tmp_credit_sample_df.cache()
            self.logger.info(f'{month} 信用卡用户召回总样本数：{tmp_credit_sample_df.count()}')
            p_credit_sample_df = (
                tmp_credit_sample_df.filter(
                    'cred_apply_dt is not null and deb_hold_card=1 and has_cred_card=0 and pmbs_ind=1'
                )
                .select('cust_id', 'cred_apply_dt')
                .withColumn(LABEL_COL, F.lit('1'))
                .withColumnRenamed('cred_apply_dt', 'sample_dt')
                .withColumn('tag', F.lit(self._user_pool.model.sample_version))
                .select('cust_id', LABEL_COL, 'sample_dt', 'tag')
            )
            self.logger.info(f'{month} 信用卡用户召回正样本数：{p_credit_sample_df.count()}')
            n_credit_sample_df = (
                tmp_credit_sample_df.filter(
                    "cred_apply_dt is null and deb_hold_card=1 and has_cred_card=0 and pmbs_ind=1 and deb_apply_dt not like '1900%' and deb_apply_dt<'{}01'".format(
                        month
                    )
                )
                .sample(0.02)
                .withColumn(LABEL_COL, F.lit('0'))
                .withColumn('sample_dt', F.lit(month))
                .withColumn('tag', F.lit(self._user_pool.model.sample_version))
                .select('cust_id', LABEL_COL, 'sample_dt', 'tag')
            )
            self.logger.info(f'{month} 信用卡用户召回负样本数：{n_credit_sample_df.count()}')
            credit_rec_all_sample_df = self.run_spark_sql(
                """
                select cust_id, prod_id, last_apply_dt
                from {cre_crd_user}
                where dt='{sample_partition}' and last_apply_dt like '{month}%'
            """.format(
                    cre_crd_user=self._base.external_table.cre_crd_user,
                    sample_partition=self._scene.train.sample_prt,
                    month=month,
                )
            )
            credit_rec_sample_df = (
                credit_rec_all_sample_df.join(p_credit_sample_df, on='cust_id')
                .select('cust_id', 'prod_id', 'last_apply_dt')
                .withColumnRenamed('last_apply_dt', 'sample_dt')
                .withColumnRenamed('prod_id', LABEL_COL)
            )
            credit_rec_sample_df = credit_rec_sample_df.withColumn(
                'tag', F.lit(self._ranking.model.sample_version)
            ).select('cust_id', LABEL_COL, 'sample_dt', 'tag')

            self.logger.info('合并样本集')
            user_df = (
                p_credit_sample_df.union(n_credit_sample_df)
                .union(credit_rec_sample_df)
                .withColumnRenamed('cust_id', 'user_id')
            )
            user_df.cache()

            if not user_df.count():
                self.logger.warning('无用户可供生成特征')
                continue

            tmp_credit_sample_df.unpersist()

            # ************************** 生成特征 **************************
            self.logger.info('生成特征')

            user_feature = feature_engine.transform(
                user_df.groupBy('user_id').agg(F.count('*').alias('num')), feature_partition
            )
            self.logger.info('{} 生成特征版本 {}'.format(feature_partition, feat_engine.feat_version))

            with df_view(self, user_feature, 'user_features_tmp_table'):
                self.run_spark_sql(
                    """
                INSERT OVERWRITE TABLE {output_table} PARTITION(dt='{feature_partition}', tag='{tag}_{scene_id}')
                SELECT user_id,
                       features
                FROM user_features_tmp_table
                """.format(
                        output_table=self._base.output_table.model_user_feature,
                        tag=feat_engine.feat_version,
                        feature_partition=feature_partition,
                        scene_id=self._scene.scene_id,
                    )
                )
                self.logger.info('{} 生成特征版本 {} 结束'.format(self.batch_id, feat_engine.feat_version))

            with df_view(self, user_df, 'user_tmp_table'):
                self.run_spark_sql(
                    """
                INSERT OVERWRITE TABLE {output_table} PARTITION(dt='{feature_partition}', tag)
                SELECT user_id, NULL AS item_id, label, sample_dt, tag
                FROM user_tmp_table
                """.format(
                        output_table=self._base.output_table.model_sample,
                        feature_partition=feature_partition,
                    )
                )

            user_df.unpersist()


@JobRegistry.register(JobRegistry.T.INFER)
class PrepareCancelModelSample(PrepareModelSample):

    _cancel_user: UserPoolConfig

    def run(self):
        for fe in self._base.feat_engines:
            self._run(fe)

    @timeit()
    def _run(self, feat_engine: FeatureEngineConfig):
        feature_engine = FeatureEngine(feat_engine)
        self.logger.info('生成训练模型需要的样本集及其特征')

        train_months = self._scene.train.train_sample_month
        eval_months = self._scene.train.eval_sample_month
        self.logger.info(f'train_months: {train_months}, eval_months: {eval_months}')
        for month in train_months + eval_months:
            self.logger.info('生成销户用户 {} 的样本'.format(month))

            # 使用T-2月特征
            feature_partition_tmp = self._date_helper.get_before_date(
                self._date_helper.from_str('{}01'.format(month)), 1
            ).str
            feature_partition = self._date_helper.get_before_date(
                self._date_helper.from_str('{}01'.format(feature_partition_tmp[:6])), 1
            ).str

            if self._sample_table.has_parition(
                dt=feature_partition, tag=self._cancel_user.model.sample_version
            ) and self._feature_table.has_parition(
                dt=feature_partition, tag=f'{feat_engine.feat_version}_{self._scene.scene_id}',
            ):
                self.logger.info('销户 {} 的样本已存在，跳过'.format(month))
                continue

            self._month_processed.append(month)
            # ************************** 销户用户样本**************************
            tmp_cancel_sample_df = self.run_spark_sql(
                """
            (
                SELECT DISTINCT bm_cust_id AS user_id, 1 as label, 'hx' as type
                FROM {hx_cancel_log}
                WHERE dt LIKE '{month}%'
                    AND bm_opr_id not in ('topcardbat','topcardweb','TOPCARD1')
                    AND bm_new_contents='C'
            ) UNION (
                SELECT DISTINCT cust_id AS user_id, 0 as label, 'hx' as type
                FROM {hx_cancel_user}
                WHERE dt='{sample_partition}' AND cust_cancel_ind='0'
                    AND cust_anniv_dt<'{month}01'
            ) UNION (
                SELECT DISTINCT cust_id AS user_id, 1 AS label, 'mt' as type
                FROM {mt_cancel_user}
                WHERE dt='{sample_partition}'
                    AND card_ccl_ind='1' AND card_distory_dt like '{month}%'
            ) UNION (
                SELECT DISTINCT cust_id AS user_id, 0 AS label, 'mt' as type
                FROM {mt_cancel_user}
                WHERE dt='{sample_partition}'
                    AND card_ccl_ind='0'
                    AND card_atv_dt<'{month}01'
            )
            """.format(
                    month=month,
                    sample_partition=self._scene.train.sample_prt,
                    hx_cancel_log=self._base.external_table.hx_cancel_log,
                    hx_cancel_user=self._base.external_table.hx_cancel_user,
                    mt_cancel_user=self._base.external_table.mt_cancel_user,
                )
            )
            tmp_cancel_sample_df.cache()
            self.logger.info(f'{month} 用户召回总样本数：{tmp_cancel_sample_df.count()}')
            p_cancel_sample_df = tmp_cancel_sample_df.filter('label=1')
            self.logger.info(f'{month} 用户召回正样本数：{p_cancel_sample_df.count()}')
            n_hx_sample_df = tmp_cancel_sample_df.filter("type='hx' and label=0").sample(0.042)
            n_mt_sample_df = tmp_cancel_sample_df.filter("type='mt' and label=0").sample(0.028)
            n_cancel_sample_df = n_hx_sample_df.union(n_mt_sample_df)
            self.logger.info(f'{month} 用户召回负样本数：{n_cancel_sample_df.count()}')
            # 可在线挽留用户
            with prt_table_view(
                self,
                self._base.external_table.can_acct_type,
                'can_acct_type',
                dt=self._scene.train.sample_prt,
            ):
                logout_online_df = self.run_spark_sql(
                    f"""
                        SELECT DISTINCT cust_id AS user_id
                        FROM can_acct_type
                        WHERE if_logout_online_value_cust=1 OR if_logout_online_nvalue_cust=1
                    """
                )

            self.logger.info('合并样本集')
            user_df = (
                p_cancel_sample_df.union(n_cancel_sample_df)
                .join(logout_online_df, on='user_id', how='inner')
                .groupBy('user_id')
                .agg(F.max(LABEL_COL).alias(LABEL_COL))
                .select('user_id', LABEL_COL)
            )
            user_df.cache()

            if not user_df.count():
                self.logger.warning('无用户可供生成特征')
                continue

            self.logger.info(f'{month} 样本数：{user_df.count()}')
            tmp_cancel_sample_df.unpersist()

            # ************************** 生成特征 **************************
            self.logger.info('生成特征')

            user_feature = feature_engine.transform(
                user_df.groupBy('user_id').agg(F.count('*').alias('num')), feature_partition
            )
            self.logger.info('{} 生成特征版本 {}'.format(feature_partition, feat_engine.feat_version))

            with df_view(self, user_feature, 'user_features_tmp_table'):
                self.run_spark_sql(
                    """
                INSERT OVERWRITE TABLE {output_table} PARTITION(dt='{feature_partition}', tag='{tag}_{scene_id}')
                SELECT user_id,
                       features
                FROM user_features_tmp_table
                """.format(
                        output_table=self._base.output_table.model_user_feature,
                        tag=feat_engine.feat_version,
                        feature_partition=feature_partition,
                        scene_id=self._scene.scene_id,
                    )
                )
                self.logger.info('{} 生成特征版本 {} 结束'.format(self.batch_id, feat_engine.feat_version))

            with df_view(self, user_df, 'user_tmp_table'):
                self.run_spark_sql(
                    """
                INSERT OVERWRITE TABLE {output_table} PARTITION(dt='{feature_partition}', tag='{tag}')
                SELECT user_id, NULL AS item_id, label, '{month}01' AS sample_dt
                FROM user_tmp_table
                """.format(
                        output_table=self._base.output_table.model_sample,
                        month=month,
                        feature_partition=feature_partition,
                        tag=self._cancel_user.model.sample_version,
                    )
                )

            user_df.unpersist()
