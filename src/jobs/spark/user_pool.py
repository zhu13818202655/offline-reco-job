# -*- coding: utf-8 -*-
# @File : user_pool.py
# @Author : r.yang
# @Date : Mon Feb 28 12:54:42 2022
# @Description : user pool

from collections import defaultdict
from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from algorithm.classifer import ClassifierModelFactory
from configs import Context
from configs.model.base import BaseConfig
from configs.model.config import GroupInfo, OperatingUserConfig, UserPoolConfig, UserType
from core.job.registry import JobRegistry
from core.spark import WithSpark
from core.spark.table import TableInserter
from core.utils import DateHelper, prt_table_view, timeit
from jobs.base import SparkRecoJob
from jobs.spark.feature import UserFeatures


class UserPool(SparkRecoJob):

    _operating_user: OperatingUserConfig
    _user_pool: UserPoolConfig

    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self._table_inserter = TableInserter(self.output_table_info_list(self._base)[0])
        self._date_helper = DateHelper(self.batch_id, self._base.react_delay)

    @staticmethod
    def get_users(job: WithSpark, base: BaseConfig, batch_id) -> DataFrame:
        raise NotImplementedError

    @timeit()
    def run(self):
        cur_user = self.get_users(self, self._base, self.batch_id)
        cur_user.cache()
        self.logger.info(f'all user num of {self.__class__.__name__} is: {cur_user.count()}')

        # 根据配置的过滤条件进行过滤，比如年龄小于60
        cur_user = self._filter_users(cur_user)
        self.logger.info(f'user num filtered by {self._user_pool.filters} is: {cur_user.count()}')

        pre_user = self._gen_pre_user_df()
        cur_user = self._with_user_type(self._with_score(cur_user), pre_user).withColumn(
            'batch_id', F.lit(self._ctx.batch_id)
        )

        self._table_inserter.insert_df(cur_user, dt=self._ctx.batch_id, dag_id=self._ctx.dag_id)

    def show(self):
        self.run_spark_sql(
            f"""
        SELECT user_type,
               count(*)
        FROM {self._table_inserter.table_name}
        WHERE dag_id='{self.dag_id}'
          AND dt='{self.batch_id}'
        GROUP BY user_type
        """
        ).show()

    @timeit()
    def _filter_users(self, user_df: DataFrame) -> DataFrame:
        if not self._user_pool.filters:
            return user_df

        with prt_table_view(
            self, self._base.external_table.user_feat_ls_crd, 'user_feat', self.batch_id
        ):
            conditions = ' and '.join([i for i in self._user_pool.filters.values()])
            filter_df = self.run_spark_sql(
                f"""
            SELECT DISTINCT cust_id
            FROM user_feat
            WHERE {conditions}
            """
            )

            return (
                user_df.join(
                    filter_df.withColumn('tmp', F.lit('1')),
                    user_df.user_id == filter_df.cust_id,
                    how='left',
                )
                .select('user_id')
                .where('tmp = 1')
            )

    @staticmethod
    def _user_split(all_df, group_weights):
        for i, df in enumerate(all_df.randomSplit(list(zip(*group_weights))[1])):
            df = df.withColumn('user_type', F.lit(group_weights[i][0]))
            yield i, df

    @timeit()
    def _with_score(self, user_df):
        columns = user_df.columns
        model = ClassifierModelFactory.create(self._user_pool.model)
        user_df = UserFeatures.with_user_feat(self, user_df, model.cfg.feat_version)
        user_df = model.transform(user_df, feature_col=UserFeatures.FEAT_COL)
        user_df = user_df.withColumn('score', F.element_at(user_df.probability, 2))

        return user_df.select('score', *columns)

    def _should_refresh(self):
        user_types = self.run_spark_sql(
            f"""
        SELECT DISTINCT user_type
        FROM {self._table_inserter.table_name}
        WHERE dag_id='{self.dag_id}' and dt='{self._date_helper.add_day(-1).str}'
        """
        ).collect()
        pre_user_types = len([r.user_type for r in user_types])
        cur_user_types = max([len(c.group_users) for c in self._operating_user.channel_cfgs])
        self.logger.info(f'previous user types: {pre_user_types} vs current: {cur_user_types}')

        push_day = self._date_helper.date_after_react_delay
        force_refresh = push_day.day in self._user_pool.user_type_refresh_days
        self.logger.info(f'push_day: {push_day}, force refresh user type: {force_refresh}')
        return pre_user_types != cur_user_types or force_refresh

    def _gen_pre_user_df(self):
        # 获取昨天的用户切分情况
        if self._should_refresh():
            # 如果和历史数据组数不一致，则取一个不存在的分区
            self.logger.warning(f'user type is not consistant with yesterday! init from scratch')
            dag_id = '__NOPE'
        else:
            dag_id = self.dag_id

        return self.run_spark_sql(
            f"""
        SELECT user_id,
               score,
               user_type
        FROM {self._table_inserter.table_name}
        WHERE dag_id='{dag_id}' and dt='{self._date_helper.add_day(-1).str}'
        """
        )

    def _calc_group_weights(self, user_num: int) -> list:
        group_users = [i.group_users for i in self._operating_user.channel_cfgs]

        # 有多个渠道信息，需要聚合成一个，用户数取最大值
        group_users_agg = defaultdict(lambda: GroupInfo(num=0))
        for group_user in group_users:
            for group in group_user:
                n = group_users_agg[group].num
                group_users_agg[group] = GroupInfo(
                    num=max(group_user[group].num, n), minimum=group_user[group].minimum
                )

        self.logger.info(f'group user agg: {group_users_agg}')
        group_weights = {}
        flatten_param = 1.1
        s_no_min = sum([v.num for v in group_users_agg.values() if not v.minimum])
        s_is_min = sum([flatten_param * v.num for v in group_users_agg.values() if v.minimum])
        for group, info in group_users_agg.items():
            if info.minimum:
                group_weights[group] = flatten_param * info.num
            else:
                group_weights[group] = 1.0 * max(
                    (info.num / s_no_min) * (user_num - s_is_min), info.num
                )
        self.logger.info(f'group user weights: {group_weights}')
        return list(group_weights.items())

    # 只有空白组和随机组不借助用户模型，分数随机给，但是要继承昨天的，
    # 否则排序取topk后每天都不一样
    _user_inherit_score = [UserType.CTL.value, UserType.NOOP.value]

    @timeit()
    def _with_user_type(self, cur_user: DataFrame, pre_user: DataFrame):
        """合并昨天和今天的用户，确保：
        1. 昨天是实验组的今天还是实验组，昨天是对照组的今天还是对照组
        2. 今天比昨天新增的人，按照比例切分随机组对照组

        # 如果 pre_user 为空，则 user_type 为空，全部重新分配
        # 如果 pre_user 不为空，则沿用昨天的 user_type，没匹配上的重新分配
        # 控制组还要继承分数，不可随机，保证下游挑选出的总人群每天都一致
        """
        user_num = cur_user.count()
        cur_user = (
            cur_user.alias('a')
            .join(
                pre_user.withColumnRenamed('score', 'b_score').alias('b'),
                pre_user.user_id == cur_user.user_id,
                how='left',
            )
            .select('a.user_id', 'b.user_type', 'a.score', 'b.b_score')
            .withColumn(
                'score',
                F.when(
                    F.col('user_type').isin(self._user_inherit_score), F.col('b_score')
                ).otherwise(F.col('score')),
            )
            .dropDuplicates(['user_id'])
            .select('user_id', 'user_type', 'score')
        )

        group_weights = self._calc_group_weights(user_num)
        groups = [cur_user.where('user_type is not NULL')]
        for i, df in self._user_split(cur_user.where('user_type is NULL'), group_weights):
            if group_weights[i][0] in self._user_inherit_score:
                df = df.withColumn('score', F.rand())
            groups.append(df)

        return reduce(DataFrame.unionAll, groups)

    @staticmethod
    def output_table_info_list(cfg: BaseConfig):
        return [
            {
                'name': cfg.output_table.user_pool,
                'comment': '用户池',
                'field_list': [
                    {'name': 'user_id', 'type': 'string', 'comment': '用户名'},
                    {'name': 'user_type', 'type': 'string', 'comment': '用户类型（实验组or对照组）'},
                    {'name': 'score', 'type': 'float', 'comment': '用户分数'},
                    {'name': 'batch_id', 'type': 'string', 'comment': '实际跑批时间'},
                ],
                'partition_field_list': [
                    {'name': 'dt', 'type': 'string', 'comment': '日期分区'},
                    {'name': 'dag_id', 'type': 'string', 'comment': '一个dag的唯一标识'},
                ],
            }
        ]


@JobRegistry.register(JobRegistry.T.INFER)
class CreOnlyUserPool(UserPool):
    @staticmethod
    def get_users(job: WithSpark, base: BaseConfig, batch_id) -> DataFrame:
        with prt_table_view(
            job, base.external_table.cre_crd_user, 'cre_crd_user', batch_id,
        ), prt_table_view(
            job, base.external_table.deb_crd_user, 'deb_crd_user', batch_id
        ), prt_table_view(
            job, base.external_table.cust_act_label, 'cust_act_label', batch_id, 'data_dt',
        ):
            return job.run_spark_sql(
                """
            SELECT DISTINCT a.cust_id AS user_id
            FROM
              (SELECT DISTINCT cust_id
               FROM cre_crd_user
               WHERE if_valid_hold_crd='1' ) AS a
            LEFT JOIN
              (SELECT DISTINCT cust_id
               FROM deb_crd_user
               WHERE if_hold_card='1' ) AS b ON a.cust_id = b.cust_id
            LEFT JOIN
              (SELECT cust_id,
                      1 AS pmbs_ind
               FROM cust_act_label
               WHERE pmbs_cust_ind='1'
               GROUP BY cust_id) AS c ON a.cust_id=c.cust_id
            WHERE b.cust_id IS NULL
              AND c.pmbs_ind=1
            """
            )


@JobRegistry.register(JobRegistry.T.INFER)
class CampCreOnlyUserPool(UserPool):
    @staticmethod
    def get_users(job: WithSpark, base: BaseConfig, batch_id) -> DataFrame:
        with prt_table_view(
            job, base.external_table.cre_crd_user, 'cre_crd_user', batch_id,
        ), prt_table_view(
            job, base.external_table.deb_crd_user, 'deb_crd_user', batch_id
        ), prt_table_view(
            job, base.external_table.user_stat_mt, 'user_stat_mt', batch_id
        ), prt_table_view(
            job, base.external_table.user_tel_number, 'user_tel_number', batch_id,
        ), prt_table_view(
            job, base.external_table.cust_act_label, 'cust_act_label', batch_id, 'data_dt',
        ):
            return job.run_spark_sql(
                """
            SELECT DISTINCT a.cust_id AS user_id
            FROM
              (SELECT DISTINCT cust_id
               FROM cre_crd_user
               WHERE if_valid_hold_crd='1') AS a
            LEFT JOIN
              (SELECT DISTINCT cust_id
               FROM deb_crd_user
               WHERE if_hold_card='1') AS b ON a.cust_id = b.cust_id
            LEFT JOIN
              (SELECT cust_id,
                      1 AS tmp
               FROM user_stat_mt
               WHERE camp_stat='1') AS e ON a.cust_id=e.cust_id
            LEFT JOIN
              ( SELECT cust_id,
                       IF(LENGTH(credit_crd_phone_no)=11, 1, 0) AS has_tel
               FROM user_tel_number) AS f ON a.cust_id = f.cust_id
            LEFT JOIN
              (SELECT cust_id,
                      1 AS pmbs_ind
               FROM cust_act_label
               WHERE pmbs_cust_ind='1'
               GROUP BY cust_id) AS c ON a.cust_id=c.cust_id
            WHERE b.cust_id IS NULL
              AND e.tmp=1
              AND f.has_tel = 1
              AND c.pmbs_ind=1
            """
            )


@JobRegistry.register(JobRegistry.T.INFER)
class DebOnlyUserPool(UserPool):
    @staticmethod
    def get_users(job: WithSpark, base: BaseConfig, batch_id) -> DataFrame:
        with prt_table_view(
            job, base.external_table.cre_crd_user, 'cre_crd_user', batch_id,
        ), prt_table_view(
            job, base.external_table.deb_crd_user, 'deb_crd_user', batch_id
        ), prt_table_view(
            job, base.external_table.cust_act_label, 'cust_act_label', batch_id, 'data_dt',
        ):
            return job.run_spark_sql(
                """
            SELECT DISTINCT a.cust_id AS user_id
            FROM
              (SELECT DISTINCT cust_id
               FROM deb_crd_user
               WHERE if_hold_card='1' ) AS a
            LEFT JOIN
              (SELECT DISTINCT cust_id
               FROM cre_crd_user
               WHERE if_valid_hold_crd='1' ) AS b ON a.cust_id = b.cust_id
            LEFT JOIN
              (SELECT cust_id,
                      1 AS pmbs_ind
               FROM cust_act_label
               WHERE pmbs_cust_ind='1'
               GROUP BY cust_id) AS c ON a.cust_id=c.cust_id
            WHERE b.cust_id IS NULL
              AND c.pmbs_ind=1
            """
            )
