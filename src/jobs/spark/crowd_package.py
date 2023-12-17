# -*- coding: utf-8 -*-
# @File : crowd_package.py
# @Author : r.yang
# @Date : Tue Mar  1 13:39:21 2022
# @Description : crowd package

import random
from typing import List

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Row

from algorithm.classifer import ClassifierModelFactory
from configs import Context
from configs.consts import PROBA_COL
from configs.model.base import BaseConfig, ItemInfo
from configs.model.config import ChannelOperateConfig, OperatingUserConfig, RankingConfig, UserType
from configs.utils import TagSelector
from core.job.registry import JobRegistry
from core.spark.table import TableInserter
from core.utils import DateHelper, df_view, timeit
from jobs.base import SparkRecoJob
from jobs.spark.feature import UserFeatures


@JobRegistry.register(JobRegistry.T.INFER)
class CrowdPackage(SparkRecoJob):

    _ranking: RankingConfig
    _operating_user: OperatingUserConfig

    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self._date_helper = DateHelper(self.batch_id, self._base.react_delay)
        self._table_inserter = TableInserter(self.output_table_info_list(self._base)[0])
        self.items: List[ItemInfo] = TagSelector(self._base.items).query(
            **self._scene.item_selector
        )

    @staticmethod
    def _rerank_fun(items: List[ItemInfo], channel_cfgs: List[ChannelOperateConfig]):
        # 取第0个为输出产品ID
        label_item = {item.label: item for item in items if item.label >= 0}
        item_ids_enable = {i.item_ids[0] for i in items if i.enable}
        channel_items = {(c.channel_id, c.banner_id): c.items_specified for c in channel_cfgs}

        def rerank(scores, channel_id, banner_id, user_type):
            item_spec = channel_items.get((channel_id, banner_id or ''), [])
            item_score_map = {}
            for i, p in enumerate(scores):
                if i in label_item:
                    for item_id in label_item[i].item_ids:
                        item_score_map[item_id] = p
            item_scores = [(label_item[i], p) for i, p in enumerate(scores) if i in label_item]
            if item_spec:
                item_scores = filter(lambda x: x[0].item_ids[0] in item_spec, item_scores)
            item_scores = sorted(item_scores, key=lambda x: x[1], reverse=True)
            item_ids = [i[0].item_ids[0] for i in item_scores]

            if not item_ids:
                item_ids = item_spec[:]

            # group random
            if user_type != UserType.EXP.value:
                random.shuffle(item_ids)

            # proportion
            item_with_prop = [item for item in items if item.prop > 0]
            if item_spec:
                item_with_prop = list(filter(lambda x: x.item_ids[0] in item_spec, item_with_prop))

            if not item_with_prop:
                # 无需按比例切分
                return [
                    Row(item_id=item_id, score=item_score_map.get(item_id, 0.0), rank=i,)
                    for i, item_id in enumerate(item_ids)
                ]

            props = [i.prop for i in item_with_prop]
            # XXX(ryang): 应付代码检查
            import numpy as np  # noqa

            top1_item: ItemInfo = eval(
                'np.random.choice(item_with_prop + [None], 1, p=props + [1 - sum(props)])'
            )[0]

            if top1_item is None:
                # 一定概率不切分
                return [
                    Row(item_id=item_id, score=item_score_map.get(item_id, 0.0), rank=i,)
                    for i, item_id in enumerate(item_ids)
                ]

            top1_item_id = top1_item.item_ids[0]
            if top1_item_id in item_ids:
                item_ids.pop(item_ids.index(top1_item_id))

            item_ids.insert(0, top1_item_id)
            item_ids = [i for i in item_ids if i in item_ids_enable]

            return [
                Row(item_id=item_id, score=item_score_map.get(item_id, 0.0), rank=i,)
                for i, item_id in enumerate(item_ids)
            ]

        return rerank

    def _rank(self):
        df = self.run_spark_sql(
            f"""
        SELECT user_id,
               user_type,
               channel_id,
               nvl(banner_id, "") as banner_id
        FROM {self._base.output_table.operating_user}
        WHERE tag='ready'
          AND dt='{self.batch_id}'
          AND dag_id='{self.dag_id}'
        """
        )

        force_rank_users = []
        for cust_id in self._ranking.force_rank_custs:
            for channel_cfg in self._operating_user.channel_cfgs:
                aux = {
                    'user_id': cust_id,
                    'user_type': 'exp',
                    'channel_id': channel_cfg.channel_id,
                    'banner_id': channel_cfg.banner_id or '',
                }
                force_rank_users.append(aux)

        df_test = self.spark.createDataFrame(force_rank_users)
        df = df.union(df_test.select('user_id', 'user_type', 'channel_id', 'banner_id'))

        model = ClassifierModelFactory.create(self._ranking.model)
        df = UserFeatures.with_user_feat(self, df, model.cfg.feat_version)
        df = model.transform(df, feature_col=UserFeatures.FEAT_COL, probability_col=PROBA_COL)
        return df

    def _rerank(self, df):
        rerank_udf = F.udf(
            self._rerank_fun(self.items, self._operating_user.channel_cfgs),
            returnType=T.ArrayType(
                T.StructType(
                    [
                        T.StructField('item_id', T.StringType()),
                        T.StructField('score', T.FloatType()),
                        T.StructField('rank', T.IntegerType()),
                    ]
                )
            ),
        )

        df = (
            df.withColumn(
                'item_score', rerank_udf(PROBA_COL, 'channel_id', 'banner_id', 'user_type'),
            )
            .withColumn('zip', F.explode('item_score'))
            .select(
                'user_id',
                'user_type',
                'channel_id',
                'banner_id',
                F.col('zip.item_id').alias('item_id'),
                F.col('zip.score').alias('score'),
                F.col('zip.rank').alias('rank'),
            )
            .where('item_id is not NULL')
            .dropDuplicates(['user_id', 'item_id', 'channel_id', 'banner_id'])
        )
        return df

    @timeit()
    def run(self):
        push_start_dt = self._date_helper.push_start_date().str
        last_batch_id = self._date_helper.add_day(-1).str

        channels = [
            self._base.get_channel(i.channel_id, i.banner_id)
            for i in self._operating_user.channel_cfgs
        ]
        no_repeat_channels = [
            f"'{c.channel_id}{c.banner_id}'" for c in channels if not c.allow_repeat
        ]
        no_repeat_sql = 'AND 1=0'  # 如果所有channel都可重复，则令b表为空
        if no_repeat_channels:
            no_repeat_sql = (
                f"AND CONCAT(channel_id, NVL(banner_id, '')) in ({', '.join(no_repeat_channels)})"
            )

        rank_df = self._rank()
        rerank_df = self._rerank(rank_df)
        with df_view(self, rerank_df, 'ranking_table'):
            # 剔除从投放日开始，已经投放过的用户-广告 pair（假设rank=1为实际投放的）
            df = self.run_spark_sql(
                f"""
            SELECT a.*
            FROM
              (SELECT *
               FROM ranking_table) AS a
            LEFT JOIN
              (SELECT user_id,
                      item_id,
                      channel_id,
                      banner_id,
                      1 AS tmp
               FROM {self._base.output_table.crowd_package}
               WHERE dag_id='{self.dag_id}'
                 AND dt BETWEEN '{push_start_dt}' AND '{last_batch_id}' {no_repeat_sql}
               GROUP BY user_id,
                        item_id,
                        channel_id,
                        banner_id) AS b ON a.user_id=b.user_id
            AND a.item_id=b.item_id
            AND a.channel_id=b.channel_id
            AND a.banner_id=b.banner_id
            WHERE b.tmp IS NULL

            """
            )
            channel_df = self.spark.createDataFrame([i.dict() for i in channels]).select(
                'channel_id', 'banner_id', 'num_items'
            )
            df = (
                df.alias('a')
                .join(
                    channel_df.alias('b'),
                    (df.channel_id == channel_df.channel_id)
                    & (df.banner_id == channel_df.banner_id),
                    how='left',
                )
                .filter(df.rank < F.col('num_items'))
                .withColumn('model_version', F.lit(self._ranking.model.model_version))
                .withColumn('scene_id', F.lit(self._scene.scene_id))
                .withColumn('batch_id', F.lit(self.batch_id))
            ).select('a.*', 'model_version', 'scene_id', 'batch_id')

            self._table_inserter.insert_df(df, dt=self.batch_id, dag_id=self.dag_id)

    def show(self):
        self.run_spark_sql(
            f"""
        SELECT channel_id,
               banner_id,
               user_type,
               count(*)
        FROM {self._table_inserter.table_name}
        WHERE dag_id='{self.dag_id}'
          AND dt='{self.batch_id}'
        GROUP BY channel_id,
                 banner_id,
                 user_type
        ORDER BY channel_id,
                 banner_id,
                 user_type
        """
        ).show()

    @staticmethod
    def output_table_info_list(cfg: BaseConfig):
        return [
            {
                'name': cfg.output_table.crowd_package,
                'comment': '人群包',
                'field_list': [
                    {'name': 'user_id', 'type': 'string', 'comment': '用户名'},
                    {'name': 'user_type', 'type': 'string', 'comment': '用户类型(exp/ctl)'},
                    {'name': 'item_id', 'type': 'string', 'comment': '产品编号'},
                    {'name': 'channel_id', 'type': 'string', 'comment': '渠道编号'},
                    {'name': 'banner_id', 'type': 'string', 'comment': '栏位编号'},
                    {'name': 'score', 'type': 'float', 'comment': '得分'},
                    {'name': 'rank', 'type': 'float', 'comment': '得分倒排序（从1开始）'},
                    {'name': 'model_version', 'type': 'string', 'comment': '模型版本'},
                    {'name': 'scene_id', 'type': 'string', 'comment': '场景ID(后处理使用)'},
                    {'name': 'batch_id', 'type': 'string', 'comment': '实际跑批时间'},
                ],
                'partition_field_list': [
                    {'name': 'dt', 'type': 'string', 'comment': '日期分区'},
                    {'name': 'dag_id', 'type': 'string', 'comment': '一个dag的唯一标识'},
                ],
            }
        ]
