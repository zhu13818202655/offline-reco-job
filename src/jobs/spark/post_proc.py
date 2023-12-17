# -*- coding: utf-8 -*-
# @File : post_proc.py
# @Author : r.yang
# @Date : Fri Mar  4 14:35:28 2022
# @Description : post process of each channel

import json
import os
from typing import List

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

from configs import Context
from configs.model.base import Activity, BaseConfig, ItemInfo, UserInfo
from configs.model.config import Selector, SMSConfig
from configs.utils import Env, TagSelector
from core.job.registry import JobRegistry
from core.spark.table import TableInserter, dump_df_with_header
from core.utils import DateHelper, df_view, prt_table_view, timeit
from jobs.base import SparkPostProcJob


def user_item_match(
    all_users: List[UserInfo],
    all_items: List[ItemInfo],
    user_selector: Selector,
    item_selector: Selector,
) -> list:
    """根据 selector 配置生成用户产品 pair

    :param all_users: 用户列表
    :param all_items: 产品列表
    :param user_selector: 用户选择器
    :param item_selector: 产品选择器
    :returns:

    """

    users: List[UserInfo] = TagSelector(all_users).query(**user_selector)
    items: List[ItemInfo] = TagSelector(all_items).query(**item_selector)
    res = []
    for u in users:
        for i in items:
            item_dict = i.dict()
            item_dict['item_id'] = i.item_ids[0]
            res.append(dict(u.dict(), **item_dict))
    return res


@JobRegistry.register(JobRegistry.T.INFER)
class PostProcessSMS(SparkPostProcJob):

    _sms: SMSConfig

    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self._table_inserter = TableInserter(self.output_table_info_list(self._base)[0])
        self._date_helper = DateHelper(self.batch_id, self._base.react_delay)

    @timeit()
    def get_result_df(self, actv: Activity):

        if actv.test_user_item:
            user_item_df = self.spark.createDataFrame(
                user_item_match(
                    self._base.test_users,
                    self._base.items,
                    actv.test_user_item.user_selector,
                    actv.test_user_item.item_selector,
                )
            ).dropDuplicates(['user_id'])

            with df_view(self, user_item_df, 'user_item'):
                return self.run_spark_sql(
                    f"""
                SELECT a.*,
                       b.content,
                       '{self.batch_id}' AS batch_id
                FROM
                  ( SELECT user_id,
                           item_id,
                           phone_no AS tel_number
                   FROM user_item ) AS a
                LEFT JOIN
                  (SELECT item_id,
                          content
                   FROM content) AS b ON a.item_id = b.item_id
                WHERE coalesce(content, '') != ''
                """
                )
        else:
            with prt_table_view(
                self, self._base.external_table.user_tel_number, 'user_tel_number', self.batch_id,
            ):
                return self.run_spark_sql(
                    f"""
                    SELECT *
                    FROM
                      (SELECT a.user_id,
                              a.item_id,
                              b.tel_number,
                              c.content,
                              '{self.batch_id}' AS batch_id
                       FROM
                         (SELECT user_id,
                                 item_id
                          FROM {self._base.output_table.crowd_package}
                          WHERE dt = '{self.batch_id}'
                            AND channel_id = '{self._config.channel_id}'
                            AND scene_id = '{actv.scene_id}'
                            AND coalesce(banner_id, '') = '{actv.banner_id}'
                            AND cast(rank as int)=0) AS a
                       LEFT JOIN
                         (SELECT *, {actv.phone_no_field} AS tel_number
                          FROM user_tel_number) AS b ON a.user_id = b.cust_id
                       LEFT JOIN
                         (SELECT item_id,
                                 content
                          FROM content) AS c ON a.item_id = c.item_id)
                    WHERE tel_number IS NOT NULL
                      AND tel_number != ''
                      AND LENGTH(tel_number)=11
                      AND coalesce(content, '') != ''
                """
                )

    _OUTPUT_COLS = ['user_id', 'tel_number', 'content']

    def _should_push(self):
        if not self._config.sms or Env.is_local:
            return True
        push_date = self._date_helper.date_after_react_delay
        return push_date.day in self._sms.push_days

    @timeit()
    def run(self):
        content_df = [
            {'item_id': item_id, 'content': item.sms_content}
            for item in self._base.items
            for item_id in item.item_ids
        ]
        with df_view(self, self.spark.createDataFrame(content_df), 'content'):
            for i, actv in enumerate(self._config.actvs):

                reco_res_df = (
                    self.get_result_df(actv)
                    .withColumn('actv_id', F.lit(actv.actv_id))
                    .dropDuplicates(['user_id'])
                    .withColumn('do_push', F.lit(int(self._should_push())))
                )
                self._table_inserter.insert_df(reco_res_df, overwrite=i == 0, dt=self.batch_id)

                file_name = (
                    f'{actv.actv_id}-{self._config.channel_id}-{actv.banner_id}-{self.batch_id}.txt'
                )
                csv_path = os.path.join(self._base.hdfs_output_dir, self.batch_id, file_name)
                dump_df_with_header(self.spark, reco_res_df.select(self._OUTPUT_COLS), csv_path)
                self.logger.info('sms reco results are dumped to {}'.format(csv_path))

    def show(self):
        self.run_spark_sql(
            f"""
        SELECT count(*)
        FROM {self._table_inserter.table_name}
        WHERE dt='{self.batch_id}'
        """
        ).show()

        self.run_spark_sql(
            f"""
        SELECT *
        FROM {self._table_inserter.table_name}
        WHERE dt='{self.batch_id}'
        """
        ).show(20, False)

    @staticmethod
    def output_table_info_list(cfg: BaseConfig):
        return [
            {
                'name': cfg.output_table.sms_reco_results,
                'comment': '短信后处理结果',
                'field_list': [
                    {'name': 'user_id', 'type': 'string', 'comment': '用户名'},
                    {'name': 'item_id', 'type': 'string', 'comment': '产品号'},
                    {'name': 'tel_number', 'type': 'string', 'comment': '用户手机号'},
                    {'name': 'content', 'type': 'string', 'comment': '用户短信内容'},
                    {'name': 'actv_id', 'type': 'string', 'comment': '活动号'},
                    {'name': 'do_push', 'type': 'int', 'comment': '当前分区是否推送,1-推 0-不推'},
                    {'name': 'batch_id', 'type': 'string', 'comment': '实际跑批时间'},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            }
        ]


@JobRegistry.register(JobRegistry.T.INFER)
class PostProcessWhitelist(SparkPostProcJob):
    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self._table_inserter = TableInserter(self.output_table_info_list(self._base)[0])

    @timeit()
    def run(self):
        for i, actv in enumerate(self._config.actvs):
            if actv.test_user_item:
                df = (
                    self.spark.createDataFrame(
                        [
                            i
                            for i in user_item_match(
                                self._base.test_users,
                                self._base.items,
                                actv.test_user_item.user_selector,
                                actv.test_user_item.item_selector,
                            )
                        ]
                    )
                    .dropDuplicates(['user_id'])
                    .withColumn('scene_id', F.lit('test'))
                    .withColumn('model_version', F.lit(''))
                )

            else:
                df = self.run_spark_sql(
                    f"""
                SELECT user_id,
                       item_id,
                       model_version,
                       scene_id
                FROM {self._base.output_table.crowd_package}
                WHERE dt = '{self.batch_id}'
                  AND channel_id = '{self._config.channel_id}'
                  AND coalesce(banner_id, '') = '{actv.banner_id}'
                  AND scene_id='{actv.scene_id}'
                  AND cast(rank as int)=0
                    """
                )

            self._table_inserter.insert_df(df, overwrite=i == 0, dt=self.batch_id)

    @staticmethod
    def output_table_info_list(cfg: BaseConfig):
        return [
            {
                'name': cfg.output_table.whitelist_results,
                'comment': '白名单后处理结果',
                'field_list': [
                    {'name': 'user_id', 'type': 'string', 'comment': '用户名'},
                    {'name': 'item_id', 'type': 'string', 'comment': '产品名'},
                    {'name': 'model_version', 'type': 'string', 'comment': '模型版本'},
                    {'name': 'scene_id', 'type': 'string', 'comment': '场景id'},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            }
        ]


@JobRegistry.register(JobRegistry.T.INFER)
class PostProcessAPP(SparkPostProcJob):
    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self._table_inserter = TableInserter(self.output_table_info_list(self._base)[0])

    @timeit()
    def get_result_df(self, actv: Activity):
        prod_type = self._base.get_scene(actv.scene_id).prod_type
        channel = self._base.get_channel(self._config.channel_id, actv.banner_id)
        if actv.test_user_item:
            df = (
                self.spark.createDataFrame(
                    user_item_match(
                        self._base.test_users,
                        self._base.items,
                        actv.test_user_item.user_selector,
                        actv.test_user_item.item_selector,
                    )
                )
                .withColumn('prod_score', F.rand())
                .withColumn(
                    'rank',
                    F.row_number().over(
                        Window.partitionBy(['user_id']).orderBy(F.desc('prod_score'))
                    ),
                )
                .where(f'rank <= {channel.num_items}')
                .withColumn('prod_id', F.col('item_id'))
                .withColumn('prod_type', F.lit(prod_type))
                .withColumn('model_version', F.lit(''))
                .withColumn('tel_number', F.col('phone_no'))
            )

        else:
            with prt_table_view(
                self, self._base.external_table.user_tel_number, 'user_tel_number', self.batch_id
            ):
                df = self.run_spark_sql(
                    f"""
                    SELECT a.*, b.tel_number
                    FROM
                      (SELECT user_id,
                              item_id AS prod_id,
                              '{prod_type}' AS prod_type,
                              score AS prod_score,
                              rank,
                              model_version
                       FROM {self._base.output_table.crowd_package}
                       WHERE dt = '{self.batch_id}'
                         AND channel_id = '{self._config.channel_id}'
                         AND coalesce(banner_id, '') = '{actv.banner_id}'
                         AND scene_id = '{actv.scene_id}' ) AS a
                    LEFT JOIN
                      (SELECT *, {actv.phone_no_field} as tel_number
                       FROM user_tel_number) AS b ON a.user_id = b.cust_id
                """
                )

        return (
            self._agg_prod(df, actv.banner_id)
            .withColumn('scene_id', F.lit(actv.scene_id))
            .withColumn('actv_id', F.lit(actv.actv_id))
            .withColumn('banner_id', F.lit(actv.banner_id))
        )

    def _agg_prod(self, df, banner_id: str):
        @F.pandas_udf(T.StringType(), functionType=F.PandasUDFType.GROUPED_AGG)
        def merge_prod(prod_ids, prod_types, rank):
            tuples = list(zip(prod_ids, prod_types, rank))
            tuples = sorted(tuples, key=lambda x: x[2])

            # ADHOC(ryang): 策略中心要求 slot 给 0
            return json.dumps(
                [
                    {
                        'positionId': banner_id,
                        'slots': [
                            {
                                'index': '0',
                                'prdList': [{'code': i[0], 'type': i[1]} for i in tuples],
                            }
                        ],
                    }
                ]
            )

        df_agg = df.groupBy('user_id', 'model_version').agg(
            merge_prod('prod_id', 'prod_type', 'rank').alias('prod_info')
        )
        return df_agg

    _OUTPUT_COLS = [
        'user_id',
        'prod_info',
    ]

    @timeit()
    def run(self):

        for i, actv in enumerate(self._config.actvs):

            reco_res_df = self.get_result_df(actv).withColumn('actv_id', F.lit(actv.actv_id))
            self._table_inserter.insert_df(reco_res_df, overwrite=i == 0, dt=self.batch_id)

            file_name = (
                f'{actv.actv_id}-{self._config.channel_id}-{actv.banner_id}-{self.batch_id}.txt'
            )
            csv_path = os.path.join(self._base.hdfs_output_dir, self.batch_id, file_name)

            dump_df_with_header(self.spark, reco_res_df.select(self._OUTPUT_COLS), csv_path)
            self.logger.info('app reco results are dumped to {}'.format(csv_path))

    @staticmethod
    def output_table_info_list(cfg: BaseConfig):
        return [
            {
                'name': cfg.output_table.app_reco_results,
                'comment': '手机银行后处理结果',
                'field_list': [
                    {'name': 'user_id', 'type': 'string', 'comment': '用户名'},
                    {'name': 'banner_id', 'type': 'string', 'comment': '栏位ID'},
                    {'name': 'prod_info', 'type': 'string', 'comment': '产品信息'},
                    {'name': 'actv_id', 'type': 'string', 'comment': '活动号'},
                    {'name': 'model_version', 'type': 'string', 'comment': '模型版本'},
                    {'name': 'scene_id', 'type': 'string', 'comment': '场景id'},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            }
        ]
