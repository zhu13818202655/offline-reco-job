# -*- coding: utf-8 -*-
# @File : crowd_feedback.py
# @Author : r.yang
# @Date : Tue Mar  1 16:11:29 2022
# @Description : crowd feedback


from functools import reduce
from configs.utils import Env

from pyspark.sql.dataframe import DataFrame

from configs import Context
from configs.init.common import C_SMS, CNAME_APP, S_DEBIT
from configs.model.base import BaseConfig, ChannelInfo
from configs.model.config import ChannelOperateConfig, CrowdFeedbackConfig, OperatingUserConfig
from core.job.registry import JobRegistry
from core.spark.table import TableInserter
from core.utils import DateHelper, df_view, timeit
from jobs.base import SparkRecoJob


@JobRegistry.register(JobRegistry.T.FEEDBACK)
class CrowdFeedback(SparkRecoJob):

    _crowd_feedback: CrowdFeedbackConfig
    _operating_user: OperatingUserConfig

    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self._date_helper = DateHelper(self.batch_id, self._base.react_delay)
        self._table_inserter = TableInserter(self.output_table_info_list(self._base)[0])

    def _sms_noop_user_feedback(self, channel_cfg: ChannelOperateConfig, crowd_package_dt):

        user_num_data = []
        for channel_cfg in self._operating_user.channel_cfgs:
            if channel_cfg.channel_id != C_SMS.channel_id:
                continue
            for group in channel_cfg.group_users:
                if group in channel_cfg.groups_no_push:
                    user_num_data.append(
                        {
                            'channel_id': channel_cfg.channel_id,
                            'banner_id': channel_cfg.banner_id,
                            'user_type': group,
                            'max_user': channel_cfg.group_users[group].num,
                        }
                    )
        self.logger.info(f'user num data: {user_num_data}')
        user_num_df = self.spark.createDataFrame(user_num_data)

        with df_view(self, user_num_df, 'user_num'):
            noop_df = self.run_spark_sql(
                f"""
            SELECT a.user_id,
                   a.user_type,
                   'CCGM' AS item_id,
                   '{C_SMS.channel_id}' AS channel_id,
                   NULL AS click,
                   NULL AS CONVERT,
                   '{self.batch_id}' AS batch_id,
                   '{C_SMS.banner_id}' AS banner_id,
                   a.score,
                   0 AS send
            FROM
              ( SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY user_type
                                          ORDER BY score DESC) AS rank
               FROM {self._base.output_table.user_pool}
               WHERE dt='{crowd_package_dt.str}'
                 AND dag_id='{self.dag_id}') AS a
            INNER JOIN user_num AS b ON a.user_type = b.user_type
            WHERE a.rank <= b.max_user
            """
            )

            return noop_df.drop('score')

    @timeit()
    def _sms_send_user_feedback(self, channel_cfg: ChannelInfo, crowd_package_dt):
        # SMS 回收 batch_id=20220101，0102计算，0103给BI投放，0106跑回收（batch_id=0105, dt=0103)
        sms_feedback_dt = self._date_helper.add_day(1 - channel_cfg.feedback_delay)

        sms_contents = [i.sms_content for i in self._base.items if i.sms_content]
        sms_condition = ''
        if sms_contents:
            sms_condition = 'AND msgcntnt IN (' + ', '.join([f"'{s}'" for s in sms_contents]) + ')'

        # ADHOC(ryang): 新增智能策略标识，目前暂时用正文匹配
        feedback_df = self.run_spark_sql(
            f"""
            SELECT a.user_id,
                   a.user_type,
                   a.item_id,
                   a.channel_id,
                   NULL AS click,
                   NULL AS CONVERT,
                   '{self.batch_id}' AS batch_id,
                   COALESCE(a.banner_id, '') AS banner_id,
                   IF(c.send>0, 1, {self._crowd_feedback.sms_default_send_num}) as send
            FROM
              (SELECT user_id,
                      user_type,
                      item_id,
                      channel_id,
                      banner_id
               FROM {self._base.output_table.crowd_package}
               WHERE dt = '{crowd_package_dt.str}'
                 AND dag_id = '{self.dag_id}'
                 AND channel_id = '{channel_cfg.channel_id}'
                 AND COALESCE(banner_id, '') = '{channel_cfg.banner_id}') AS a
            LEFT JOIN
              (SELECT tel_number,
                      user_id
               FROM {self._base.output_table.sms_reco_results}
               WHERE dt = '{crowd_package_dt.str}') AS b ON a.user_id = b.user_id
            LEFT JOIN
              (SELECT mblno,
                      SUM(IF(failreason = 'DELIVRD'
                             AND msgst = '0', 1, 0)) AS send
               FROM {self._base.external_table.sms_feedback}
               WHERE dt = '{sms_feedback_dt.str}'
                 AND mblno IS NOT NULL {sms_condition}
               GROUP BY mblno) AS c ON c.mblno = b.tel_number
            """
        )
        return feedback_df

    def _get_activity_code(self, push_dt: str, actv_id: str) -> str:
        # -- Step1:
        # ---- 获取营销当天借记卡手机银行活动的活动号，在后续埋点的统计中，需要对活动号进行限制
        # ---- 得到debit_activity_code变量，在下方本文件的52行进行使用（dt_delivery=20221220时，为20221108170334393606）

        # 由于测试环境 activity 表损坏，暂时关掉活动号匹配
        if not self.spark._jsparkSession.catalog().tableExists(
            *self._base.external_table.epm_activity.split('.', 1)
        ) or not Env.is_prod:
            return 'NOPE'
        df = self.run_spark_sql(
            f"""
            SELECT max(activity_code) as max_activity_code
            FROM {self._base.external_table.epm_activity}
            WHERE partitionbid = {push_dt}
            AND activity_name like "%{actv_id}%"
            AND from_unixtime(unix_timestamp(activity_start_date,'yyyy-MM-dd'),'yyyyMMdd') <= {push_dt}
            AND from_unixtime(unix_timestamp(activity_end_date,'yyyy-MM-dd'),'yyyyMMdd') >= {push_dt}
            """
        )
        res = df.first()['max_activity_code']
        return res or 'NOPE'

    @timeit()
    def _app_popup_feedback(self, channel_cfg: ChannelInfo, crowd_package_dt):
        # app 弹窗回收 batch_id=20220101，0102计算，0103给BI, 0104投放，0105跑回收(batch_id=0104)
        # 0105跑回收时，外部数据的分区还是 0104(app_feedback_dt)，对应 crowd_package 的分区是 0101
        app_feedback_dt = self._date_helper.add_day(1 - channel_cfg.feedback_delay)
        if self._scene.scene_id == S_DEBIT.scene_id:
            activity_code = self._get_activity_code(app_feedback_dt.str, 'S2011')
        else:
            activity_code = self._get_activity_code(app_feedback_dt.str, 'S2021')

        feedback_df = self.run_spark_sql(
            f"""
            SELECT crowd.user_id,
                   crowd.user_type,
                   crowd.item_id,
                   crowd.channel_id,
                   nvl(aa.click, 0) AS click,
                   nvl(aa.expose, 0) AS send,
                   NULL AS CONVERT,
                   '{self.batch_id}' AS batch_id,
                   COALESCE(crowd.banner_id, '') AS banner_id
            FROM
              (SELECT user_id,
                      user_type,
                      item_id,
                      channel_id,
                      banner_id
               FROM {self._base.output_table.crowd_package}
               WHERE dt = '{crowd_package_dt.str}'
                 AND dag_id = '{self.dag_id}'
                 AND channel_id = '{channel_cfg.channel_id}'
                 AND COALESCE(banner_id, '') = '{channel_cfg.banner_id}') AS crowd
            LEFT JOIN
              (SELECT a.cust_id,
                      c.content AS item_id,
                      1 AS expose,
                      max(if(b.adviceid is null, 0, 1)) AS click
               FROM
                 (SELECT cast(advice_id AS string) AS advice_id,
                         customer_id AS cust_id,
                         strategy_id,
                         resource_id
                  FROM {self._base.external_table.s70_exposure}
                  WHERE data_dt = '{app_feedback_dt.str}'
                    AND strategy_category = 'engine'
                    AND split(strategy_id, '--')[1] = '{activity_code}' ) AS a
               LEFT JOIN
                 (SELECT DISTINCT adviceid
                  FROM {self._base.external_table.s38_behavior}
                  WHERE dt = '{app_feedback_dt.str}'
                    AND seed = 'pmbs_na_bbsy_dcgg_2_tp_na' ) AS b ON a.advice_id=b.adviceid
               LEFT JOIN
                 (SELECT resource_id,
                         content
                  FROM {self._base.external_table.s70_t2_resource}
                  WHERE dt = '{app_feedback_dt.str}'
                    AND material_key = 'prdCardCode' ) AS c ON a.resource_id = c.resource_id
               GROUP BY a.cust_id,
                        c.content) AS aa ON crowd.user_id=aa.cust_id
            AND crowd.item_id=aa.item_id
            """
        )
        return feedback_df

    @timeit()
    def run(self):
        for i, channel_cfg in enumerate(self._operating_user.channel_cfgs):
            # batch_id=T-2，T BI拿到数据, T+feedback_delay日凌晨结果入仓
            # batch_id=T+feedback_delay-1 跑 crowd_feedback，结果写到batch_id=T-2中
            channel = self._base.get_channel(channel_cfg.channel_id, channel_cfg.banner_id)
            crowd_package_dt = self._date_helper.add_day(
                1 - (channel.feedback_delay + self._base.react_delay + channel.push_delay)
            )
            self.logger.info(f'channel config: {channel} {channel_cfg}')
            self.logger.info(f'crowd package date: {crowd_package_dt}')

            if channel.channel_id == C_SMS.channel_id:
                feedback_df = self._sms_send_user_feedback(channel, crowd_package_dt)
                noop_feedback_df = self._sms_noop_user_feedback(channel_cfg, crowd_package_dt)
                feedback_df = reduce(DataFrame.unionAll, [feedback_df, noop_feedback_df])
            elif channel.channel_id == CNAME_APP:
                feedback_df = self._app_popup_feedback(channel, crowd_package_dt)
            else:
                self.logger.warning(f'unknown channel_cfg: {channel_cfg} to feedback')
                continue

            self._table_inserter.insert_df(
                feedback_df, overwrite=i == 0, dt=crowd_package_dt.str, dag_id=self.dag_id
            )

    def show(self):
        for _, channel_cfg in enumerate(self._operating_user.channel_cfgs):
            channel = self._base.get_channel(channel_cfg.channel_id, channel_cfg.banner_id)
            crowd_package_dt = self._date_helper.add_day(
                1 - (channel.feedback_delay + self._base.react_delay + channel.push_delay)
            )
            self.run_spark_sql(
                f"""
            SELECT channel_id,
                   banner_id,
                   user_type,
                   count(*)
            FROM {self._table_inserter.table_name}
            WHERE dag_id='{self.dag_id}'
              AND dt='{crowd_package_dt.str}'
              AND channel_id = '{channel_cfg.channel_id}'
              AND COALESCE(banner_id, '') = '{channel_cfg.banner_id}'
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
                'name': cfg.output_table.crowd_feedback,
                'comment': '人群反馈',
                'field_list': [
                    {'name': 'user_id', 'type': 'string', 'comment': '用户名'},
                    {'name': 'user_type', 'type': 'string', 'comment': '用户类型(exp/ctl)'},
                    {'name': 'item_id', 'type': 'string', 'comment': '产品编号'},
                    {'name': 'channel_id', 'type': 'string', 'comment': '渠道编号'},
                    {'name': 'banner_id', 'type': 'string', 'comment': '栏位编号'},
                    {'name': 'send', 'type': 'int', 'comment': '发送'},
                    {'name': 'click', 'type': 'int', 'comment': '点击'},
                    {'name': 'convert', 'type': 'string', 'comment': '转化指标(json)'},
                    {'name': 'batch_id', 'type': 'string', 'comment': '实际跑批时间'},
                ],
                'partition_field_list': [
                    {'name': 'dt', 'type': 'string', 'comment': '日期分区'},
                    {'name': 'dag_id', 'type': 'string', 'comment': '一个dag的唯一标识'},
                ],
            }
        ]
