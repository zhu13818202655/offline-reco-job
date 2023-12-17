# -*- coding: utf-8 -*-
# @File : daily_report.py
# @Author : r.yang
# @Date : Wed Jun  1 14:54:18 2022
# @Description : 日报作业


from datetime import datetime, timedelta
from functools import reduce

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.dataframe import DataFrame

from client.lowcode.v1 import LowCode
from client.modules import ReportAgg, ReportAggList
from configs import Context
from configs.init.common import C_SMS, CNAME_APP, CNAME_SMS, S_CREDIT, S_DEBIT
from configs.model.base import BaseConfig, ChannelInfo
from configs.model.config import (
    ChannelOperateConfig,
    CrowdFeedbackConfig,
    OperatingUserConfig,
    ReportConfig,
)
from core.job.registry import JobRegistry
from core.spark.table import TableInserter
from core.utils import DateHelper, Datetime, df_view, prt_table_view
from jobs.base import SparkPostProcJob, SparkRecoJob


@JobRegistry.register(JobRegistry.T.FEEDBACK)
class DailyReportSMS(SparkRecoJob):

    _operating_user: OperatingUserConfig
    _crowd_feedback: CrowdFeedbackConfig

    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self._date_helper = DateHelper(self.batch_id, self._base.react_delay)

    def run(self):
        self._user_agg_data()
        # self._user_act_data()

    def _user_act_data(self):
        table_inserter = TableInserter(self.output_table_info_list(self._base)[0])
        with prt_table_view(
            self, self._base.external_table.custom_data, 'custom_data', self.batch_id
        ), prt_table_view(
            self, self._base.external_table.s21_ecusr_h, 's21_ecusr_h', self.batch_id
        ), prt_table_view(
            self, self._base.external_table.s21_ecextcifno_h, 's21_ecextcifno_h', self.batch_id
        ):
            act_df_ext = self.run_spark_sql(
                f"""
            SELECT e.cust_id AS user_id,
                   e.card_type,
                   sum(CASE WHEN event_identifier='pweixin_postCardinfo_cfxq' THEN 1 ELSE 0 END) pweixin_postCardinfo_cfxq,
                   sum(CASE WHEN event_identifier='pweixin_postCardinfo_cfsq' THEN 1 ELSE 0 END) pweixin_postCardinfo_cfsq,
                   sum(CASE WHEN event_identifier='pweixin_postCardinfo_cfsx' THEN 1 ELSE 0 END) pweixin_postCardinfo_cfsx,
                   '{self.batch_id}' AS batch_id
            FROM
              (SELECT d.cifno AS cust_id,
                      a.card_type,
                      a.event_identifier
               FROM
                 ( SELECT useridentifier,
                          custom_data,
                          event_identifier,
                          regexp_extract(custom_data,'("card_type":")(.*?)(",")',2) card_type
                  FROM custom_data
                  WHERE event_identifier IN ('pweixin_postCardinfo_cfxq',
                                             'pweixin_postCardinfo_cfsq',
                                             'pweixin_postCardinfo_cfsx')
                    AND useridentifier IS NOT NULL
                    AND useridentifier<> '' ) a
               LEFT JOIN {self._base.external_table.s21_ecusrdevice} b ON a.useridentifier=b.deviceno
               LEFT JOIN s21_ecusr_h c ON b.userseq=c.userseq
               LEFT JOIN s21_ecextcifno_h d ON c.cifseq=d.cifseq
               WHERE b.deviceno <> ""
                 AND d.cifnotype='C') AS e
            GROUP BY e.cust_id,
                     e.card_type
            """
            )
            table_inserter.insert_df(act_df_ext, dt=self.batch_id)

    def _user_agg_data(self):
        # 必须从base配置里拿，因为配置可能被修改，不能用C_SMS
        channel = self._base.get_channel(C_SMS.channel_id, C_SMS.banner_id)
        channel_cfg = self._operating_user.get_channel_cfg(C_SMS.channel_id, C_SMS.banner_id)
        crowd_package_dt = self._date_helper.add_day(
            1 - (channel.feedback_delay + self._base.react_delay + channel.push_delay)
        )

        table_inserter = TableInserter(self.output_table_info_list(self._base)[1])
        # 统计近一年的人数，从 20220601 开始
        push_start = max(
            self._crowd_feedback.user_agg_start_dt,
            self._date_helper.add_day(-self._crowd_feedback.user_agg_days).str,
        )
        push_start_sub1 = (self._date_helper.push_start_date() - timedelta(days=1)).str

        groups_no_push_sql = ','.join([f"'{i}'" for i in channel_cfg.groups_no_push])
        df = self.run_spark_sql(
            f"""
        SELECT user_id,
               user_type,
               item_id as prod_id,
               from_unixtime(unix_timestamp(to_date(date_sub(from_unixtime(unix_timestamp(
                   dt,'yyyyMMdd'),'yyyy-MM-dd'),1)),'yyyy-MM-dd'),'yyyyMMdd') dt_sub1,
               '{self.batch_id}' AS batch_id
        FROM
          (SELECT user_id,
                  user_type,
                  item_id,
                  dt,
                  row_number() over (partition BY user_id
                             ORDER BY dt DESC) num
           FROM {self._base.output_table.crowd_feedback}
           WHERE ((dt BETWEEN '{push_start}' AND '{crowd_package_dt.str}'
                  AND send > 0
                  AND user_type NOT IN ({groups_no_push_sql}))
             OR (dt = '{crowd_package_dt.str}'
                 AND user_type IN ({groups_no_push_sql})))
            AND dag_id='{self.dag_id}'
            AND channel_id='{channel.channel_id}' ) t1
        WHERE t1.num=1
        """
        )
        df = df.withColumn(
            'dt_sub1',
            F.when(F.col('user_type').isin(channel_cfg.groups_no_push), push_start).otherwise(
                F.col('dt_sub1')
            ),
        )

        with df_view(self, df, 'user_agg_tmp'):
            df_ext = self.run_spark_sql(
                f"""
            SELECT a.*,
                   (CASE
                        WHEN b2.fenhang rlike '上海银行*' THEN regexp_extract(b2.fenhang,'(上海银行)(.*?)()',2)
                        WHEN b2.fenhang rlike '.*(分行级)' THEN regexp_extract(b2.fenhang,'()(.*?)(.分行级.)',2)
                        ELSE b2.fenhang
                    END) fenhang,
                   b1.tot_asset_n_zhixiao_month_avg_bal AS pre_tot_asset_n_zhixiao_month_avg_bal,
                   b2.tot_asset_n_zhixiao_month_avg_bal,
                   c1.aum_tm AS pre_aum_tm,
                   c2.aum_tm,
                   c1.dpst_tm AS pre_dpst_tm,
                   c2.dpst_tm
            FROM user_agg_tmp a
            LEFT JOIN
              (SELECT *
               FROM {self._base.external_table.user_feat_ls_crd}
               WHERE dt>='{push_start_sub1}') b1 ON a.user_id=b1.cust_id
            AND a.dt_sub1=b1.dt
            LEFT JOIN
              (SELECT *
               FROM {self._base.external_table.user_feat_ls_crd}
               WHERE dt='{self.batch_id}') b2 ON a.user_id=b2.cust_id
            LEFT JOIN
              (SELECT *
               FROM {self._base.external_table.rtl_asset_bal}
               WHERE dt>='{push_start_sub1}') c1 ON a.user_id=c1.cust_id
            AND a.dt_sub1=c1.dt
            LEFT JOIN
              (SELECT *
               FROM {self._base.external_table.rtl_asset_bal}
               WHERE dt='{self.batch_id}') c2 ON a.user_id=c2.cust_id
            """
            )
            table_inserter.insert_df(df_ext, dt=crowd_package_dt.str, dag_id=self.dag_id)

    @staticmethod
    def output_table_info_list(cfg: BaseConfig):
        return [
            {
                'name': cfg.output_table.weixin_feedback,
                'comment': '微信银行人群反馈（日更）',
                'field_list': [
                    {'name': 'user_id', 'type': 'string', 'comment': '用户名'},
                    {'name': 'card_type', 'type': 'string', 'comment': '产品编号'},
                    {'name': 'pweixin_postCardinfo_cfxq', 'type': 'int', 'comment': ''},
                    {'name': 'pweixin_postCardinfo_cfsq', 'type': 'int', 'comment': ''},
                    {'name': 'pweixin_postCardinfo_cfsx', 'type': 'int', 'comment': ''},
                    {'name': 'batch_id', 'type': 'string', 'comment': '实际跑批时间'},
                ],
                'partition_field_list': [{'name': 'dt', 'type': 'string', 'comment': '日期分区'},],
            },
            {
                'name': cfg.output_table.attr_feedback,
                'comment': '用户行为特征回收表',
                'field_list': [
                    {'name': 'user_id', 'type': 'string', 'comment': '用户名'},
                    {'name': 'user_type', 'type': 'string', 'comment': '用户类型(exp/ctl)'},
                    {'name': 'prod_id', 'type': 'string', 'comment': '用户类型(exp/ctl)'},
                    {'name': 'batch_id', 'type': 'string', 'comment': '实际跑批时间'},
                    {'name': 'dt_sub1', 'type': 'string', 'comment': '短信触达时间前一天'},
                    {'name': 'fenhang', 'type': 'string', 'comment': '分行'},
                    {
                        'name': 'pre_tot_asset_n_zhixiao_month_avg_bal',
                        'type': 'float',
                        'comment': '',
                    },
                    {'name': 'tot_asset_n_zhixiao_month_avg_bal', 'type': 'float', 'comment': ''},
                    {'name': 'pre_aum_tm', 'type': 'float', 'comment': ''},
                    {'name': 'aum_tm', 'type': 'float', 'comment': ''},
                    {'name': 'pre_dpst_tm', 'type': 'float', 'comment': ''},
                    {'name': 'dpst_tm', 'type': 'float', 'comment': ''},
                ],
                'partition_field_list': [
                    {'name': 'dt', 'type': 'string', 'comment': '日期分区'},
                    {'name': 'dag_id', 'type': 'string', 'comment': '一个dag的唯一标识'},
                ],
            },
        ]


@JobRegistry.register(JobRegistry.T.FEEDBACK)
class AggReportSync(SparkPostProcJob):
    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self._date_helper = DateHelper(self.batch_id, self._base.react_delay)
        self._lowcode = None

    def run(self):
        if not self._lowcode:
            self._lowcode = LowCode(
                base_url=self._base.lowcode.url.rstrip('/') + '/api/compose',
                auth_url=self._base.lowcode.url.rstrip('/') + '/auth/oauth2/token',
                secret=self._base.lowcode.secret,
                client_id=self._base.lowcode.client_id,
            ).with_ns_by_handle('bos')

        table_name = self.output_table_info_list(self._base)[0]['name']
        df = self.run_spark_sql(
            f"""
        select * from {table_name}
        where dt='{self.batch_id}' and channel_id='{self._config.channel_id}'
        """
        ).dropDuplicates(['channel_id', 'banner_id', 'user_type', 'prod_type', 'during'])
        data = df.collect()
        res = []
        dt = datetime.strptime(self.batch_id, '%Y%m%d').strftime('%Y-%m-%d')
        for row in data:
            send = row.send or 0
            click = row.click or 0
            convert = row.convert or 0
            res.append(
                ReportAgg(
                    channel_id=row.channel_id,
                    banner_id=row.banner_id or '',
                    scene_id=row.scene_id,
                    user_type=row.user_type,
                    prod_type=row.prod_type,
                    operate=row.operate,
                    send=send,
                    click=click,
                    click_ratio=int(click) / max(int(send), 1),
                    convert=convert,
                    convert_ratio=int(convert) / max(int(send), 1),
                    during=row.during,
                    dt=dt,
                )
            )
        reports = ReportAggList(res)
        reports.to_lowcode(self._lowcode, dt=dt, channel_id=self._config.channel_id)

    @staticmethod
    def output_table_info_list(cfg: BaseConfig):
        return [
            {
                'name': cfg.output_table.report_agg,
                'comment': '聚合报表',
                'field_list': [
                    {'name': 'channel_id', 'type': 'string', 'comment': '渠道ID'},
                    {'name': 'banner_id', 'type': 'string', 'comment': '栏位ID'},
                    {'name': 'user_type', 'type': 'string', 'comment': '用户组别'},
                    {'name': 'prod_type', 'type': 'string', 'comment': '产品组别'},
                    {'name': 'scene_id', 'type': 'string', 'comment': '场景ID'},
                    {'name': 'operate', 'type': 'int', 'comment': '营销人数'},
                    {'name': 'send', 'type': 'int', 'comment': '曝光人数'},
                    {'name': 'click', 'type': 'int', 'comment': '点击人数'},
                    {'name': 'convert', 'type': 'int', 'comment': '办卡人数'},
                    {'name': 'during', 'type': 'string', 'comment': '统计区间'},
                ],
                'partition_field_list': [
                    {'name': 'dag_id', 'type': 'string', 'comment': 'dag id'},
                    {'name': 'dt', 'type': 'string', 'comment': '日期分区'},
                ],
            },
        ]


@JobRegistry.register(JobRegistry.T.FEEDBACK)
class AggReport(SparkRecoJob):

    _operating_user: OperatingUserConfig
    _crowd_feedback: CrowdFeedbackConfig
    _report: ReportConfig

    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self._date_helper = DateHelper(self.batch_id, self._base.react_delay)
        self._table_inserter = TableInserter(self.output_table_info_list(self._base)[0])

    def run(self):
        for i, channel_cfg in enumerate(self._operating_user.channel_cfgs):
            channel = self._base.get_channel(channel_cfg.channel_id, channel_cfg.banner_id)
            crowd_package_dt = self._date_helper.add_day(
                1 - (channel.feedback_delay + self._base.react_delay + channel.push_delay)
            )
            self.logger.info(f'channel config: {channel} {channel_cfg}')
            self.logger.info(f'crowd package date: {crowd_package_dt}')

            if channel.channel_id == CNAME_APP and self._scene.scene_id == S_DEBIT.scene_id:
                report_df = self._deb_app_agg(channel_cfg, channel, crowd_package_dt)
            elif channel.channel_id == CNAME_APP and self._scene.scene_id == S_CREDIT.scene_id:
                report_df = self._cre_app_agg(channel_cfg, channel, crowd_package_dt)
            elif channel.channel_id == CNAME_SMS:
                report_df = self._sms_agg(channel_cfg, channel, crowd_package_dt)
            else:
                continue
            self._table_inserter.insert_df(
                report_df, overwrite=i == 0, dt=self.batch_id, dag_id=self.dag_id
            )

    @staticmethod
    def output_table_info_list(cfg: BaseConfig):
        return AggReportSync.output_table_info_list(cfg)

    def _sms_agg(
        self, channel_cfg: ChannelOperateConfig, channel_info: ChannelInfo, package_dt: Datetime
    ):
        # SMS 回收 batch_id=20220101，0102计算，0103给BI投放，0106跑回收（batch_id=0105, dt=0103)
        package_end_dt = package_dt

        group_map = {k: v.num for k, v in channel_cfg.group_users.items()}
        group_map_udf = F.udf(lambda x: group_map.get(x, 0), returnType=T.IntegerType())

        res = []
        for stat_period in self._report.stat_periods_in_day:
            package_start_dt = package_end_dt + timedelta(days=-stat_period)

            package_df = self.run_spark_sql(
                f"""
            SELECT dt
            FROM {self._base.output_table.sms_reco_results}
            WHERE dt >= '{package_start_dt.str}'
              AND do_push=1
            """
            )

            # 因为短信是间歇投放的，先找到实际投放的人群包日期
            package_dts = sorted([i.dt for i in package_df.select('dt').distinct().collect()])
            if not package_dts:
                package_dts.append(package_dt.str)
            package_dt_sql = ','.join([f"'{i}'" for i in package_dts])
            # FIXME(ryang): 在计算实际投放的日期分区，这里应该往后几天
            push_dt = package_dts[0]

            report_df = self.run_spark_sql(
                f"""
            SELECT a.user_id,
                   a.user_type,
                   nvl(a.maxsend, 0) AS send,
                   nvl(a.maxclick, 0) AS click,
                   nvl(b.apply_ind, 0) AS CONVERT,
                   '{channel_info.channel_id}' AS channel_id,
                   '{channel_info.banner_id}' AS banner_id,
                   '' AS prod_type,
                   '{self._scene.scene_id}' AS scene_id,
                   '{package_start_dt.str}-{package_end_dt.str}' AS during
            FROM
              (SELECT user_id,
                      user_type,
                      max(click) AS maxclick,
                      max(send) AS maxsend
               FROM {self._base.output_table.crowd_feedback}
               WHERE channel_id='{channel_info.channel_id}'
                 AND banner_id='{channel_info.banner_id}'
                 AND user_type not in ('ctl1', 'noop')
                 AND dt in ({package_dt_sql})
               GROUP BY user_id,
                        user_type) AS a
            LEFT JOIN
              (SELECT cust_id,
                      1 AS apply_ind
               FROM {self._base.external_table.deb_crd_user}
               WHERE dt='{self.batch_id}'
                 AND firsr_apply_dt>='{push_dt}'
               GROUP BY cust_id) AS b ON a.user_id=b.cust_id
            """
            )
            res.append(report_df.withColumn('operate', group_map_udf('user_type')))
        return reduce(DataFrame.unionAll, res)

    def _deb_app_agg(
        self, channel_cfg: ChannelOperateConfig, channel_info: ChannelInfo, package_dt: Datetime,
    ):
        # app 弹窗回收 batch_id=20220101，0102计算，0103给BI, 0104投放，0105跑回收(batch_id=0104)
        # 0105跑报表时:
        #     crowd_feedback/crowd_package 截止日期分区为 0101，开始日期为 0101 - period
        #     统计办卡表的截止日期应当为当前 batch 日期，即 self.batch_id
        #     统计办卡表的开始日期应当为当前 batch - period
        package_end_dt = package_dt
        group_map = {k: v.num for k, v in channel_cfg.group_users.items()}
        group_map_udf = F.udf(lambda x: group_map.get(x, 0), returnType=T.IntegerType())

        res = []

        for stat_period in self._report.stat_periods_in_day:
            pacakge_start_dt = package_end_dt + timedelta(days=-stat_period)
            push_start_dt = self._date_helper.add_day(-stat_period)

            report_df = self.run_spark_sql(
                f"""
                SELECT '{channel_info.channel_id}' AS channel_id,
                       '{channel_info.banner_id}' AS banner_id,
                       user_type,
                       '' AS prod_type,
                       '{self._scene.scene_id}' AS scene_id,
                       sum(send) AS send,
                       sum(click) AS click,
                       sum(card_num) AS CONVERT,
                       '{pacakge_start_dt.str}-{package_end_dt.str}' AS during
                FROM
                  (SELECT a.*,
                          nvl(c.card_num, 0) card_num
                   FROM
                     (SELECT user_id, user_type, max(send) as send,
                             max(click) AS click
                      FROM {self._base.output_table.crowd_feedback}
                      WHERE dt BETWEEN '{pacakge_start_dt.str}' AND '{package_end_dt.str}'
                      AND channel_id='{channel_info.channel_id}'
                      AND COALESCE(banner_id, '')='{channel_info.banner_id}'
                      AND dag_id='{self.dag_id}'
                      GROUP BY user_id, user_type) AS a
                   LEFT JOIN
                     (SELECT cust_id,
                             1 AS card_num
                      FROM {self._base.external_table.deb_crd_user}
                      WHERE dt='{self.batch_id}'
                        AND firsr_apply_dt>='{push_start_dt.str}'
                      GROUP BY cust_id) AS c ON a.user_id=c.cust_id) AS tmp2
                GROUP BY user_type
            """
            )

            res.append(report_df.withColumn('operate', group_map_udf('user_type')))
        return reduce(DataFrame.unionAll, res)

    def _cre_app_agg(
        self, channel_cfg: ChannelOperateConfig, channel_info: ChannelInfo, package_dt: Datetime,
    ):
        # app 弹窗回收 batch_id=20220101，0102计算，0103给BI, 0104投放，0105跑回收(batch_id=0104)
        # 0105跑报表时:
        #     crowd_feedback/crowd_package 截止日期分区为 0101，开始日期为 0101 - period
        #     统计办卡表的截止日期应当为当前 batch 日期，即 self.batch_id
        #     统计办卡表的开始日期应当为当前 batch - period
        package_end_dt = package_dt
        group_map = {k: v.num for k, v in channel_cfg.group_users.items()}
        group_map_udf = F.udf(lambda x: group_map.get(x, 0), returnType=T.IntegerType())

        res = []

        for stat_period in self._report.stat_periods_in_day:
            pacakge_start_dt = package_end_dt + timedelta(days=-stat_period)
            push_start_dt = self._date_helper.add_day(-stat_period)

            report_df = self.run_spark_sql(
                f"""
                SELECT '{channel_info.channel_id}' AS channel_id,
                       '{channel_info.banner_id}' AS banner_id,
                       user_type,
                       '' AS prod_type,
                       '{self._scene.scene_id}' AS scene_id,
                       sum(send) AS send,
                       sum(click) AS click,
                       sum(card_num) AS CONVERT,
                       '{pacakge_start_dt.str}-{package_end_dt.str}' AS during
                FROM
                  (SELECT a.*,
                          nvl(c.card_num, 0) card_num
                   FROM
                     (SELECT user_id, user_type, max(send) as send,
                             max(click) AS click
                      FROM {self._base.output_table.crowd_feedback}
                      WHERE dt BETWEEN '{pacakge_start_dt.str}' AND '{package_end_dt.str}'
                      AND channel_id='{channel_info.channel_id}'
                      AND COALESCE(banner_id, '')='{channel_info.banner_id}'
                      AND dag_id='{self.dag_id}'
                      GROUP BY user_id, user_type) AS a
                   LEFT JOIN
                     (SELECT cust_id,
                             1 AS card_num
                      FROM {self._base.external_table.cre_crd_user}
                      WHERE dt='{self.batch_id}'
                        AND last_apply_dt>='{push_start_dt.str}'
                      GROUP BY cust_id) AS c ON a.user_id=c.cust_id) AS tmp2
                GROUP BY user_type
            """
            )

            res.append(report_df.withColumn('operate', group_map_udf('user_type')))
        return reduce(DataFrame.unionAll, res)
