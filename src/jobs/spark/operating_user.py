# -*- coding: utf-8 -*-
# @File : operating_user.py
# @Author : r.yang
# @Date : Tue Mar  1 13:38:31 2022
# @Description : operating users
# @Design: gitlab:rec-system/bos/documents/03_design/jobs/operating_user.md

"""

,----------------.   ,--------------.
|UserPool输出名单|   |历史投放结果表|
|----------------|   |--------------|
`----------------'   `--------------'
          |                  |
,------------------.         |
|当前未投放客户名单|         |
|------------------|         |
`------------------'         |
          |                  |
 ,----------------.  ,-------------.
 |客户办卡意愿得分|  |当日投放人数k|
 |----------------|  |-------------|
 `----------------'  `-------------'
           \\               //
      ,------------------------.
      |取topk得到当日应投放名单|
      |------------------------|
      `------------------------'
                   |
                 ,---.
                 |end|
                 |---|
                 `---'
"""

from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from configs import Context
from configs.model.base import BaseConfig
from configs.model.config import OperatingUserConfig
from core.job.registry import JobRegistry
from core.spark.table import TableInserter
from core.utils import DateHelper, df_view, timeit
from jobs.base import SparkRecoJob


@JobRegistry.register(JobRegistry.T.INFER)
class OperatingUser(SparkRecoJob):

    _operating_user: OperatingUserConfig

    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self._date_helper = DateHelper(self.batch_id, self._base.react_delay)
        self._table_inserter = TableInserter(self.output_table_info_list(self._base)[0])

    @timeit()
    def run(self):
        channel_df = self.get_channel()
        user_num_df = self.get_max_user_num()
        all_user_df = self.get_all_user(channel_df, user_num_df)
        self._table_inserter.insert_df(
            all_user_df.withColumn('tag', F.lit('all')), dt=self.batch_id, dag_id=self.dag_id
        )

        filtered_user_df = self.filter_all_user(all_user_df)
        self._table_inserter.insert_df(
            filtered_user_df.withColumn('tag', F.lit('filtered')),
            overwrite=False,
            dt=self.batch_id,
            dag_id=self.dag_id,
        )

        finished_user_df = self.get_finished_user()
        self._table_inserter.insert_df(
            finished_user_df, overwrite=False, dt=self.batch_id, dag_id=self.dag_id
        )

        unfinished_user_df = self.get_unfinished_user(channel_df)
        self._table_inserter.insert_df(
            unfinished_user_df, overwrite=False, dt=self.batch_id, dag_id=self.dag_id
        )

    def filter_all_user(self, user_df: DataFrame) -> DataFrame:
        # 剔除掉最近 feedback_delay 内尝试投放的人，因为这部分人还没有回收，只能假设全部投放成功
        filter_dfs = []
        push_start_dt = self._date_helper.push_start_date().str
        for channel_cfg in self._operating_user.channel_cfgs:
            channel = self._base.get_channel(channel_cfg.channel_id, channel_cfg.banner_id)
            # 如果 max_push >= 30，意味每天都投（比如banner位），此时不做过滤
            if channel.max_push >= 30:
                continue

            # batch_id=T-2，T 日BI拿到数据, T+feedback_delay日凌晨结果入仓
            # batch_id=T+feedback_delay-1时，刚好能拿到 batch_id=T-2的反馈，拿不到batch_id=T-1的反馈
            # 当天跑完 对 T-2 的 feedback后，跑T+feedback_delay-1的operating_user
            # 所以 batch_id between T-1 and T+feedback_delay-1-1 的反馈都没有，只能假设全部投放成功
            # 也因此当天crowd_feedback任务必须在 operating_user 之前
            start_dt = max(
                push_start_dt,
                self._date_helper.add_day(-channel.feedback_delay - channel.push_delay - 1).str,
            )
            end_dt = self._date_helper.add_day(-1).str
            filter_dfs.append(
                self.run_spark_sql(
                    f"""
                    SELECT DISTINCT user_id,
                           channel_id,
                           NVL(banner_id, "") AS banner_id
                    FROM {self._base.output_table.crowd_package}
                    WHERE dag_id='{self.dag_id}'
                      AND dt BETWEEN '{start_dt}' AND '{end_dt}'
                      AND rank=1
                      AND channel_id='{channel_cfg.channel_id}'
                      AND COALESCE(banner_id, '') = '{channel_cfg.banner_id}'
                """
                )
            )
        if not filter_dfs:
            return user_df

        filter_df = reduce(DataFrame.unionAll, filter_dfs)
        return (
            user_df.join(
                filter_df.withColumn('tmp', F.lit('1')),
                on=['user_id', 'channel_id', 'banner_id'],
                how='left',
            )
            .where('tmp IS NULL')
            .drop('tmp')
        )

    @timeit()
    def get_channel(self):
        channel_data = []
        for channel_cfg in self._operating_user.channel_cfgs:
            channel = self._base.get_channel(channel_cfg.channel_id, channel_cfg.banner_id)
            if channel.max_push >= 30:
                plan_push = self._date_helper.gap(self._date_helper.push_start_date()).days + 1
                start_dt = self.batch_id
                end_dt = self.batch_id
                max_push = (
                    self._date_helper.push_end_date() - self._date_helper.push_start_date()
                ).days + 1
            else:
                plan_push, start_dt, end_dt = self._date_helper.range_info(channel.max_push)
                max_push = channel.max_push

            days_remain = 1 + abs(self._date_helper.gap(end_dt).days)
            channel_data.append(
                {
                    'channel_id': channel_cfg.channel_id,
                    'banner_id': channel_cfg.banner_id,
                    'plan_push': plan_push,  # 截止当前日期，本批次计划推送次数
                    'max_push': max_push,  # 当月最大推送次数
                    'start_dt': start_dt,  # 本批次开始时间
                    'end_dt': end_dt,  # 本批次结束时间
                    'days_remain': days_remain,
                    'feedback_delay': channel.feedback_delay,
                }
            )

        self.logger.info(f'channel_data: {channel_data}')
        channel_df = self.spark.createDataFrame(channel_data)
        channel_df.show()
        return channel_df

    def get_max_user_num(self):
        user_num_data = []
        for channel_cfg in self._operating_user.channel_cfgs:
            for group in channel_cfg.group_users:

                if group in channel_cfg.groups_no_push:
                    continue

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
        user_num_df.show()
        return user_num_df

    @timeit()
    def get_all_user(self, channel_df, user_num_df):
        # 起投日期(月初 - react_delay)
        push_start_dt = self._date_helper.push_start_date().str
        # 前一天的batch_id
        last_batch_id = self._date_helper.add_day(-1).str

        with df_view(self, channel_df, 'channel_cfg_table'), df_view(
            self, user_num_df, 'user_num_table'
        ):
            # 如果 max_push>=30，意味着每天都投，就不管历史投放情况了
            user_df = self.run_spark_sql(
                f"""
                SELECT a.user_id,
                       a.user_type,
                       b.channel_id,
                       b.banner_id,
                       b.max_push,
                       b.plan_push,
                       nvl(c.try_push, 0) AS try_push,
                       nvl(d.send_num, 0) AS actual_push,
                       a.score,
                       g.max_user,
                       "{self.batch_id}" AS batch_id,
                       ROW_NUMBER() OVER (PARTITION BY b.channel_id,
                                                       b.banner_id,
                                                       a.user_type
                                          ORDER BY a.score DESC) AS rank
                FROM
                  (SELECT DISTINCT user_id,
                                   user_type,
                                   score
                   FROM {self._base.output_table.user_pool}
                   WHERE dt='{self.batch_id}'
                     AND dag_id='{self.dag_id}' ) AS a
                CROSS JOIN
                  (SELECT channel_id,
                          nvl(banner_id, "") AS banner_id,
                          max_push,
                          plan_push
                   FROM channel_cfg_table) AS b
                LEFT JOIN
                  (SELECT channel_id,
                          nvl(banner_id, "") AS banner_id,
                          user_type,
                          max_user
                   FROM user_num_table) AS g ON a.user_type=g.user_type
                AND b.channel_id=g.channel_id
                AND b.banner_id=g.banner_id
                LEFT JOIN
                  (SELECT user_id,
                          channel_id,
                          nvl(banner_id, "") AS banner_id,
                          count(*) AS try_push
                   FROM {self._base.output_table.crowd_package}
                   WHERE dt BETWEEN '{push_start_dt}' AND '{last_batch_id}'
                     AND dag_id='{self.dag_id}'
                     AND rank=1
                   GROUP BY user_id,
                            channel_id,
                            banner_id) AS c ON a.user_id=c.user_id
                AND b.channel_id=c.channel_id
                AND b.banner_id=c.banner_id
                LEFT JOIN
                  (SELECT f.user_id,
                          f.channel_id,
                          f.banner_id,
                          sum(f.send) AS send_num
                   FROM
                     (SELECT user_id,
                             channel_id,
                             nvl(banner_id, "") AS banner_id,
                             dt,
                             if(sum(send)>0, 1, 0) AS send
                      FROM {self._base.output_table.crowd_feedback}
                      WHERE dag_id='{self.dag_id}'
                        AND dt BETWEEN '{push_start_dt}' AND '{last_batch_id}'
                      GROUP BY user_id,
                               channel_id,
                               banner_id,
                               dt) AS f
                   GROUP BY user_id,
                            channel_id,
                            banner_id) AS d ON a.user_id=d.user_id
                AND b.channel_id=d.channel_id
                AND b.banner_id=d.banner_id
            """
            )
        return user_df.where('rank <= max_user')  # pylint: disable=E1102

    @timeit()
    def get_finished_user(self) -> DataFrame:
        candidate_user_df = self.run_spark_sql(
            f"""
            SELECT *
               FROM {self._base.output_table.operating_user}
               WHERE dag_id='{self.dag_id}'
                 AND dt='{self.batch_id}'
                 AND tag='filtered'
                 AND plan_push<=actual_push
        """
        )
        return candidate_user_df.withColumn('tag', F.lit('done'))

    @timeit()
    def get_unfinished_user(self, channel_df: DataFrame) -> DataFrame:
        # 对照组用rand，实验组用模型分数
        candidate_user_df = self.run_spark_sql(
            f"""
            SELECT a.*,
                   b.score
            FROM
              (SELECT *
               FROM {self._base.output_table.operating_user}
               WHERE dag_id='{self.dag_id}'
                 AND dt='{self.batch_id}'
                 AND tag='filtered'
                 AND plan_push>actual_push ) AS a
            LEFT JOIN
              (SELECT user_id,
                      score
               FROM {self._base.output_table.user_pool}
               WHERE dt='{self.batch_id}'
                 AND dag_id='{self.dag_id}' ) AS b ON a.user_id=b.user_id
        """
        )

        with df_view(self, candidate_user_df, 'candidate_user_table'):
            num_df = self.run_spark_sql(
                """
            SELECT channel_id,
                   banner_id,
                   user_type,
                   count(*) AS num
            FROM candidate_user_table
            GROUP BY channel_id,
                     banner_id,
                     user_type
            """
            )
            # DONE(ryang): 考虑fail rate
            channel_df = channel_df.join(num_df, on=['channel_id', 'banner_id'], how='inner')
            channel_df = channel_df.withColumn(
                'operating_num',
                F.lit(1 / (1.0 - self._operating_user.fail_rate))
                * F.col('num')
                / F.col('days_remain'),
            )

            self.logger.info('channel_df: ')
            channel_df.show(100, False)

            with df_view(self, channel_df, 'operating_num_table'):
                current_user = self.run_spark_sql(
                    """
                SELECT a.user_id,
                       a.user_type,
                       a.channel_id,
                       a.banner_id,
                       a.max_push,
                       a.plan_push,
                       a.try_push,
                       a.actual_push,
                       a.batch_id,
                       if(a.rank<=b.operating_num, "ready", "later") AS tag
                FROM
                  (SELECT *,
                          ROW_NUMBER() OVER (PARTITION BY channel_id,
                                                          banner_id,
                                                          user_type
                                             ORDER BY try_push ASC, score DESC) AS rank
                   FROM candidate_user_table) AS a
                LEFT JOIN
                  (SELECT channel_id,
                          banner_id,
                          user_type,
                          cast(operating_num AS int) AS operating_num
                   FROM operating_num_table) AS b ON a.channel_id=b.channel_id
                AND a.banner_id=b.banner_id
                AND a.user_type = b.user_type
                    """.format(
                        operating_user=self._base.output_table.operating_user,
                        dag_id=self.dag_id,
                        batch_id=self.batch_id,
                    )
                )
        return current_user

    def show(self):
        self.run_spark_sql(
            f"""
        SELECT tag,
               user_type,
               channel_id,
               banner_id,
               count(*)
        FROM {self._table_inserter.table_name}
        WHERE dag_id='{self.dag_id}'
          AND dt='{self.batch_id}'
        GROUP BY channel_id,
                 banner_id,
                 tag,
                 user_type
        ORDER BY tag,
                 user_type,
                 channel_id,
                 banner_id
        """
        ).show(100, False)

    @staticmethod
    def output_table_info_list(cfg: BaseConfig):
        return [
            {
                'name': cfg.output_table.operating_user,
                'comment': '用户运营',
                'field_list': [
                    {'name': 'user_id', 'type': 'string', 'comment': '用户名'},
                    {'name': 'user_type', 'type': 'string', 'comment': '用户类型(exp/ctl)'},
                    {'name': 'channel_id', 'type': 'string', 'comment': ''},
                    {'name': 'banner_id', 'type': 'string', 'comment': ''},
                    {'name': 'max_push', 'type': 'int', 'comment': ''},
                    {'name': 'plan_push', 'type': 'int', 'comment': ''},
                    {'name': 'try_push', 'type': 'int', 'comment': ''},
                    {'name': 'actual_push', 'type': 'int', 'comment': ''},
                    {'name': 'tag', 'type': 'string', 'comment': ''},
                    {'name': 'batch_id', 'type': 'string', 'comment': '实际跑批时间'},
                ],
                'partition_field_list': [
                    {'name': 'dt', 'type': 'string', 'comment': '日期分区'},
                    {'name': 'dag_id', 'type': 'string', 'comment': '一个dag的唯一标识'},
                ],
            }
        ]
