# -*- coding: utf-8 -*-
# @File : collect_reco_results.py
# @Author : r.yang
# @Date : Fri Mar  4 17:49:51 2022
# @Description : 下载 hdfs 上的数据 + 更新用户数目

import os
import shutil

from client.offline_backend import OfflineBackendClient
from configs import Context
from configs.model.base import Activity
from configs.utils import Env
from core.job.registry import JobRegistry
from core.spark.table import HEADER_SUFFIX
from core.utils import DateHelper, run_cmd, run_cmd_rtn
from jobs.base import PostProcJob


@JobRegistry.register(JobRegistry.T.INFER)
class CollectRecoResults(PostProcJob):
    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self.offline_backend = OfflineBackendClient(self._base.offline_backend_address, 'reco')
        self._date_helper = DateHelper(self.batch_id, self._base.react_delay)

    def _update_user_count(self, actv: Activity, user_count: int):
        self.logger.info(f'user count is : {user_count}')
        if not user_count:
            return
        self.offline_backend.patch(
            'activity',
            {'actv_id': actv.actv_id, 'batch_id': self.batch_id, 'user_count': user_count,},
        )

    def _should_push(self):
        if not self._config.sms or Env.is_local:
            return True
        push_date = self._date_helper.date_after_react_delay
        return push_date.day in self._config.sms.push_days

    def run(self):
        rtns = []

        push_date = self._date_helper.date_after_react_delay.str
        for actv in self._config.actvs:

            file_prefix = f'{actv.actv_id}-{self._config.channel_id}-{actv.banner_id}-{{}}.txt'
            hdfs_path = os.path.join(
                self._base.hdfs_output_dir, self.batch_id, file_prefix.format(self.batch_id)
            )
            nas_file = file_prefix.format(push_date)
            nas_backup_path = os.path.join(self._base.nas_output_dir, 'backup', push_date, nas_file)

            # 确保输出目录存在，写入数据头
            os.makedirs(os.path.dirname(nas_backup_path), exist_ok=True)

            # backup first
            cat = 'cat' if Env.is_local else 'hadoop fs -cat'
            header_cmd = f"{cat} {hdfs_path}{HEADER_SUFFIX}/*.csv | head -n 1 | sed s@$'\001'@'\@\!\@'@g > {nas_backup_path}"
            rtns.append(run_cmd(header_cmd, self.logger))
            if self._should_push():
                self.logger.info(f'prods will be pushed at {push_date}')
                data_cmd = (
                    f"{cat} {hdfs_path}/*.csv | sed s@$'\001'@'\@\!\@'@g >> {nas_backup_path}"
                )
                rtns.append(run_cmd(data_cmd, self.logger))
                user_count = len(open(nas_backup_path).readlines()) - 1
            else:
                wc_cmd_res = run_cmd_rtn(f'{cat} {hdfs_path}/*.csv | wc -l', self.logger)
                try:
                    user_count = int(wc_cmd_res.strip())
                except:
                    user_count = 0
                self.logger.info(f'prods will NOT be pushed at {push_date}')

            # output then
            nas_output_path = os.path.join(self._base.nas_output_dir, 'output', push_date, nas_file)
            os.makedirs(os.path.dirname(nas_output_path), exist_ok=True)
            shutil.copy(nas_backup_path, nas_output_path)
            try:
                os.chmod(os.path.dirname(nas_output_path), 0o775)
            except:
                self.logger.warning(f'dir: {os.path.dirname(nas_output_path)} already exists')

            # 写 flg 文件标记已经传输完成
            with open(nas_output_path.replace('.txt', '.flg'), 'w') as f:
                size = os.path.getsize(nas_output_path)
                f.write(f'{nas_output_path.split("/")[-1]}\t{size}\t{user_count+1}')
            shutil.copy(
                nas_output_path.replace('.txt', '.flg'), nas_backup_path.replace('.txt', '.flg')
            )

            # update user count
            self._update_user_count(actv, user_count)

        if any(rtns):
            raise RuntimeError
