# -*- coding: utf-8 -*-
# @File : clean_reco_results.py
# @Author : r.yang
# @Date : Mon Mar  7 10:00:49 2022
# @Description : clean reco results before X days

import glob
import os
import shutil
from datetime import datetime

from dateutil.parser import ParserError, parse

from client.offline_backend import OfflineBackendClient
from configs import Context
from configs.utils import Env
from core.job.registry import JobRegistry
from core.utils import run_cmd
from jobs.base import BaseJob


@JobRegistry.register(JobRegistry.T.BASE)
class Cleanup(BaseJob):
    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self.offline_backend = OfflineBackendClient(self._base.offline_backend_address, 'reco')

    def _list_batch_ids(self, date_dir):
        cur_dt = datetime.strptime(self.batch_id, '%Y%m%d')
        if not os.path.exists(date_dir):
            return
        batch_ids = os.listdir(date_dir)
        self.logger.info(f'batch id date in {date_dir} is {batch_ids}')

        for batch_id in batch_ids:
            try:
                dt = datetime.strptime(batch_id, '%Y%m%d')
            except ValueError:
                continue

            if (cur_dt - dt).days > self._base.data_keep_day:
                yield dt.strftime('%Y%m%d')

    def _rm_dir(self, dir_name, hdfs=False):
        if hdfs and not Env.is_local:
            cmd = 'hadoop fs -rm -f -r {}'.format(dir_name)
            run_cmd(cmd, self.logger)
        else:
            shutil.rmtree(dir_name, ignore_errors=True)
            self.logger.info(f'dir: {dir_name} removed')

    def _clean_reco_results(self):
        """清理 NAS 上与下游 bi 交互的文件
        """
        nas_backup_dir = os.path.join(self._base.nas_output_dir, 'backup')

        for batch_id in self._list_batch_ids(nas_backup_dir):
            self.logger.info(f'cleaning reco results of batch_id: {batch_id}')
            hdfs_path = os.path.join(self._base.hdfs_output_dir, batch_id)

            self._rm_dir(hdfs_path, hdfs=True)
            self._rm_dir(os.path.join(nas_backup_dir, batch_id))

        nas_output_dir = os.path.join(self._base.nas_output_dir, 'output')
        for batch_id in self._list_batch_ids(nas_output_dir):
            self.logger.info(f'cleaning reco results of batch_id: {batch_id}')
            self._rm_dir(os.path.join(nas_output_dir, batch_id))

    def _clean_mysql(self):
        batch_id_list = self.offline_backend.get('/activity/batches')
        routes = ['/activity']
        for batch in batch_id_list:
            batch_id = batch['batch_id']
            try:
                date = datetime.strptime(batch_id, '%Y%m%d')
                if (datetime.today() - date).days <= self._base.data_keep_day:
                    continue
            except ValueError:
                pass
            self.logger.info(f'cleaning mysql data of batch_id: {batch_id}')
            for route in routes:
                self.offline_backend.delete(route, {'batch_id': batch_id, 'mode': 'UNSCOPED'})

    def _clean_local_configs(self):
        if not self._ctx.config_path:
            return
        config_dir = os.path.join(os.path.dirname(self._ctx.config_path), '../')
        for batch_id in self._list_batch_ids(config_dir):
            self.logger.info(f'cleaning local configs of batch_id: {batch_id}')
            self._rm_dir(os.path.join(config_dir, batch_id))

    def _clean_runtime_workspace(self):
        if not self._ctx.config_path:
            return
        runtime_dir = os.path.join(os.path.dirname(self._ctx.config_path), '../../runtime')
        for batch_id in self._list_batch_ids(runtime_dir):
            self.logger.info(f'cleaning runtime workspace of batch_id: {batch_id}')
            self._rm_dir(os.path.join(runtime_dir, batch_id))

    def _clean_airflow_logs(self):
        if not self._ctx.config_path:
            return
        log_dir = os.path.join(os.path.dirname(self._ctx.config_path), '../../../logs')
        cur_dt = datetime.strptime(self.batch_id, '%Y%m%d')
        for path in glob.glob(f'{log_dir}/*/*/*/*.log'):
            if 'startup' in path:
                continue
            batch_dir = os.path.dirname(path)
            date_str = os.path.basename(batch_dir)
            try:
                dt = parse(date_str)
            except ParserError:
                continue
            if (cur_dt.astimezone(dt.tzinfo) - dt).days > self._base.data_keep_day:
                self._rm_dir(batch_dir)

    def run(self):
        self._clean_reco_results()
        self._clean_mysql()
        self._clean_local_configs()
        self._clean_runtime_workspace()
        self._clean_airflow_logs()
