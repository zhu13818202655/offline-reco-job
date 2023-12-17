# -*- coding: utf-8 -*-
# @File : online.py
# @Author : r.yang
# @Date : Mon Apr  3 17:33:31 2023
# @Description :

import requests

from core.job.registry import JobRegistry
from jobs.base import BaseJob


@JobRegistry.register(JobRegistry.T.INFER)
class StartConsumer(BaseJob):
    def run(self):
        url = self._base.online_backend_address + '/recommend/online/consumer'
        resp = requests.post(url, json={'action': 'start'}, timeout=10)
        if resp.status_code == 200:
            self.logger.info(f'start success, resp: {resp.json()}')
        else:
            self.logger.info(f'start failed, resp: {resp.text}')
            raise RuntimeError(resp.text)


@JobRegistry.register(JobRegistry.T.INFER)
class StopConsumer(BaseJob):
    def run(self):
        url = self._base.online_backend_address + '/recommend/online/consumer'
        resp = requests.post(url, json={'action': 'stop'}, timeout=10)
        if resp.status_code == 200:
            self.logger.info(f'start success, resp: {resp.json()}')
        else:
            self.logger.info(f'start failed, resp: {resp.text}')
            raise RuntimeError(resp.text)
