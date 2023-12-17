# -*- coding: utf-8 -*-

import os
from typing import List

from pydantic import BaseModel

from configs.model.base import BaseModel
from configs.model.config import BaseDagConfig, PostProcessDagConfig, RecoDagConfig

cur_dir = os.path.abspath(os.path.dirname(__file__))


# 确保 config 都注册到
def _preload_configs():
    if '.zip' in cur_dir:
        from zipfile import ZipFile

        zip_path = cur_dir.split('.zip')[0] + '.zip'
        with ZipFile(zip_path, 'r') as f:
            for __module in f.namelist():
                if __module.startswith('configs/model/'):
                    __module = __module[len('configs/model/') :]
                    if __module.endswith('.py') and __module != '__init__.py':
                        __import__(f'configs.model.' + __module[:-3], locals(), globals())
    else:
        for __module in os.listdir(cur_dir):
            if __module == '__init__.py' or not __module.endswith('.py'):
                continue
            __import__(f'configs.model.' + __module[:-3], locals(), globals())


_preload_configs()


class AllInOneConfig(BaseModel):

    reco_dags: List[RecoDagConfig]
    post_proc_dags: List[PostProcessDagConfig]
    base_dag: BaseDagConfig
