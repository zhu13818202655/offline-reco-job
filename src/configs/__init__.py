# -*- coding: utf-8 -*-
# @File : __init__.py
# @Author : r.yang
# @Date : Thu Nov 24 16:35:17 2022
# @Description : format string

import functools
from typing import Optional

import yaml
from pydantic import BaseModel

from configs.init import aio_config
from configs.model import AllInOneConfig
from configs.model.base import BaseConfig, BaseModel
from configs.model.config import BaseDagConfig, PostProcessDagConfig, RecoDagConfig
from configs.utils import ConfigRegistry
from core.job.registry import JobRegistry
from core.logger import tr_logger

logger = tr_logger.getChild('configs.model')
__MAGIC_FUNC = open


class Context(BaseModel):

    dag_id: str
    batch_id: str
    job_name: str
    module_name: str

    # 运行时从 mysql 中获取的配置 json，包括：
    #   - BaseDagConfig
    #   - RecoDagConfig
    #   - PostProcessDagConfig
    runtime_config: dict
    config_path: str = ''

    @classmethod
    def default(cls, dag_id, batch_id, job_name):
        dag_config = lookup_dag_config(dag_id)
        # XXX
        if job_name == 'test':
            module_name = 'test'
        else:
            module_name = JobRegistry.lookup(job_name).clz.__module__
        return cls(
            dag_id=dag_id,
            batch_id=batch_id,
            job_name=job_name,
            module_name=module_name,
            runtime_config=ConfigRegistry.dumps_cfg(dag_config),
        )

    # for lru cache
    def __hash__(self):
        return hash(self.json())

    @property
    @functools.lru_cache()
    def base(self) -> BaseConfig:
        base = 'base'
        res = []

        def get_key(d):
            if isinstance(d, dict):
                for k in d:
                    if k == base and d[k]:
                        res.append(d[k])
                    else:
                        get_key(d[k])
            elif isinstance(d, list):
                for i in d:
                    get_key(i)

        get_key(self.runtime_config)
        if res:
            return BaseConfig(**res[0])
        logger.error(f'no base config in runtime_config: {self.runtime_config}')
        raise RuntimeError


def merge_cfg(dag_config: dict, *, base_config: dict = {}):
    d = {}
    if base_config:
        d['base'] = base_config
    return dict(dag_config, **d)


def load_or_init_dag_cfg(dag_configs_path):
    from pydantic import ValidationError
    from yaml.scanner import ScannerError

    try:
        aio_cfg = AllInOneConfig(**yaml.safe_load(__MAGIC_FUNC(dag_configs_path)))
    except (ValidationError, FileNotFoundError, ScannerError) as e:
        print(f'config error for {e}')
        aio_cfg = aio_config()
        with __MAGIC_FUNC(dag_configs_path, 'w') as f:
            yaml.dump(aio_cfg.dict(), f, default_flow_style=False, allow_unicode=True, indent=2)
        print(f'aio config is dumped to {dag_configs_path}')
    return aio_cfg


def lookup_dag_config(dag_id: str, aio: Optional[AllInOneConfig] = None) -> BaseModel:
    if aio is None:
        aio = aio_config()
    base_config = aio.base_dag.base or aio_config().base_dag.base
    if not base_config:
        raise ValueError
    if dag_id == aio.base_dag.dag_id:
        return BaseDagConfig.parse_obj(aio.base_dag.dict())

    for dag in aio.post_proc_dags:
        if dag_id == dag.dag_id:
            cfg_dict = merge_cfg(dag.dict(), base_config=base_config.dict(),)
            return PostProcessDagConfig.parse_obj(cfg_dict)

    for dag in aio.reco_dags:
        if dag_id == dag.dag_id:
            cfg_dict = merge_cfg(dag.dict(), base_config=base_config.dict(),)
            return RecoDagConfig.parse_obj(cfg_dict)

    raise IndexError(f'dag_id: {dag_id} not found in default dag configs')
