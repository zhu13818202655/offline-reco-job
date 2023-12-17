# -*- coding: utf-8 -*-
# @File : __init__.py
# @Author : r.yang
# @Date : Thu Nov 24 16:29:11 2022
# @Description :


from configs.init.local import local_config
from configs.init.prod import prod_config
from configs.init.stage import stage_config
from configs.model import AllInOneConfig
from configs.model.base import BaseConfig
from configs.model.table import OutputTable
from configs.utils import Env

INIT_FUCN_MAP = {
    Env.PROD: prod_config,
    Env.SIT: stage_config,
    Env.UAT: stage_config,
    Env.PI: stage_config,
    Env.PP: stage_config,
    Env.LOCAL: local_config,
}


def aio_config() -> AllInOneConfig:
    return INIT_FUCN_MAP[Env.value]()


def base_config() -> BaseConfig:
    b = INIT_FUCN_MAP[Env.value]().base_dag.base
    if b:
        return b
    raise ValueError


def output_table() -> OutputTable:
    return base_config().output_table
