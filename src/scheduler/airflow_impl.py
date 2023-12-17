# -*- coding: utf-8 -*-
# @File : _factory.py
# @Author : r.yang
# @Date : Fri Apr 29 13:57:37 2022
# @Description :

import os
import types
from datetime import timedelta
from os.path import join as pathjoin

import yaml
from airflow.models.dag import DAG
from airflow.operators.bash_operator import BashOperator
from pydantic.utils import import_string

from configs import load_or_init_dag_cfg
from core.logger import tr_logger
from scheduler.config.init import get_env_settings, get_scheduler_config
from scheduler.config.model import SchedulerConfig, SchedulerDagConfig, SchedulerTaskConfig

logger = tr_logger.getChild('scheduler')


def gen_dag_config_task(cfg: SchedulerConfig, dag_id: str, task_id: str, subdir=None, **kwargs):
    cfg_dir = (
        pathjoin(cfg.env_settings.config_dir, subdir) if subdir else cfg.env_settings.config_dir
    )

    return BashOperator(
        task_id=task_id,
        bash_command=(
            'python -m jobs.init.gen_dag_config '
            '--config_path {{ params.dag_configs_path }} '
            '--dag_id {{ params.dag_id }} '
            '--batch_id {{ ds_nodash }} '
            '--output_dir {{ params.config }} '
        ),
        params={
            'dag_configs_path': cfg.env_settings.dag_config_path,
            'dag_id': dag_id,
            'config': cfg_dir,
        },
        execution_timeout=timedelta(
            seconds=cfg.dag_config_map.get(dag_id, cfg.dag_config_map['DEFAULT']).timeout
        ),
    )


def gen_spark_task(
    cfg: SchedulerConfig, dag_id, task_id, subdir=None, master='yarn', mode='cluster', **kwargs
):

    cfg_dir = (
        cfg.env_settings.config_dir if not subdir else pathjoin(cfg.env_settings.config_dir, subdir)
    )
    runtime_dir = (
        cfg.env_settings.runtime_dir
        if not subdir
        else pathjoin(cfg.env_settings.runtime_dir, subdir)
    )

    return BashOperator(
        task_id=f'{task_id}',
        bash_command=(
            '{{ params.envs }} {{ params.entrypoint }} '
            '{{ params.dag_id }} '
            '{{ ds_nodash }} '
            '{{ params.name }} '
            '{{ params.config }} '
            '{{ params.runtime }} '
            '{{ params.master }} '
            '{{ params.mode }} '
        ),
        params={
            'envs': ' '.join(f'{k}={v}' for k, v in cfg.spark_env.items()),
            'entrypoint': cfg.env_settings.entrypoint_path,
            'dag_id': dag_id,
            'name': task_id.split('_')[0],
            'config': cfg_dir,
            'runtime': runtime_dir,
            'master': master,
            'mode': mode,
        },
        execution_timeout=timedelta(
            seconds=cfg.dag_config_map.get(dag_id, cfg.dag_config_map['DEFAULT']).timeout
        ),
    )


def gen_local_task(cfg: SchedulerConfig, dag_id, task_id, **kwargs):

    return BashOperator(
        task_id=f'{task_id}',
        bash_command=(
            '{{ params.entrypoint }} '
            '{{ params.dag_id }} '
            '{{ ds_nodash }} '
            '{{ params.name }} '
            '{{ params.config }} '
        ),
        params={
            'entrypoint': pathjoin(
                os.path.dirname(cfg.env_settings.entrypoint_path), 'entrypoint_local.sh'
            ),
            'dag_id': dag_id,
            'name': task_id,
            'config': cfg.env_settings.config_dir,
        },
        execution_timeout=timedelta(
            seconds=cfg.dag_config_map.get(dag_id, cfg.dag_config_map['DEFAULT']).timeout
        ),
    )


class DagFactory:

    _SUPPORT_OPS = {BashOperator, gen_local_task, gen_spark_task, gen_dag_config_task}

    def __init__(self, cfg: SchedulerConfig) -> None:
        self.cfg = cfg

    def create(self, dag_id) -> DAG:
        dag_config = self.cfg.get_dag_config(dag_id)
        with DAG(dag_id, default_args=self.cfg.get_default_args(), **dag_config.args()) as dag:
            tasks = {}
            for task_id in dag_config.tasks:
                task_param = dag_config.tasks[task_id]
                task = self._build_task(dag_id, task_id, task_param)
                tasks[task_id] = task

            for task_id in dag_config.tasks:
                for upstream_task_id in dag_config.tasks[task_id].dependencies:
                    tasks[task_id].set_upstream(tasks[upstream_task_id])

            self.validate(dag)
            return dag

    def validate(self, dag: DAG):
        dag.test_cycle()

    def _build_task(self, dag_id: str, task_id: str, param: SchedulerTaskConfig):
        try:
            op = import_string(param.operator)
        except ImportError:
            if param.operator in globals():
                op = globals()[param.operator]
            else:
                raise ImportError(f'wrong operator: {param.operator}')

        if op not in self._SUPPORT_OPS:
            raise ValueError(f'op: {param.operator} is not supported')

        if isinstance(op, types.FunctionType):
            kwargs = dict({'dag_id': dag_id}, **param.params)
            task = op(cfg=self.cfg, task_id=task_id, **kwargs,)
        else:
            param.params['params'] = dict(
                self.cfg.env_settings.dict(), **param.params.get('params', {})
            )
            task = op(task_id=task_id, **param.params,)
        # logger.info(f"{task_id} builded, op: {param.operator}, param: {param.params}")
        return task


def load():
    settings = get_env_settings()
    scheduler_cfg = get_scheduler_config(settings)

    # 新版本发布时
    if not os.path.exists(settings.scheduler_config_path):
        # overwrite
        scheduler_cfg.env_settings = settings
        with open(settings.scheduler_config_path, 'w') as f:
            yaml.dump(
                scheduler_cfg.dict(), f, default_flow_style=False, allow_unicode=True, indent=2
            )

    logger.info(f'init scheduler configs from yaml: {settings.scheduler_config_path}')
    scheduler_cfg = SchedulerConfig.parse_obj(yaml.safe_load(open(settings.scheduler_config_path)))

    if 'DEFAULT' not in scheduler_cfg.dag_config_map:
        scheduler_cfg.dag_config_map['DEFAULT'] = SchedulerDagConfig()

    # 定时检查DAG配置，如果格式不对，用默认配置替代
    logger.info(f'init dag configs from yaml: {settings.dag_config_path}')
    if settings.dag_config_path:
        load_or_init_dag_cfg(settings.dag_config_path)

    dag_factory = DagFactory(scheduler_cfg)
    for dag_id in scheduler_cfg.dag_config_map:
        if dag_id != 'DEFAULT':
            yield dag_id, dag_factory.create(dag_id)
