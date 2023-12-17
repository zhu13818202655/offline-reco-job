# -*- coding: utf-8 -*-
# @File : config.py
# @Author : r.yang
# @Date : Fri Nov 25 15:06:22 2022
# @Description : format string

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from pydantic import BaseSettings, Field
from pydantic.main import BaseModel

from configs.utils import Env

DEFAULT = 'DEFAULT'

logger = logging.getLogger('tr.scheduler')


class SchedulerTaskConfig(BaseSettings):

    operator: str
    params: Dict[str, Any]
    dependencies: List[str] = []


class SchedulerDagConfig(BaseSettings):
    catchup: bool = False
    start_date: str = Field(default_factory=lambda: '2022-05-01' if Env.is_prod else '2022-04-10')
    end_date: str = '2024-12-31'
    schedule_interval: str = '0 20 * * *'
    description: str = ''
    max_active_runs: int = 1  # 最多同时只能1个 dag 跑
    concurrency: int = 4  # 最多4个作业同时跑
    timeout: int = 7200  # 任务超时(秒)

    tasks: Dict[str, SchedulerTaskConfig] = {}

    def args(self):
        return {
            'catchup': self.catchup,
            'start_date': datetime.strptime(self.start_date, '%Y-%m-%d'),
            'end_date': datetime.strptime(self.end_date, '%Y-%m-%d'),
            'schedule_interval': self.schedule_interval,
            'description': self.description,
            'max_active_runs': self.max_active_runs,
            'concurrency': self.concurrency,
        }


class EnvSetttings(BaseModel):

    entrypoint_path: str
    workspace_dir: str
    config_dir: str
    runtime_dir: str
    dag_config_path: str
    scheduler_config_path: str

    offline_backend_addr: str
    lowcode_url: str
    lowcode_auth_secret: str
    lowcode_auth_client_id: str


class SchedulerConfig(BaseSettings):

    dag_config_map: Dict[str, SchedulerDagConfig]
    spark_env: Dict[str, str]
    env_settings: EnvSetttings

    def with_mock(self):
        if not Env.is_stage:
            return self

        mock_task = 'Mock'
        if mock_task not in self.dag_config_map['base'].tasks:
            self.dag_config_map['base'].tasks['Mock'] = SchedulerTaskConfig(
                operator='airflow.operators.bash_operator.BashOperator',
                dependencies=['UploadDagConfig'],
                params=dict(
                    bash_command=(
                        'python -m jobs.init.mock_reco_results '
                        '--data /tianrang/reco_results/mock/mockdata.txt '
                        '--output_dir /tianrang/reco_results '
                        '--batch_id {{ ds_nodash }}'
                    ),
                ),
            )
        return self

    def get_dag_config(self, dag_name):
        return self.dag_config_map.get(dag_name, self.dag_config_map[DEFAULT])

    def get_default_args(self) -> dict:
        return {
            'owner': 'tianrang',
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            'depends_on_past': False,
            'email': ['r.yang@tianrang-inc.com'],
            'email_on_failure': False,
            'email_on_retry': False,
        }
