# -*- coding: utf-8 -*-

import json
import time
from abc import ABC
from datetime import datetime

from pydantic import BaseModel

from configs import Context
from configs.init import base_config
from configs.model.base import BaseConfig
from configs.model.config import (
    BaseDagConfig,
    PostProcessDagConfig,
    PostProcJobConfig,
    RecoDagConfig,
    RecoJobConfig,
    SceneConfig,
)
from configs.utils import ConfigRegistry
from core.job.base import DagRuntimeConfigDAO, IOutputTable, Job, WithContext
from core.logger import tr_logger
from core.spark import WithSpark
from core.spark.table import TableUtils
from core.utils import bind_logger

logger = tr_logger.getChild('jobs.base')


class ConfigProtocol:

    _CODING = 'utf-8'  # to use

    class Dict:
        @staticmethod
        def to_str(item):
            return json.dumps(item)

        @staticmethod
        def from_str(content):
            return json.loads(content)


class HiveDagRuntimeConfigDAO(DagRuntimeConfigDAO, WithSpark, IOutputTable):
    def __init__(self, cfg: BaseConfig) -> None:
        super().__init__()
        self._base = cfg

    log_query = False

    def create(self, ctx: Context, config: dict):
        now = time.time()
        timestamp = int(now * 1000)
        date_str = datetime.fromtimestamp(int(now)).strftime('%Y-%m-%d')
        for module_name, configuration in config.items():
            sql = """
            INSERT INTO TABLE {table_name} PARTITION(dag_id = '{dag_id}', dt = '{batch_id}')
            VALUES('{module_name}', '{configuration}', '{date_str}_{timestamp_str}')
                    """.format(
                table_name=self._base.output_table.runtime_config,
                dag_id=ctx.dag_id,
                batch_id=ctx.batch_id,
                module_name=module_name,
                configuration=ConfigProtocol.Dict.to_str(configuration),
                date_str=date_str,
                timestamp_str=str(timestamp),
            )
            if self.log_query:
                logger.info('sql: {}'.format(sql))
            else:
                logger.info('sql: {}'.format(TableUtils.truncate_middle(sql)))
            self.spark.sql(sql)

    def get(self, ctx: Context) -> dict:
        sql = """
            SELECT module_name,
                   config_content
            FROM
              (SELECT module_name,
                      config_content,
                      ROW_NUMBER() OVER (PARTITION BY module_name
                                         ORDER BY time_stamp DESC) AS row_no
               FROM {table_name}
               WHERE dag_id = '{dag_id}'
                 AND dt = '{batch_id}' ) t
            WHERE row_no = 1
        """.format(
            table_name=self._base.output_table.runtime_config,
            dag_id=ctx.dag_id,
            batch_id=ctx.batch_id,
        )
        logger.info('sql: {}'.format(sql))
        df = self.spark.sql(sql)
        rows = df.collect()
        dag_configuration_map = {}
        for row in rows:
            dag_configuration_map.update(
                {row['module_name']: ConfigProtocol.Dict.from_str(row['config_content'])}
            )
        return dag_configuration_map

    @staticmethod
    def output_table_info_list(cfg: BaseConfig):
        return [
            {
                'name': cfg.output_table.runtime_config,
                'comment': 'dag runtime 配置',
                'field_list': [
                    {'name': 'module_name', 'type': 'string', 'comment': '模块名称'},
                    {'name': 'config_content', 'type': 'string', 'comment': '配置详情，json格式'},
                    {'name': 'time_stamp', 'type': 'string', 'comment': '创建时间，用于排序取最新的'},
                ],
                'partition_field_list': [
                    {'name': 'dt', 'type': 'string', 'comment': '批次号，当前使用日期: 2020-10-11'},
                    {'name': 'dag_id', 'type': 'string', 'comment': '一个dag 的唯一标识'},
                ],
            }
        ]


class ConfiglessJob(WithContext, Job, ABC):
    _ctx: Context

    def __init__(self, ctx: Context):
        super().__init__(ctx)
        self._ctx = ctx

    @property
    def logger(self):
        return bind_logger(
            logger, dag=self._ctx.dag_id, batch=self._ctx.batch_id, job=self._ctx.job_name,
        )


class SparkConfiglessJob(ConfiglessJob, WithSpark):
    pass


class ConfigurableJob(WithContext, Job, ABC):
    _ctx: Context
    _config: BaseModel

    def __init__(self, ctx: Context):
        self._ctx = ctx
        self._config = ConfigRegistry.parse_cfg(ctx.runtime_config)
        self.logger.info(f'config: {self._config.json(indent=4, ensure_ascii=False)}')
        self.batch_id = self._ctx.batch_id
        self.dag_id = self._ctx.dag_id

    @property
    def logger(self):
        return bind_logger(
            logger, dag=self._ctx.dag_id, batch=self._ctx.batch_id, job=self._ctx.job_name,
        )


class BaseJob(ConfigurableJob):
    _config: BaseDagConfig
    _base: BaseConfig

    def __init__(self, ctx: Context):
        super().__init__(ctx)
        if self._config.base is None:
            self.logger.error('dag have no base config, use default')
            self._base = base_config()
        else:
            self._base = self._config.base


class PostProcJob(ConfigurableJob):
    _config: PostProcessDagConfig
    _base: BaseConfig

    def __init__(self, ctx: Context):
        super().__init__(ctx)
        if self._config.base is None:
            self.logger.error('dag have no base config, use default')
            self._base = base_config()
        else:
            self._base = self._config.base

        self.__set_annotations()

    def __set_annotations(self):
        for name, typ in self.__annotations__.items():  # pylint: disable=no-member
            if issubclass(typ, PostProcJobConfig):
                cfg = getattr(self._config, name.strip('_'))
                if cfg is None or not isinstance(cfg, typ):
                    raise ValueError(f'config {name} not exists or type mismatch in job config')
                setattr(self, name, cfg)


class RecoJob(ConfigurableJob):
    _config: RecoDagConfig
    _scene: SceneConfig
    _base: BaseConfig

    def __init__(self, ctx: Context):
        super().__init__(ctx)
        if self._config.scene is None:
            self.logger.error('dag have no scene config')
            raise ValueError
        else:
            self._scene = self._config.scene

        if self._config.base is None:
            self.logger.error('dag have no base config, use default')
            self._base = base_config()
        else:
            self._base = self._config.base

        self.__set_annotations()

    def __set_annotations(self):
        for name, typ in self.__annotations__.items():  # pylint: disable=no-member
            if issubclass(typ, RecoJobConfig):
                for i in self._scene.__dict__:
                    if isinstance(self._scene.__dict__[i], typ):
                        cfg = self._scene.__dict__[i]
                        if cfg is None:
                            raise ValueError(f'config {name} not exists in scene config')
                        setattr(self, name, cfg)


class WithRuntimeCfg:
    _base: BaseConfig

    @property
    def runtime_config(self):
        return HiveDagRuntimeConfigDAO(self._base or base_config())


class SparkRecoJob(RecoJob, WithSpark, WithRuntimeCfg, IOutputTable):
    pass


class SparkPostProcJob(PostProcJob, WithSpark, WithRuntimeCfg, IOutputTable):
    pass


class IDestructHook:
    @property
    def fixture_destruct_hook(self):
        # 测试fixture结束时的 hook
        raise NotImplementedError
