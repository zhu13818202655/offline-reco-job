# -*- coding: utf-8 -*-
# @File : modules.py
# @Author : r.yang
# @Date : Tue Nov 22 17:48:48 2022
# @Description :

from __future__ import annotations

import functools
import json
import typing
from typing import Dict, List, Optional, Type, TypeVar

import pydantic
from pydantic import BaseModel

from client.lowcode.v1 import LowCode
from configs.init.common import S_CREDIT, S_DEBIT
from configs.model import AllInOneConfig
from configs.model.base import Activity, ChannelInfo, FeatureEngineConfig, ItemInfo, UserItemMatcher
from configs.model.config import ChannelOperateConfig, GroupInfo, SMSConfig
from configs.utils import ProdType
from core.logger import tr_logger
from core.utils import camel_to_snake
from scheduler.config.model import SchedulerConfig

logger = tr_logger.getChild('lowcode')


class ConfigBase(BaseModel):
    id: str

    @classmethod
    @functools.lru_cache()
    def from_lowcode(cls, lowcode: LowCode) -> ConfigList:
        res = []
        module = lowcode.get_module_by_handle(camel_to_snake(cls.__name__))
        records = lowcode.get_record_list(module.module_id)

        multi_value_fields = set()
        rel_module_map = {}

        for field, type_ in typing.get_type_hints(cls).items():
            if isinstance(type_, typing._GenericAlias) and issubclass(type_.__origin__, list):
                multi_value_fields.add(field)
                item_type = type_.__args__[0]
                if isinstance(item_type, type) and issubclass(item_type, ConfigBase):
                    rel_module_map[field] = {
                        r.id: r.dict() for r in item_type.from_lowcode(lowcode)
                    }
            elif not isinstance(type_, typing._GenericAlias) and issubclass(type_, ConfigBase):
                rel_module_map[field] = {r.id: r.dict() for r in type_.from_lowcode(lowcode)}

        for r in records:
            val_dict = {}
            val_dict['id'] = r.record_id

            # set value
            for v in r.values:
                if v.value is None:
                    continue
                if v.name in multi_value_fields:
                    if v.name not in val_dict:
                        val_dict[v.name] = []
                    val_dict[v.name].append(v.value)

                else:
                    val_dict[v.name] = v.value

            # set related value
            for field in rel_module_map:
                if field not in val_dict:
                    continue
                if isinstance(val_dict[field], list):
                    val_dict[field] = [
                        rel_module_map[field][i]
                        for i in val_dict[field]
                        if i in rel_module_map[field]
                    ]
                else:
                    val_dict[field] = rel_module_map[field][val_dict[field]]

            try:
                res.append(cls.parse_obj(val_dict))
            except BaseException as e:
                logger.error(f'parse error: {e}, data: {val_dict}')
        return ConfigList.lookup(cls)(res)


T = TypeVar('T')


class ConfigList(List[ConfigBase]):
    def pretty_log(self):
        for i, c in enumerate(self):
            logger.info(f'{c.__class__.__name__}[{i}] {c.json(indent=4, ensure_ascii=False)}')

    def merge_aio_cfg(self, aio_cfg: AllInOneConfig) -> AllInOneConfig:
        return aio_cfg

    def merge_scheduler_cfg(self, cfg: SchedulerConfig) -> SchedulerConfig:
        return cfg

    _registry = {}

    @classmethod
    def register(cls, rel: Type):
        registry = cls._registry
        name = rel.__name__

        def _register(cfg_cls: Type[List[T]]) -> Type[List[T]]:
            if name not in registry:
                # patch the pretty_log method
                cfg_cls.pretty_log = cls.pretty_log
                registry[name] = cfg_cls
            return cfg_cls

        return _register

    @classmethod
    def lookup(cls, clz: Type):
        return cls._registry.get(clz.__name__, cls)


class ConfigItem(ConfigBase):

    item_name: str
    item_id: List[str]
    item_type: str
    label: int
    prop: float = 0.0
    sms_content: str = ''
    disable: bool = False

    @classmethod
    def from_aio_cfg(cls, aio_cfg: AllInOneConfig) -> ConfigItemList:
        if not aio_cfg.base_dag.base:
            return []

        return ConfigItemList(
            [
                ConfigItem(
                    id='0',
                    item_id=i.item_ids,
                    item_name=i.desc,
                    item_type=i.type,
                    prop=i.prop,
                    label=i.label,
                    sms_content=i.sms_content,
                    disable=not i.enable,
                )
                for i in aio_cfg.base_dag.base.items
            ]
        )


@ConfigList.register(ConfigItem)
class ConfigItemList(List[ConfigItem]):
    def merge_aio_cfg(self, aio_cfg: AllInOneConfig) -> AllInOneConfig:
        if len(self) == 0 or not aio_cfg.base_dag.base:
            return aio_cfg

        aio_cfg.base_dag.base.items = []
        for c in self:
            aio_cfg.base_dag.base.items.append(
                ItemInfo(
                    item_ids=c.item_id,
                    desc=c.item_name,
                    type=ProdType(c.item_type),
                    prop=c.prop,
                    label=c.label,
                    sms_content=c.sms_content,
                    enable=not c.disable,
                )
            )
        return aio_cfg


class ConfigChannel(ConfigBase):
    channel_id: str
    banner_id: str = ''
    channel_name: str
    feedback_delay: int
    push_delay: int
    max_push: int
    allow_repeat: bool
    num_items: int


@ConfigList.register(ConfigChannel)
class ConfigChannelList(List[ConfigChannel]):
    def merge_aio_cfg(self, aio_cfg: AllInOneConfig) -> AllInOneConfig:
        if len(self) == 0 or not aio_cfg.base_dag.base:
            return aio_cfg

        aio_cfg.base_dag.base.channels = []

        # drop duplicates
        uniq_channels = {f'{c.channel_id}__{c.banner_id}': c for c in self}

        for c in uniq_channels.values():
            aio_cfg.base_dag.base.channels.append(
                ChannelInfo(
                    channel_id=c.channel_id,
                    channel_name=c.channel_name,
                    banner_id=c.banner_id,
                    feedback_delay=c.feedback_delay,
                    push_delay=c.push_delay,
                    max_push=c.max_push,
                    allow_repeat=c.allow_repeat,
                    num_items=c.num_items,
                )
            )
        return aio_cfg


class ConfigFeatEngine(ConfigBase):
    version: str

    standard_scaler: List[str] = []
    quantile_discretizer: List[str] = []
    bucketizer: List[Dict[str, List[float]]] = []
    string_index: List[str] = []
    onehot: List[str] = []
    log1p: List[str] = []
    date_diff: List[str] = []
    train_dt: str  # YYYY-mm-dd

    @pydantic.validator('bucketizer', pre=True)
    def parse_bucketizer(cls, value, values):
        if value and isinstance(value[0], dict):
            return value
        return [json.loads(i) for i in value]

    @pydantic.validator('train_dt', pre=True)
    def parse_train_dt(cls, value, values):
        v = value.replace('-', '')
        assert len(v) == 8
        return v


@ConfigList.register(ConfigFeatEngine)
class ConfigFeatEngineList(List[ConfigFeatEngine]):
    def merge_aio_cfg(self, aio_cfg: AllInOneConfig) -> AllInOneConfig:
        base = aio_cfg.base_dag.base
        if not base or len(self) == 0:
            return aio_cfg

        feat_engines = []
        for cfg in self:
            feat_engines.append(
                FeatureEngineConfig(
                    feat_version=cfg.version,
                    feat_map={
                        'default': [],
                        'standardScaler': cfg.standard_scaler,
                        'quantileDiscretizer': cfg.quantile_discretizer,
                        'bucketizer': [[k, v] for i in cfg.bucketizer for k, v in i.items()],
                        'stringIndex': cfg.string_index,
                        'onehot': cfg.onehot,
                        'log1p': cfg.log1p,
                        'dateDiff': cfg.date_diff,
                    },
                    train_dt=cfg.train_dt,
                )
            )
        base.feat_engines = feat_engines
        return aio_cfg


class ConfigModel(ConfigBase):
    model_name: str
    model_version: str
    model_clz: str
    custom_train_args: dict = {}
    model_type: str
    feat_engine: ConfigFeatEngine

    # train_args 在 lowcode 上是string，需要自定义反序列化方法
    @pydantic.validator('custom_train_args', pre=True)
    def parse_train_args(cls, value, values):
        if not value:
            return {}
        if isinstance(value, str):
            value = json.loads(value)
        return value


class ConfigGroup(ConfigBase):

    channel: ConfigChannel
    group_type: List[str]
    group_size: List[int]
    items: List[ConfigItem] = []


class ConfigReco(ConfigBase):
    scene_id: str
    scene_name: str
    dag_id: str
    dag_name: str
    rank_model: List[ConfigModel] = []
    user_model: List[ConfigModel] = []
    user_filters: Dict[str, str] = {}
    user_group: List[ConfigGroup] = []
    user_refresh_day: List[int] = []
    report_stat_period: List[int] = []

    @pydantic.validator('user_filters', pre=True)
    def parse_filters(cls, value, values):
        if not value:
            return {}
        if isinstance(value, str):
            value = json.loads(value)
        return value


@ConfigList.register(ConfigReco)
class ConfigRecoList(List[ConfigReco]):
    def merge_aio_cfg(self, aio_cfg: AllInOneConfig) -> AllInOneConfig:
        if len(self) == 0:
            return aio_cfg

        cmap = {c.dag_id: c for c in self}
        for reco_dag in aio_cfg.reco_dags:
            if reco_dag.dag_id not in cmap:
                continue

            cfg = cmap[reco_dag.dag_id]
            reco_dag.dag_name = cfg.dag_name
            reco_dag.scene.scene_id = cfg.scene_id
            reco_dag.scene.scene_name = cfg.scene_name
            if reco_dag.scene.user_pool:
                reco_dag.scene.user_pool.filters.update(cfg.user_filters)
                reco_dag.scene.user_pool.user_type_refresh_days = cfg.user_refresh_day

                if not cfg.user_model:
                    logger.error('user model is empty')
                    continue
                reco_dag.scene.user_pool.model.model_version = cfg.user_model[0].model_version
                reco_dag.scene.user_pool.model.model_clz = cfg.user_model[0].model_clz
                reco_dag.scene.user_pool.model.feat_version = cfg.user_model[0].feat_engine.version
                reco_dag.scene.user_pool.model.custom_train_args = cfg.user_model[
                    0
                ].custom_train_args
            if reco_dag.scene.operating_user and cfg.user_group:
                reco_dag.scene.operating_user.channel_cfgs = []
                for group in cfg.user_group:
                    if len(group.group_size) != len(group.group_type):
                        continue
                    reco_dag.scene.operating_user.channel_cfgs.append(
                        ChannelOperateConfig(
                            channel_id=group.channel.channel_id,
                            banner_id=group.channel.banner_id,
                            group_users={
                                i: GroupInfo(num=j, minimum=i in ['ctl', 'noop'])
                                for i, j in zip(group.group_type, group.group_size)
                            },
                            items_specified=[i.item_id[0] for i in group.items],
                        )
                    )
            if reco_dag.scene.ranking:
                if not cfg.rank_model:
                    logger.error('rank model is empty')
                    continue
                reco_dag.scene.ranking.model.model_version = cfg.rank_model[0].model_version
                reco_dag.scene.ranking.model.model_clz = cfg.rank_model[0].model_clz
                reco_dag.scene.ranking.model.feat_version = cfg.rank_model[0].feat_engine.version
                reco_dag.scene.ranking.model.custom_train_args = cfg.rank_model[0].custom_train_args

            if reco_dag.scene.report and cfg.report_stat_period:
                reco_dag.scene.report.stat_periods_in_day = cfg.report_stat_period

        return aio_cfg


class ConfigActv(ConfigBase):
    actv_id: str
    reco: ConfigReco
    is_test: bool = False
    channel: ConfigChannel


class ConfigPostProc(ConfigBase):
    dag_id: str
    dag_name: str

    actv: List[ConfigActv]
    sms_push_days: List[int] = []


@ConfigList.register(ConfigPostProc)
class ConfigPostProcList(List[ConfigPostProc]):
    def merge_aio_cfg(self, aio_cfg: AllInOneConfig) -> AllInOneConfig:
        if len(self) == 0:
            return aio_cfg

        cmap = {c.dag_id: c for c in self}
        for post_dag in aio_cfg.post_proc_dags:
            if post_dag.dag_id not in cmap:
                continue

            cfg = cmap[post_dag.dag_id]
            post_dag.dag_name = cfg.dag_name
            post_dag.channel_id = cfg.actv[0].channel.channel_id

            if cfg.sms_push_days:
                post_dag.sms = SMSConfig(push_days=cfg.sms_push_days)

            post_dag.actvs = []
            for c in cfg.actv:
                post_dag.actvs.append(
                    Activity(
                        actv_id=c.actv_id,
                        banner_id=c.channel.banner_id,
                        scene_id=c.reco.scene_id,
                        to_BI=True,
                        test_user_item=self._to_selector(c),
                    )
                )

        return aio_cfg

    def _to_selector(self, actv: ConfigActv) -> Optional[UserItemMatcher]:
        if not actv.is_test:
            return None
        item_selector_map = {
            S_CREDIT.scene_id: S_CREDIT.item_selector,
            S_DEBIT.scene_id: S_DEBIT.item_selector,
        }
        item_selector = item_selector_map[actv.reco.scene_id]

        user_selector_map = {
            S_CREDIT.scene_id: {'scope': ['all', 'credit']},
            S_DEBIT.scene_id: {'scope': ['all', 'debit']},
        }
        user_selector = user_selector_map[actv.reco.scene_id]
        return UserItemMatcher(
            user_selector=user_selector or {}, item_selector=item_selector or {},
        )


class ConfigDag(ConfigBase):
    dag_id: str
    schedule_interval: str
    concurrency: int
    max_active_runs: int
    timeout: int
    catchup: bool = False

    @classmethod
    def from_cfg(cls, scheduler_cfg: SchedulerConfig) -> List[ConfigDag]:

        return [
            ConfigDag(
                id='0',
                dag_id=dag_id,
                schedule_interval=dag.schedule_interval,
                concurrency=dag.concurrency,
                max_active_runs=dag.max_active_runs,
                timeout=dag.timeout,
                catchup=dag.catchup,
            )
            for dag_id, dag in scheduler_cfg.dag_config_map.items()
        ]


@ConfigList.register(ConfigDag)
class ConfigDagList(List[ConfigDag]):
    @staticmethod
    def _is_cron_valid(cron: str) -> bool:

        items = cron.strip().split(' ')
        if not items:
            return False

        if len(items) == 1:
            candidates = {'@once', '@daily', '@monthly', '@weekly'}
            return items[0] != '' and items[0][0] == '@' and items[0] in candidates

        return len(items) == 5

    def merge_scheduler_cfg(self, cfg: SchedulerConfig) -> SchedulerConfig:
        m = {dag.dag_id: dag for dag in self}
        for dag_id in cfg.dag_config_map:
            if dag_id not in m:
                logger.warning(f'dag_id: {dag_id} does not exist!')
                continue

            if self._is_cron_valid(m[dag_id].schedule_interval):
                cfg.dag_config_map[dag_id].schedule_interval = m[dag_id].schedule_interval
            else:
                logger.warning(f'cron: {m[dag_id].schedule_interval} is not valid')
            cfg.dag_config_map[dag_id].concurrency = m[dag_id].concurrency
            cfg.dag_config_map[dag_id].max_active_runs = m[dag_id].max_active_runs
            cfg.dag_config_map[dag_id].timeout = m[dag_id].timeout
            cfg.dag_config_map[dag_id].catchup = m[dag_id].catchup
        return cfg


class ReportAgg(BaseModel):
    channel_id: str
    banner_id: str
    user_type: str
    prod_type: str
    scene_id: str
    operate: int
    send: int
    click: int
    click_ratio: float
    convert: int
    convert_ratio: float
    during: str
    dt: str


class ReportAggList(List[ReportAgg]):
    def to_lowcode(self, lowcode: LowCode, **kwargs):
        module = lowcode.get_module_by_handle(camel_to_snake(ReportAgg.__name__))

        query = ' and '.join([f"{k}='{v}'" for k, v in kwargs.items()])
        records = lowcode.get_record_list(module.module_id, query=query)
        for r in records:
            lowcode.delete_record(module.module_id, r.record_id)
            logger.info(f'origin data: {r.record_id} is deleted')

        for r in self:
            lowcode.create_record(module.module_id, r.dict())
            logger.info(f'record: {r} is created')
