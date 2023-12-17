# -*- coding: utf-8 -*-

import os
from enum import Enum
from typing import Type, TypeVar

import pydantic
from pydantic import BaseModel

from core.logger import tr_logger

logger = tr_logger.getChild('configs.utils')


class DagType(int, Enum):
    Reco = 1
    PostProc = 2
    Base = 3


class ProdType(str, Enum):
    # 下游策略中心提供的数据字典
    CREDIT = 'creditCard'
    DEBIT = 'debitCard'


class UserType(str, Enum):
    CTL = 'ctl'
    CTL1 = 'ctl1'
    EXP = 'exp'
    EXP1 = 'exp1'
    NOOP = 'noop'


class ConfigRegistry:
    CLZ_FIELD = '__clz'
    CTN_FIELD = '__ctn'
    _registry = {}

    @classmethod
    def register(cls):

        registry = cls._registry

        def _register(cfg_cls: Type[T]) -> Type[T]:
            name = cfg_cls.__name__
            assert issubclass(cfg_cls, BaseModel), f'{name} is not subclass of pydantic.BaseModel'
            if name not in registry:
                registry[name] = cfg_cls
                logger.info(f'config class: {name} registered!')
            return cfg_cls

        return _register

    @staticmethod
    def lookup(cfg_name):
        return ConfigRegistry._registry.get(cfg_name)

    @classmethod
    def wrap_cfg(cls, cfg: dict, t: Type[BaseModel]):
        assert cls.lookup(t.__name__), f'type: {t} is not registered'
        try:
            t(**cfg)
        except pydantic.ValidationError as exc:
            logger.error(f'Invalid schema: {exc}')
            raise exc

        return {
            cls.CLZ_FIELD: t.__name__,
            cls.CTN_FIELD: cfg,
        }

    @classmethod
    def parse_cfg(cls, cfg: dict):
        assert cls.CLZ_FIELD in cfg and cls.CTN_FIELD in cfg
        return cls.lookup(cfg[cls.CLZ_FIELD]).parse_obj(cfg[cls.CTN_FIELD])

    @classmethod
    def dumps_cfg(cls, cfg: BaseModel):
        if cfg.__class__.__name__ not in cls._registry:
            cls.register()(cfg.__class__)
        return cls.wrap_cfg(cfg.dict(), cfg.__class__)


T = TypeVar('T')


class ClassifierRegistry:
    _registry = {}

    @classmethod
    def register(cls):

        registry = cls._registry

        def _register(model_cls: Type[T]) -> Type[T]:
            name = model_cls.__name__
            if name not in registry:
                registry[name] = model_cls
                logger.info(f'model class: {name} registered!')
            return model_cls

        return _register

    @staticmethod
    def lookup(model_name) -> Type:
        return ClassifierRegistry._registry[model_name]

    @staticmethod
    def list_all():
        return ClassifierRegistry._registry.values()


class CurEnv(str, Enum):
    PROD = 'prod'
    UAT = 'uat'
    SIT = 'sit'
    PP = 'pp'
    PI = 'pi'
    LOCAL = 'local'

    @property
    def is_prod(self):
        return self.value == self.PROD

    @property
    def is_uat(self):
        return self.value == self.UAT

    @property
    def is_sit(self):
        return self.value == self.SIT

    @property
    def is_pp(self):
        return self.value == self.PP

    @property
    def is_pi(self):
        return self.value == self.PI

    @property
    def is_local(self):
        return self.value == self.LOCAL

    @property
    def is_stage(self):
        return self.value in {self.UAT, self.SIT, self.PP, self.PI}


def _get_cur_env() -> CurEnv:

    env_name = 'RECO_ENVIRONMENT'
    env_value = os.environ.get(env_name, 'prod')
    print(f'reco environment is: {env_value}')
    return CurEnv(env_value)


Env = _get_cur_env()


class TagSelector:
    def __init__(self, data: list):
        self.data = data

    def match(self, record, tag, value):
        if isinstance(value, (int, str)):
            return getattr(record, tag) == value
        if isinstance(value, list):
            return getattr(record, tag) in value
        raise ValueError(f'invalid tag {tag}, value {value}')

    def query(self, **tags):
        res = []
        for record in self.data:
            if all([self.match(record, k, v) for k, v in tags.items()]):
                res.append(record)
        return res
