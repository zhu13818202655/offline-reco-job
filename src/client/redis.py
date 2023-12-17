# -*- coding: utf-8 -*-
# @File : redis.py
# @Author : r.yang
# @Date : Sun Apr  2 16:02:20 2023
# @Description : redis client

from typing import List, Optional

import redis
from redis.sentinel import Sentinel

from configs.model.base import RedisConfig
from core.logger import tr_logger


def new_redis(addrs: List[str], master: str, password: str, db: int) -> redis.Redis:
    addr_ports = [(i.split(':')[0], int(i.split(':')[1])) for i in addrs]
    if master and len(addrs) == 3:
        sentinel = Sentinel(addr_ports)
        host, port = sentinel.discover_master(master)
        return redis.StrictRedis(host=host, port=port, password=password, db=db, socket_timeout=10,)

    elif not master and len(addrs) == 1:
        return redis.Redis(
            host=addr_ports[0][0],
            port=addr_ports[0][1],
            password=password,
            db=db,
            socket_timeout=10,
        )

    raise ValueError(f'addrs: {addrs}, master: {master}, db: {db}')


class RedisClientRegistry:
    _instance = None

    @classmethod
    def register(cls, cfg: RedisConfig):
        tr_logger.info(f'redis cfg: {cfg}')
        redis = new_redis(cfg.addrs, cfg.master_name, cfg.password, cfg.db)
        redis.ping()
        cls._instance = redis

    @classmethod
    def get(cls, cfg: Optional[RedisConfig] = None) -> redis.Redis:
        if not cls._instance and cfg:
            cls.register(cfg)
        assert cls._instance
        return cls._instance
