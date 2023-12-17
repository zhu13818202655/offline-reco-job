# -*- coding: utf-8 -*-
# @File : test_redis.py
# @Author : r.yang
# @Date : Sun Apr  2 16:29:23 2023
# @Description :

from client.redis import RedisClientRegistry, RedisConfig
from core.tr_logging import TRLogging
from core.tr_logging.color import ColorLoggerHanlder

TRLogging.once(ColorLoggerHanlder())
import os

os.environ['MASTER_NAME'] = 'mymaster'
os.environ['REDIS_HOST'] = '172.18.192.89'
os.environ['REDIS_PORT'] = '36379'
os.environ['REDIS_HOST2'] = '172.18.192.89'
os.environ['REDIS_PORT2'] = '36380'
os.environ['REDIS_HOST3'] = '172.18.192.89'
os.environ['REDIS_PORT3'] = '36381'
os.environ['REDIS_DBNUM'] = '9'
os.environ['REDIS_PASSWORD'] = 'c3RyMG5nX3Bhc3N3MHJk'


redis = RedisClientRegistry.get(RedisConfig.from_env())


redis.ping()
