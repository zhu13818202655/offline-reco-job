# -*- coding: utf-8 -*-
# @File : plain.py
# @Author : r.yang
# @Date : Sun Nov 28 20:35:05 2021
# @Description : plain log without color


import logging


def PlainLoggerHandler():
    handler = logging.StreamHandler()
    # fmt = '[%(levelname)s][%(asctime)s][%(name)s(%(process)d)][%(filename)s:%(lineno)d][%(funcName)s] %(message)s'
    fmt = (
        '[%(levelname)s][%(asctime)s][%(name)s][%(filename)s:%(lineno)d][%(funcName)s] %(message)s'
    )
    handler.setFormatter(logging.Formatter(fmt))
    return handler
