# -*- coding: utf-8 -*-
# @File : __init__.py
# @Author : r.yang
# @Date : Tue Jan 11 16:01:02 2022
# @Description :

import logging
import os

from core.logger import LOGGER_NAME


class TRLogging:
    level = os.environ.get('LOG_LEVEL', logging.INFO)

    # use __ for mangling
    __instance = None
    _handler_classes = []
    # 支持 reset，否则 reset 将失效
    resetable = True

    @classmethod
    def once(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = cls(*args, **kwargs)
        return cls.__instance

    @classmethod
    def reset(cls, *args, **kwargs):
        if not cls.resetable and cls.__instance:
            return cls.__instance

        cls.unload_handlers()
        cls.__instance = cls(*args, **kwargs)
        return cls.__instance

    def __init__(self, *handlers: logging.Handler):
        logger = logging.getLogger(LOGGER_NAME)

        logger.propagate = False  # close double output if stdout
        logger.setLevel(self.level)

        TRLogging._handler_classes = [handler.__class__ for handler in handlers]

        for custom_handler in handlers:
            for handler in logger.handlers:
                if isinstance(handler, custom_handler.__class__):
                    break
            else:
                logger.addHandler(custom_handler)

    @classmethod
    def unload_handlers(cls):
        if cls.__instance is None:
            return

        logger = logging.getLogger(LOGGER_NAME)
        for custom_handler_clz in cls.__instance._handler_classes:
            for handler in logger.handlers:
                if isinstance(handler, custom_handler_clz):
                    handler.close()
                    logger.removeHandler(handler)
