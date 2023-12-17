# -*- coding: utf-8 -*-
# @File : redis_mock.py
# @Author : r.yang
# @Date : Sun Apr  2 17:11:32 2023
# @Description :


class RedisMock:
    def __init__(self) -> None:
        self.kv = {}

    def set(self, key, value, ex: int):
        self.kv[key] = value

    def get(self, key):
        return self.kv[key]
