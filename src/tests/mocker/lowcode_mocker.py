# -*- coding: utf-8 -*-
# @File : lowcode_mocker.py
# @Author : r.yang
# @Date : Thu Dec  1 13:53:37 2022
# @Description : format string

from pydantic.main import BaseModel


class Module(BaseModel):
    module_id: str = ''


class Record(BaseModel):
    record_id: str = ''


class Lowcode:
    def __init__(self) -> None:
        self.records = []

    def get_module_by_handle(self, name):
        return Module()

    def get_record_list(self, module_id, **kwargs):
        return [Record()]

    def delete_record(self, module_id, record_id):
        return

    def create_record(self, module_id, data):
        self.records.append(data)
