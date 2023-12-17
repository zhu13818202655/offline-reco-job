# -*- coding: utf-8 -*-
# @File : __init__.py
# @Author : r.yang
# @Date : Tue Mar  1 13:58:58 2022
# @Description : load all module for registery


import os


def in_zip(path: str):
    return '.zip' in path


cur_dir = os.path.abspath(os.path.dirname(__file__))


def _pre_load_modules(name):
    if in_zip(cur_dir):
        # spark-submit
        from zipfile import ZipFile

        zip_path = cur_dir.split('.zip')[0] + '.zip'
        with ZipFile(zip_path, 'r') as f:
            for __module in f.namelist():
                if __module.startswith(f'jobs/{name}/'):
                    __module = __module[len(f'jobs/{name}/') :]
                    if __module.endswith('.py') and __module != '__init__.py':
                        __import__(f'jobs.{name}.' + __module[:-3], locals(), globals())
    else:
        for __module in os.listdir(os.path.join(cur_dir, name)):
            if __module == '__init__.py' or not __module.endswith('.py'):
                continue
            __import__(f'jobs.{name}.' + __module[:-3], locals(), globals())


_pre_load_modules('spark')
_pre_load_modules('local')
