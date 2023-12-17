# -*- coding: utf-8 -*-
# @File : hive.py
# @Author : r.yang
# @Date : Tue Feb 22 14:14:34 2022
# @Description : hive log handler


import json
import logging
import os
from logging import Handler

from configs import Context
from configs.model.base import BaseConfig
from core.job.base import IOutputTable
from core.spark import WithSpark
from core.spark.table import SchemaCreator, TableInserter


class HiveLoggerHandler(Handler, WithSpark, IOutputTable):
    _initialized = False

    log_level = os.environ.get('LOG_LEVEL', logging.INFO)
    hive_logger_formatter_seperator = ' | '
    hive_logger_formatter_list = ('asctime', 'levelname', 'funcName', 'message')
    create_table_key = 'log_constant_create_log_table'

    def __init__(self, ctx: Context, capacity=32):
        super().__init__()
        self.ctx = ctx
        self._dag_id = ctx.dag_id
        self._module_name = ctx.module_name + '.' + ctx.job_name
        self._batch_id = ctx.batch_id
        self._log_table_name = ctx.base.output_table.log
        self._log_formatter_seperator = self.hive_logger_formatter_seperator
        self._log_formatter_list = self.hive_logger_formatter_list

        self.__buffer = []
        self.__capacity = capacity

        if not self.spark._jsparkSession.catalog().tableExists(*self._log_table_name.split('.', 1)):
            self._create_log_table(ctx)

    def _create_log_table(self, ctx: Context):
        output_table_info = self.output_table_info_list(ctx.base)[0]
        SchemaCreator().create_table(output_table_info)

    def initialize(self):
        log_formatter = self._log_formatter_seperator.join(
            [
                '%({formatter})s'.format(formatter=formatter)
                for formatter in self._log_formatter_list
            ]
        )
        self.setFormatter(logging.Formatter(log_formatter))

    def emit(self, record):
        if not self._initialized:
            self.initialize()
            self._initialized = True

        self.format(record)

        self.acquire()
        self.__buffer.append(record)
        self.release()

        if len(self.__buffer) >= self.__capacity:
            self.flush()

    def flush(self):
        self.acquire()

        data = []
        for record in self.__buffer:
            data.append(
                [
                    self._module_name,
                    record.__dict__['filename'],
                    record.__dict__['funcName'],
                    record.__dict__['lineno'],
                    record.__dict__['asctime'],
                    record.__dict__['levelname'],
                    json.dumps(record.__dict__['message']),
                ]
            )

        inserter = TableInserter(self.output_table_info_list(self.ctx.base)[0])

        df = self.spark.createDataFrame(data, inserter._schema)
        table_tmp = 'temp_view_log_flush'
        df.createOrReplaceTempView(table_tmp)
        partition_kwargs = dict(dag_id=self._dag_id, dt=self._batch_id)

        self.spark.sql(
            """
                INSERT into table {table_name}
                    partition({partition})
                SELECT
                    {fields}
                FROM {table_tmp}
            """.format(
                partition=', '.join([f"{k}='{v}'" for k, v in partition_kwargs.items()]),
                table_name=inserter._table_info['name'],
                table_tmp=table_tmp,
                fields=','.join([i['name'] for i in inserter._table_info['field_list']]),
            )
        )

        try:
            self.__buffer = []
        finally:
            self.release()
            self.spark.catalog.dropTempView(table_tmp)

    def close(self):
        try:
            self.flush()
        finally:
            logging.Handler.close(self)

    @staticmethod
    def output_table_info_list(cfg: BaseConfig):
        return [
            {
                'name': cfg.output_table.log,
                'comment': '作业日志表',
                'field_list': [
                    {'name': 'MODULE_NAME', 'type': 'string', 'comment': 'job模块名称'},
                    {'name': 'FILE_NAME', 'type': 'string', 'comment': '文件名称'},
                    {'name': 'FUNC_NAME', 'type': 'string', 'comment': '函数名称'},
                    {'name': 'LINE_NUM', 'type': 'int', 'comment': '函数'},
                    {'name': 'LOG_TIME', 'type': 'string', 'comment': '日志时间'},
                    {'name': 'LOG_LEVEL', 'type': 'string', 'comment': '日志级别'},
                    {'name': 'LOG_MSG', 'type': 'string', 'comment': '日志内容'},
                ],
                'partition_field_list': [
                    {'name': 'dt', 'type': 'string', 'comment': '日分区表'},
                    {'name': 'dag_id', 'type': 'string', 'comment': 'DAG标识'},
                ],
            }
        ]
