from __future__ import annotations

import atexit
import calendar
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta
from functools import wraps
from typing import AnyStr, Dict, Union, overload

import requests
from pyspark.sql.dataframe import DataFrame

from core.logger import tr_logger

logger = tr_logger.getChild('core.utils')


class Datetime(datetime):
    __FORMAT = '%Y%m%d'

    @property
    def str(self):
        return self.strftime(self.__FORMAT)

    @classmethod
    def from_str(cls, dt: AnyStr) -> Datetime:
        date = cls.strptime(dt, cls.__FORMAT)
        return cls._cvt(date)

    @staticmethod
    def _cvt(date: datetime) -> Datetime:
        return Datetime(year=date.year, month=date.month, day=date.day)

    def __add__(self, other) -> Datetime:
        dt = super().__add__(other)
        return self._cvt(dt)

    @overload
    def __sub__(self, other: datetime) -> timedelta:
        ...

    @overload
    def __sub__(self, other: timedelta) -> Datetime:
        ...

    def __sub__(self, other: Union[datetime, timedelta]) -> Union[datetime, timedelta]:
        if isinstance(other, timedelta):
            return self + -other
        dt = super().__sub__(other)
        if isinstance(dt, datetime):
            return self._cvt(dt)
        return dt

    __radd__ = __add__


class DateHelper:
    def __init__(self, base_date: AnyStr, react_delay: int):
        """
         初始化日期工具类
         :param base_date: 当前系统输入时间
         :param react_delay: 投放系统延时, 单位:天
        """
        base_dt = Datetime.from_str(base_date)
        self._base_date = base_dt
        if react_delay < 0:
            raise Exception('react_delay({}) cannot be negative'.format(react_delay))
        self._react_delay = react_delay

    @property
    def base_date(self):
        """
         当前系统时间
        """
        return self._base_date

    def add_day(self, days):
        return self._base_date + timedelta(days=days)

    @property
    def str(self):
        return self._base_date.str

    @property
    def date_before_react_delay(self):
        return self._base_date - timedelta(days=self._react_delay)

    @property
    def date_after_react_delay(self):
        return self._base_date + timedelta(days=self._react_delay)

    @property
    def month_start(self) -> Datetime:
        return self.get_month_start_date(self._base_date + timedelta(self._react_delay))

    @property
    def month_end(self) -> Datetime:
        return self.get_month_end_date(self._base_date + timedelta(self._react_delay))

    def gap(self, date: Union[datetime, str]) -> timedelta:
        if isinstance(date, str):
            date = self.from_str(date)
        return self._base_date - date

    def push_start_date(self, operating_start_day: Datetime = None) -> Datetime:
        """
        计算产出人群包的开始日期
        :param operating_start_day: 投放开始时间, 人群包开始产出以后, 开始时间无法修改.
        """
        if operating_start_day is None:
            operating_start_day = self.month_start
        return operating_start_day - timedelta(days=self._react_delay)

    def push_end_date(self, operating_end_day: Datetime = None):
        """
         产出人群包结束时间
        :param operating_end_day: 投放结束时间, 可修改为 T + 3 以后.
        """
        if operating_end_day is None:
            operating_end_day = self.month_end
        return operating_end_day - timedelta(days=self._react_delay)

    def range_info(
        self,
        range_list_size: int,
        operating_start_day: Datetime = None,
        operating_end_day: Datetime = None,
    ):
        if not operating_start_day:
            operating_start_day = self.month_start
        if not operating_end_day:
            operating_end_day = self.month_end

        start = self.push_start_date(operating_start_day)
        end = self.push_end_date(operating_end_day)
        date_range_list = self._date_range(start=start, end=end, range_list_size=range_list_size,)
        range_index, range_start, range_end = self._range_info(
            range_list=date_range_list, value=self._base_date,
        )
        return (
            range_index + 1,
            self.to_str(range_start),
            self.to_str(range_end - timedelta(days=1)),
        )

    def last_n_year(self, n) -> Datetime:
        return self._base_date - n * timedelta(days=365)

    @staticmethod
    def from_str(date_str: str) -> Datetime:
        return Datetime.from_str(date_str)

    @staticmethod
    def to_str(date: Datetime) -> str:
        return date.str

    @staticmethod
    def _date_range(start, end, range_list_size):
        return list(
            DateHelper._date_range_generator(start=start, end=end, range_list_size=range_list_size,)
        )

    @staticmethod
    def _date_range_generator(start, end, range_list_size):
        sum_days = (end - start).days
        period_days = int(sum_days / range_list_size)
        period_time_delta = timedelta(days=period_days)
        for range_index in range(range_list_size):
            yield start + period_time_delta * range_index
        yield end + timedelta(days=1)

    @staticmethod
    def _range_info(range_list, value):
        for range_index in range(len(range_list) - 1):
            start = range_list[range_index]
            end = range_list[range_index + 1]
            if start <= value < end:
                return range_index, start, end
        raise Exception('value({}) is out of range_list({})'.format(value, range_list))

    @staticmethod
    def get_before_date(date: Datetime, react_delay) -> Datetime:
        return date - timedelta(days=react_delay)

    @staticmethod
    def get_month_start_date(date: Datetime) -> Datetime:
        return Datetime(date.year, date.month, 1)

    @staticmethod
    def get_month_end_date(date):
        return Datetime(date.year, date.month, calendar.monthrange(date.year, date.month)[1])

    @staticmethod
    def get_month_end_date_str(date_str):
        return DateHelper.to_str(DateHelper.get_month_end_date(DateHelper.from_str(date_str)))

    @staticmethod
    def compare_date(date1, date2):
        return (date1 - date2).days

    @staticmethod
    def compare_date_str(date_str1, date_str2):
        return DateHelper.compare_date(
            DateHelper.from_str(date_str1), DateHelper.from_str(date_str2)
        )


@contextmanager
def log_upper(depth):
    """
    打印日志时，显示往上第 n 层调用栈的文件/行数信息
    ====
    this is  NOT THREAD SAFE !!!
    ====
    """
    _frame_stuff = [0, logging.Logger.findCaller]

    def findCaller(*args):
        f = logging.currentframe()
        for _ in range(2 + _frame_stuff[0]):
            if f is not None:
                f = f.f_back
        rv = '(unknown file)', 0, '(unknown function)', ''
        while hasattr(f, 'f_code'):
            co = f.f_code
            filename = os.path.normcase(co.co_filename)
            if filename == logging._srcfile:
                f = f.f_back
                continue
            rv = (co.co_filename, f.f_lineno, co.co_name, '')
            break
        return rv

    d = _frame_stuff[0]
    try:
        logging.Logger.findCaller = findCaller
        _frame_stuff[0] = d + depth
        yield True
    except:
        raise
    finally:
        logging.Logger.findCaller = _frame_stuff[1]
        _frame_stuff[0] = d


class _Node:
    def __init__(self, name):
        """简单的调用栈实现，用于创建和获取函数执行位置
        """
        self.name = name
        self.childs: Dict[str, _Node] = {}
        self.activate = False  # 是否在栈中

    def add_child(self, node: _Node):
        """从调用树中找到当前调用栈，以及节点应该在的位置，如果不存在则创建，激活该节点
        """
        for _, child in self.childs.items():
            if child.activate:
                return child.add_child(node)
        if node.name not in self.childs:
            self.childs[node.name] = node
        self.childs[node.name].activate = True
        return self.childs[node.name]


class TimeMonitor:
    """用来进行函数计时，最后做总的汇报"""

    _start = 0
    _elapsed = defaultdict(float)
    _count = defaultdict(int)
    _root = _Node('root')

    enable = False

    @classmethod
    def init(cls):
        cls._start = time.time()

    @classmethod
    def add(cls, name, data: float):
        # 因为 GIL，自建结构默认 thread safe，无需加锁
        cls._elapsed[name] += data
        cls._count[name] += 1

    @classmethod
    def report(cls):
        total_cost = time.time() - cls._start
        print('')
        spaces = [80, 10, 10, 15, 15]
        cols = ['function name', 'count', 'total(s)', 'average(s)', 'percentage(%)']
        length_mark = (sum(spaces), '=')
        print(' Table 1. TimeMonitor Report '.center(length_mark[0], ' '))
        print(length_mark[1] * length_mark[0])
        print('\033[1m' + ''.join([col.ljust(spaces[i]) for i, col in enumerate(cols)]) + '\033[0m')

        sums = [0] * 4

        def _log_by_name(node, elapsed, depth=0):
            if node.name == 'root':
                return
            gap = ' ' * 2
            data = [
                gap * depth + '+ ' + node.name,
                str(cls._count[node]),
                '{:.2f}'.format(elapsed),
                '{:.2f}'.format(elapsed / max(1, cls._count[node])),
                '{:.4f}'.format(elapsed / total_cost * 100),
            ]
            if depth == 1:
                for i in range(len(sums)):
                    sums[i] += float(data[i + 1])
                print('-' * length_mark[0])
                print(
                    '\033[1;32m'
                    + ''.join([col.ljust(spaces[i]) for i, col in enumerate(data)])
                    + '\033[0m'
                )
            else:
                print(''.join([col.ljust(spaces[i]) for i, col in enumerate(data)]))

        def _log_node(node, depth):
            _log_by_name(node, cls._elapsed[node], depth)
            for child in node.childs.values():
                _log_node(child, depth + 1)

        _log_node(cls._root, 0)

        print('-' * length_mark[0])
        print(
            '\033[1;32m'
            + ''.join(
                [
                    col.ljust(spaces[i])
                    for i, col in enumerate(
                        [
                            '  + SUM',
                            str(int(sums[0])),
                            '{:.2f}'.format(sums[1]),
                            '{:.2f}'.format(sums[2]),
                            '{:.4f}'.format(sums[3]),
                        ]
                    )
                ]
            )
            + '\033[0m'
        )

        print(length_mark[1] * length_mark[0])
        print('Total elapsed: {:.4f}s'.format(total_cost).rjust(length_mark[0], ' '))
        print('')

    class ScopedCumulator:
        _start: float
        _node: _Node

        def __init__(self, name: str):
            self._name = name
            self._node = None

        def __enter__(self):
            self._start = time.time()
            if TimeMonitor.enable:
                self._node = TimeMonitor._root.add_child(_Node(self._name))
            return self._start

        def __exit__(self, *args):
            if TimeMonitor.enable and self._node is not None:
                TimeMonitor.add(self._node, time.time() - self._start)
                self._node.activate = False


TimeMonitor.init()


def timeit(freq=1, level=logging.DEBUG):
    """同样是函数计时，这个作为注解调用，直接打印时间

    为了防止一些函数频繁调用，日志太多，可以用 freq 控制
    - freq 每 `freq` 次调用打印一次日志，并且报告平均耗时
    - level 日志级别，默认 debug

    """

    count_map = defaultdict(list)
    if level == logging.INFO:
        log = lambda x: logger.info(x)
    elif level == logging.DEBUG:
        log = lambda x: logger.debug(x)
    else:
        raise ValueError

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            func_name = func.__qualname__

            with TimeMonitor.ScopedCumulator(func_name) as start_time:
                res = func(*args, **kwargs)

            cost_time = time.time() - start_time
            count_map[func_name].append(cost_time)
            if len(count_map[func_name]) < freq:
                return res

            sum_time = sum(count_map[func_name])
            avg_time = sum_time / freq
            count_map[func_name] = []

            with log_upper(1):
                log(
                    'func: {}, average time: {:.4f} s / {} loop = {:.4f} s'.format(
                        func_name, sum_time, freq, avg_time
                    )
                )
            return res

        return wrapper

    return decorator


def try_invoke(func):
    def log(log, resp, message, *args):
        with log_upper(1):
            msg = '[func=%s][address=%s] ' + message
            log(msg, func.__qualname__, resp.url, *args)

    @wraps(func)
    def inner(self, *args, **kwargs):
        resp = func(self, *args, **kwargs)

        if not isinstance(resp, requests.models.Response):
            return resp

        if resp.status_code != 200:
            log(logger.error, resp, 'status error: %d', resp.status_code)
            return

        try:
            resp_data = json.loads(resp.content)
            log(
                logger.info,
                resp,
                'response data: %s',
                json.dumps(resp_data, indent=4, ensure_ascii=False),
            )
        except json.decoder.JSONDecodeError:
            log(logger.error, resp, 'decode response data failed: %s', resp.content)
            return None

        if resp_data['code'] != 0:
            log(
                logger.error,
                resp,
                'request failed, code: %d, message: %s',
                resp_data['code'],
                resp_data['message'],
            )
            return None
        return resp_data.get('data', None)

    return inner


class BindedLoggerWrapper:
    def __init__(self, logger, bind_msg):
        self.logger = logger
        self.bind_msg = bind_msg
        self.depth = 0

    def info(self, msg, *args, **kwargs):
        with log_upper(self.depth):
            self.logger.info(f'{self.bind_msg} {msg}', *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        with log_upper(self.depth):
            self.logger.error(f'{self.bind_msg} {msg}', *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        with log_upper(self.depth):
            self.logger.exception(f'{self.bind_msg} {msg}', *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        with log_upper(self.depth):
            self.logger.warning(f'{self.bind_msg} {msg}', *args, **kwargs)


def bind_logger(logger, **kv):

    bind_msg = []
    for k in kv:
        bind_msg.append(f'[{k}={kv[k]}]')

    return BindedLoggerWrapper(logger, ''.join(bind_msg))


from contextlib import contextmanager


@contextmanager
def df_view(job, df: DataFrame, name: str):
    df.createOrReplaceTempView(name)
    logger.info(f'create temp view: {name}')
    try:
        yield name
    finally:
        job.spark.catalog.dropTempView(name)
        logger.info(f'drop temp view: {name}')


@contextmanager
def prt_table_view(job, table: str, name: str, dt: str, dt_name='dt'):
    """创建视图，尝试按照 dt 取，如果没有指定 dt 则取最新的 dt
    """
    if not dt:
        df = job.run_spark_sql(f'select * from {table}')
    else:
        prt_dts = (
            job.run_spark_sql('show partitions {}'.format(table)).rdd.flatMap(lambda x: x).collect()
        )
        dts = []
        for i in prt_dts:
            for prt in i.split('/'):
                n, v = prt.split('=')
                if n == dt_name:
                    dts.append(v)
        dt = dt if dt in dts or not dts else max(dts)
        df = job.run_spark_sql(f"select * from {table} where {dt_name}='{dt}'")

    df.createOrReplaceTempView(name)
    logger.info(f'create temp view: {name}')
    try:
        yield dt
    finally:
        job.spark.catalog.dropTempView(name)
        logger.info(f'drop temp view: {name}')


def run_pipe_cmd(cmd, *args, **kwargs):
    cmd = 'set -o pipefail; ' + cmd
    return run_cmd(cmd, *args, **kwargs)


def _run_cmd(cmd, logger, raise_now=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE):
    res = subprocess.run(
        cmd, stdout=stdout, stderr=stderr, shell=True, executable='/bin/bash'
    )  # nosec
    if res.returncode != 0:
        logger.error(f'cmd: {cmd} FAILED, rtn: {res}')
        if raise_now:
            raise RuntimeError(f'{res}')
    else:
        logger.info(f'cmd: {cmd} SUCCESSED!')

    return res


def run_cmd(cmd, logger, raise_now=True) -> int:
    return _run_cmd(cmd, logger, raise_now).returncode


def run_cmd_rtn(cmd, logger, raise_now=True) -> bytes:
    return _run_cmd(cmd, logger, raise_now).stdout


def new_tmp_file(suffix='') -> str:
    tmp_dir = tempfile.gettempdir()
    sub_dir = os.path.join(tmp_dir, 'bdasire')
    os.makedirs(sub_dir, exist_ok=True)

    tmp_file = tempfile.mktemp(suffix=suffix, dir=sub_dir)

    def _rm_tmp(name):
        try:
            if not os.path.exists(name):
                return
            if os.path.isfile(name):
                os.remove(name)
            else:
                shutil.rmtree(name, ignore_errors=True)
        except BaseException as e:
            logger.error(f'rm tmp file {name} error: {e}')

    atexit.register(_rm_tmp, tmp_file)
    return tmp_file


import numpy as np


def numpy_feature_to_df(spark, X: np.ndarray, y: np.ndarray) -> DataFrame:
    return spark.createDataFrame(
        [([float(ii) for ii in i], int(j)) for i, j in zip(X, y)], ['features', 'label']
    )


def camel_to_snake(name: str) -> str:
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


def rdd_iterate(rdd, chunk_size=100000):
    indexed_rows = rdd.zipWithIndex().cache()
    count = indexed_rows.count()
    logger.info('iterate through RDD of count {}'.format(count))
    start = 0
    end = start + chunk_size
    while start < count:
        logger.info('Grabbing new chunk: start = {}, end = {}'.format(start, end))
        chunk = indexed_rows.filter(lambda r: r[1] >= start and r[1] < end).collect()
        for row in chunk:
            yield row[0]
        start = end
        end = start + chunk_size
