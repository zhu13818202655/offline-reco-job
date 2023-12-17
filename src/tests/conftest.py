# -*- coding: utf-8 -*-
# @File : conftest.py
# @Author : r.yang
# @Date : Tue Feb 22 14:00:42 2022
# @Description : conftest of pytest

import os

os.environ['LOG_LEVEL'] = 'WARNING'
os.environ['RECO_ENVIRONMENT'] = 'local'  # noqa
os.environ['OFFLINE_BACKEND_ADDR'] = ''
os.environ['ONLINE_BACKEND_ADDR'] = ''
os.environ['LOWCODE_URL'] = ''


import pyspark
import pytest
from pyspark.sql import SparkSession

from configs.init.common import get_reco_db
from core.tr_logging import TRLogging
from core.tr_logging.color import ColorLoggerHanlder
from jobs.spark.prepare import PrepareTable
from tests.jobs.load_data import LoadData
from tests.lib import JobTestRunner
from tests.mocker.op_user_test import mock_crowd_feedback, mock_crowd_package, mock_user_pool

TRLogging.once(ColorLoggerHanlder())


@pytest.fixture(scope='session')
def spark():
    # session
    os.environ['SPARK_HOME'] = os.path.dirname(pyspark.__file__)
    spark_session = (
        SparkSession.builder.enableHiveSupport()
        .config('spark.sql.crossJoin.enabled', 'true')
        .getOrCreate()
    )
    spark_session.sql('set hive.stats.autogather=false')
    spark_session.sql('create database if not exists {}'.format(get_reco_db()))
    spark_session.sparkContext.setLogLevel('ERROR')

    log4jLogger = spark_session.sparkContext._jvm.org.apache.log4j
    log4jLogger.LogManager.getLogger('org.apache.parquet').setLevel(log4jLogger.Level.ERROR)
    log4jLogger.LogManager.getLogger('org').setLevel(log4jLogger.Level.ERROR)
    log4jLogger.LogManager.getLogger('hive.ql.metadata.Hive').setLevel(log4jLogger.Level.ERROR)

    yield spark_session

    # tear up
    JobTestRunner.hook(TRLogging.unload_handlers)
    JobTestRunner.destroy()


TEST_BATCH_ID = '20220101'


@pytest.fixture(scope='session')
def table(spark):
    job = JobTestRunner(PrepareTable).run('base', TEST_BATCH_ID)
    yield spark, job.ctx


@pytest.fixture(scope='session')
def data(table):
    path = os.path.join(os.path.dirname(__file__), '../../mocker/.runtime/data')

    mock_user_pool(path)
    mock_crowd_package(path)
    mock_crowd_feedback(path)
    job = JobTestRunner(LoadData).run('base', TEST_BATCH_ID)
    yield spark, job.ctx
