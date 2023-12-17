# -*- coding: utf-8 -*-
# @File : unit_test.py
# @Author : r.yang
# @Date : Mon Apr 11 14:05:27 2022
# @Description :

import decimal
import functools
import json
import logging
import operator
import os
import random
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime

import numpy
import numpy as np
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest
from attr import dataclass

from algorithm.classifer import (
    ClassifierModelFactory,
    LGBIncrClassifierModel,
    SparkGBTClassifierModel,
)
from configs import Context, load_or_init_dag_cfg, lookup_dag_config
from configs.consts import FEAT_COL, LABEL_COL, PROBA_COL
from configs.init import base_config
from configs.init.common import get_group_cfg
from configs.model.base import ChannelInfo, ClassifierConfig, ItemInfo, Selector, TrainConfig
from configs.model.config import ChannelOperateConfig
from configs.utils import ConfigRegistry, ProdType, TagSelector, UserType
from core.job.registry import JobRegistry
from core.logger import LOGGER_NAME
from core.spark.table import TableInserter
from core.tr_logging import TRLogging
from core.tr_logging.color import ColorLoggerHanlder
from core.tr_logging.hive import HiveLoggerHandler
from core.utils import DateHelper, TimeMonitor, df_view, new_tmp_file, numpy_feature_to_df
from entrypoint import EntrypointFactory
from jobs.base import ConfigurableJob, HiveDagRuntimeConfigDAO
from jobs.init.gen_dag_config import LocalFetcher, gen_dag_config
from jobs.init.upload_dag_config import save_activity, save_products
from jobs.local.collect_reco_results import CollectRecoResults
from jobs.spark.crowd_package import CrowdPackage
from jobs.spark.feature import PrepareModelSample
from jobs.spark.model import FitRecoModel
from jobs.spark.operating_user import OperatingUser
from jobs.spark.post_proc import user_item_match
from jobs.spark.reco_s1106 import REQUIRED_KEYS, get_prod_output_info
from jobs.spark.reco_s1109 import RecoS1109
from jobs.spark.user_pool import DebOnlyUserPool, UserPool
from tests.conftest import TEST_BATCH_ID

# from tests.job_test import *  # noqa
from tests.lib import JobTestRunner
from tests.mocker.sv_mocker import MockClient


@functools.lru_cache(16)
def config(dag_id):
    config_path = tempfile.mktemp()
    print('config_path: ', config_path)
    dag_config = lookup_dag_config(dag_id)

    with open(config_path, 'w') as f:
        json.dump(ConfigRegistry.dumps_cfg(dag_config), f, indent=4, ensure_ascii=False)
    return config_path


@dataclass
class _Args:
    dag_id: str = ''
    batch_id: str = ''
    job_name: str = ''
    config_path: str = ''


def test_entrypoint_input_0():

    args = _Args()
    args.dag_id = 'reco_credit_card_ca_0000'
    args.batch_id = TEST_BATCH_ID
    args.job_name = 'CrowdPackage'
    args.config_path = config(args.dag_id)

    entry = EntrypointFactory.create(args)
    job = entry.build_jobs()[0]
    assert job.__class__.__name__ == args.job_name


def test_entrypoint_input_1():

    args = _Args()
    args.dag_id = 'reco_credit_card_ca_0000'
    args.batch_id = TEST_BATCH_ID
    args.job_name = 'UnfefinedJob'
    args.config_path = config(args.dag_id)

    with pytest.raises(ModuleNotFoundError):
        entry = EntrypointFactory.create(args)


def test_user_pool_split_0(spark):
    user_df = spark.createDataFrame([{'user_id': i} for i in range(1000)])
    group_weights = [('exp', 0.9), ('ctl', 0.1)]

    for i, df in UserPool._user_split(user_df, group_weights):
        if i == 0:
            assert df.first().user_type == 'exp'
            assert abs(df.count() - 900) < 30
        elif i == 1:
            assert df.first().user_type == 'ctl'
            assert abs(df.count() - 100) < 30


def test_user_pool_split_1(spark):
    user_df = spark.createDataFrame([{'user_id': i} for i in range(10)])
    group_weights = [('exp', 0.9), ('ctl', 0.1)]

    for i, df in UserPool._user_split(user_df.where('user_id=999'), group_weights):
        assert df.count() == 0


def test_user_model_infer_0():
    pass


def test_user_model_train_0():
    pass


def test_reco_model_infer_0():
    pass


def test_reco_model_train_0():
    pass


def test_user_pool_user_type_0(spark):
    ctx = Context.default('reco_credit_card_ca_0000', '20220103', DebOnlyUserPool.__name__)
    job = DebOnlyUserPool(ctx)

    cur_user = spark.createDataFrame(
        [{'user_id': f'{i:03d}', 'score': (100 - i) / 100} for i in range(200)]
    )
    pre_user = spark.createDataFrame(
        [
            {
                'user_id': f'{i:03d}',
                'user_type': 'exp' if i < 50 else 'ctl' if i < 65 else 'exp1',
                'score': i / 100,
            }
            for i in range(80)
        ]
    )

    df = job._with_user_type(cur_user, pre_user)
    res = {int(r.user_id): r for r in df.collect()}
    # 检查 user_type 继承
    for i in range(50):
        assert res[i].user_type == 'exp'
    for i in range(50, 65):
        assert res[i].user_type == 'ctl'
        # 检查分数继承
        assert res[i].score == i / 100
    for i in range(65, 80):
        assert res[i].user_type == 'exp1'

    # 剩下的 120 人中按比例随机切分
    cnt = 0
    for i in range(80, 200):
        if res[i].user_type == 'exp':
            cnt += 1
    assert 80 <= cnt <= 115


def test_user_pool_user_type_1(spark):
    ctx = Context.default('reco_credit_card_ca_0000', '20220104', DebOnlyUserPool.__name__)
    job = DebOnlyUserPool(ctx)

    cur_user = spark.createDataFrame(
        [{'user_id': f'{i:03d}', 'score': (100 - i) / 100} for i in range(200)]
    )
    pre_user = spark.createDataFrame(
        [],
        T.StructType(
            [
                T.StructField('user_id', T.StringType()),
                T.StructField('user_type', T.StringType()),
                T.StructField('score', T.FloatType()),
            ]
        ),
    )
    group_weights = job._calc_group_weights(10000000)

    exp_n = get_group_cfg('', '').group_users['exp'].num
    ctl_n = get_group_cfg('', '').group_users['ctl'].num
    ctl1_n = get_group_cfg('', '').group_users['ctl1'].num
    exp1_n = get_group_cfg('', '').group_users['exp1'].num

    assert int(dict(group_weights)[UserType.EXP.value]) == int(
        exp_n / (exp_n + ctl_n + exp1_n) * (10000000 - ctl_n * 1.1 * 2)
    )
    assert int(dict(group_weights)[UserType.CTL.value]) == int(ctl_n * 1.1)
    assert int(dict(group_weights)[UserType.CTL1.value]) == int(
        ctl1_n / (exp_n + exp1_n + ctl_n) * (10000000 - ctl_n * 1.1 * 2)
    )

    df = job._with_user_type(cur_user, pre_user)
    res = {int(r.user_id): r for r in df.collect()}

    cnt = 0
    for i in range(200):
        if res[i].user_type == 'exp':
            cnt += 1
    assert 120 <= cnt <= 175


def test_operate_user_0(spark):
    ctx = Context.default('reco_credit_card_ca_0000', '20220103', OperatingUser.__name__)
    job = OperatingUser(ctx)

    channel_df = spark.createDataFrame(
        [
            {
                'channel_id': 'TEST_CHANNEL',
                'banner_id': '',
                'plan_push': 1,
                'max_push': 4,
                'start_dt': '20211230',
                'end_dt': '20210105',
                'days_remain': 3,
                'feedback_delay': 2,
                'max_user': 10,
            }
        ]
    )

    user_num_df = spark.createDataFrame(
        [{'channel_id': 'TEST_CHANNEL', 'banner_id': '', 'user_type': 'exp', 'max_user': 10,}]
    )

    crowd_package = spark.createDataFrame(
        [
            {
                'user_id': '1',
                'user_type': 'exp',
                'channel_id': 'TEST_CHANNEL',
                'banner_id': '',
                'dag_id': 'reco_credit_card_ca_0000',
                'rank': 1,
                'dt': '20220102',
            }
        ]
    )
    user_pool = spark.createDataFrame(
        [
            {
                'user_id': '1',
                'user_type': 'exp',
                'score': 0.9,
                'dt': '20220103',
                'dag_id': 'reco_credit_card_ca_0000',
            },
            {
                'user_id': '2',
                'user_type': 'exp',
                'score': 0.8,
                'dt': '20220103',
                'dag_id': 'reco_credit_card_ca_0000',
            },
        ]
    )
    feedback = spark.createDataFrame(
        [
            {
                'user_id': '1',
                'user_type': 'exp',
                'channel_id': 'TEST_CHANNEL',
                'banner_id': '',
                'dag_id': 'reco_credit_card_ca_0000',
                'dt': '20220102',
                'send': 1,
            }
        ]
    )

    with df_view(job, crowd_package, 'crowd_package'), df_view(
        job, user_pool, 'user_pool'
    ), df_view(job, feedback, 'feedback'):
        job._base.output_table.user_pool = 'user_pool'
        job._base.output_table.crowd_package = 'crowd_package'
        job._base.output_table.crowd_feedback = 'feedback'
        user_df = job.get_all_user(channel_df, user_num_df)
        data = user_df.collect()
        for i in data:
            if i.user_id == '1':
                assert i.try_push == 1
                assert i.actual_push == 1
            else:
                assert i.try_push == 0
                assert i.actual_push == 0

        job._base.channels.append(
            ChannelInfo(
                **{
                    'channel_id': 'TEST_CHANNEL',
                    'channel_name': '',
                    'banner_id': '',
                    'max_push': 4,
                    'feedback_delay': 2,
                    'allow_repeat': False,
                    'num_items': 1,
                    'push_delay': 0,
                }
            )
        )

        job._scene.operating_user.channel_cfgs = [
            ChannelOperateConfig(channel_id='TEST_CHANNEL', banner_id='', group_users={},)
        ]
        user_df = job.filter_all_user(user_df)
        assert user_df.count() == 1 and user_df.first().user_id == '2'
        job._table_inserter.insert_df(
            user_df.withColumn('tag', F.lit('filtered')), dt=job.batch_id, dag_id=job.dag_id
        )

        user_df = job.get_unfinished_user(channel_df)
        assert user_df.count() == 1 and user_df.first().user_id == '2'
        assert user_df.first().tag == 'later'


def test_crowd_feedback_0(data):
    pass


def test_crowd_feedback_1(data):
    pass


def test_post_process_0(data):
    pass


def test_post_process_1(data):
    pass


def test_config_gen_0():
    tmpfile = new_tmp_file()
    assert not os.path.exists(tmpfile)
    cfg = load_or_init_dag_cfg(tmpfile)
    assert os.path.exists(tmpfile)
    cfg2 = load_or_init_dag_cfg(tmpfile)
    assert cfg == cfg2


def test_config_load_0():
    pass


def test_config_upload_0():
    ctx = Context.default('reco_credit_card_ca_0000', '20220101', CollectRecoResults.__name__)

    tmpfile = new_tmp_file()
    assert not os.path.exists(tmpfile)
    aio_cfg = load_or_init_dag_cfg(tmpfile)

    client = MockClient()
    client.register_rtn('get', 'activities', [])
    client.register_rtn('post', 'activities', None)
    client.register_rtn('post', 'products', None)

    def check_post_param(body):
        assert isinstance(body, list)
        assert len(body) == sum(
            [len([j for j in i.actvs if j.to_BI]) for i in aio_cfg.post_proc_dags]
        )
        for i in body:
            if i['actv_id'].startswith('T'):
                assert i['scene_name'].endswith('(测试)')
            assert i['batch_id'] == '20220101'

    client.register_check_callback('post', 'activities', check_post_param)
    save_activity(
        '20220101',
        client,
        aio_cfg.post_proc_dags,
        [i.scene for i in aio_cfg.reco_dags],
        aio_cfg.base_dag.base,
    )
    save_products(
        '20220101', client, aio_cfg.base_dag.base.items,
    )

    client = MockClient()
    client.register_rtn('get', 'activities', [{'actv_id': 'T1011', 'user_count': 100}])
    client.register_rtn('post', 'activities', None)

    def check_post_param2(body):
        assert isinstance(body, list)
        assert len(body) == sum(
            [len([j for j in i.actvs if j.to_BI]) for i in aio_cfg.post_proc_dags]
        )
        for i in body:
            if i['actv_id'] == 'T1011':
                assert i['user_count'] == 100
            else:
                assert i['user_count'] == 0

    client.register_check_callback('post', 'activities', check_post_param2)
    save_activity(
        '20220101',
        client,
        aio_cfg.post_proc_dags,
        [i.scene for i in aio_cfg.reco_dags],
        aio_cfg.base_dag.base,
    )


@pytest.mark.skip()
def test_scene_update_0():
    ctx = Context.default('reco_credit_card_ca_0000', '20220101', CollectRecoResults.__name__)
    job = CollectRecoResults(ctx)

    job.offline_backend.post(
        'activity', {'actv_id': 'T1011', 'batch_id': '20220101', 'user_count': 100}
    )
    data = job.offline_backend.get('activities', {'batch_id': '20220101'})
    for i in data:
        if i['actv_id'] == 'T1011':
            assert i['user_count'] == 100


def test_spark_entry(data):
    @dataclass
    class _Args:
        dag_id: str = ''
        batch_id: str = ''
        job_name: str = ''
        config_path: str = ''

    args = _Args()
    args.dag_id = 'reco_credit_card_ca_0000'
    args.batch_id = TEST_BATCH_ID
    args.job_name = 'CrowdFeedback'
    args.config_path = config(args.dag_id)

    entry = EntrypointFactory.create(args)
    entry.run(entry.build_jobs())


def test_hive_log(table):
    ctx = Context.default('base', '2020-10-31', 'test')
    TRLogging.resetable = True
    TRLogging.reset(HiveLoggerHandler(ctx), ColorLoggerHanlder())

    @JobRegistry.register(JobRegistry.T.BASE)
    class MockJob(ConfigurableJob):
        def __init__(self, ctx):
            super().__init__(ctx)

        def run(self):
            self.logger.info('info log')
            self.logger.warning('warning log')
            self.logger.error('error log')

            # test buffer flush
            logger = logging.getLogger(LOGGER_NAME)
            for handler in list(logger.handlers):
                if isinstance(handler, HiveLoggerHandler):
                    for i in range(getattr(handler, '_HiveLoggerHandler__capacity') + 1):
                        self.logger.info(f'info log {i}')
                    break
            else:
                raise RuntimeError('HiveLoggerHandler register failed')

    JobTestRunner(MockJob).run('base', '2020-10-31')
    TRLogging.unload_handlers()

    TRLogging.reset(ColorLoggerHanlder())


def test_runtime_config(table):
    ctx = Context.default('base', '2020-10-31', 'test')
    dao = HiveDagRuntimeConfigDAO(ctx.base)
    dao.create(ctx, {'test_key': 'test_value'})
    cfg = dao.get(ctx)
    assert cfg['test_key'] == 'test_value'


def test_classifier_model(spark):

    import xgboost as xgb
    from sklearn.datasets import make_classification

    num_features = 5

    X, y = make_classification(n_samples=100, n_features=num_features, n_classes=2)
    xgb_model = xgb.XGBClassifier(n_jobs=1).fit(X, y)
    model_path = '/tmp/bos_nas/v0.0/xgb.json'
    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    xgb_model.save_model(model_path)

    cfg = ClassifierConfig(
        model_dir='/tmp/bos_nas',
        model_version='v0.0',
        model_clz='XGBClassifierModel',
        feat_version='v0.4',
    )
    model = ClassifierModelFactory.create(cfg)

    data = [{'feature': [random.random() for i in range(num_features)]} for _ in range(100)]
    # test if feature is null
    data.append({'feature': None})
    df = spark.createDataFrame(data)
    df = model.transform(df, feature_col='feature')
    scores = df.select(PROBA_COL).rdd.flatMap(lambda x: x).collect()
    pos_scores = [i[1] for i in scores]

    numpy.testing.assert_array_less(pos_scores, 1)
    numpy.testing.assert_array_compare(operator.__ge__, pos_scores, 0)
    assert pos_scores[-1] == 0.0


def test_date_helper():
    helper = DateHelper('20220430', 2)
    assert helper.gap(helper.push_start_date()).days == 1
    assert helper.push_start_date().str == '20220429'
    assert helper.push_end_date().str == '20220529'

    plan_push, start_dt, end_dt = helper.range_info(2)
    assert plan_push == 1
    assert start_dt == '20220429'
    assert end_dt == '20220513'

    helper = DateHelper('20220513', 2)
    plan_push, start_dt, end_dt = helper.range_info(2)
    assert plan_push == 1
    assert start_dt == '20220429'
    assert end_dt == '20220513'

    helper = DateHelper('20220514', 2)
    plan_push, start_dt, end_dt = helper.range_info(2)
    assert plan_push == 2
    assert start_dt == '20220514'
    assert end_dt == '20220529'

    assert helper.add_day(1).str == '20220515'
    assert helper.add_day(-1).str == '20220513'
    assert helper.date_before_react_delay.str == '20220512'
    assert helper.get_before_date(helper.from_str('20220501'), 1).str == '20220430'


def test_tag_selector():
    items = base_config().items
    selector = {'type': ProdType.CREDIT}
    res = TagSelector(items).query(**selector)
    assert len(res) == len([1 for i in items if i.type == ProdType.CREDIT])


def test_user_item_match():
    items = base_config().items
    users = base_config().test_users
    item_selector: Selector = {'type': ProdType.DEBIT}
    user_selector: Selector = {'user_id': ['3127852100', '3095681436']}
    res = user_item_match(users, items, user_selector, item_selector)
    assert len(res) == 2 * len(TagSelector(items).query(**item_selector))


def test_time_monitor():
    tmp = TimeMonitor.enable
    TimeMonitor.enable = True

    try:
        with TimeMonitor.ScopedCumulator('test_time_monitor'):
            time.sleep(1)
        for node in TimeMonitor._count:
            if node.name == 'test_time_monitor':
                assert TimeMonitor._count[node] == 1
        TimeMonitor.report()
    finally:
        TimeMonitor.enable = tmp


def test_dag_config_gen():
    tmp_file = new_tmp_file()
    load_or_init_dag_cfg(tmp_file)

    class _Arg:
        config_path = tmp_file

    fetcher = LocalFetcher(_Arg())
    cfg: dict = gen_dag_config(fetcher, 'reco_credit_card_ca_0000', TEST_BATCH_ID)
    assert cfg['__clz'] == 'RecoDagConfig'
    ConfigRegistry.parse_cfg(cfg)

    cfg: dict = gen_dag_config(fetcher, 'base', TEST_BATCH_ID)
    assert cfg['__clz'] == 'BaseDagConfig'
    ConfigRegistry.parse_cfg(cfg)

    cfg: dict = gen_dag_config(fetcher, 'post_proc_sms', TEST_BATCH_ID)
    assert cfg['__clz'] == 'PostProcessDagConfig'
    ConfigRegistry.parse_cfg(cfg)


def test_dataframe_gather(spark):
    df = spark.createDataFrame(
        [
            # 我们的
            ('a', 'popup', 'pid_00', 'ptyp_0', 0, 0),
            ('a', 'popup', 'pid_01', 'ptyp_0', 1, 0),
            ('a', 'popup', 'pid_02', 'ptyp_0', 2, 0),
            ('b', 'popup', 'pid_03', 'ptyp_0', 0, 0),
            # 隔壁的
            ('a', 'popup', 'pid_10', 'ptyp_1', 0, 0),
            ('a', 'popup', 'pid_11', 'ptyp_1', 1, 0),
            ('c', 'popup', 'pid_12', 'ptyp_1', 0, 0),
        ],
        ('user_id', 'banner_id', 'prod_id', 'prod_type', 'rank', 'slot'),
    )
    # schema = T.StructType([
    #     T.StructField("user_id", T.StringType()),
    #     T.StructField("banner_id", T.StringType()),
    #     T.StructField("prod_id", T.StringType()),
    #     T.StructField("prod_type", T.StringType()),
    #     T.StructField("rank", T.IntegerType()),
    # ])

    priority = {'ptyp_0': 1, 'ptyp_1': 2}

    @F.pandas_udf(T.StringType(), functionType=F.PandasUDFType.GROUPED_AGG)
    def merge_prod(prod_ids, prod_types, ranks, slots):
        tuples = list(zip(prod_ids, prod_types, ranks, slots))
        tuples = sorted(tuples, key=lambda x: x[2] - 10 * priority.get(x[1], 0))

        return json.dumps([{'code': i[0], 'type': i[1], 'slot': i[3],} for i in tuples])

    df_agg = df.groupBy('user_id', 'banner_id').agg(
        merge_prod('prod_id', 'prod_type', 'rank', 'slot').alias('content')
    )

    df_agg.show(10, False)

    @F.pandas_udf(T.StringType(), functionType=F.PandasUDFType.GROUPED_AGG)
    def merge_banner(banner_ids, contents):
        return json.dumps(
            [{'positionId': i[0], 'prdList': i[1]} for i in zip(banner_ids, contents)]
        )

    df_agg.groupBy('user_id').agg(merge_banner('banner_id', 'content')).alias('data').show(
        10, False
    )


def test_lgb_incr_model_train(spark):
    df = spark.createDataFrame([{'user_id': i} for i in range(1000)])
    df = df.withColumn(LABEL_COL, F.when(F.rand() > 0.5, 1).otherwise(0))

    @F.udf(returnType=T.ArrayType(T.DoubleType()))
    def mock_feature():
        return [1.0 * random.choice([0, 1]) for _ in range(10)]

    df = df.withColumn(FEAT_COL, mock_feature())
    model_cfg = ClassifierConfig(
        feat_version='v0.4',
        model_version='test',
        model_clz=LGBIncrClassifierModel.__name__,
        model_dir='/tmp/bos_hdfs/model',
    )
    model_cfg.custom_train_args['batch_num'] = 2
    model = ClassifierModelFactory.create(model_cfg)
    model.fit_df(df)


def test_spark_gbt_model_train(spark):
    df = spark.createDataFrame([{'user_id': i} for i in range(1000)])
    df = df.withColumn(LABEL_COL, F.when(F.rand() > 0.5, 1).otherwise(0))

    @F.udf(returnType=T.ArrayType(T.FloatType()))
    def mock_feature():
        return [1.0 * random.choice([0, 1]) for _ in range(10)]

    df = df.withColumn(FEAT_COL, mock_feature())
    model_cfg = ClassifierConfig(
        feat_version='v0.4',
        model_version='test',
        model_clz=SparkGBTClassifierModel.__name__,
        model_dir='/tmp/bos_hdfs/model',
    )
    model = ClassifierModelFactory.create(model_cfg)
    model.fit_df(df)


def test_reco_model_fit_padding(spark):
    ctx = Context.default('reco_debit_card_ca_0000', '20220103', FitRecoModel.__name__)
    job = FitRecoModel(ctx)
    model = ClassifierModelFactory.create(job._ranking.model)

    X = np.random.randn(100, 20)
    y = np.random.randint(len(job.items) - 1, size=100)
    y[y == 5] = 1
    df = numpy_feature_to_df(spark, X, y)
    job._fit(model, df)


def test_train_cfg():
    cfg1 = TrainConfig(train_sample_month_num=5, eval_sample_month_num=1)
    assert cfg1._sample_dt.month == 3
    assert cfg1.train_sample_month == ['202201', '202112', '202111', '202110', '202109']
    assert cfg1.eval_sample_month == ['202202']

    cfg1 = TrainConfig(train_sample_month_num=5, eval_sample_month_num=2)
    assert cfg1._sample_dt.month == 3
    assert cfg1.train_sample_month == ['202112', '202111', '202110', '202109', '202108']
    assert cfg1.eval_sample_month == ['202202', '202201']

    cfg1 = TrainConfig(sample_dt='20220201', train_sample_month_num=5, eval_sample_month_num=1)
    assert cfg1._sample_dt.month == 2
    assert cfg1.train_sample_month == ['202112', '202111', '202110', '202109', '202108']
    assert cfg1.eval_sample_month == ['202201']

    cfg1 = TrainConfig(sample_dt='20220101', train_sample_month_num=5, eval_sample_month_num=1)
    assert cfg1._sample_dt.month == 1
    assert cfg1.train_sample_month == ['202111', '202110', '202109', '202108', '202107']
    assert cfg1.eval_sample_month == ['202112']

    cfg1 = TrainConfig(sample_dt=None, train_sample_month_num=5, eval_sample_month_num=1)
    assert cfg1._sample_dt.month == datetime.today().month


def test_table_inserter_alter_partition(spark, table):

    table_inserter = TableInserter(PrepareModelSample.output_table_info_list(base_config())[0])
    assert not table_inserter.has_parition(dt='20000101', tag='test_tag')
    df = spark.createDataFrame([{'user_id': '1', 'item_id': '1', 'label': '1', 'sample_dt': ''}])
    table_inserter.insert_df(df, dt='20000101', tag='test_tag')
    assert table_inserter.has_parition(dt='20000101', tag='test_tag')

    table_inserter.drop_partitions(dt='20000101', tag='test_tag')
    assert not table_inserter.has_parition(dt='20000101', tag='test_tag')


def test_rerank():
    items = [
        ItemInfo(item_ids=['00'], desc='', label=0, type=ProdType.CREDIT,),
        ItemInfo(item_ids=['01'], desc='', label=1, type=ProdType.CREDIT,),
    ]
    channel_cfgs = [
        ChannelOperateConfig(channel_id='', banner_id='', group_users={}, items_specified=[],)
    ]

    rerank = CrowdPackage._rerank_fun(items, channel_cfgs)
    items = rerank([0.1, 0.2], '', '', 'exp')
    assert [i.item_id for i in items] == ['01', '00']


def test_rerank2():
    items = [
        ItemInfo(item_ids=['00'], desc='', label=0, type=ProdType.CREDIT,),
        ItemInfo(item_ids=['01'], desc='', label=1, type=ProdType.CREDIT,),
    ]
    channel_cfgs = [
        ChannelOperateConfig(channel_id='', banner_id='', group_users={}, items_specified=[],)
    ]

    rerank = CrowdPackage._rerank_fun(items, channel_cfgs)
    items = rerank([0.1, 0.2, 0.3], '', '', 'exp')
    assert [i.item_id for i in items] == ['01', '00']


def test_rerank3():
    items = [
        ItemInfo(item_ids=['00'], desc='', label=0, type=ProdType.CREDIT,),
        ItemInfo(item_ids=['01'], desc='', label=1, type=ProdType.CREDIT,),
    ]
    channel_cfgs = [
        ChannelOperateConfig(channel_id='', banner_id='', group_users={}, items_specified=[],)
    ]

    rerank = CrowdPackage._rerank_fun(items, channel_cfgs)
    items = rerank([0.1], '', '', 'exp')
    assert [i.item_id for i in items] == ['00']


def test_rerank4():
    items = [
        ItemInfo(item_ids=['00'], desc='', label=0, type=ProdType.CREDIT,),
        ItemInfo(item_ids=['02'], desc='', label=2, type=ProdType.CREDIT,),
    ]
    channel_cfgs = [
        ChannelOperateConfig(channel_id='', banner_id='', group_users={}, items_specified=[],)
    ]

    rerank = CrowdPackage._rerank_fun(items, channel_cfgs)
    items = rerank([0.1, 0.2, 0.3], '', '', 'exp')
    assert [i.item_id for i in items] == ['02', '00']


def test_rerank_ctl_group():
    items = [
        ItemInfo(item_ids=['00'], desc='', label=0, type=ProdType.CREDIT,),
        ItemInfo(item_ids=['01'], desc='', label=1, type=ProdType.CREDIT,),
    ]
    channel_cfgs = [
        ChannelOperateConfig(channel_id='', banner_id='', group_users={}, items_specified=[],)
    ]

    rerank = CrowdPackage._rerank_fun(items, channel_cfgs)

    all_items = []
    for _ in range(10):
        items = rerank([0.1, 0.2], '', '', 'ctl')
        all_items.append(tuple(i.item_id for i in items))

    assert set(all_items) == {('01', '00'), ('00', '01')}


def test_rerank_exp_group():
    items = [
        ItemInfo(item_ids=['00'], desc='', label=0, type=ProdType.CREDIT,),
        ItemInfo(item_ids=['01'], desc='', label=1, type=ProdType.CREDIT,),
    ]
    channel_cfgs = [
        ChannelOperateConfig(channel_id='', banner_id='', group_users={}, items_specified=[],)
    ]

    rerank = CrowdPackage._rerank_fun(items, channel_cfgs)

    all_items = []
    for _ in range(10):
        items = rerank([0.1, 0.2], '', '', 'exp')
        all_items.append(tuple(i.item_id for i in items))

    assert set(all_items) == {('01', '00')}


def test_rerank_item_spec():
    items = [
        ItemInfo(item_ids=['00'], desc='', label=0, type=ProdType.CREDIT,),
        ItemInfo(item_ids=['01'], desc='', label=1, type=ProdType.CREDIT,),
    ]
    channel_cfgs = [
        ChannelOperateConfig(
            channel_id='', banner_id='', group_users={}, items_specified=['01', '02', '03'],
        )
    ]

    rerank = CrowdPackage._rerank_fun(items, channel_cfgs)

    items = rerank([0.1, 0.2], '', '', 'exp')
    assert [i.item_id for i in items] == ['01']


def test_rerank_item_spec2():
    items = [
        ItemInfo(item_ids=['00'], desc='', label=0, type=ProdType.CREDIT,),
        ItemInfo(item_ids=['01'], desc='', label=1, type=ProdType.CREDIT,),
    ]
    channel_cfgs = [
        ChannelOperateConfig(
            channel_id='', banner_id='', group_users={}, items_specified=['02', '03'],
        )
    ]

    rerank = CrowdPackage._rerank_fun(items, channel_cfgs)

    items = rerank([0.1, 0.2], '', '', 'exp')
    assert [i.item_id for i in items] == ['02', '03']


def test_rerank_with_prop():
    items = [
        ItemInfo(item_ids=['00'], desc='', label=0, type=ProdType.CREDIT,),
        ItemInfo(item_ids=['01'], desc='', label=1, type=ProdType.CREDIT,),
        ItemInfo(item_ids=['03'], desc='', label=2, type=ProdType.CREDIT, prop=0.5,),
    ]
    channel_cfgs = [
        ChannelOperateConfig(channel_id='', banner_id='', group_users={}, items_specified=[],)
    ]

    rerank = CrowdPackage._rerank_fun(items, channel_cfgs)

    cnt_map = {'with': 0, 'without': 0}
    iter_num = 100
    for _ in range(iter_num):
        items = rerank([0.1, 0.2], '', '', 'exp')
        items = [i.item_id for i in items]
        if items == ['03', '01', '00']:
            cnt_map['with'] += 1
        elif items == ['01', '00']:
            cnt_map['without'] += 1
    assert cnt_map['with'] + cnt_map['without'] == iter_num
    assert abs(cnt_map['with'] - iter_num // 2) <= iter_num * 0.2


def test_rerank_with_prop2():
    items = [
        ItemInfo(item_ids=['00'], desc='', label=0, type=ProdType.CREDIT,),
        ItemInfo(item_ids=['01'], desc='', label=1, type=ProdType.CREDIT,),
        ItemInfo(item_ids=['03'], desc='', label=2, type=ProdType.CREDIT, prop=0.5,),
    ]
    channel_cfgs = [
        ChannelOperateConfig(channel_id='', banner_id='', group_users={}, items_specified=[],)
    ]

    rerank = CrowdPackage._rerank_fun(items, channel_cfgs)

    cnt_map = {'with': 0, 'without': 0}
    iter_num = 100
    for _ in range(iter_num):
        items = rerank([0.1, 0.2, 0.0], '', '', 'exp')
        items = [i.item_id for i in items]
        if items == ['03', '01', '00']:
            cnt_map['with'] += 1
        elif items == ['01', '00', '03']:
            cnt_map['without'] += 1
        else:
            print(items)
    assert cnt_map['with'] + cnt_map['without'] == iter_num
    assert abs(cnt_map['with'] - iter_num // 2) <= iter_num * 0.2


@pytest.mark.parametrize(
    'data,answer_key,answer_value',
    [
        ({'prod_id': 'J172411SA799'}, 'msgmodel_id', 'chrem_J172411SA799'),
        ({'prod_id': 'X', 'reserve1': 'X'}, 'msgmodel_id', ''),
        ({'prod_id': 'X', 'reserve1': '1301'}, 'msgmodel_id', 'chrem_1301'),
        ({'prod_id': 'X', 'reserve1': None}, 'msgmodel_id', ''),
        ({'nav': ' '}, 'nav', ''),
        ({'nav': '1.042'}, 'nav', '产品净值1.042'),
        ({'nav': '1.042000 '}, 'nav', '产品净值1.042'),
        ({'nav': '1.042000 '}, 'nav', '产品净值1.042'),
        ({'nav': '1.0000 '}, 'nav', '产品净值1.0'),
        ({'nav_date': '20230101 '}, 'nav_date', '截至2023/01/01'),
        ({'nav_date': '  '}, 'nav_date', ''),
        ({'ipo_start_date': 20230101}, 'ipo_start_date', '2023/01/01'),
        ({'ipo_start_date': '  '}, 'ipo_start_date', ''),
        ({'ipo_start_date': '2023/01/01 '}, 'ipo_start_date', ''),
        ({'risk_level': '0'}, 'risk_level', ''),
        ({'risk_level': '0 '}, 'risk_level', ''),
        ({'risk_level': ' '}, 'risk_level', ''),
        ({'risk_level': ' 1 '}, 'risk_level', '风险等级极低风险'),
        ({'risk_level': ' 6 '}, 'risk_level', ''),
        ({'benchmark': ''}, 'benchmark', ''),
        ({'benchmark': '一年定期基准利率4.0%'}, 'benchmark', '业绩比较基准：一年定期基准利率4.0%'),
        ({'benchmark': '4.0%'}, 'benchmark', '业绩比较基准：4.0%'),
        ({'benchmark': '4.0%~5.2%'}, 'benchmark', '业绩比较基准：4.0%-5.2%'),
        ({'nav_ratio': ''}, 'nav_ratio', ''),
        ({'nav_ratio': '0'}, 'nav_ratio', ''),
        ({'nav_ratio': '0.0'}, 'nav_ratio', ''),
        ({'nav_ratio': '0.000'}, 'nav_ratio', ''),
        ({'nav_ratio': '0.012000'}, 'nav_ratio', '上一投资周期年化收益率1.2%、'),
        ({'term_enjoy_type': '0'}, 'term_enjoy_type', '每日'),
        ({'term_enjoy_type': 1}, 'term_enjoy_type', '每周'),
        ({'term_enjoy_type': ''}, 'term_enjoy_type', ''),
        ({'guest_type': '0'}, 'guest_type', '成立以来年化收益率'),
        ({'guest_type': 'C', 'benchmark_show': '1.1%'}, 'guest_type', '1.1%'),
        ({'estab_ratio': '0.0156'}, 'estab_ratio', '1.56%'),
        ({'estab_ratio': 0.0156}, 'estab_ratio', '1.56%'),
        ({'estab_ratio': decimal.Decimal('0E-8')}, 'estab_ratio', ''),
        ({'estab_ratio': decimal.Decimal('-7.30000')}, 'estab_ratio', '-730.0%'),
        ({'estab_ratio': '0.015600000'}, 'estab_ratio', '1.56%'),
        ({'estab_ratio': ''}, 'estab_ratio', ''),
        ({'lock_days': '0'}, 'tb_reco_three', ''),
        ({'lock_days': ''}, 'tb_reco_three', ''),
        ({'lock_days': 0}, 'tb_reco_three', ''),
        ({'lock_days': 6}, 'tb_reco_three', 6),
        ({'lock_days': '7'}, 'tb_reco_three', '7'),
    ],
)
def test_s1106_prod_info_convert(data: dict, answer_key: str, answer_value):
    for k in REQUIRED_KEYS + ['prd_invest_type', 'lock_days', 'prod_id']:
        data[k] = data.get(k, '')

    new_data = get_prod_output_info(data, 's1106')
    assert new_data[answer_key] == answer_value


def test_s1109_truncate_prod_list_0(spark):
    df = spark.createDataFrame(
        [
            {'cust_id': 'c0', 'prod_list': ['p0', 'p1', 'p2', 'p4']},
            {'cust_id': 'c1', 'prod_list': None},
            {'cust_id': 'c2', 'prod_list': ['p5', 'p0']},
            {'cust_id': 'c3', 'prod_list': ['p5']},
        ]
    )

    prod_info = {
        'p0': {'rct1y_profit_rate': 0.1},
        'p1': {'rct1y_profit_rate': 0.2},
        'p2': {'rct1y_profit_rate': 0.3},
        'p3': {'rct1y_profit_rate': 0.4},
        'p4': {'rct1y_profit_rate': 0.4},
        'p5': {'rct1y_profit_rate': None},
    }

    res = RecoS1109._truncate_prod_list(df, 1, prod_info)
    res = {row['cust_id']: row.asDict() for row in res.collect()}
    assert res['c0']['prod_list'] == ['p4']
    assert not res['c1']['prod_list']
    assert res['c2']['prod_list'] == ['p0']
    assert not res['c3']['prod_list']

    res = RecoS1109._truncate_prod_list(df, 3, prod_info)
    res = {row['cust_id']: row.asDict() for row in res.collect()}
    assert res['c0']['prod_list'] == ['p4', 'p2', 'p1']
    assert not res['c1']['prod_list']
    assert res['c2']['prod_list'] == ['p0']
    assert not res['c3']['prod_list']


def test_struct_zip(spark):
    from pyspark.sql import Row

    df = spark.createDataFrame([{'user_id': i} for i in range(10)])

    def struct_zip(user_id):
        return [
            Row(item_id=f'{user_id}_1', score=0.9, rank=1),
            Row(item_id=f'{user_id}_2', score=0.8, rank=2),
        ]

    udf = F.udf(
        struct_zip,
        returnType=T.ArrayType(
            T.StructType(
                [
                    T.StructField('item_id', T.StringType()),
                    T.StructField('score', T.FloatType()),
                    T.StructField('rank', T.IntegerType()),
                ]
            )
        ),
    )
    df = (
        df.withColumn('item_score', udf('user_id'))
        .withColumn('zip', F.explode('item_score'))
        .select(
            'user_id',
            F.col('zip.item_id').alias('item_id'),
            F.col('zip.score').alias('score'),
            F.col('zip.rank').alias('rank'),
        )
    )
    df.show()
