# -*- coding: utf-8 -*-
# @File : job_test.py
# @Author : r.yang
# @Date : Tue Feb 22 13:38:31 2022
# @Description : test spark jobs


import json
import os

import pytest

from client.redis import RedisClientRegistry
from configs import Context
from configs.init import base_config, output_table
from configs.model.base import ChannelInfo
from configs.model.config import ChannelOperateConfig, GroupInfo
from configs.utils import ConfigRegistry, ProdType, UserType
from core.spark.table import TableInserter
from jobs.local.cleanup import Cleanup
from jobs.local.collect_reco_results import CollectRecoResults
from jobs.spark.cancel_user import CancelUser
from jobs.spark.crowd_feedback import CrowdFeedback
from jobs.spark.crowd_package import CrowdPackage
from jobs.spark.daily_report import AggReport, AggReportSync, DailyReportSMS
from jobs.spark.feature import (
    FitFeatureEngine,
    PrepareCancelModelSample,
    PrepareCreditModelSample,
    PrepareDebitModelSample,
    UserFeatures,
)
from jobs.spark.ire_model_result import CollectIREModelResults, ModelResultsToDW
from jobs.spark.ire_user_pred_result import CollectIREUserPredResults, UserPredResultsToDW
from jobs.spark.model import (
    EvalCancelUserModel,
    EvalRecoModel,
    EvalUserModel,
    FitCancelUserModel,
    FitRecoModel,
    FitUserModel,
    Inference,
)
from jobs.spark.operating_user import OperatingUser
from jobs.spark.post_proc import PostProcessAPP, PostProcessSMS, PostProcessWhitelist
from jobs.spark.prepare import CleanTable
from jobs.spark.reco_s1101 import RecoS1101
from jobs.spark.reco_s1106 import RecoS1106
from jobs.spark.reco_s1108 import RecoS1108
from jobs.spark.reco_s1109 import RecoS1109
from jobs.spark.reco_s1110 import RecoS1110
from jobs.spark.user_percentile import UserPercentile
from jobs.spark.user_pool import CampCreOnlyUserPool, CreOnlyUserPool, DebOnlyUserPool
from tests.conftest import TEST_BATCH_ID
from tests.lib import JobTestRunner
from tests.mocker.lowcode_mocker import Lowcode
from tests.mocker.redis_mock import RedisMock
from tests.mocker.sv_mocker import MockClient


def train_dt_callback(ctx: Context) -> Context:
    dag_config = ConfigRegistry.parse_cfg(ctx.runtime_config)
    dag_config.base.feat_engines[0].train_dt = '20220101'
    ctx.runtime_config = ConfigRegistry.dumps_cfg(dag_config)
    return ctx


@pytest.fixture
def feature_engine(data):
    # 训练 pipeline，生成特征: pytest -s src/tests -k feature_engine
    JobTestRunner(FitFeatureEngine).run('base', TEST_BATCH_ID, train_dt_callback)
    JobTestRunner(UserFeatures).run('base', TEST_BATCH_ID)


@pytest.fixture
def prepare_sample(feature_engine):
    JobTestRunner(PrepareDebitModelSample).run('reco_debit_card_ca_0000', TEST_BATCH_ID)
    JobTestRunner(PrepareCreditModelSample).run('reco_credit_card_ca_0000', TEST_BATCH_ID)


@pytest.fixture
def user_model(prepare_sample):
    # 训练用户召回模： pytest -s src/tests -k user_model
    JobTestRunner(FitUserModel).run('reco_credit_card_ca_0000', TEST_BATCH_ID, train_dt_callback)
    JobTestRunner(FitUserModel).run('reco_debit_card_ca_0000', TEST_BATCH_ID, train_dt_callback)


def test_user_eval(user_model):
    JobTestRunner(EvalUserModel).run('reco_credit_card_ca_0000', TEST_BATCH_ID)


def test_reco_eval(reco_model):
    JobTestRunner(EvalRecoModel).run('reco_credit_card_ca_0000', TEST_BATCH_ID)


def test_user_model(user_model):
    pass


@pytest.fixture
def reco_model(prepare_sample):
    # 训练用户召回模： pytest -s src/tests -k reco_model
    JobTestRunner(FitRecoModel).run('reco_credit_card_ca_0000', TEST_BATCH_ID, train_dt_callback)
    JobTestRunner(FitRecoModel).run('reco_debit_card_ca_0000', TEST_BATCH_ID, train_dt_callback)


def test_reco_model(reco_model):
    pass


def test_crowd_package(reco_model):
    JobTestRunner(CrowdPackage).run('reco_credit_card_ca_0000', TEST_BATCH_ID)
    JobTestRunner(CrowdPackage).run('reco_debit_card_ca_0000', TEST_BATCH_ID)


@pytest.fixture
def inference(reco_model):
    JobTestRunner(Inference).run('reco_credit_card_ca_0000', TEST_BATCH_ID)
    JobTestRunner(Inference).run('reco_debit_card_ca_0000', TEST_BATCH_ID)


def test_inference(inference):
    pass


def test_prepare_table(table):
    pass


def test_user_pool(user_model):
    JobTestRunner(CreOnlyUserPool).run('reco_debit_card_ca_0000', TEST_BATCH_ID)
    JobTestRunner(DebOnlyUserPool).run('reco_credit_card_ca_0000', TEST_BATCH_ID)


def test_user_percentile():
    JobTestRunner(UserPercentile).run('reco_debit_card_ca_0000', TEST_BATCH_ID)
    JobTestRunner(UserPercentile).run('reco_credit_card_ca_0000', TEST_BATCH_ID)
    JobTestRunner(UserPredResultsToDW).run('base', TEST_BATCH_ID)
    JobTestRunner(CollectIREUserPredResults).run('base', TEST_BATCH_ID)


def test_camp_user_pool(user_model):
    JobTestRunner(CampCreOnlyUserPool).run('reco_debit_card_ca_0000', TEST_BATCH_ID)


def test_s1106(data):
    RedisClientRegistry._instance = RedisMock()
    JobTestRunner(RecoS1106).run('reco_adhoc_0000', TEST_BATCH_ID)


def test_s1108(data):
    RedisClientRegistry._instance = RedisMock()
    JobTestRunner(RecoS1108).run('reco_adhoc_0000', TEST_BATCH_ID)


def test_s1109(data):
    RedisClientRegistry._instance = RedisMock()
    JobTestRunner(RecoS1109).run('reco_adhoc_0000', TEST_BATCH_ID)


def test_s1101(data):
    RedisClientRegistry._instance = RedisMock()
    JobTestRunner(RecoS1101).run('reco_adhoc_0000', TEST_BATCH_ID)


def test_s1110(data):
    RedisClientRegistry._instance = RedisMock()
    JobTestRunner(RecoS1110).run('reco_adhoc_0000', TEST_BATCH_ID)


def test_operating_user(data):
    JobTestRunner(OperatingUser).run('reco_credit_card_ca_0000', TEST_BATCH_ID)

    # JobTestRunner(OperatingUser).run('reco_credit_card_ca_0000', '20211230')
    # JobTestRunner(OperatingUser).run('reco_credit_card_ca_0000', '20211231')
    # JobTestRunner(OperatingUser).run('reco_credit_card_ca_0000', '20220105')


def test_crowd_feedback(data):
    JobTestRunner(CrowdFeedback).run('reco_debit_card_ca_0001', '20220104')


def test_crowd_feedback_app(data):
    JobTestRunner(CrowdFeedback).run('reco_debit_card_ca_0000', '20220104')


@pytest.fixture
def post_proc_sms(data):
    JobTestRunner(PostProcessSMS).run('post_proc_sms', TEST_BATCH_ID)


@pytest.fixture
def post_proc_app(data):
    JobTestRunner(PostProcessAPP).run('post_proc_app', TEST_BATCH_ID)


def test_post_proc_sms(spark, post_proc_sms):

    test_df_rows = spark.sql(
        f"""
        SELECT *
        FROM {output_table().sms_reco_results}
        WHERE dt='{TEST_BATCH_ID}'
          AND actv_id='T1011'
    """
    ).collect()

    user_ids = [row.user_id for row in test_df_rows]
    assert len(user_ids) == 12
    assert set(user_ids) == {
        '3001078054',
        '3032284975',
        '3095681436',
        '3126953062',
        '3127199832',
        '3127852100',
        '3133523681',
        '3133523682',
        '3133523683',
        '3133523684',
        '3133523685',
        '3133523686',
    }
    assert all([len(row.item_id) == 4 for row in test_df_rows])


@pytest.mark.skip('白名单下掉')
def test_post_proc_whitelist(spark, data):
    JobTestRunner(PostProcessWhitelist).run('post_proc_whitelist', TEST_BATCH_ID)
    test_df_rows = spark.sql(
        f"""
        SELECT *
        FROM {output_table().whitelist_results}
        WHERE dt='{TEST_BATCH_ID}'
          AND scene_id='test'
    """
    ).collect()

    assert len(test_df_rows) == 25

    credit_rows = [i for i in test_df_rows if i.item_id == '6158']
    debit_rows = [i for i in test_df_rows if i.item_id == 'CCGM']
    assert len(credit_rows) == 13
    assert len(debit_rows) == 12

    debit_users = [
        '3001078054',
        '3032284975',
        '3095681436',
        '3126953062',
        '3127199832',
        '3127852100',
    ]
    assert set(i.user_id for i in debit_rows) & set(debit_users) == set(debit_users)

    credit_users = [
        '3001076536',
        '6011259147',
        '3127852100',
        '3095681436',
        '3127199832',
        '3032284975',
        '3126953062',
    ]
    assert set(i.user_id for i in credit_rows) & set(credit_users) == set(credit_users)


def test_post_proc_app(spark, post_proc_app):
    test_df_rows = spark.sql(
        f"""
        SELECT *
        FROM {output_table().app_reco_results}
        WHERE dt='{TEST_BATCH_ID}'
          AND actv_id='T2011'
    """
    ).collect()
    assert len(test_df_rows) == 12
    assert len(json.loads(test_df_rows[0].prod_info)[0]['slots'][0]['prdList']) == 1


def test_collect_sms_reco_results(post_proc_sms):
    ctx = Context.default('post_proc_sms', TEST_BATCH_ID, CollectRecoResults.__name__)
    job = CollectRecoResults(ctx)
    client = MockClient()
    client.register_rtn('patch', 'activity', None)

    def check_patch_input(body):
        assert isinstance(body, dict)
        assert 'actv_id' in body
        assert 'batch_id' in body
        assert 'user_count' in body and body['user_count'] >= 0

    client.register_check_callback('patch', 'activity', check_patch_input)
    job.offline_backend = client
    job.run()

    txt_path = os.path.join(
        job._base.nas_output_dir, 'backup/20220103/T1011-PSMS_IRE--20220103.txt'
    )
    assert os.path.exists(txt_path)
    data = open(txt_path).read().splitlines()
    assert len(data) == 13
    assert data[0] == 'user_id@!@tel_number@!@content'


@pytest.mark.skip()
def test_collect_app_reco_results(post_proc_app):
    ctx = Context.default('post_proc_app', TEST_BATCH_ID, CollectRecoResults.__name__)
    job = CollectRecoResults(ctx)
    client = MockClient()
    client.register_rtn('patch', 'activity', None)

    def check_patch_input(body):
        assert isinstance(body, dict)
        assert 'actv_id' in body
        assert 'batch_id' in body
        assert 'user_count' in body and body['user_count'] >= 0

    client.register_check_callback('patch', 'activity', check_patch_input)
    job.offline_backend = client
    job.run()

    txt_path = os.path.join(
        job._base.nas_output_dir,
        f'backup/20220103/T2021-PMBS_IRE-71_sy_navigation_tab_popup-20220103.txt',
    )
    assert os.path.exists(txt_path)
    data = open(txt_path).read().splitlines()
    assert len(data) == 14
    assert data[0] == 'user_id@!@prod_info'
    slot = json.loads(data[1].split('@!@')[-1])[0]['slots'][0]
    prod_list = slot['prdList']
    assert slot['index'] == '0'
    assert len(prod_list) == 4
    assert set(prod_list[0].keys()) == {'code', 'type'}
    assert prod_list[0]['type'] == ProdType.CREDIT


@pytest.mark.skip()
def test_clean_reco_results():
    # clean data before
    JobTestRunner(Cleanup).run('base', '20220201')


def test_clean_table(data):
    ctx = Context.default('base', '20220101', CleanTable.__name__)
    job = CleanTable(ctx)
    job.run()
    assert set(job.tables_cleaned) == {
        'recodb.bdasire_card_runtime_config',
        'recodb.bdasire_card_spark_job_log',
        'recodb.bdasire_card_user_feature',
        'recodb.bdasire_card_crowd_package',
        'recodb.bdasire_card_user_pool',
        'recodb.bdasire_card_operating_user',
        'recodb.bdasire_card_report_agg',
        'recodb.bdasire_card_sms_reco_results_v2',
        'recodb.bdasire_card_crowd_whitelist',
        'recodb.bdasire_card_app_reco_results',
        'recodb.bdasire_card_crowd_feedback',
        'recodb.bdasire_card_weixin_feedback',
        'recodb.bdasire_card_cancel_user',
        'recodb.bdasire_card_s1106_results',
        # reuse s1106 table
        # 'recodb.bdasire_card_s1108_results',
        # 'recodb.bdasire_card_s1109_results',
        'recodb.bdasire_card_ire_model_result',
        'recodb.bdasire_card_user_percentile',
        'recodb.bdasire_card_ire_user_pred_result',
    }


def test_app_banner(reco_model):
    ctx = Context.default('reco_credit_card_ca_0000', '20220103', OperatingUser.__name__)

    channel = ChannelInfo(
        channel_id='PMBS_IRE',
        channel_name='',
        banner_id='banner0',
        feedback_delay=1,
        push_delay=1,
        max_push=30,
        allow_repeat=True,
        num_items=4,
    )
    banner_cfg = ChannelOperateConfig(
        channel_id='PMBS_IRE',
        banner_id='banner0',
        group_users={
            UserType.CTL.value: GroupInfo(num=25000, minimum=True),
            UserType.EXP.value: GroupInfo(num=450000),
            UserType.EXP1.value: GroupInfo(num=25000),
            UserType.CTL1.value: GroupInfo(num=25000),
            UserType.NOOP.value: GroupInfo(num=25000, minimum=True),
        },
    )

    job = OperatingUser(ctx)
    job._scene.operating_user.channel_cfgs.append(banner_cfg)
    job._base.channels.append(channel)
    job.run()

    job = CrowdPackage(ctx)
    job._scene.operating_user.channel_cfgs.append(banner_cfg)
    job._base.channels.append(channel)
    job.run()
    df = (
        job.run_spark_sql(
            """
            SELECT *
            FROM recodb.bdasire_card_crowd_package
            WHERE dt='20220103'
              AND dag_id='reco_credit_card_ca_0000'
              AND banner_id='banner0'
            """
        )
        .groupBy('banner_id', 'rank')
        .count()
    )
    assert df.count() == 4
    assert {int(i.rank) for i in df.collect()} == {0, 1, 2, 3}


def test_daily_report_sms(table):
    JobTestRunner(DailyReportSMS).run('reco_debit_card_ca_0001', '20220101')


def test_agg_report_app(table):
    JobTestRunner(AggReport).run('reco_debit_card_ca_0000', '20220101')


def test_agg_report_sms(table):
    JobTestRunner(AggReport).run('reco_debit_card_ca_0001', '20220101')


def test_user_cancel(feature_engine):
    JobTestRunner(PrepareCancelModelSample).run('reco_credit_cancel_0000', '20220101')
    JobTestRunner(FitCancelUserModel).run('reco_credit_cancel_0000', '20220101')
    JobTestRunner(CancelUser).run('reco_credit_cancel_0000', '20220101')
    JobTestRunner(EvalCancelUserModel).run('reco_credit_cancel_0000', '20220101')


def test_crowd_package_item_spec(reco_model):
    ctx = Context.default('reco_credit_card_ca_0000', '20220103', CrowdPackage.__name__)
    job = CrowdPackage(ctx)

    job._operating_user.channel_cfgs.clear()
    channel_cfg = [
        ChannelOperateConfig(
            channel_id='PSMS_IRE', banner_id='', group_users={}, items_specified=['6152', '6158']
        ),
        ChannelOperateConfig(
            channel_id='PMBS_IRE',
            banner_id='71_bbcreditCard_tjbk',
            group_users={},
            items_specified=['6152', '6158'],
        ),
    ]
    job._operating_user.channel_cfgs.extend(channel_cfg)

    df = job._rank()
    df = job._rerank(df)
    df = df.filter(df.banner_id.isin([i.banner_id for i in channel_cfg]))

    item_ids = [r.item_id for r in df.collect()]
    assert set(item_ids) == {'6152', '6158'}


def test_report_sync(data):
    ctx = Context.default('post_proc_sms', '20220103', AggReportSync.__name__)
    job = AggReportSync(ctx)

    job._lowcode = Lowcode()
    job.run()

    assert len(job._lowcode.records) == 15


def test_ire_model_result(data):
    data = [
        ['U01', '中文1', '13500000001', ['P01', 'P02', 'P03'],],
        ['U02', '中文2', '13500000002', ['P02', 'P03'],],
        ['U03', '中文3', '13500000003', ['P04', 'P05', 'P03'],],
    ]
    table_inserter = TableInserter(RecoS1106.output_table_info_list(base_config())[0])
    table_inserter.insert_list(data, overwrite=False, dag_id='S1106', dt='20220102')
    table_inserter.insert_list(data, overwrite=False, dag_id='S1108', dt='20220102')
    table_inserter.insert_list(data, overwrite=False, dag_id='S1109', dt='20220102')

    data2 = [
        ['U04', 'exp', 'I001', 'PMBS_IRE', '', 0.1, 1.0, 'V0', 'scene_debit_card_ca', '20220102'],
        ['U05', 'exp', 'I002', 'PMBS_IRE', None, 0.1, 1.0, 'V0', 'scene_debit_card_ca', '20220102']
    ]
    table_inserter2 = TableInserter(CrowdPackage.output_table_info_list(base_config())[0])
    table_inserter2.insert_list(data2, overwrite=False, dag_id='reco_debit_card_ca_0000', dt='20220102')

    JobTestRunner(ModelResultsToDW).run('base', '20220103')
    JobTestRunner(CollectIREModelResults).run('base', '20220103')
