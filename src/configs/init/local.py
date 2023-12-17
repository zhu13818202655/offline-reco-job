# -*- coding: utf-8 -*-
# @File : local.py
# @Author : r.yang
# @Date : Thu Nov 24 13:48:24 2022
# @Description : format string


import os
from typing import List

from configs.init.common import (
    CNAME_APP,
    CNAME_SMS,
    S_CREDIT,
    S_CREDIT_CANCEL,
    S_DEBIT,
    adhoc_scene,
    get_channels,
    get_external_table,
    get_feature_map,
    get_group_cfg,
    get_items,
    get_output_table,
    get_scenes,
    get_test_users,
)
from configs.model import AllInOneConfig
from configs.model.base import (
    Activity,
    BaseConfig,
    ClassifierConfig,
    FeatureEngineConfig,
    LowcodeConfig,
    RedisConfig,
    TrainConfig,
    UserItemMatcher,
)
from configs.model.config import (
    BaseDagConfig,
    ChannelOperateConfig,
    CrowdFeedbackConfig,
    GroupInfo,
    OperatingUserConfig,
    PostProcessDagConfig,
    RankingConfig,
    RecoDagConfig,
    ReportConfig,
    SceneConfig,
    SMSConfig,
    UserPoolConfig,
)
from configs.utils import UserType


def local_config() -> AllInOneConfig:
    return AllInOneConfig(
        base_dag=BaseDagConfig(dag_id='base', dag_name='基础DAG', base=_local_base_config()),
        reco_dags=[
            RecoDagConfig(
                dag_id='reco_credit_card_ca_0000',
                dag_name='信用卡获客DAG 0000',
                scene=_local_credit_app_scene(),
            ),
            RecoDagConfig(
                dag_id='reco_debit_card_ca_0000',
                dag_name='借记卡获客DAG 0000',
                scene=_local_debit_app_scene(),
            ),
            RecoDagConfig(
                dag_id='reco_debit_card_ca_0001',
                dag_name='借记卡获客DAG 0001(短信渠道)',
                scene=_local_debit_sms_scene(),
            ),
            RecoDagConfig(
                dag_id='reco_credit_cancel_0000', dag_name='信用卡销户场景', scene=_local_cancel_scene(),
            ),
            RecoDagConfig(dag_id='reco_adhoc_0000', dag_name='资金流失场景', scene=adhoc_scene(),),
        ],
        post_proc_dags=[_local_sms_post_proc(), _local_app_post_proc(),],
    )


def _local_base_config() -> BaseConfig:
    base = BaseConfig(
        react_delay=2,
        data_keep_day=30,
        partition_keep_day=30,
        output_table=get_output_table(_output_table_name_builder),
        external_table=get_external_table(_ext_table_name_builder),
        nas_output_dir='/tmp/bos_nas/reco_results',
        hdfs_output_dir='/tmp/bos_hdfs/reco_results',
        feat_dir=os.path.join(f'/tmp/bos_hdfs/', 'feature'),
        offline_backend_address=os.environ['OFFLINE_BACKEND_ADDR'],
        online_backend_address=os.environ['ONLINE_BACKEND_ADDR'],
        lowcode=LowcodeConfig(
            url=os.environ.get('LOWCODE_URL', ''),
            client_id=os.environ.get('LOWCODE_AUTH_CLIENT_ID', '311858455747009744'),
            secret=os.environ.get(
                'LOWCODE_AUTH_SECRET',
                'wNqSXlY2nTOdJZnFLgWDnGkp81rEwteanucbxOiLfVpwer0yURECrRmIFzlS9uZM',
            ),
        ),
        redis=RedisConfig(),
        feat_engines=_local_feat_engines(),
        channels=get_channels(),
        scenes=get_scenes(),
        items=get_items(),
        test_users=get_test_users(),
        test_banners=[],
    )
    return base


def _ext_table_name_builder(table: str) -> str:
    if '.' in table:
        return table

    # DONE(ryang): 这个cdp环境里表名不对，是bdrsyncmask.appmlm_finc_loss_base_txn_info
    if table == 'appmlm_mlm_rtl_finc_loss_base_txn_info':
        table = 'appmlm_finc_loss_base_txn_info'
    return f"bdrsyncmask.{table.split('.', 1)[-1]}"


def _output_table_name_builder(table: str) -> str:
    return f"recodb.bdasire_card_{table.split('.', 1)[-1]}"


def _local_feat_engines() -> List[FeatureEngineConfig]:
    return [
        FeatureEngineConfig(
            feat_version='v0.4', feat_map=get_feature_map(is_local=True), train_dt='20220131',
        )
    ]


def _local_scene_cfg(
    id, name, user_filter, rank_model, user_model, item_selector, channel_cfgs, feedback_ident,
) -> SceneConfig:
    return SceneConfig(
        scene_id=id,
        scene_name=name,
        user_pool=UserPoolConfig(
            filters=user_filter,
            user_type_refresh_days=[i for i in range(30) if (i - 1) % 5 == 0],
            model=user_model,
        ),
        ranking=RankingConfig(model=rank_model, force_rank_custs=['3095681436', '3127199832',]),
        operating_user=OperatingUserConfig(fail_rate=0.01, channel_cfgs=channel_cfgs,),
        crowd_feedback=CrowdFeedbackConfig(
            popup_identifiers=feedback_ident,
            sms_default_send_num=1,
            user_agg_start_dt='20220630',
            user_agg_days=180,
        ),
        train=TrainConfig(train_sample_month_num=6, eval_sample_month_num=1, sample_dt='20220301',),
        item_selector=item_selector,
        report=ReportConfig(stat_periods_in_day=[14],),
    )


def _local_credit_app_scene() -> SceneConfig:
    return _local_scene_cfg(
        id=S_CREDIT.scene_id,
        name=S_CREDIT.scene_name,
        user_filter={'age': 'age < 60'},
        rank_model=ClassifierConfig(
            model_version='credit_rec_1_1_0-lgb0',
            feat_version='v0.4',
            model_clz='LGBClassifierModel',
            custom_train_args={'objective': 'multiclass', 'metric': 'multi_logloss',},
            model_dir='/tmp/bos_hdfs/model',
        ),
        user_model=ClassifierConfig(
            model_version='credit_user_1_1_0-lgb0',
            feat_version='v0.4',
            model_clz='LGBClassifierModel',
            model_dir='/tmp/bos_hdfs/model',
        ),
        item_selector=S_CREDIT.item_selector,
        channel_cfgs=[
            get_group_cfg(CNAME_APP, '71_sy_navigation_tab_popup', ['6324', '6153', '6158']),
            get_group_cfg(CNAME_APP, '71_bbsy_banner', ['6158', '6153', '6324', '6331', '6319']),
            get_group_cfg(
                CNAME_APP, '71_zxcreditCard_Banner', ['6158', '6153', '6324', '6331', '6319']
            ),
            get_group_cfg(
                CNAME_APP, '71_bbcreditCard_tjbk', ['6158', '6153', '6324', '6331', '6319']
            ),
            get_group_cfg(CNAME_APP, '71_bbsy_mrhdtj', ['6158', '6153', '6324', '6331', '6319']),
        ],
        feedback_ident=['dgpp_na_dp_djjr', 'syzs_sy_pddc'],
    )


def _local_debit_app_scene() -> SceneConfig:
    return _local_scene_cfg(
        id=S_DEBIT.scene_id,
        name=S_DEBIT.scene_name,
        user_filter={},
        rank_model=ClassifierConfig(
            model_version='debit_rec_1_1_0-lgb0',
            feat_version='v0.4',
            model_clz='LGBClassifierModel',
            custom_train_args={'objective': 'multiclass', 'metric': 'multi_logloss',},
            model_dir='/tmp/bos_hdfs/model',
        ),
        user_model=ClassifierConfig(
            feat_version='v0.4',
            model_version='debit_user_1_1_0-lgb0',
            model_clz='LGBClassifierModel',
            model_dir='/tmp/bos_hdfs/model',
        ),
        item_selector=S_DEBIT.item_selector,
        channel_cfgs=[
            get_group_cfg(
                CNAME_APP,
                '71_sy_navigation_tab_popup',
                ['CCGM', 'CCLN', 'CCGJ', 'CCGI', 'CCGH', 'CCGG'],
            ),
        ],
        feedback_ident=['dgpp_na_dp_djjr', 'syzs_sy_pddc'],
    )


def _local_debit_sms_scene() -> SceneConfig:
    return _local_scene_cfg(
        id=S_DEBIT.scene_id,
        name=S_DEBIT.scene_name,
        user_filter={},
        rank_model=ClassifierConfig(
            feat_version='v0.4',
            model_version='debit_rec_1_1_0-lgb0',
            model_clz='LGBClassifierModel',
            custom_train_args={'objective': 'multiclass', 'metric': 'multi_logloss',},
            model_dir='/tmp/bos_hdfs/model',
        ),
        user_model=ClassifierConfig(
            feat_version='v0.4',
            model_version='debit_user_1_1_0-lgb0',
            model_clz='LGBClassifierModel',
            model_dir='/tmp/bos_hdfs/model',
        ),
        item_selector=S_DEBIT.item_selector,
        channel_cfgs=[
            ChannelOperateConfig(
                channel_id='PSMS_IRE',
                banner_id='',
                group_users={
                    UserType.CTL.value: GroupInfo(num=5000, minimum=True),
                    UserType.EXP.value: GroupInfo(num=90000),
                    UserType.EXP1.value: GroupInfo(num=5000),
                    UserType.CTL1.value: GroupInfo(num=5000),
                    UserType.NOOP.value: GroupInfo(num=5000, minimum=True),
                },
                groups_no_push=[UserType.CTL1, UserType.NOOP],
                items_specified=[],
            )
        ],
        feedback_ident=['dgpp_na_dp_djjr', 'syzs_sy_pddc'],
    )


def _local_cancel_scene() -> SceneConfig:
    return SceneConfig(
        scene_id=S_CREDIT_CANCEL.scene_id,
        scene_name=S_CREDIT_CANCEL.scene_name,
        user_pool=UserPoolConfig(
            filters={},
            user_type_refresh_days=[],
            model=ClassifierConfig(
                feat_version='v0.4',
                model_version='credit_cancel_1_0_1-lgb1',
                model_clz='LGBIncrClassifierModel',
                custom_train_args={'batch_num': 2},
                model_dir='/tmp/bos_hdfs/model',
            ),
        ),
        item_selector=S_CREDIT_CANCEL.item_selector or {},
        train=TrainConfig(sample_dt='20220501', train_sample_month_num=4, eval_sample_month_num=1,),
    )


def _local_sms_post_proc() -> PostProcessDagConfig:
    return PostProcessDagConfig(
        dag_id='post_proc_sms',
        dag_name='短信',
        channel_id=CNAME_SMS,
        actvs=[
            Activity(actv_id='S1011', banner_id='', scene_id=S_DEBIT.scene_id,),
            Activity(
                actv_id='T1011',
                banner_id='',
                scene_id=S_DEBIT.scene_id,
                test_user_item=UserItemMatcher(
                    user_selector={'scope': ['all', 'debit']},
                    item_selector=S_DEBIT.item_selector or {},
                ),
            ),
        ],
        sms=SMSConfig(push_days=[i for i in range(30) if i & 1],),
    )


def _local_app_post_proc() -> PostProcessDagConfig:
    return PostProcessDagConfig(
        dag_id='post_proc_app',
        dag_name='app后处理',
        channel_id=CNAME_APP,
        actvs=[
            # 借记卡
            Activity(
                actv_id='S2011', banner_id='71_sy_navigation_tab_popup', scene_id=S_DEBIT.scene_id,
            ),
            Activity(
                actv_id='T2011',
                banner_id='71_sy_navigation_tab_popup',
                scene_id=S_DEBIT.scene_id,
                test_user_item=UserItemMatcher(
                    user_selector={'scope': ['all', 'debit']},
                    item_selector=S_DEBIT.item_selector or {},
                ),
            ),
            # 信用卡
            Activity(
                actv_id='S2021', banner_id='71_sy_navigation_tab_popup', scene_id=S_CREDIT.scene_id,
            ),
            Activity(
                actv_id='T2021',
                banner_id='71_sy_navigation_tab_popup',
                scene_id=S_CREDIT.scene_id,
                test_user_item=UserItemMatcher(
                    user_selector={'scope': ['all', 'credit']},
                    item_selector=S_CREDIT.item_selector or {},
                ),
            ),
            Activity(actv_id='S2022', banner_id='71_bbsy_banner', scene_id=S_CREDIT.scene_id,),
            Activity(
                actv_id='S2023', banner_id='71_zxcreditCard_Banner', scene_id=S_CREDIT.scene_id,
            ),
            Activity(
                actv_id='S2024', banner_id='71_bbcreditCard_tjbk', scene_id=S_CREDIT.scene_id,
            ),
            Activity(actv_id='S2025', banner_id='71_bbsy_mrhdtj', scene_id=S_CREDIT.scene_id,),
        ],
    )
