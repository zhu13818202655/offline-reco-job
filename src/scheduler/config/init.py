# -*- coding: utf-8 -*-
# @File : init.py
# @Author : r.yang
# @Date : Fri Nov 25 15:09:45 2022
# @Description :


import os

from configs.utils import Env
from scheduler.config.model import (
    EnvSetttings,
    SchedulerConfig,
    SchedulerDagConfig,
    SchedulerTaskConfig,
)

CUR_DIR = os.path.dirname(os.path.realpath(__file__))


def get_env_settings(mock=False) -> EnvSetttings:
    if mock:
        return EnvSetttings(
            entrypoint_path=os.path.join(CUR_DIR, '../../../scripts', Env.value, 'entrypoint.sh'),
            workspace_dir='/tianrang/airflow/workspace',
            config_dir='/tianrang/airflow/workspace/config',
            runtime_dir='/tianrang/airflow/workspace/runtime',
            dag_config_path='',
            scheduler_config_path='',
            offline_backend_addr='',
            lowcode_url='',
            lowcode_auth_client_id='',
            lowcode_auth_secret='',
        )
    workspace_dir = os.environ.get('JOBS_WORKSPACE_DIR', '/tianrang/airflow/workspace')
    config_dir = os.path.join(workspace_dir, 'config')
    runtime_dir = os.path.join(workspace_dir, 'runtime')

    version = os.environ['IMAGE_VERSION']
    inst = EnvSetttings(
        entrypoint_path=os.path.join(CUR_DIR, '../../../scripts', Env.value, 'entrypoint.sh'),
        workspace_dir=workspace_dir,
        config_dir=config_dir,
        runtime_dir=runtime_dir,
        dag_config_path=os.path.join(config_dir, 'mutable', 'dag', f'{version}.yaml'),
        scheduler_config_path=os.path.join(config_dir, 'mutable', 'airflow', f'{version}.yaml'),
        offline_backend_addr=os.environ['OFFLINE_BACKEND_ADDR'],
        lowcode_url=os.environ['LOWCODE_URL'],
        lowcode_auth_client_id=os.environ['LOWCODE_AUTH_CLIENT_ID'],
        lowcode_auth_secret=os.environ['LOWCODE_AUTH_SECRET'],
    )

    os.makedirs(os.path.dirname(inst.dag_config_path), exist_ok=True)
    os.makedirs(os.path.dirname(inst.scheduler_config_path), exist_ok=True)

    return inst


def get_scheduler_config(env_settings: EnvSetttings) -> SchedulerConfig:

    inst = SchedulerConfig(
        dag_config_map={
            'DEFAULT': SchedulerDagConfig(),
            'train_feature_engine': SchedulerDagConfig(
                start_date='2022-06-27',
                schedule_interval='0 16 28 * *',  # 08:00
                concurrency=1,
                tasks={
                    'GenDagConfig_base': SchedulerTaskConfig(
                        operator='gen_dag_config_task',
                        params=dict(dag_id='base', subdir='startup'),
                        dependencies=[],
                    ),
                    'FitFeatureEngine': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='base', subdir='startup',),
                        dependencies=['GenDagConfig_base'],
                    ),
                },
            ),
            'train_debit_card': SchedulerDagConfig(
                start_date='2022-06-27',
                schedule_interval='0 17 28 * *',  # 09:00
                concurrency=1,
                tasks={
                    'GenDagConfig_base': SchedulerTaskConfig(
                        operator='gen_dag_config_task',
                        params=dict(dag_id='base', subdir='startup'),
                        dependencies=[],
                    ),
                    'FitFeatureEngine': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='base', subdir='startup',),
                        dependencies=['GenDagConfig_base'],
                    ),
                    'GenDagConfig_debit': SchedulerTaskConfig(
                        operator='gen_dag_config_task',
                        params=dict(dag_id='reco_debit_card_ca_0000', subdir='startup',),
                        dependencies=['GenDagConfig_base'],
                    ),
                    'PrepareDebitModelSample': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_debit_card_ca_0000', subdir='startup',),
                        dependencies=['GenDagConfig_debit', 'FitFeatureEngine'],
                    ),
                    'FitUserModel_debit': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(
                            dag_id='reco_debit_card_ca_0000',
                            subdir='startup',
                            master='local[*]',
                            mode='client',
                        ),
                        dependencies=['PrepareDebitModelSample'],
                    ),
                    'EvalUserModel_debit': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(
                            dag_id='reco_debit_card_ca_0000',
                            subdir='startup',
                            master='local[*]',
                            mode='client',
                        ),
                        dependencies=['FitUserModel_debit'],
                    ),
                    'FitRecoModel_debit': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(
                            dag_id='reco_debit_card_ca_0000',
                            subdir='startup',
                            master='local[*]',
                            mode='client',
                        ),
                        dependencies=['FitUserModel_debit'],
                    ),
                    'EvalRecoModel_debit': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(
                            dag_id='reco_debit_card_ca_0000',
                            subdir='startup',
                            master='local[*]',
                            mode='client',
                        ),
                        dependencies=['FitRecoModel_debit'],
                    ),
                },
            ),
            'train_credit_card': SchedulerDagConfig(
                start_date='2022-06-27',
                schedule_interval='0 18 28 * *',  # 10:00
                concurrency=1,
                tasks={
                    'GenDagConfig_base': SchedulerTaskConfig(
                        operator='gen_dag_config_task',
                        params=dict(dag_id='base', subdir='startup'),
                        dependencies=[],
                    ),
                    'GenDagConfig_credit': SchedulerTaskConfig(
                        operator='gen_dag_config_task',
                        params=dict(dag_id='reco_credit_card_ca_0000', subdir='startup',),
                        dependencies=['GenDagConfig_base'],
                    ),
                    'FitFeatureEngine': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='base', subdir='startup',),
                        dependencies=['GenDagConfig_base'],
                    ),
                    'PrepareCreditModelSample': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_credit_card_ca_0000', subdir='startup',),
                        dependencies=['GenDagConfig_credit', 'FitFeatureEngine'],
                    ),
                    'FitUserModel_credit': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(
                            dag_id='reco_credit_card_ca_0000',
                            subdir='startup',
                            master='local[*]',
                            mode='client',
                        ),
                        dependencies=['PrepareCreditModelSample'],
                    ),
                    'EvalUserModel_credit': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(
                            dag_id='reco_credit_card_ca_0000',
                            subdir='startup',
                            master='local[*]',
                            mode='client',
                        ),
                        dependencies=['FitUserModel_credit'],
                    ),
                    'FitRecoModel_credit': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(
                            dag_id='reco_credit_card_ca_0000',
                            subdir='startup',
                            master='local[*]',
                            mode='client',
                        ),
                        dependencies=['FitUserModel_credit'],
                    ),
                    'EvalRecoModel_credit': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(
                            dag_id='reco_credit_card_ca_0000',
                            subdir='startup',
                            master='local[*]',
                            mode='client',
                        ),
                        dependencies=['FitRecoModel_credit'],
                    ),
                },
            ),
            'train_credit_cancel': SchedulerDagConfig(
                start_date='2022-06-27',
                schedule_interval='0 19 28 * *',  # 11:00
                concurrency=1,
                tasks={
                    'GenDagConfig_base': SchedulerTaskConfig(
                        operator='gen_dag_config_task',
                        params=dict(dag_id='base', subdir='startup'),
                        dependencies=[],
                    ),
                    'GenDagConfig_credit_cancel': SchedulerTaskConfig(
                        operator='gen_dag_config_task',
                        params=dict(dag_id='reco_credit_cancel_0000', subdir='startup',),
                        dependencies=['GenDagConfig_base'],
                    ),
                    'FitFeatureEngine': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='base', subdir='startup',),
                        dependencies=['GenDagConfig_base'],
                    ),
                    'PrepareCancelModelSample': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_credit_cancel_0000', subdir='startup',),
                        dependencies=['GenDagConfig_credit_cancel', 'FitFeatureEngine'],
                    ),
                    'FitCancelUserModel_credit': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(
                            dag_id='reco_credit_cancel_0000',
                            subdir='startup',
                            master='local[*]',
                            mode='client',
                        ),
                        dependencies=['PrepareCancelModelSample'],
                    ),
                    'EvalCancelUserModel_credit': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(
                            dag_id='reco_credit_cancel_0000',
                            subdir='startup',
                            master='local[*]',
                            mode='client',
                        ),
                        dependencies=['FitCancelUserModel_credit'],
                    ),
                },
            ),
            'startup': SchedulerDagConfig(
                schedule_interval='@once',
                tasks={
                    'GenDagConfig_base': SchedulerTaskConfig(
                        operator='gen_dag_config_task',
                        params=dict(dag_id='base', subdir='startup'),
                        dependencies=[],
                    ),
                    'PrepareTable': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='base', subdir='startup'),
                        dependencies=['GenDagConfig_base'],
                    ),
                },
            ),
            'base': SchedulerDagConfig(
                schedule_interval='0 20 * * *',  # 12:00
                description='base jobs like config/cleanup/feature',
                tasks={
                    'FetchLowcodeConfig': SchedulerTaskConfig(
                        operator='airflow.operators.bash_operator.BashOperator',
                        params=dict(
                            bash_command=(
                                'python -m jobs.init.fetch_lowcode_config '
                                '--lowcode_url {{ params.lowcode_url }} '
                                '--auth_secret {{ params.lowcode_auth_secret }} '
                                '--auth_client_id {{ params.lowcode_auth_client_id }} '
                                '--dag_config_path {{ params.dag_config_path }} '
                                '--scheduler_config_path {{ params.scheduler_config_path }} '
                            ),
                        ),
                    ),
                    'UploadDagConfig': SchedulerTaskConfig(
                        operator='airflow.operators.bash_operator.BashOperator',
                        params=dict(
                            bash_command=(
                                'python -m jobs.init.upload_dag_config '
                                '--offline_backend_addr {{ params.offline_backend_addr }} '
                                '--config_path {{ params.dag_config_path }} '
                                '--batch_id {{ ds_nodash }}'
                            ),
                        ),
                        dependencies=['FetchLowcodeConfig'],
                    ),
                    'GenDagConfig': SchedulerTaskConfig(
                        operator='gen_dag_config_task',
                        params=dict(dag_id='base',),
                        dependencies=['UploadDagConfig'],
                    ),
                    'UserFeatures': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='base',),
                        dependencies=['GenDagConfig'],
                    ),
                    'Cleanup': SchedulerTaskConfig(
                        operator='gen_local_task',
                        params=dict(dag_id='base',),
                        dependencies=['GenDagConfig'],
                    ),
                    'CleanTable': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='base',),
                        dependencies=['UserFeatures'],
                    ),
                },
            ),
            'post_proc_sms': SchedulerDagConfig(
                schedule_interval='10 3 * * *',  # 19:10
                tasks={
                    'GenDagConfig': SchedulerTaskConfig(
                        operator='gen_dag_config_task',
                        params=dict(dag_id='post_proc_sms',),
                        dependencies=[],
                    ),
                    'PostProcessSMS': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='post_proc_sms',),
                        dependencies=['GenDagConfig'],
                    ),
                    'CollectRecoResults': SchedulerTaskConfig(
                        operator='gen_local_task',
                        params=dict(dag_id='post_proc_sms',),
                        dependencies=['PostProcessSMS'],
                    ),
                    'AggReportSync': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='post_proc_sms',),
                        dependencies=['CollectRecoResults'],
                    ),
                },
            ),
            'post_proc_app': SchedulerDagConfig(
                schedule_interval='0 3 * * *',  # 19:30
                tasks={
                    'GenDagConfig': SchedulerTaskConfig(
                        operator='gen_dag_config_task',
                        params=dict(dag_id='post_proc_app'),
                        dependencies=[],
                    ),
                    'PostProcessAPP': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='post_proc_app'),
                        dependencies=['GenDagConfig'],
                    ),
                    'CollectRecoResults': SchedulerTaskConfig(
                        operator='gen_local_task',
                        params=dict(dag_id='post_proc_app'),
                        dependencies=['PostProcessAPP'],
                    ),
                    'AggReportSync': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='post_proc_app',),
                        dependencies=['CollectRecoResults'],
                    ),
                },
            ),
            'reco_credit_card_ca_0000': SchedulerDagConfig(
                schedule_interval='30 0 * * *',  # 16:30
                tasks={
                    'GenDagConfig': SchedulerTaskConfig(
                        operator='gen_dag_config_task',
                        params=dict(dag_id='reco_credit_card_ca_0000',),
                        dependencies=[],
                    ),
                    'DebOnlyUserPool': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_credit_card_ca_0000',),
                        dependencies=['GenDagConfig'],
                    ),
                    'CrowdFeedback': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_credit_card_ca_0000',),
                        dependencies=['GenDagConfig'],
                    ),
                    'OperatingUser': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_credit_card_ca_0000',),
                        dependencies=['DebOnlyUserPool', 'CrowdFeedback'],
                    ),
                    'CrowdPackage': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_credit_card_ca_0000',),
                        dependencies=['OperatingUser'],
                    ),
                    'AggReport': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_credit_card_ca_0000',),
                        dependencies=['CrowdPackage'],
                    ),
                    'UserPercentile': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_credit_card_ca_0000',),
                        dependencies=['DebOnlyUserPool'],
                    ),
                },
            ),
            'reco_debit_card_ca_0000': SchedulerDagConfig(
                schedule_interval='10 1 * * *',  # 17:10
                tasks={
                    'GenDagConfig': SchedulerTaskConfig(
                        operator='gen_dag_config_task',
                        params=dict(dag_id='reco_debit_card_ca_0000',),
                        dependencies=[],
                    ),
                    'CreOnlyUserPool': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_debit_card_ca_0000',),
                        dependencies=['GenDagConfig'],
                    ),
                    'CrowdFeedback': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_debit_card_ca_0000',),
                        dependencies=['GenDagConfig'],
                    ),
                    'OperatingUser': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_debit_card_ca_0000',),
                        dependencies=['CreOnlyUserPool', 'CrowdFeedback'],
                    ),
                    'CrowdPackage': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_debit_card_ca_0000',),
                        dependencies=['OperatingUser'],
                    ),
                    'AggReport': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_debit_card_ca_0000',),
                        dependencies=['CrowdPackage'],
                    ),
                    'UserPercentile': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_debit_card_ca_0000',),
                        dependencies=['CreOnlyUserPool'],
                    ),
                },
            ),
            'reco_debit_card_ca_0001': SchedulerDagConfig(
                start_date='2022-06-07',
                catchup=False,
                schedule_interval='50 1 * * *',  # 17:50
                tasks={
                    'GenDagConfig': SchedulerTaskConfig(
                        operator='gen_dag_config_task',
                        params=dict(dag_id='reco_debit_card_ca_0001',),
                        dependencies=[],
                    ),
                    'CampCreOnlyUserPool': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_debit_card_ca_0001',),
                        dependencies=['GenDagConfig'],
                    ),
                    'CrowdFeedback': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_debit_card_ca_0001',),
                        dependencies=['GenDagConfig'],
                    ),
                    'OperatingUser': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_debit_card_ca_0001',),
                        dependencies=['CampCreOnlyUserPool', 'CrowdFeedback'],
                    ),
                    'CrowdPackage': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_debit_card_ca_0001',),
                        dependencies=['OperatingUser'],
                    ),
                    'AggReport': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_debit_card_ca_0001',),
                        dependencies=['CrowdPackage'],
                    ),
                },
            ),
            'reco_credit_cancel_0000': SchedulerDagConfig(
                start_date='2022-06-07',
                catchup=False,
                schedule_interval='50 2 */7 * *',  #  18:50
                tasks={
                    'GenDagConfig': SchedulerTaskConfig(
                        operator='gen_dag_config_task',
                        params=dict(dag_id='reco_credit_cancel_0000',),
                        dependencies=[],
                    ),
                    'CancelUser': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_credit_cancel_0000',),
                        dependencies=['GenDagConfig'],
                    ),
                },
            ),
            'reco_adhoc_0000': SchedulerDagConfig(
                start_date='2023-04-01',
                catchup=False,
                schedule_interval='0 16 * * *',  #  08:00
                tasks={
                    'GenDagConfig': SchedulerTaskConfig(
                        operator='gen_dag_config_task',
                        params=dict(dag_id='reco_adhoc_0000',),
                        dependencies=[],
                    ),
                    'RecoS1106': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_adhoc_0000',),
                        dependencies=['GenDagConfig'],
                    ),
                    'RecoS1108': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_adhoc_0000',),
                        dependencies=['RecoS1106'],
                    ),
                    'RecoS1109': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_adhoc_0000',),
                        dependencies=['RecoS1108'],
                    ),
                    'RecoS1101': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_adhoc_0000',),
                        dependencies=['RecoS1109'],
                    ),
                    'StartConsumer': SchedulerTaskConfig(
                        operator='gen_local_task',
                        params=dict(dag_id='reco_adhoc_0000',),
                        dependencies=['RecoS1101'],
                    ),
                },
            ),
            'online_redis_consumer_start': SchedulerDagConfig(
                start_date='2023-04-01',
                catchup=False,
                schedule_interval='0 17 * * *',  #  09:00
                tasks={
                    'GenDagConfig': SchedulerTaskConfig(
                        operator='gen_dag_config_task',
                        params=dict(dag_id='reco_adhoc_0000',),
                        dependencies=[],
                    ),
                    'StartConsumer': SchedulerTaskConfig(
                        operator='gen_local_task',
                        params=dict(dag_id='reco_adhoc_0000',),
                        dependencies=['GenDagConfig'],
                    ),
                },
            ),
            'online_redis_consumer_stop': SchedulerDagConfig(
                start_date='2023-04-01',
                catchup=False,
                schedule_interval='0 4 * * *',  #  20:00
                tasks={
                    'GenDagConfig': SchedulerTaskConfig(
                        operator='gen_dag_config_task',
                        params=dict(dag_id='reco_adhoc_0000',),
                        dependencies=[],
                    ),
                    'StopConsumer': SchedulerTaskConfig(
                        operator='gen_local_task',
                        params=dict(dag_id='reco_adhoc_0000',),
                        dependencies=['GenDagConfig'],
                    ),
                },
            ),
            'ire_model_sync': SchedulerDagConfig(
                start_date='2023-07-25',
                catchup=False,
                schedule_interval='0 9 * * *',  #  24:00
                tasks={
                    'GenDagConfig': SchedulerTaskConfig(
                        operator='gen_dag_config_task',
                        params=dict(dag_id='reco_adhoc_0000',),
                        dependencies=[],
                    ),
                    'ModelResultsToDW': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='reco_adhoc_0000',),
                        dependencies=['GenDagConfig'],
                    ),
                    'CollectIREModelResults': SchedulerTaskConfig(
                        operator='gen_local_task',
                        params=dict(dag_id='reco_adhoc_0000',),
                        dependencies=['ModelResultsToDW'],
                    ),
                },
            ),
            'ire_user_pred_sync': SchedulerDagConfig(
                start_date='2023-09-20',
                catchup=False,
                schedule_interval='30 3 * * *',  #  19:30
                tasks={
                    'GenDagConfig': SchedulerTaskConfig(
                        operator='gen_dag_config_task',
                        params=dict(dag_id='base',),
                        dependencies=[],
                    ),
                    'UserPredResultsToDW': SchedulerTaskConfig(
                        operator='gen_spark_task',
                        params=dict(dag_id='base',),
                        dependencies=['GenDagConfig'],
                    ),
                    'CollectIREUserPredResults': SchedulerTaskConfig(
                        operator='gen_local_task',
                        params=dict(dag_id='base',),
                        dependencies=['UserPredResultsToDW'],
                    ),
                },
            ),
        },
        spark_env={
            'SPARK_DRIVER_CORES': '2',
            'SPARK_DRIVER_MEMORY': '4g',
            'SPARK_EXECUTOR_CORES': '4',
            'SPARK_EXECUTOR_MEMORY': '8g',
            'SPARK_EXECUTOR_MIN_NUM': '40',
            'SPARK_EXECUTOR_MAX_NUM': '100',
        },
        env_settings=env_settings,
    ).with_mock()

    return inst
