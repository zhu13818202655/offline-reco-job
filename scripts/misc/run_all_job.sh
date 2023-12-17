#!/bin/bash

set -ex

ENV=uat
BATCH_ID=20220301
OFFLINE_ADDR=http://bdasire-offline-reco-api-${ENV}:5000
ENTRYPOINT_SH=/tianrang/airflow/offline-reco-jobs/scripts/prod/entrypoint.sh
ENTRYPOINT_LOCAL_SH=/tianrang/airflow/offline-reco-jobs/scripts/prod/entrypoint_local.sh
DAG_CONFIG_PATH=/tianrang/airflow/workspace/config/dag_configs/${IMAGE_VERSION}.yaml

function startup() {
    python -m jobs.init.gen_dag_config\
           --config_path ${DAG_CONFIG_PATH} \
           --dag_id base \
           --batch_id ${BATCH_ID} \
           --output_dir /tianrang/airflow/workspace/config/startup


    ${ENTRYPOINT_SH} base ${BATCH_ID} PrepareTable \
                     /tianrang/airflow/workspace/config/startup \
                     /tianrang/airflow/workspace/runtime/startup

    ${ENTRYPOINT_SH} base ${BATCH_ID} FitFeatureEngine \
                     /tianrang/airflow/workspace/config/startup \
                     /tianrang/airflow/workspace/runtime/startup


    python -m jobs.init.gen_dag_config \
           --config_path ${DAG_CONFIG_PATH} \
           --dag_id reco_credit_card_ca_0000 \
           --batch_id ${BATCH_ID} \
           --output_dir /tianrang/airflow/workspace/config/startup


    ${ENTRYPOINT_SH} reco_credit_card_ca_0000 ${BATCH_ID} FitUserModel \
                     /tianrang/airflow/workspace/config/startup \
                     /tianrang/airflow/workspace/runtime/startup

    ${ENTRYPOINT_SH} reco_credit_card_ca_0000 ${BATCH_ID} FitRecoModel \
                     /tianrang/airflow/workspace/config/startup \
                     /tianrang/airflow/workspace/runtime/startup


    python -m jobs.init.gen_dag_config \
           --config_path ${DAG_CONFIG_PATH} \
           --dag_id reco_debit_card_ca_0000 \
           --batch_id ${BATCH_ID} \
           --output_dir /tianrang/airflow/workspace/config/startup


    ${ENTRYPOINT_SH} reco_debit_card_ca_0000 ${BATCH_ID} FitUserModel \
                     /tianrang/airflow/workspace/config/startup \
                     /tianrang/airflow/workspace/runtime/startup

    ${ENTRYPOINT_SH} reco_debit_card_ca_0000 ${BATCH_ID} FitRecoModel \
                     /tianrang/airflow/workspace/config/startup \
                     /tianrang/airflow/workspace/runtime/startup

}

function base() {
    python -m jobs.init.upload_dag_config \
           --offline_backend_addr ${OFFLINE_ADDR} \
           --config_path /tianrang/airflow/workspace/config/dag_configs/v0.0.0.yaml \
           --batch_id ${BATCH_ID}

    python -m jobs.init.gen_dag_config \
           --config_path ${DAG_CONFIG_PATH} \
           --batch_id ${BATCH_ID} \
           --dag_id base \
           --output_dir /tianrang/airflow/workspace/config


    ${ENTRYPOINT_SH} base ${BATCH_ID} UserFeatures \
                     /tianrang/airflow/workspace/config \
                     /tianrang/airflow/workspace/runtime


    ${ENTRYPOINT_LOCAL_SH} base ${BATCH_ID} Cleanup \
                           /tianrang/airflow/workspace/config
}


function dag0() {
    # reco_credit_card_ca_0000
    python -m jobs.init.gen_dag_config \
           --config_path ${DAG_CONFIG_PATH} \
           --batch_id ${BATCH_ID} \
           --dag_id reco_credit_card_ca_0000 \
           --output_dir /tianrang/airflow/workspace/config

    ${ENTRYPOINT_SH} reco_credit_card_ca_0000 ${BATCH_ID} DebOnlyUserPool\
                     /tianrang/airflow/workspace/config \
                     /tianrang/airflow/workspace/runtime

    ${ENTRYPOINT_SH} reco_credit_card_ca_0000 ${BATCH_ID} OperatingUser\
                     /tianrang/airflow/workspace/config \
                     /tianrang/airflow/workspace/runtime

    ${ENTRYPOINT_SH} reco_credit_card_ca_0000 ${BATCH_ID} CrowdPackage\
                     /tianrang/airflow/workspace/config \
                     /tianrang/airflow/workspace/runtime

    ${ENTRYPOINT_SH} reco_credit_card_ca_0000 ${BATCH_ID} CrowdFeedback\
                     /tianrang/airflow/workspace/config \
                     /tianrang/airflow/workspace/runtime
}

function dag1() {
    # reco_debit_card_ca_0000
    python -m jobs.init.gen_dag_config \
           --config_path ${DAG_CONFIG_PATH} \
           --batch_id ${BATCH_ID} \
           --dag_id reco_debit_card_ca_0000 \
           --output_dir /tianrang/airflow/workspace/config

    ${ENTRYPOINT_SH} reco_debit_card_ca_0000 ${BATCH_ID} CreOnlyUserPool\
                     /tianrang/airflow/workspace/config \
                     /tianrang/airflow/workspace/runtime

    ${ENTRYPOINT_SH} reco_debit_card_ca_0000 ${BATCH_ID} OperatingUser\
                     /tianrang/airflow/workspace/config \
                     /tianrang/airflow/workspace/runtime

    ${ENTRYPOINT_SH} reco_debit_card_ca_0000 ${BATCH_ID} CrowdPackage\
                     /tianrang/airflow/workspace/config \
                     /tianrang/airflow/workspace/runtime

    ${ENTRYPOINT_SH} reco_debit_card_ca_0000 ${BATCH_ID} CrowdFeedback\
                     /tianrang/airflow/workspace/config \
                     /tianrang/airflow/workspace/runtime
}

function post_proc0() {
    # post_proc_sms
    python -m jobs.init.gen_dag_config \
           --config_path ${DAG_CONFIG_PATH} \
           --batch_id ${BATCH_ID} \
           --dag_id post_proc_sms \
           --output_dir /tianrang/airflow/workspace/config

    ${ENTRYPOINT_SH} post_proc_sms ${BATCH_ID} PostProcessSMS\
                     /tianrang/airflow/workspace/config \
                     /tianrang/airflow/workspace/runtime

    ${ENTRYPOINT_LOCAL_SH} post_proc_sms ${BATCH_ID} CollectRecoResults \
                           /tianrang/airflow/workspace/config
}


function post_proc1() {
    # post_proc_whitelist

    python -m jobs.init.gen_dag_config \
           --config_path ${DAG_CONFIG_PATH} \
           --batch_id ${BATCH_ID} \
           --dag_id post_proc_whitelist \
           --output_dir /tianrang/airflow/workspace/config

    ${ENTRYPOINT_SH} post_proc_whitelist ${BATCH_ID} PostProcessWhitelist\
                     /tianrang/airflow/workspace/config \
                     /tianrang/airflow/workspace/runtime
}


startup
base
dag0
dag1
post_proc0
post_proc1
