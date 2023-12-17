#!/bin/bash

set -ex

ENV=${1:-uat}
BATCH_ID=${2:-20220131}

OFFLINE_ADDR=http://bdasire-offline-reco-api-${ENV}:5000
ENTRYPOINT_SH=/tianrang/airflow/offline-reco-jobs/scripts/prod/entrypoint.sh
ENTRYPOINT_LOCAL_SH=/tianrang/airflow/offline-reco-jobs/scripts/prod/entrypoint_local.sh
DAG_CONFIG_PATH=/tianrang/airflow/workspace/config/dag_configs/${IMAGE_VERSION}.yaml


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

${ENTRYPOINT_SH} base ${BATCH_ID} PrepareModelSample \
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

