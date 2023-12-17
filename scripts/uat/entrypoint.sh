#!/bin/bash

set -x

config_dir=${4}
workdir=${5:-./}/${2}/${1}
mkdir -p ${workdir}
master=${6:-yarn}
deploy_mode=${7:-cluster}

config_path="${config_dir}/${2}/${1}.json"

LOG_INFO() {
    local msg=$*
    echo "[INFO ][$(date +'%Y-%m-%d %H:%M:%S')][$0] ${msg}"
}

LOG_ERROR() {
    local msg=$*
    echo "[ERROR][$(date +'%Y-%m-%d %H:%M:%S')][$0] ${msg}"
}


LOG_INFO "task $0 starts with parameters: $*"
TIME_STAMP=`date +%s`
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

# export PYSPARK_PYTHON=/tianrang/miniconda/bin/python3
# export PYSPARK_DRIVER_PYTHON=/tianrang/miniconda/bin/python3

cd ${SRC_DIR}
cp ${SRC_DIR}/log4j.properties ${workdir}

pushd ${SRC_DIR}/../../src/
(

    # 打 zip 包在 dag 前加个任务做，每个 dag 一个任务包（带上版本信息）
    # 同时调offline服务，生成配置文件
    # workdir/DAG00/code.zip
    # workdir/DAG00/runtime_config.json
    rm -f ${workdir}/code.zip
    zip -rq ${workdir}/code.zip ./ -x "*.pyc"

    if [ ! -f ${config_path} ]; then
        LOG_ERROR "config ${config_path} not exist, treat as an ConfiglessJob"
        echo "{}" > ${workdir}/runtime_config.json
    else
        cp ${config_path} ${workdir}/runtime_config.json
    fi
    cp entrypoint.py ${workdir}/
)
popd

pushd ${workdir}

# miniconda.zip 打包方式： cd /tianrang/miniconda && zip -rq miniconda.zip *
${KAUTH_SCRIPT} spark-submit \
    --name reco_${1}_${2}_${3} \
    --master ${master}\
    --deploy-mode ${deploy_mode} \
    --driver-cores ${SPARK_DRIVER_CORES:-2}  \
    --driver-memory ${SPARK_DRIVER_MEMORY:-4g}  \
    --executor-cores ${SPARK_EXECUTOR_CORES:-2}  \
    --executor-memory ${SPARK_EXECUTOR_MEMORY:-8g} \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.initialExecutors=${SPARK_EXECUTOR_MIN_NUM:-8} \
    --conf spark.dynamicAllocation.minExecutors=${SPARK_EXECUTOR_MIN_NUM:-8} \
    --conf spark.dynamicAllocation.maxExecutors=${SPARK_EXECUTOR_MAX_NUM:-16} \
    --conf spark.sql.autoBroadcastJoinThreshold=-1 \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
    --archives hdfs:///user/tech/uat/env/miniconda.zip#miniconda \
    --conf spark.yarn.appMasterEnv.RECO_ENVIRONMENT=${RECO_ENVIRONMENT} \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./miniconda/bin/python \
    --conf spark.executorEnv.PYSPARK_PYTHON=./miniconda/bin/python \
    --files "log4j.properties" \
    --py-files code.zip,runtime_config.json \
    entrypoint.py --dag_id $1 --batch_id $2 --job_name $3 2>&1 | tee ${workdir}/client_${3}.log

RET=${PIPESTATUS[0]}
LOG_INFO "======== job $* end, return code is ${RET}"

application_id=`cat ${workdir}/client_${3}.log | grep "tracking URL:" | sed 's/\/$//' | awk -F '/' 'END { print $(NF) }'`
user_id=`cat ${workdir}/client_${3}.log | grep "user:" | awk -F ' ' 'END { print $(NF) }'`
yarn_log_dir=/tmp/logs/${user_id}/logs-ifile/${application_id}
if hadoop fs -test -e ${yarn_log_dir} > /dev/null 2>&1; then
    hadoop fs -cat ${yarn_log_dir}/* | tr -cd '\11\12\15\40-\176'
else
    LOG_ERROR "yarn log ${yarn_log_dir} not exist in hdfs!"

    yarn_log_dir=/tmp/logs/${user_id}/logs-tfile/${application_id}
    if hadoop fs -test -e ${yarn_log_dir} > /dev/null 2>&1; then
        hadoop fs -cat ${yarn_log_dir}/* | tr -cd '\11\12\15\40-\176'
    else
        LOG_ERROR "yarn log ${yarn_log_dir} not exist in hdfs!"
    fi
fi

popd

TIME_STAMP2=`date +%s`
TIME_CONSUMING=`expr $TIME_STAMP2 - $TIME_STAMP`

if [ ${RET} -eq 0 ]; then
    LOG_INFO "task: $1-$2-$3 succeed in ${TIME_CONSUMING}s."
else
    LOG_ERROR "task: $1-$2-$3 failed in ${TIME_CONSUMING}s."
    exit -1
fi
