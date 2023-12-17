#!/bin/bash

set -x

config_dir=${4}
workdir=${5:-./}/${2}/${1}
mkdir -p ${workdir}

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

export PYSPARK_PYTHON=/tianrang/miniconda/bin/python3
export PYSPARK_DRIVER_PYTHON=/tianrang/miniconda/bin/python3

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
    --name recommend_v2_${1}_${2}_${3} \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
    --files "log4j.properties" \
    --py-files code.zip,runtime_config.json \
    entrypoint.py --dag_id $1 --batch_id $2 --job_name $3

RET=$?
LOG_INFO "======== job $* end, return code is ${RET}"
popd

TIME_STAMP2=`date +%s`
TIME_CONSUMING=`expr $TIME_STAMP2 - $TIME_STAMP`

if [ ${RET} -eq 0 ]; then
    LOG_INFO "task: $1-$2-$3 succeed in ${TIME_CONSUMING}s."
else
    LOG_ERROR "task: $1-$2-$3 failed in ${TIME_CONSUMING}s."
    exit -1
fi
