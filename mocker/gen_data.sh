#!/bin/bash

set -eo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

NUM=${1:-5000}

OUTPUT_DIR=${DIR}/../mocker/.runtime/data/
mkdir -p ${OUTPUT_DIR}

pushd ${DIR}
(
    # npm install

    PIDS=()
    IDX=0

    for file in `find ${DIR}/data -name "*.js"`; do
        basename=`basename ${file}`
        echo "正在生成：" ${basename}
        ext="${basename##*.}"
        sleep 1 && node lib/cli.js mock --count=${NUM} --csv-delimiter $'\001' --csv-header ${file} > ${OUTPUT_DIR}/"${basename%.*}.csv" 2>&1 &
        PIDS[${IDX}]=$! && let "IDX+=1"
    done

    for pid in ${PIDS[@]};
    do
        if wait $pid; then
            :
        else
            echo "异常退出，请检查 js 文件!"
            exit 1
        fi
    done
    echo "全部数据生成完成!"
)

popd
