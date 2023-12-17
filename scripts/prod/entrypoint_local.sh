#!/bin/bash

set -x

${KAUTH_SCRIPT} python -m entrypoint \
       --dag_id ${1} \
       --batch_id ${2} \
       --job_name ${3} \
       --config_path ${4}/${2}/${1}.json
