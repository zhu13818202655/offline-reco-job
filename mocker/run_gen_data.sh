#!/bin/bash

docker run -it --rm \
       -v $PWD:/ganjiang \
       --entrypoint bash \
       --workdir /ganjiang/mocker \
       node:16.0.0 gen_data.sh
