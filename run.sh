#!/bin/bash
# ---------------------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------------------

# statement: if you have MacOS, please replace 0.0.0.0 with docker.for.mac.host.internal
# ${MODE} == development, test, production


MODE=${1-'development'}


if [[ ${MODE} = 'development' ]]; then
    python -B -u main.py \
        Information --Mode=development
elif [[ ${MODE} = 'test' ]]; then
    python -B -u main.py \
        Information --Mode=test \
        MinIO --Bucket=quant-test \
        RabbitMQ --CallbackQueue=quant-callback-test
elif [[ ${MODE} = 'production' ]]; then
    python -B -u main.py \
        Information --Mode=production \
        MinIO --Bucket=quant-production \
        RabbitMQ --CallbackQueue=quant-callback-production
else
    echo -e "算法服务未启动，请检查命令行: bash docker.sh ${MODE}"
fi
