#!/bin/bash
# ---------------------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------------------

# statement: if you have MacOS, please replace 0.0.0.0 with docker.for.mac.host.internal
# ${MODE} == development, test, production


MODE=${1-'development'}


if [[ ${MODE} = 'development' ]]; then
    printf '算法启动模式：development \n'
elif [[ ${MODE} = 'test' ]]; then
    python -B -u main.py \
        Information --Mode=test \
        MinIO --Bucket=non-standard-aps \
        RabbitMQ --CallbackQueue=non-standard-aps-callback
elif [[ ${MODE} = 'production' ]]; then
    python -B -u main.py \
        Information --Mode=production \
        MinIO --Bucket=non-standard-aps-prod \
        RabbitMQ --CallbackQueue=non-standard-aps-callback-prod
else
    printf '请检查命令行输入。\n'
fi
