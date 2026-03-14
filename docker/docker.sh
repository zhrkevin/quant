#!/bin/bash
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

# statement: if you have MacOS, please replace 0.0.0.0 (or localhost) with docker.for.mac.host.internal
# ${Operation} == build, up, down, remove, check
# ${Version} == environment, stable, compile, main


OPERATION=${1-'check'}
BRANCH=${2-'test'}
IMAGE='quant'
CONTAINER='quant'


if [[ ${OPERATION} = 'build' ]]; then
    docker build \
        --no-cache \
        --progress plain \
        --file 'docker/dockerfile' \
        --target=${BRANCH} \
        --tag ${IMAGE}:${BRANCH}-latest .
elif [[ ${OPERATION} = 'up' ]]; then
    docker run \
        --restart always \
        --name ${CONTAINER} \
        --hostname ${CONTAINER} \
        --publish 10001:10001 \
        --detach --interactive --tty ${IMAGE}:${BRANCH}-latest
elif [[ ${OPERATION} = 'check' ]]; then
    pwd
    ls -alhF --color=auto ./
    docker ps
    docker images
else
    printf '请检查命令行输入。\n'
fi
