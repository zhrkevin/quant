#!/bin/bash
# ---------------------------------------------------------------
# Copyright 2024 Jingzhi A&I and Level D&T. All Rights Reserved.
# ---------------------------------------------------------------

# statement: if you have MacOS, please replace 0.0.0.0 (or localhost) with docker.for.mac.host.internal
# ${Operation} == build, up, down, remove, check
# ${Version} == environment, stable, compile, main


OPERATION=${1-'check'}
BRANCH=${2-'stable'}
IMAGE='quant'
CONTAINER='quant'


if [[ ${Operation} = 'build' ]]; then
    docker build \
        --no-cache \
        --progress plain \
        --file 'docker/dockerfile' \
        --target=${BRANCH} \
        --tag ${IMAGE}:${BRANCH}-latest .
elif [[ ${Operation} = 'up' ]]; then
    docker run \
        --restart always \
        --name ${CONTAINER} \
        --hostname ${CONTAINER} \
        --publish 15001:10000 \
        --detach --interactive --tty ${IMAGE}:${BRANCH}-latest
elif [[ ${Operation} = 'down' ]]; then
    docker rm --force ${CONTAINER}
elif [[ ${Operation} = 'remove' ]]; then
    docker rmi --force ${IMAGE}:${BRANCH}-latest
elif [[ ${Operation} = 'check' ]]; then
    pwd
    ls -alhF --color=auto ./
    docker ps
    docker images
else
    printf '请检查命令行输入。\n'
fi
