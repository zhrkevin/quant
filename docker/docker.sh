#!/bin/bash
# statement: if you have MacOS, please replace 0.0.0.0 (or localhost) with docker.for.mac.host.internal
# ${Operation} == build, pull, up, down, remove, check
# ${Version} == environment, stable


Registry='acr-uat-registry-vpc.cn-shanghai.cr.aliyuncs.com/fx-alliance/algorithms'
Project='chatgpt-modeling'
Operation=${1-'check'}
Version=${2-'stable'}
Compile=${3-'false'}


if [[ ${Operation} = 'build' ]]; then
    docker build \
        --no-cache \
        --progress plain \
        --build-arg Compile=${Compile} \
        --file docker/dockerfile-${Version} \
        --tag ${Registry}/${Project}:${Version} .
elif [[ ${Operation} = 'pull' ]]; then
    docker pull ${Registry}/${Project}:${Version}
    docker tag ${Registry}/${Project}:${Version} ${Project}:${Version}
    docker rmi --force ${Registry}/${Project}:${Version} || true
elif [[ ${Operation} = 'up' ]]; then
    docker run \
        --restart always \
        --name ${Project} \
        --hostname ${Project} \
        --network algorithms \
        --publish 15001:15001 \
        --volume /app/projects/chatgpt-modeling/model:/application/data/model \
        --detach --interactive --tty ${Project}:${Version} \
        python -B -u /application/main.py \
            Information \
                --PublicHost=10.76.49.106 \
                --Mode=production \
            RabbitMQ \
                --URL=amqp://guest:guest@10.76.49.108:5672// \
            MinIO \
                --Endpoint=10.76.49.108:9089
elif [[ ${Operation} = 'down' ]]; then
    docker rm --force ${Project} || true
elif [[ ${Operation} = 'remove' ]]; then
    docker rmi --force ${Project}:${Version} || true
elif [[ ${Operation} = 'check' ]]; then
    pwd && ls -alhF --color=auto ./
    docker images && docker ps && docker logs --tail 200 ${Project}
else
    printf '命令行错误，请再次检查。\n'
fi
