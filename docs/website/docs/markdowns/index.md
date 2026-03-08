#


#### **[项目算法接口（Project API）](/quant/swagger/index.html)**



<!-- ???+ quote "必要依赖服务"
    ``` json
    --8<-- "./version"
    ``` -->


???+ abstract "配置与执行命令"

    **Docker / Kubernetes 镜像构建命令**
    ``` shell
    docker build \
        --no-cache \
        --progress plain \
        --file 'docker/dockerfile' \
        --target ${分支} \
        --tag ${镜像名称}:${分支}-latest .
    ```

    **Docker / Kubernetes 容器启动命令**
    ``` shell
    docker run \
        --restart always \
        --name ${容器名称} \
        --hostname ${容器名称} \
        --publish ${宿主机算法服务端口}:${容器算法服务端口} \
        --detach --interactive --tty ${镜像名称}:${分支}-latest
    ```

    **算法引擎启动命令**
    ``` shell
    python -B -u /application/project_main.py Information --Mode=production
    ```
