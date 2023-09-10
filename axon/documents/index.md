#


### **[项目算法接口（Project API）](/swagger/index.html)**


#### **前期准备**

???+ quote "相关依赖服务"

    ``` yaml linenums="1"
    RabbitMQ 版本 3.11.17
    MinIO 版本 20230601
    ```


#### **配置文件**

???+ info "docker 命令行"

    **可修改相关启动配置参数**
    ``` shell linenums="1"
    docker run \
        --restart always \
        --network algorithms \
        --publish ${宿主机算法服务端口}:${容器算法服务端口} \
        --name ${容器名称} \
        --hostname ${容器名称} \
        --detach --interactive --tty ${镜像名称}:${版本号} \
        python -B -u /application/project_main.py \
            Information \
                --PublicHost=${宿主机对外 IP 地址} \
            Callbacks \
                --InputsCallback=${输入数据回调 URL} \
                --OutputsCallback=${输出结果回调 URL} \
            RabbitMQ \
                --Mode=production \
                --URL=${RabbitMQ 通信 URL} \
            MinIO \
                --Mode=production \
                --Endpoint=${MinIO 通信 Endpoint}
    ```
