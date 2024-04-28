#


### **[项目算法接口（Project API）](/quant/swagger/index.html)**



#### **相关依赖服务**

???+ quote "版本号"

    ``` yaml linenums="1"
    MinIO版本: 2024-03-30
    RabbitMQ版本: 3.13.0
    ```


#### **配置文件**

???+ info "命令行"

    **kubernetes / docker 容器配置**
    ``` shell linenums="1"
    docker run \
        --restart always \
        --name ${容器名称} \
        --hostname ${容器名称} \
        --publish ${宿主机算法服务端口}:${容器算法服务端口} \
        --detach --interactive --tty ${镜像名称}:${分支号}-latest
    ```

    **算法引擎启动命令**
    ``` shell linenums="1"
    python -B -u /application/project_main.py Information --Mode=production
    ```
