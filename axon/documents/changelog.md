#


### **[更新日志（Changelog）](#)**


#### **Version 20230801**

???+ algorithm-changelog "算法功能 Algorithm"
    * 升级了依赖的相关版本
    * 重新剥离了 middlewares 目录作为单独的主目录
    * 替换原始路径包 os 为 pathlib
    * 替换 pip 打包安装工具为 pdm 打包安装工具，实现 requirement 一键导出，无需再人工维护
    * 修改了 api 和 algorithms 目录中 .py 文件的命名方式

???+ project-changelog "工程功能 Project"
    * 合并了工程的 documents 和 swagger 目录
    * 升级了 mkdocs 和 swagger-ui 包的版本
    * 修改了 copyright
    * 重新配置了 dockerfile 部署方式
    * 重新配置了 aliyun flow 流水线部署流程
    * 修改了 docker.sh 部署命令行，现在可以直接与 aliyun flow 结合使用


#### **Version 20230701**

???+ algorithm-changelog "算法功能 Algorithm"
    * 新增内容生成（content classification）算法 API
    * 修改自然语言转化 SQL（NLSQL）算法 API

???+ project-changelog "工程功能 Project"
    * 更新了工程的整体框架和文档信息


#### **Version 20230601**

???+ algorithm-changelog "算法功能 Algorithm"
    * 新增自然语言转化 SQL（NLSQL）算法 API

???+ project-changelog "工程功能 Project"
    * 新建框架工程
