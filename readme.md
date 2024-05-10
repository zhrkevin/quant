
### 服务接口地址
```text
# 算法 API 地址

# Non Standard APS 系统地址
```


### Docker / Kubernetes 镜像
```text
# 环境镜像 
quant:environment-latest

# 测试服务镜像 
quant:test-latest

# 生产服务镜像 
quant:production-latest    
```


### Python 启动命令
```text
python -B -u main.py Information --Mode=production    
```


### 项目目录结构

```text
./   
├──  algorithms/         算法目录   
├──  api/                网络通信 API 目录    
├──  axon/               在线文档与 swagger 目录   
├──  data/               数据存储目录    
├──  docker/             镜像构建目录    
├──  project/            项目与配置目录     
├──  .gitignore          gitignore 忽略文件   
├──  .gitlab-ci.yml      gitlab runner 自动集成配置文件   
├──  main.py             主函数文件   
├──  pyproject.toml      依赖包配置文件   
├──  readme.md           说明文件    
└──  requirements.txt    依赖包安装文件                     
```
