
### 服务接口地址
```text
# 算法 API 地址

# Quant 系统地址
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
```commandline
python -B -u main.py Information --Mode=production    
```

###
```text
# pdm 更新
pdm update --no-self --update-all --unconstrained --verbose
```

### 项目目录结构

```text
./   
├──  algorithms/         算法目录
├──  api/                API 目录
├──  data/               数据存储目录
├──  docker/             镜像构建目录
├──  docs/               文档目录
├──  project/            项目与配置目录
├──  main.py             主函数文件
├──  pyproject.toml      依赖包配置文件
├──  readme.md           说明文件
└──  run.sh              运行脚本文件
```
