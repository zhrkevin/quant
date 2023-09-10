### Docker Distribution, Starting and Debugging


##### docker image operation
```shell script
# build image
docker build --no-cache --file docker/dockerfile-${version} --tag ${image} .

# remove image
docker rmi --force $(docker images --quiet)
```


##### docker container operation
```shell script
# check log 
docker logs -f project-framework-main >> running.log

# debug container
docker exec --interactive --tty project-framework-main /bin/bash

# clean all unused container
docker rm --force $(docker ps --quiet --all)
```


##### docker network operation
```shell script
# list all networks
docker network ls

# create network
docker network create algorithms

# inspect network
docker network inspect algorithms
```
