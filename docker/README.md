### Docker post install
1. Create a docker's virtual machine if not exists
```
docker-machine ls
docker-machine create -d virtualbox default
docker-machine ls

docker-machine env default
eval $(docker-machine env default)
env | grep DOCKER

docker-machine start default
docker-machine ip default
```
#### Port Forwarding
A Docker Machine is a virtual machine running under VirtualBox in your host machine. We can use the Port Forwarding feature of VirtualBox in order to access the Docker VM as localhost.

To achieve this does the following:

First, make sure your Docker Machine is stopped by executing the following:
```
docker-machine stop tableau-reporter.local
```
Then:
* Open VirtualBox Manager
* Select your Docker Machine VirtualBox image (e.g.: default)
* Open Settings -> Network -> Advanced -> Port Forwarding
* Add your app name, the desired host port and your guest port

Now you are ready to start your Docker Machine by executing the following:
```
docker-machine start default
eval $(docker-machine env default)
```

Then just start your Docker container, and you will be able to access it via localhost.

#### Login to Oracle repository and accept licence terms
Sign in on https://container-registry.oracle.com and then go to Database -> enterprise links. You should be asked to accept licence terms and finally land on an "Oracle Database Server 21.3.0.0 Docker Image Documentation" page. Then you should be able to pull the image.


#### Docker commands
```
docker login container-registry.oracle.com


docker-compose -f docker-compose.yml build
docker-compose -f docker-compose.yml up -d --no-build
docker-compose -f docker-compose.yml down

docker-compose -f docker-compose.yml build oracle
docker-compose -f docker-compose.yml up -d --no-build oracle
docker-compose -f docker-compose.yml stop oracle

docker-compose -f docker-compose.yml build airflow-database
docker-compose -f docker-compose.yml up -d --no-build airflow-database
docker-compose -f docker-compose.yml stop airflow-database

docker-compose -f docker-compose.yml build airflow
docker-compose -f docker-compose.yml up -d --no-build airflow
docker-compose -f docker-compose.yml stop airflow

docker exec -it partition-based-cdc-airflow /bin/sh
docker exec --user="root" -it partition_based_cdc_airflow /bin/bash

docker image ls --all
docker image prune -a --force

```

Restart Docker daemon
```
killall Docker && open /Applications/Docker.app
```
