#!/usr/bin/env bash
# Start up a mysql container with initial user/database setup.
MYSQL_VERSION=8.0.32

start() {
    echo "attempting to remove old $CONTAINER container..."
    docker rm -f $CONTAINER

    # start up mysql
    echo "starting up dataregistry mysql container..."
    BASEDIR=$(dirname "$0")
    docker create --name $CONTAINER --health-cmd="mysqladmin ping" --health-start-period="2s" --health-interval="1s" \
    -e MYSQL_ALLOW_EMPTY_PASSWORD=true -p $PORT:3306 mysql:$MYSQL_VERSION
    docker cp $BASEDIR/local-mysql-init.sql $CONTAINER:/docker-entrypoint-initdb.d/docker_mysql_init.sql
    docker start $CONTAINER
    # don't let this script finish until mysql is actually up and running aka the health check (mysqladmin ping) gives the ok
    while [ $(docker inspect --format "{{json .State.Health.Status }}" $CONTAINER) != "\"healthy\"" ]; do printf "."; sleep 1; done
    sleep 8;

}

stop() {
    echo "Stopping docker $CONTAINER container..."
    docker stop $CONTAINER || echo " stop failed. $CONTAINER already stopped."
    docker rm -v $CONTAINER
    exit 0
}

CONTAINER=data-registry-mysql
COMMAND=$1
PORT=${2:-"3307"}

if [ ${#@} == 0 ]; then
    echo "Usage: $0 stop|start"
    exit 1
fi

if [ $COMMAND = "start" ]; then
    start
elif [ $COMMAND = "stop" ]; then
    stop
else
    exit 1
fi
