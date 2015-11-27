#!/bin/env bash
#PBS -l walltime=01:00:00
#PBS -l nodes=3:ppn=12
#PBS -A plggrzmiejski2015a
#PBS -q plgrid-testing

usage(){
	echo "Usage: $0 numberOfCores verticesCount edgesCount debug"
	exit 1
}

if [[ "$#" -lt 4 ]]; then
    usage
fi

echo $1
echo $2
echo $3

module load plgrid/apps/spark
start-multinode-spark-cluster.sh
$SPARK_HOME/bin/spark-submit \
--master spark://`hostname`:7077 \
--class connectedcomponents.ConnectedComponentsGeneratedGraph \
$HOME/lab2/connected-components.jar $@
stop-multinode-spark-cluster.sh
