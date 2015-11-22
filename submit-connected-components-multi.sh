#!/bin/env bash
#PBS -l walltime=00:40:00
#PBS -l nodes=3:ppn=12
#PBS -A plggrzmiejski2015a
#PBS -q plgrid-testing


module load plgrid/apps/spark
start-multinode-spark-cluster.sh
$SPARK_HOME/bin/spark-submit --master spark://`hostname`:7077 \
    --class connectedcomponents.SimpleConnectedComponents connected-components.jar false
stop-multinode-spark-cluster.sh
