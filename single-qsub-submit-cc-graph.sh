#!/bin/env bash
#PBS -l walltime=01:00:00
#PBS -l nodes=3:ppn=12
#PBS -A plggrzmiejski2015a
#PBS -q plgrid-testing

usage(){
	echo "Usage: $0 numberOfCores verticesCount edgesCount debug"
	exit 1
}

resultsFolder="results"

rm -rf $resultsFolder/
mkdir $resultsFolder

module load plgrid/apps/spark
start-multinode-spark-cluster.sh

`cat $PBS_NODEFILE | uniq | grep -v $SPARK_MASTER_HOST`


if [[ "$#" -lt 4 ]]; then
    usage
fi
executorsCounts=(2 4 6 8)
verticesCounts=(1000000 2000000 4000000)
edgesCounts=(100000 200000 400000)

for verticesCount in "${verticesCounts[@]}"
do
    echo "Vertices count: " $verticesCount
    for edgesCount in "${edgesCounts[@]}"
    do
        echo "-- Edges count: " $edgesCount
        for executorsCount in "${executorsCounts[@]}"
        do
            echo "---- Executors count: " $executorsCount
            $SPARK_HOME/bin/spark-submit --master spark://`hostname`:7077 --class connectedcomponents.ConnectedComponentsGeneratedGraph  $HOME/lab2/connected-components.jar $@
        done
    done
done



stop-multinode-spark-cluster.sh
