#!/bin/env bash
#PBS -l walltime=01:00:00
#PBS -l nodes=3:ppn=12
#PBS -A plggrzmiejski2015a
#PBS -q plgrid-testing

resultsFolder="results"

rm -rf $resultsFolder/
mkdir $resultsFolder
echo "creating project "$resultsFolder

ls $resultsFolder

module load plgrid/apps/spark
start-multinode-spark-cluster.sh

`cat $PBS_NODEFILE | uniq | grep -v $SPARK_MASTER_HOST`

executorsCounts=(2 4 8)
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
            $SPARK_HOME/bin/spark-submit --master spark://`hostname`:7077 --class connectedcomponents.ConnectedComponentsGeneratedGraph  $HOME/lab2/connected-components.jar $@  >> $resultsFolder/results_${verticesCount}_${edgesCount}_${executorsCount} 2>&1
        done
    done
done



stop-multinode-spark-cluster.sh
