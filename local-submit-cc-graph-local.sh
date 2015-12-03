
#PBS -l walltime=01:00:00
#PBS -l nodes=3:ppn=12
#PBS -A plggrzmiejski2015a
#PBS -q plgrid-testing

resultsFolder="results"

#mkdir $resultsFolder
#echo "creating project "$resultsFolder

ls $resultsFolder

verticesCounts=(1000000)
edgesCounts=(100000)
executorsCounts=(2 4)

for verticesCount in "${verticesCounts[@]}"
do
    echo "Vertices count: " $verticesCount
    for edgesCount in "${edgesCounts[@]}"
    do
        echo "-- Edges count: " $edgesCount
        for executorsCount in "${executorsCounts[@]}"
        do
            echo "---- Executors count: " $executorsCount
            /Users/grzegorz.miejski/programming/spark/spark-1.5.0-bin-hadoop2.6/bin/spark-submit --master "local[$executorsCount]
            " --class connectedcomponents.ConnectedComponentsGeneratedGraphPregel  connected-components.jar $executorsCount $verticesCount $edgesCount true  >> $resultsFolder/results_${verticesCount}_${edgesCount}_${executorsCount} 2>&1
        done
    done
done



stop-multinode-spark-cluster.sh
