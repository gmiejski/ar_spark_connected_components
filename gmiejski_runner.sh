#!/bin/bash                                     
#PBS -l walltime=01:00:00                       
#PBS -l pmem=64mb                              
#PBS -l nodes=1:ppn=12                        
#PBS -A plggrzmiejski2015a                   
#PBS -q plgrid-testing                      


#module load tools/mpiexec/0.84-impi
#module load el6/openmpi/1.8-pgi-14.3-ib
#module load mpiexec
#module load mvapich2
#module load tools/python/3.4.0
#pip install --user mpi4py
#module load mvapich2
#module load mpiexec


module load mvapich2
module load mpiexec
#module add plgrid/tools/python/3.4.0
module add plgrid/tools/python/2.7.5

cd /people/plggrzmiejski/lab1

rm -rf results_big/                            
mkdir results_big
 
tableSizes=(60 90)
threadCounts=(1 3 5)
 
for tableSize in "${tableSizes[@]}"
do
    echo "Problem Size: " $tableSize
    for threadCount in "${threadCounts[@]}"
    do
        echo "--Thread number: " $threadCount
	mpiexec -n $threadCount python ./src/main.py  $tableSize 100 >> results_big/results_${tableSize}_proc${threadCount}
    done
done
