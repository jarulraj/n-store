#!/usr/bin/bash
# RUN EXPERIMENTS

DEFAULT_LATENCY=100
LOG_DIR=data
SDV_SCRIPT=/data/devel/sdv-tools/sdv-release/ivt_pm_sdv.sh
NSTORE=./src/nstore 

# NSTORE FLAGS
KEYS=100000 
TXNS=100000 
PATH=/mnt/pmfs/n-store/

for ((i=2; i<=2; i*=4))
do
    l=$(($i*$DEFAULT_LATENCY))

    echo "LATENCY" $l
    #sudo $SDV_SCRIPT --enable --pm-latency=$l
    echo "---------------------------------------------------"

    #rw_mix=(0 10 50)
    #skew=(0.5 1.0 1.5)

    rw_mix=(10)
    skew=(1.0)
    
    for j  in "${rw_mix[@]}"
    do
        for k  in "${skew[@]}"
        do
            echo "RW MIX ::" $j  " SKEW ::" $k
            $NSTORE -k $KEYS -x $TXNS -f $PATH
        done
    done
done


