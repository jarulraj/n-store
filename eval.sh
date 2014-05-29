#!/bin/bash
# RUN EXPERIMENTS

DEFAULT_LATENCY=100
LOG_DIR=data

SDV_DIR=/data/devel/sdv-tools/sdv-release
SDV_SCRIPT=$SDV_DIR/ivt_pm_sdv.sh

NSTORE=./src/nstore 

BASH=/bin/zsh
NUMACTL=/usr/bin/numactl 
NUMACTL_FLAGS="--membind=2"

# NSTORE FLAGS
KEYS=100000 
TXNS=100000 
FS_PATH=/mnt/pmfs/n-store/
#FS_PATH=./

for ((i=8; i<=8; i*=4))
do
    l=$(($i*$DEFAULT_LATENCY))

    echo "LATENCY" $l
    cd $SDV_DIR
    #$SDV_SCRIPT --enable --pm-latency=$l
    cd -
    echo "---------------------------------------------------"

    #rw_mix=(0 10 50)
    #skew=(0.5 0.75 1.0 1.25 1.5)
    
    rw_mix=(0 50)
    skew=(0.1 5.0)
    
    for rw_mix_itr  in "${rw_mix[@]}"
    do
        for skew_itr  in "${skew[@]}"
        do
            echo "RW MIX ::" $rw_mix_itr  " SKEW ::" $skew_itr
            #$NUMACTL $NUMACTL_FLAGS $NSTORE -k $KEYS -x $TXNS -w $rw_mix_itr -q $skew_itr -f $FS_PATH
            $NSTORE -k $KEYS -x $TXNS -w $rw_mix_itr -q $skew_itr -f $FS_PATH
        done
    done
done


