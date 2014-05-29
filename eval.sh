#!/bin/bash
# RUN EXPERIMENTS

VERBOSE=false
LOCAL_ENABLE=false
SDV_DISABLE=true

while getopts 'lsv' flag; do
    case "${flag}" in
        l) LOCAL_ENABLE=true ;; 
        s) SDV_DISABLE=false  ;;
        v) VERBOSE=true    ;;
    esac
done


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
#KEYS=10 
#TXNS=10 

if [ "$LOCAL_ENABLE" = true ]; 
then
    FS_PATH=./
else
    FS_PATH=/mnt/pmfs/n-store/
fi

echo "FS PATH:" $FS_PATH

for ((i=8; i<=8; i*=4))
do
    l=$(($i*$DEFAULT_LATENCY))

    echo "LATENCY" $l
    
    if [ "$LOCAL_ENABLE" = false ] && [ "$SDV_DISABLE" = false ]; 
    then
        cd $SDV_DIR
        $SDV_SCRIPT --enable --pm-latency=$l
        cd -
    fi
    
    echo "---------------------------------------------------"

    #rw_mix=(0 10 50)
    #skew=(0.5 0.75 1.0 1.25 1.5)

    rw_mix=(0 50)
    skew=(0.1 5.0)

    for rw_mix_itr  in "${rw_mix[@]}"
    do
        for skew_itr  in "${skew[@]}"
        do
            echo "---------------------------------------------------"
            echo "RW MIX ::" $rw_mix_itr  " SKEW ::" $skew_itr

            if [ "$LOCAL_ENABLE" = true ]; 
            then
                $NSTORE -k $KEYS -x $TXNS -w $rw_mix_itr -q $skew_itr -f $FS_PATH
            else
                $NUMACTL $NUMACTL_FLAGS $NSTORE -k $KEYS -x $TXNS -w $rw_mix_itr -q $skew_itr -f $FS_PATH
            fi

            echo "---------------------------------------------------"
        done
    done
done


