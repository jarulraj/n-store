# RUN EXPERIMENTS

VERBOSE=false
LOCAL_ENABLE=false
SDV_DISABLE=true
PERF_ENABLE=false
TRIALS_ENABLE=false

while getopts 'lsvpt' flag; do
    case "${flag}" in
        l) LOCAL_ENABLE=true ;; 
        p) PERF_ENABLE=true ;;  
        s) SDV_DISABLE=false  ;;
        v) VERBOSE=true    ;;
        t) TRIALS_ENABLE=true ;;
    esac
done


DEFAULT_LATENCY=100
LOG_DIR=data

SDV_DIR=/data/devel/sdv-tools/sdv-release
SDV_SCRIPT=$SDV_DIR/ivt_pm_sdv.sh

NSTORE=./src/nstore 

NUMACTL=`which numactl` 
NUMACTL_FLAGS="--membind=2"

PERF="perf"
PERF_FLAGS="stat -e LLC-store-misses:u,LLC-load-misses:u"

# NSTORE FLAGS
KEYS=1000000 
TXNS=1000000 
#KEYS=100 
#TXNS=100 

if [ "$LOCAL_ENABLE" = true ]; 
then
    FS_PATH=./
else
    FS_PATH=/mnt/pmfs/n-store/
fi
 
if [ "$TRIALS_ENABLE" = true ]; 
then
    NUM_TRIALS=3
else
    NUM_TRIALS=1
fi
 
echo "FS PATH:" $FS_PATH

latency_factors=(2 8)
rw_mix=(0 0.1 0.5)
skew=(0.5 0.75 1.0 1.25 1.5)

#rw_mix=(0 0.5)
#skew=(0.5 1.5)

for latency_factor in "${latency_factors[@]}"
do
    l=$(($latency_factor*$DEFAULT_LATENCY))

    echo "LATENCY" $l

    if [ "$LOCAL_ENABLE" = false ] && [ "$SDV_DISABLE" = false ]; 
    then
        cd $SDV_DIR
        $SDV_SCRIPT --enable --pm-latency=$l
        cd -
    fi

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
                if [ "$PERF_ENABLE" = false ]; 
                then
                    $NUMACTL $NUMACTL_FLAGS $NSTORE -k $KEYS -x $TXNS -w $rw_mix_itr -q $skew_itr -f $FS_PATH -t $NUM_TRIALS
                else
                    $PERF $PERF_FLAGS $NUMACTL $NUMACTL_FLAGS $NSTORE -k $KEYS -x $TXNS -w $rw_mix_itr -q $skew_itr -f $FS_PATH -t $NUM_TRIALS
                fi
            fi
 
        done
    done
done

