# RUN EXPERIMENTS

VERBOSE=false
LOCAL_ENABLE=false
SDV_DISABLE=true
TRIALS_ENABLE=false

while getopts 'lsvt' flag; do
    case "${flag}" in
        l) LOCAL_ENABLE=true ;; 
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

# NSTORE FLAGS
#KEYS=100000 
#TXNS=100000
KEYS=100 
TXNS=100 

if [ "$LOCAL_ENABLE" = true ]; 
then
    FS_PATH=./
    CLEANUP="rm -f ./zfile ./log"
else
    FS_PATH=/mnt/pmfs/n-store/
    CLEANUP="rm -f /mnt/pmfs/n-store/*"
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
skew=(0.1 1)

for latency_factor in "${latency_factors[@]}"
do
    l=$(($latency_factor*$DEFAULT_LATENCY))

    echo "LATENCY" $l

    #if [ "$LOCAL_ENABLE" = false ] && [ "$SDV_DISABLE" = false ]; 
    #then
        #cd $SDV_DIR
        #$SDV_SCRIPT --enable --pm-latency=$l
        #cd -
    #fi

    for (( trial_itr=0; trial_itr<$NUM_TRIALS ; trial_itr++ ))
    do
        for rw_mix_itr  in "${rw_mix[@]}"
        do
            for skew_itr  in "${skew[@]}"
            do
                echo "---------------------------------------------------"
                echo "TRIAL ::" $trial_itr " RW MIX ::" $rw_mix_itr  " SKEW ::" $skew_itr

                $NSTORE -k $KEYS -x $TXNS -w $rw_mix_itr -q $skew_itr -f $FS_PATH -l 
                $CLEANUP
                
                $NSTORE -k $KEYS -x $TXNS -w $rw_mix_itr -q $skew_itr -f $FS_PATH -a 
                $CLEANUP

            done
        done
    done
    
# LATENCY
done

