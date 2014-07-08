# RUN EXPERIMENTS

VERBOSE=false
SDV_DISABLE=true
TRIALS_ENABLE=false

while getopts 'svt' flag; do
    case "${flag}" in
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

# NSTORE FLAGS
KEYS=1000000 
TXNS=1000000
#KEYS=100 
#TXNS=100 

CLEANUP="rm -f /mnt/pmfs/n-store/*"
 
if [ "$TRIALS_ENABLE" = true ]; 
then
    NUM_TRIALS=3
else
    NUM_TRIALS=1
fi

latency_factors=(2 8)
rw_mix=(0 0.1 0.5)
skew=(0.1 1)

for latency_factor in "${latency_factors[@]}"
do
    l=$(($latency_factor*$DEFAULT_LATENCY))

    echo "LATENCY" $l

    if [ "$SDV_DISABLE" = false ]; 
    then
        cd $SDV_DIR
        $SDV_SCRIPT --enable --pm-latency=$l
        cd -
    fi

    for (( trial_itr=0; trial_itr<$NUM_TRIALS ; trial_itr++ ))
    do
        for rw_mix_itr  in "${rw_mix[@]}"
        do
            for skew_itr  in "${skew[@]}"
            do
                echo "---------------------------------------------------"
                echo "TRIAL ::" $trial_itr " RW MIX ::" $rw_mix_itr  " SKEW ::" $skew_itr

                $NSTORE -k $KEYS -x $TXNS -w $rw_mix_itr -q $skew_itr -l 
                $CLEANUP
                
                $NSTORE -k $KEYS -x $TXNS -w $rw_mix_itr -q $skew_itr -a 
                $CLEANUP

            done
        done
    done
    
# LATENCY
done

