#!/usr/bin/env python
# Evaluation

from __future__ import print_function
import os
import subprocess
import argparse
import pprint
import numpy       

def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i + n]


def parse_ycsb(log_name):
    
    benchmark_dir = "../results/ycsb/"
    
    tput = {}
    mean = {}
    sdev = {}
    latency = 0
    rw_mix = 0.0
    skew = 0.0
    
    skew_factors = []
    latency_factors = []
    engine_types = []
    
    log_file = open(log_name, "r")    
    
    for line in log_file:
        if "LATENCY" in line:
            entry = line.strip().split(' ');
            if entry[0] == "LATENCY":
                latency = entry[1]
            if latency not in latency_factors:
                latency_factors.append(latency)
                    
        if "RW MIX" in line:
            entry = line.strip().split(' ');
            trial = entry[2]
            rw_mix = entry[6]
            skew = entry[9]
            
            if skew not in skew_factors:
                skew_factors.append(skew)
       
        if "Throughput" in line:
            entry = line.strip().split(':');
            engine_type = entry[0].split(' ');
            val = float(entry[4]);
            
            if(engine_type[0] == "ARIES"):
                engine_type[0] = "aries"                
            elif(engine_type[0] == "WAL"):
                engine_type[0] = "wal"
            elif(engine_type[0] == "SP"):
                engine_type[0] = "sp"
            
            if engine_type not in engine_types:
                engine_types.append(engine_type)
                
            key = (rw_mix, skew, latency, engine_type[0]);
            if key in tput:
                tput[key].append(val)
            else:
                tput[key] = [ val ]
                            

    # CLEAN UP RESULT FILE
    subprocess.call(['rm', '-rf', benchmark_dir])          
    
    for key in sorted(tput.keys()):
        mean[key] = round(numpy.mean(tput[key]), 2)
        mean[key] = str(mean[key]).rjust(10)
            
        sdev[key] = numpy.std(tput[key])
        sdev[key] /= float(mean[key])
        sdev[key] = round(sdev[key], 3)
        sdev[key] = str(sdev[key]).rjust(10)
        
        # LOG TO RESULT FILE
        engine_type =  str(key[3]);
        
        if(key[0] == '0.0'):
            workload_type = 'read-only'
        elif(key[0] == '0.1'):
            workload_type = 'read-heavy'
        elif(key[0] == '0.5'):
            workload_type = 'write-heavy'
    
        latency_factor =  str(key[2]);
        
        result_directory = benchmark_dir + engine_type + "/" + workload_type + "/" + latency_factor + "/";
        if not os.path.exists(result_directory):
            os.makedirs(result_directory)

        result_file_name = result_directory + "results.csv"
        result_file = open(result_file_name, "a")
        result_file.write(str(key[1] + " , " + mean[key] + "\n"))
        result_file.close()    
                    
    read_only = []
    read_heavy = []
    write_heavy = []
    
    #pprint.pprint(tput)
    
    # ARRANGE DATA INTO TABLES    
    for key in sorted(mean.keys()):
        if key[0] == '0.0':
            read_only.append(str(mean[key] + "\t" + sdev[key] + "\t"))
        elif key[0] == '0.1':
            read_heavy.append(str(mean[key] + "\t" + sdev[key] + "\t"))
        elif key[0] == '0.5':
            write_heavy.append(str(mean[key] + "\t" + sdev[key] + "\t"))
        
    col_len = len(latency_factors) * len(engine_types)   
        
    # pprint.pprint(read_only)
        
    ro_chunks = list(chunks(read_only, col_len))
    print('\n'.join('\t'.join(map(str, row)) for row in zip(*ro_chunks)))
    print('\n', end="")
        
    rh_chunks = list(chunks(read_heavy, col_len))
    print('\n'.join('\t'.join(map(str, row)) for row in zip(*rh_chunks)))
    print('\n', end="")
        
    wh_chunks = list(chunks(write_heavy, col_len))
    print('\n'.join('\t'.join(map(str, row)) for row in zip(*wh_chunks)))
    print('\n', end="")
    
    log_file.close()

def eval(enable_sdv, enable_trials, log_name):        
    dram_latency = 100
    results_dir = "results"
    
    sdv_dir = "/data/devel/sdv-tools/sdv-release"
    sdv_script = sdv_dir + "/ivt_pm_sdv.sh"
    
    nstore = "./src/nstore"
    fs_path = "/mnt/pmfs/n-store/"
    
    # NSTORE FLAGS
    keys = 1000000 
    txns = 1000000
    # KEYS=100 
    # TXNS=100 
    
     # CLEANUP
    def cleanup():
        subprocess.call(['rm', '-f', fs_path + './zfile'])
        subprocess.call(['rm', '-f', fs_path + './log'])
        
    
    num_trials = 1 
    if enable_trials: 
        num_trials = 3
    
    latency_factors = [2, 8]
    rw_mixes = [0, 0.1, 0.5]
    skew_factors = [0.1, 1]
 
    #latency_factors = [2]
    #rw_mixes = [0, 0.5]
    #skew_factors = [0.1, 1]
    
    # LOG RESULTS
    log_file = open(log_name, 'w')
    
    # LATENCY
    for latency_factor in latency_factors:
        nvm_latency = latency_factor * dram_latency

        ostr = ("LATENCY %d \n" % nvm_latency)    
        print (ostr, end="")
        log_file.write(ostr)
        log_file.flush()
        
        if enable_sdv :
            cwd = os.getcwd()
            os.chdir(sdv_dir)
            subprocess.call(['sudo', sdv_script, '--enable', '--pm-latency', str(nvm_latency)], stdout=log_file)
            os.chdir(cwd)
                   
        for trial in (0, num_trials):
            # RW MIX
            for rw_mix  in rw_mixes:
                # SKEW FACTOR
                for skew_factor  in skew_factors:
                    ostr = ("--------------------------------------------------- \n")
                    print (ostr, end="")
                    log_file.write(ostr)
                    ostr = ("TRIAL :: %d RW MIX :: %.1f SKEW :: %.1f \n" % (trial, rw_mix, skew_factor))
                    print (ostr, end="")
                    log_file.write(ostr)                    
                    log_file.flush()
                    

                    cleanup()
                    subprocess.call([nstore, '-k', str(keys), '-x', str(txns), '-w', str(rw_mix), '-f', fs_path, 'q', str(skew_factor), '-l'], stdout=log_file)
     
                    cleanup()
                    subprocess.call([nstore, '-k', str(keys), '-x', str(txns), '-w', str(rw_mix), '-f', fs_path, 'q', str(skew_factor), '-a'], stdout=log_file)

                    cleanup()
                    subprocess.call([nstore, '-k', str(keys), '-x', str(txns), '-w', str(rw_mix), '-f', fs_path, 'q', str(skew_factor), '-s'], stdout=log_file)

if __name__ == '__main__':
    enable_sdv = False
    enable_trials = False
    
    parser = argparse.ArgumentParser(description='Run experiments')
    parser.add_argument("-s", "--enable-sdv", help='enable sdv', action='store_true')
    parser.add_argument("-t", "--enable-trials", help='enable trials', action='store_true')
    
    args = parser.parse_args()
    
    if args.enable_sdv:
        enable_sdv = True
    if args.enable_trials:
        enable_trials = True

    log_name = "data.log"
    
    #eval(enable_sdv, enable_trials, log_name)
    
    parse_ycsb(log_name)

    
