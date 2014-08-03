#!/usr/bin/env python
# Evaluation

from __future__ import print_function
import os
import subprocess
import argparse
import pprint
import numpy
import sys
import re
import logging
import fnmatch
import string
import argparse
import pylab

import numpy as np
import matplotlib.pyplot as plot

from matplotlib.font_manager import FontProperties
from matplotlib.ticker import MaxNLocator
from pprint import pprint, pformat
from matplotlib.backends.backend_pdf import PdfPages

import csv
import brewer2mpl

from options import *
from functools import wraps       


# # LOGGING CONFIGURATION
LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(
    fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
    datefmt='%m-%d-%Y %H:%M:%S'
)
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.INFO)

# UTILS

def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i + n]

def loadDataFile(n_rows, n_cols, path):
    file = open(path, "r")
    reader = csv.reader(file)
    
    data = [[0 for x in xrange(n_cols)] for y in xrange(n_rows)]
    
    row_num = 0
    for row in reader:
        column_num = 0
        for col in row:
            data[row_num][column_num] = float(col)
            column_num += 1
        row_num += 1
                
    return data

# # MAKE GRID
def makeGrid(ax):
    axes = ax.get_axes()
    axes.yaxis.grid(True, color=OPT_GRID_COLOR)
    ax.set_axisbelow(True)

# # SAVE GRAPH
def saveGraph(fig, output, width, height):
    size = fig.get_size_inches()
    dpi = fig.get_dpi()
    LOG.debug("Current Size Inches: %s, DPI: %d" % (str(size), dpi))

    new_size = (width / float(dpi), height / float(dpi))
    fig.set_size_inches(new_size)
    new_size = fig.get_size_inches()
    new_dpi = fig.get_dpi()
    LOG.debug("New Size Inches: %s, DPI: %d" % (str(new_size), new_dpi))
    
    pp = PdfPages(output)
    fig.savefig(pp, format='pdf', bbox_inches='tight')
    pp.close()
    LOG.info("OUTPUT: %s", output)


# # CONFIGURATION

BASE_DIR = os.path.dirname(__file__)
OPT_FONT_NAME = 'Droid Sans'
OPT_GRAPH_HEIGHT = 300
OPT_GRAPH_WIDTH = 400
OPT_LABEL_WEIGHT = 'bold'
# OPT_COLORS = brewer2mpl.get_map('Set2', 'qualitative', 8).mpl_colors
OPT_COLORS = brewer2mpl.get_map('Set1', 'qualitative', 9).mpl_colors
OPT_GRID_COLOR = 'gray'
OPT_LEGEND_SHADOW = False
OPT_MARKERS = (['o', 's', 'v', ">", "h", "v", "^", "x", "d", "<", "|", "8", "|", "_"])


# # NSTORE
SDV_DIR = "/data/devel/sdv-tools/sdv-release"
SDV_SCRIPT = SDV_DIR + "/ivt_pm_sdv.sh"    
NSTORE = "./src/nstore"
FS_PATH = "/mnt/pmfs/n-store/"
PMEM_CHECK = "./src/pmem_check"
#PERF = "/usr/bin/perf"
PERF = "/usr/lib/linux-tools/3.11.0-12-generic/perf"
NUMACTL = "numactl"
NUMACTL_FLAGS="--membind=2"

SYSTEMS = ("wal", "sp", "lsm", "opt_wal", "opt_sp", "opt_lsm")
LATENCIES = ("200", "800")
ENGINES = ['-a', '-s', '-m', '-w', '-c', '-l']

YCSB_KEYS = 200000
YCSB_TXNS = 200000
YCSB_WORKLOAD_MIX = ("read-only", "read-heavy" "write-heavy")
YCSB_SKEW_FACTORS = [0.1, 1.0]
YCSB_RW_MIXES = [0, 0.1, 0.5]

TPCC_WORKLOAD_MIX = ("all", "stock-level")
TPCC_RW_MIXES = [0.5, 0]

YCSB_PERF_DIR = "../results/ycsb/performance/"
YCSB_STORAGE_DIR = "../results/ycsb/storage/"
YCSB_NVM_DIR = "../results/ycsb/nvm/"
TPCC_PERF_DIR = "../results/tpcc/performance/"
TPCC_STORAGE_DIR = "../results/tpcc/storage/"
TPCC_NVM_DIR = "../results/tpcc/nvm/"

TPCC_TXNS = 1000


###################################################################################                   
# PLOT
###################################################################################                   

def create_ycsb_perf_bar_chart(datasets, workload_mix):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
     
    labels = ("WAL", "SP", "LSM",
              "PM-WAL", "PM-SP", "PM-LSM")

    x_values = YCSB_SKEW_FACTORS
    N = len(x_values)
    x_labels = ["Low", "High"]

    ind = np.arange(N)  
    width = 0.05  # the width of the bars
    offset = 0.15

    for group in xrange(len(datasets)):
        # GROUP
        perf_data = []               
        LOG.info("GROUP :: %s", datasets[group])

        for line in  xrange(len(datasets[group])):
            for col in  xrange(len(datasets[group][line])):
                if col == 1:
                    perf_data.append(datasets[group][line][col])
  
        LOG.info("%s perf_data = %s ", labels[group], str(perf_data))
                
        ax1.bar(ind + group * width, perf_data, width, color=OPT_COLORS[group])
    
    # GRID
    axes = ax1.get_axes()
    if workload_mix == "read-only":
        axes.set_ylim(0, 120000)
    elif workload_mix == "read-heavy":
        axes.set_ylim(0, 120000)
    elif workload_mix == "write-heavy":
        axes.set_ylim(0, 120000)
        
    makeGrid(ax1)
    
    # LEGEND
    fp = FontProperties(family=OPT_FONT_NAME, weight=OPT_LABEL_WEIGHT)
    num_col = 2
    ax1.legend(labels,
                prop=fp,
                bbox_to_anchor=(0.0, 1.1, 1.0, 0.10),
                loc=1,
                ncol=num_col,
                mode="expand",
                shadow=OPT_LEGEND_SHADOW,
                borderaxespad=0.0,
    )
    
    # Y-AXIS
    ax1.set_ylabel("Throughput", fontproperties=fp)
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    ax1.minorticks_on()
        
    # X-AXIS
    ax1.set_xlabel("Skew", fontproperties=fp)
    ax1.minorticks_on()
    ax1.set_xticklabels(x_labels)
    print (x_values)
    ax1.set_xticks(ind + width * len(datasets))
    print(x_labels)
        
    return (fig)

def create_ycsb_storage_bar_chart(datasets, workload_mix):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
     
    labels = ("WAL", "SP", "LSM",
              "PM-WAL", "PM-SP", "PM-LSM")

    x_values = YCSB_SKEW_FACTORS
    N = len(x_values)
    x_labels = ["Low", "High"]

    ind = np.arange(N)  
    width = 0.05  # the width of the bars
    offset = 0.15
    bars = [None] * len(labels)
    
    for group in xrange(len(datasets)):
        # GROUP
        fs_data = []       
        pm_data = [] 

        for line in  xrange(len(datasets[group])):
            for col in  xrange(len(datasets[group][line])):
                if col == 1:
                    fs_data.append(datasets[group][line][col] / (1024 * 1024))
                if col == 2:
                    pm_data.append(datasets[group][line][col] / (1024 * 1024))
  
        LOG.info("%s fs_data = %s pm_data = %s ", labels[group], str(fs_data), str(pm_data))
                
        bars[group] = ax1.bar(ind + group * width, fs_data, width, color=OPT_COLORS[group])
        ax1.bar(ind + group * width, pm_data, width, bottom=fs_data, color=OPT_COLORS[group], hatch='/')
        
    # GRID
    axes = ax1.get_axes()
    if workload_mix == "read-only":
        axes.set_ylim(0, 3000)
    elif workload_mix == "read-heavy":
        axes.set_ylim(0, 3000)
    elif workload_mix == "write-heavy":
        axes.set_ylim(0, 3000)
        
    makeGrid(ax1)
    
    # LEGEND
    fp = FontProperties(family=OPT_FONT_NAME, weight=OPT_LABEL_WEIGHT)
    ax1.legend(bars, labels,
                prop=fp,
                bbox_to_anchor=(0.0, 1.1, 1.0, 0.10),
                loc=1,
                ncol=2,
                mode="expand",
                shadow=OPT_LEGEND_SHADOW,
                borderaxespad=0.0,
    )
    
    # Y-AXIS
    ax1.set_ylabel("Storage (MB)", fontproperties=fp)
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    ax1.minorticks_on()
    for tick in ax1.yaxis.get_major_ticks():
        tick.label.set_fontname(OPT_FONT_NAME)
        
    # X-AXIS
    ax1.set_xlabel("Skew", fontproperties=fp)
    ax1.minorticks_on()
    ax1.set_xticklabels(x_labels)
    print (x_values)
    ax1.set_xticks(ind + width * len(x_labels))
    print(x_labels)
    for tick in ax1.xaxis.get_major_ticks():
        tick.label.set_fontname(OPT_FONT_NAME)
        
    return (fig)

def create_ycsb_nvm_bar_chart(datasets, workload_mix):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
     
    labels = ("WAL", "SP", "LSM",
              "PM-WAL", "PM-SP", "PM-LSM")

    x_values = YCSB_SKEW_FACTORS
    N = len(x_values)
    x_labels = ["Low", "High"]

    ind = np.arange(N)  
    width = 0.05  # the width of the bars
    offset = 0.15
    bars = [None] * len(labels)
    
    for group in xrange(len(datasets)):
        # GROUP
        l_misses = []       
        s_misses = [] 

        for line in  xrange(len(datasets[group])):
            for col in  xrange(len(datasets[group][line])):
                if col == 1:
                    l_misses.append(datasets[group][line][col] / (1024 * 1024))
                if col == 2:
                    s_misses.append(datasets[group][line][col] / (1024 * 1024))
  
        LOG.info("%s l_misses = %s s_misses = %s ", labels[group], str(l_misses), str(s_misses))
                
        bars[group] = ax1.bar(ind + group * width, l_misses, width, color=OPT_COLORS[group])
        ax1.bar(ind + group * width, s_misses, width, bottom=l_misses, color=OPT_COLORS[group], hatch='/')
        
    # GRID
    axes = ax1.get_axes()
    if workload_mix == "read-only":
        axes.set_ylim(0, 200)
    elif workload_mix == "read-heavy":
        axes.set_ylim(0, 200)
    elif workload_mix == "write-heavy":
        axes.set_ylim(0, 200)
        
    makeGrid(ax1)
    
    # LEGEND
    fp = FontProperties(family=OPT_FONT_NAME, weight=OPT_LABEL_WEIGHT)
    ax1.legend(bars, labels,
                prop=fp,
                bbox_to_anchor=(0.0, 1.1, 1.0, 0.10),
                loc=1,
                ncol=2,
                mode="expand",
                shadow=OPT_LEGEND_SHADOW,
                borderaxespad=0.0,
    )
    
    # Y-AXIS
    ax1.set_ylabel("NVM accesses (M)", fontproperties=fp)
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    ax1.minorticks_on()
    for tick in ax1.yaxis.get_major_ticks():
        tick.label.set_fontname(OPT_FONT_NAME)
        
    # X-AXIS
    ax1.set_xlabel("Skew", fontproperties=fp)
    ax1.minorticks_on()
    ax1.set_xticklabels(x_labels)
    print (x_values)
    ax1.set_xticks(ind + width * len(x_labels))
    print(x_labels)
    for tick in ax1.xaxis.get_major_ticks():
        tick.label.set_fontname(OPT_FONT_NAME)
        
    return (fig)

def create_tpcc_perf_bar_chart(datasets):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
     
    labels = ("WAL", "SP", "LSM",
              "PM-WAL", "PM-SP", "PM-LSM")

    x_values = TPCC_RW_MIXES
    N = len(x_values)
    x_labels = ["All", "Stock-level"]

    ind = np.arange(N)  
    width = 0.05  # the width of the bars
    offset = 0.15

    for group in xrange(len(datasets)):
        # GROUP
        perf_data = []               
        LOG.info("GROUP :: %s", datasets[group])

        for line in  xrange(len(datasets[group])):
            for col in  xrange(len(datasets[group][line])):
                if col == 1:
                    perf_data.append(datasets[group][line][col])
  
        LOG.info("%s perf_data = %s ", labels[group], str(perf_data))
                
        ax1.bar(ind + group * width, perf_data, width, color=OPT_COLORS[group])
    
    # GRID
    axes = ax1.get_axes()
    axes.set_ylim(0, 5000)        
    makeGrid(ax1)
    
    # LEGEND
    fp = FontProperties(family=OPT_FONT_NAME, weight=OPT_LABEL_WEIGHT)
    num_col = 2
    ax1.legend(labels,
                prop=fp,
                bbox_to_anchor=(0.0, 1.1, 1.0, 0.10),
                loc=1,
                ncol=num_col,
                mode="expand",
                shadow=OPT_LEGEND_SHADOW,
                borderaxespad=0.0,
    )
    
    # Y-AXIS
    ax1.set_ylabel("Throughput", fontproperties=fp)
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    ax1.minorticks_on()
        
    # X-AXIS
    ax1.set_xlabel("Workload", fontproperties=fp)
    ax1.minorticks_on()
    ax1.set_xticklabels(x_labels)
    print (x_values)
    ax1.set_xticks(ind + width * len(datasets))
    print(x_labels)
        
    return (fig)

def create_tpcc_storage_bar_chart(datasets, workload_mix):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
     
    labels = ("WAL", "SP", "LSM",
              "PM-WAL", "PM-SP", "PM-LSM")

    x_values = TPCC_RW_MIXES
    N = len(x_values)
    x_labels = ["All", "Stock-level"]

    ind = np.arange(N)  
    width = 0.05  # the width of the bars
    offset = 0.15
    bars = [None] * len(labels)
    
    for group in xrange(len(datasets)):
        # GROUP
        fs_data = []       
        pm_data = [] 

        for line in  xrange(len(datasets[group])):
            for col in  xrange(len(datasets[group][line])):
                if col == 1:
                    fs_data.append(datasets[group][line][col] / (1024 * 1024))
                if col == 2:
                    pm_data.append(datasets[group][line][col] / (1024 * 1024))
  
        LOG.info("%s fs_data = %s pm_data = %s ", labels[group], str(fs_data), str(pm_data))
                
        bars[group] = ax1.bar(ind + group * width, fs_data, width, color=OPT_COLORS[group])
        ax1.bar(ind + group * width, pm_data, width, bottom=fs_data, color=OPT_COLORS[group], hatch='/')
        
    # GRID
    axes = ax1.get_axes()
    axes.set_ylim(0, 3000)        
    makeGrid(ax1)
    
    # LEGEND
    fp = FontProperties(family=OPT_FONT_NAME, weight=OPT_LABEL_WEIGHT)
    ax1.legend(bars, labels,
                prop=fp,
                bbox_to_anchor=(0.0, 1.1, 1.0, 0.10),
                loc=1,
                ncol=2,
                mode="expand",
                shadow=OPT_LEGEND_SHADOW,
                borderaxespad=0.0,
    )
    
    # Y-AXIS
    ax1.set_ylabel("Storage (MB)", fontproperties=fp)
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    ax1.minorticks_on()
    for tick in ax1.yaxis.get_major_ticks():
        tick.label.set_fontname(OPT_FONT_NAME)
        
    # X-AXIS
    ax1.set_xlabel("Workload", fontproperties=fp)
    ax1.minorticks_on()
    ax1.set_xticklabels(x_labels)
    print (x_values)
    ax1.set_xticks(ind + width * len(x_labels))
    print(x_labels)
    for tick in ax1.xaxis.get_major_ticks():
        tick.label.set_fontname(OPT_FONT_NAME)
        
    return (fig)

def create_tpcc_nvm_bar_chart(datasets, workload_mix):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
     
    labels = ("WAL", "SP", "LSM",
              "PM-WAL", "PM-SP", "PM-LSM")

    x_values = TPCC_RW_MIXES
    N = len(x_values)
    x_labels = ["All", "Stock-level"]

    ind = np.arange(N)  
    width = 0.05  # the width of the bars
    offset = 0.15
    bars = [None] * len(labels)
    
    for group in xrange(len(datasets)):
        # GROUP
        l_misses = []       
        s_misses = [] 

        for line in  xrange(len(datasets[group])):
            for col in  xrange(len(datasets[group][line])):
                if col == 1:
                    l_misses.append(datasets[group][line][col] / (1024 * 1024))
                if col == 2:
                    s_misses.append(datasets[group][line][col] / (1024 * 1024))
  
        LOG.info("%s l_misses = %s s_misses = %s ", labels[group], str(l_misses), str(s_misses))
                
        bars[group] = ax1.bar(ind + group * width, l_misses, width, color=OPT_COLORS[group])
        ax1.bar(ind + group * width, s_misses, width, bottom=l_misses, color=OPT_COLORS[group], hatch='/')
        
    # GRID
    axes = ax1.get_axes()
    axes.set_ylim(0, 200)        
    makeGrid(ax1)
    
    # LEGEND
    fp = FontProperties(family=OPT_FONT_NAME, weight=OPT_LABEL_WEIGHT)
    ax1.legend(bars, labels,
                prop=fp,
                bbox_to_anchor=(0.0, 1.1, 1.0, 0.10),
                loc=1,
                ncol=2,
                mode="expand",
                shadow=OPT_LEGEND_SHADOW,
                borderaxespad=0.0,
    )
    
    # Y-AXIS
    ax1.set_ylabel("NVM accesses (M)", fontproperties=fp)
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    ax1.minorticks_on()
    for tick in ax1.yaxis.get_major_ticks():
        tick.label.set_fontname(OPT_FONT_NAME)
        
    # X-AXIS
    ax1.set_xlabel("Workload", fontproperties=fp)
    ax1.minorticks_on()
    ax1.set_xticklabels(x_labels)
    print (x_values)
    ax1.set_xticks(ind + width * len(x_labels))
    print(x_labels)
    for tick in ax1.xaxis.get_major_ticks():
        tick.label.set_fontname(OPT_FONT_NAME)
        
    return (fig)


# YCSB PERF -- PLOT
def ycsb_perf_plot():
    for workload in YCSB_WORKLOAD_MIX:    
        for lat in LATENCIES:
            datasets = []
        
            for sy in SYSTEMS:    
                dataFile = loadDataFile(2, 2, os.path.realpath(os.path.join(YCSB_PERF_DIR, sy + "/" + workload + "/" + lat + "/performance.csv")))
                datasets.append(dataFile)
                       
            fig = create_ycsb_perf_bar_chart(datasets, workload)
            
            fileName = "ycsb-perf-%s-%s.pdf" % (workload, lat)
            saveGraph(fig, fileName, width=OPT_GRAPH_WIDTH, height=OPT_GRAPH_HEIGHT)
                   
# YCSB STORAGE -- PLOT               
def ycsb_storage_plot():    
    for workload in YCSB_WORKLOAD_MIX:    
        datasets = []
    
        for sy in SYSTEMS:    
            dataFile = loadDataFile(2, 3, os.path.realpath(os.path.join(YCSB_STORAGE_DIR, sy + "/" + workload + "/storage.csv")))
            datasets.append(dataFile)
                                      
        fig = create_ycsb_storage_bar_chart(datasets, workload)
                        
        fileName = "ycsb-storage-%s.pdf" % (workload)
        saveGraph(fig, fileName, width=OPT_GRAPH_WIDTH, height=OPT_GRAPH_HEIGHT)

# YCSB NVM -- PLOT               
def ycsb_nvm_plot():    
    for workload in YCSB_WORKLOAD_MIX:    
        datasets = []
    
        for sy in SYSTEMS:    
            dataFile = loadDataFile(2, 3, os.path.realpath(os.path.join(YCSB_NVM_DIR, sy + "/" + workload + "/nvm.csv")))
            datasets.append(dataFile)
                   
        fig = create_ycsb_nvm_bar_chart(datasets, workload)
                        
        fileName = "ycsb-nvm-%s.pdf" % (workload)
        saveGraph(fig, fileName, width=OPT_GRAPH_WIDTH, height=OPT_GRAPH_HEIGHT)

# TPCC PERF -- PLOT
def tpcc_perf_plot():
    for lat in LATENCIES:
        for workload in TPCC_WORKLOAD_MIX:  

            datasets = []

            for sy in SYSTEMS:    
                dataFile = loadDataFile(2, 2, os.path.realpath(os.path.join(TPCC_PERF_DIR, sy + "/" + lat + "/performance.csv")))
                datasets.append(dataFile)
                       
        fig = create_tpcc_perf_bar_chart(datasets)
            
        fileName = "tpcc-perf-%s.pdf" % (lat)
        saveGraph(fig, fileName, width=OPT_GRAPH_WIDTH, height=OPT_GRAPH_HEIGHT)

# TPCC STORAGE -- PLOT               
def tpcc_storage_plot():    
    for workload in TPCC_WORKLOAD_MIX:    
        datasets = []
    
        for sy in SYSTEMS:    
            dataFile = loadDataFile(2, 3, os.path.realpath(os.path.join(TPCC_STORAGE_DIR, sy + "/storage.csv")))
            datasets.append(dataFile)
                                      
    fig = create_tpcc_storage_bar_chart(datasets, workload)
                        
    fileName = "tpcc-storage.pdf"
    saveGraph(fig, fileName, width=OPT_GRAPH_WIDTH, height=OPT_GRAPH_HEIGHT) 

# TPCC NVM -- PLOT               
def tpcc_nvm_plot():    
    for workload in TPCC_WORKLOAD_MIX:    
        datasets = []
    
        for sy in SYSTEMS:    
            dataFile = loadDataFile(2, 3, os.path.realpath(os.path.join(TPCC_STORAGE_DIR, sy + "/storage.csv")))
            datasets.append(dataFile)
                                      
    fig = create_tpcc_nvm_bar_chart(datasets, workload)
                        
    fileName = "tpcc-nvm.pdf"
    saveGraph(fig, fileName, width=OPT_GRAPH_WIDTH, height=OPT_GRAPH_HEIGHT) 
                   
###################################################################################                   
# EVAL                   
###################################################################################                   

# YCSB PERF -- EVAL
def ycsb_perf_eval(enable_sdv, enable_trials, log_name):        
    dram_latency = 100
    keys = YCSB_KEYS
    txns = YCSB_TXNS
                    
     # CLEANUP
    def cleanup():
        subprocess.call(["rm -f " + FS_PATH + "./*"], shell=True)        
    
    num_trials = 1 
    if enable_trials: 
        num_trials = 3
    
    nvm_latencies = LATENCIES
    rw_mixes = YCSB_RW_MIXES
    skew_factors = YCSB_SKEW_FACTORS
    engines = ENGINES
    
    # LOG RESULTS
    log_file = open(log_name, 'w')
    
    for nvm_latency in nvm_latencies:

        ostr = ("LATENCY %s \n" % nvm_latency)    
        print (ostr, end="")
        log_file.write(ostr)
        log_file.flush()
        
        if enable_sdv :
            cwd = os.getcwd()
            os.chdir(SDV_DIR)
            subprocess.call(['sudo', SDV_SCRIPT, '--enable', '--pm-latency', str(nvm_latency)], stdout=log_file)
            os.chdir(cwd)
                   
        for trial in range(num_trials):
            # RW MIX
            for rw_mix  in rw_mixes:
                # SKEW FACTOR
                for skew_factor  in skew_factors:
                    ostr = ("--------------------------------------------------- \n")
                    print (ostr, end="")
                    log_file.write(ostr)
                    ostr = ("TRIAL :: %d RW MIX :: %.1f SKEW :: %.2f \n" % (trial, rw_mix, skew_factor))
                    print (ostr, end="")
                    log_file.write(ostr)                    
                    log_file.flush()
                               
                    for eng in engines:
                        cleanup()
                        subprocess.call([NUMACTL, NUMACTL_FLAGS, NSTORE, '-k', str(keys), '-x', str(txns), '-p', str(rw_mix), '-q', str(skew_factor), eng], stdout=log_file)

    # RESET
    if enable_sdv :
        cwd = os.getcwd()
        os.chdir(SDV_DIR)
        subprocess.call(['sudo', SDV_SCRIPT, '--enable', '--pm-latency', "200"], stdout=log_file)
        os.chdir(cwd)
 
    # PARSE LOG
    log_file.close()   
    log_file = open(log_name, "r")    

    tput = {}
    mean = {}
    sdev = {}
    latency = 0
    rw_mix = 0.0
    skew = 0.0
    
    skew_factors = []
    nvm_latencies = []
    engine_types = []
    
    for line in log_file:
        if "LATENCY" in line:
            entry = line.strip().split(' ');
            if entry[0] == "LATENCY":
                latency = entry[1]
            if latency not in nvm_latencies:
                nvm_latencies.append(latency)
                    
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
            
            if(engine_type[0] == "WAL"):
                engine_type[0] = "wal"                
            elif(engine_type[0] == "SP"):
                engine_type[0] = "sp"
            elif(engine_type[0] == "LSM"):
                engine_type[0] = "lsm"
            elif(engine_type[0] == "OPT_WAL"):
                engine_type[0] = "opt_wal"
            elif(engine_type[0] == "OPT_SP"):
                engine_type[0] = "opt_sp"
            elif(engine_type[0] == "OPT_LSM"):
                engine_type[0] = "opt_lsm"
            
            if engine_type not in engine_types:
                engine_types.append(engine_type)
                            
            key = (rw_mix, skew, latency, engine_type[0]);
            if key in tput:
                tput[key].append(val)
            else:
                tput[key] = [ val ]
                            

    # CLEAN UP RESULT DIR
    subprocess.call(['rm', '-rf', YCSB_PERF_DIR])          
    
    for key in sorted(tput.keys()):
        mean[key] = round(numpy.mean(tput[key]), 2)
        mean[key] = str(mean[key]).rjust(10)
            
        sdev[key] = numpy.std(tput[key])
        sdev[key] /= float(mean[key])
        sdev[key] = round(sdev[key], 3)
        sdev[key] = str(sdev[key]).rjust(10)
        
        engine_type = str(key[3]);        
        if(key[0] == '0.0'):
            workload_type = 'read-only'
        elif(key[0] == '0.1'):
            workload_type = 'read-heavy'
        elif(key[0] == '0.5'):
            workload_type = 'write-heavy'
    
        nvm_latency = str(key[2]);
        
        result_directory = YCSB_PERF_DIR + engine_type + "/" + workload_type + "/" + nvm_latency + "/";
        if not os.path.exists(result_directory):
            os.makedirs(result_directory)

        result_file_name = result_directory + "performance.csv"
        result_file = open(result_file_name, "a")
        result_file.write(str(key[1] + " , " + mean[key] + "\n"))
        result_file.close()    
                    
    read_only = []
    read_heavy = []
    write_heavy = []
    
    # ARRANGE DATA INTO TABLES    
    for key in sorted(mean.keys()):
        if key[0] == '0.0':
            read_only.append(str(mean[key] + "\t" + sdev[key] + "\t"))
        elif key[0] == '0.1':
            read_heavy.append(str(mean[key] + "\t" + sdev[key] + "\t"))
        elif key[0] == '0.5':
            write_heavy.append(str(mean[key] + "\t" + sdev[key] + "\t"))
        
    col_len = len(nvm_latencies) * len(engine_types)           
        
    ro_chunks = list(chunks(read_only, col_len))
    print('\n'.join('\t'.join(map(str, row)) for row in zip(*ro_chunks)))
    print('\n', end="")
        
    rh_chunks = list(chunks(read_heavy, col_len))
    print('\n'.join('\t'.join(map(str, row)) for row in zip(*rh_chunks)))
    print('\n', end="")
        
    wh_chunks = list(chunks(write_heavy, col_len))
    print('\n'.join('\t'.join(map(str, row)) for row in zip(*wh_chunks)))
    print('\n', end="")
     
     
# YCSB STORAGE -- EVAL
def ycsb_storage_eval(log_name):            
    subprocess.call(['rm', '-rf', YCSB_STORAGE_DIR])          
    keys = YCSB_KEYS 
    txns = YCSB_TXNS
    
     # CLEANUP
    def cleanup():
        subprocess.call(["rm -f " + FS_PATH + "./*"], shell=True)        
        
    # GET STATS
    def get_stats(engine_type, rw_mix, skew_factor):
        print ("eng : %s rw_mix : %lf" % (engine_type, rw_mix))
                
        subprocess.call(['ls', '-larth', FS_PATH ], stdout=log_file)
        find_cmd = subprocess.Popen(['find', FS_PATH , '-name', '*.nvm', '-exec', 'ls', '-lart', '{}', ';'], stdout=subprocess.PIPE)
        log_file.write("FS STORAGE :: ")
        log_file.flush()

        fs_st = subprocess.check_output(['awk', '{ sum += $5 } END { printf "%.2f", sum }'], stdin=find_cmd.stdout)
        fs_st = fs_st.replace(" ", "").strip()
        if not fs_st:
            fs_st = "0"
        print("FS STORAGE :: " + fs_st)
        log_file.write(fs_st + "\n")
        log_file.flush()

        subprocess.call([PMEM_CHECK, FS_PATH + "./zfile" ], stdout=log_file)           
        pmem_cmd = subprocess.Popen([PMEM_CHECK, FS_PATH + "./zfile" ], stdout=subprocess.PIPE)
        grep_cmd = subprocess.Popen(['grep', 'Active'], stdin=pmem_cmd.stdout, stdout=subprocess.PIPE)
        log_file.write("PM STORAGE :: ")
        log_file.flush()

        pm_st = subprocess.check_output(['awk', '{ print $2 }'], stdin=grep_cmd.stdout)
        pm_st = pm_st.replace(" ", "").strip()
        if not pm_st:
            pm_st = "0"
        print("PM STORAGE :: " + pm_st)
        log_file.write(pm_st + "\n")
        log_file.flush()
    
        if(engine_type == "-a"):
            engine_type = "wal"                
        elif(engine_type == "-s"):
            engine_type = "sp"
        elif(engine_type == "-m"):
            engine_type = "lsm"
        elif(engine_type == "-w"):
            engine_type = "opt_wal"
        elif(engine_type == "-c"):
            engine_type = "opt_sp"
        elif(engine_type == "-l"):
            engine_type = "opt_lsm"
      
        if(rw_mix == 0):
            workload_type = 'read-only'
        elif(rw_mix == 0.1):
            workload_type = 'read-heavy'
        elif(rw_mix == 0.5):
            workload_type = 'write-heavy'    
            
        result_directory = YCSB_STORAGE_DIR + engine_type + "/" + workload_type + "/";
        if not os.path.exists(result_directory):
            os.makedirs(result_directory)

        result_file_name = result_directory + "storage.csv"
        result_file = open(result_file_name, "a")
        print(str(skew_factor) + " , " + str(fs_st) + " , " + str(pm_st))
        result_file.write(str(skew_factor) + " , " + str(fs_st) + " , " + str(pm_st) + "\n")
        result_file.close()    

        
    rw_mixes = YCSB_RW_MIXES
    skew_factors = YCSB_SKEW_FACTORS
    engines = ENGINES   

    # LOG RESULTS
    log_file = open(log_name, 'w')
                   
    # RW MIX
    for rw_mix  in rw_mixes:
        # SKEW FACTOR
        for skew_factor  in skew_factors:
            ostr = ("--------------------------------------------------- \n")
            print (ostr, end="")
            log_file.write(ostr)
            ostr = ("RW MIX :: %.1f SKEW :: %.2f \n" % (rw_mix, skew_factor))
            print (ostr, end="")
            log_file.write(ostr)                    
            log_file.flush()
    
            for eng in engines:
                cleanup()
                subprocess.call([NUMACTL, NUMACTL_FLAGS, NSTORE, '-k', str(keys), '-x', str(txns), '-p', str(rw_mix), '-q', str(skew_factor), '-z', eng], stdout=log_file)
                get_stats(eng, rw_mix, skew_factor)


# YCSB NVM -- EVAL
def ycsb_nvm_eval(log_name):            
    subprocess.call(['rm', '-rf', YCSB_NVM_DIR])          
    keys = YCSB_KEYS 
    txns = YCSB_TXNS

    PERF_STAT = "stat"    
    PERF_STAT_FLAGS = "-e LLC-load-misses,LLC-store-misses"

     # CLEANUP
    def cleanup():
        subprocess.call(["rm -f " + FS_PATH + "./*"], shell=True)        

    rw_mixes = YCSB_RW_MIXES
    skew_factors = YCSB_SKEW_FACTORS
    engines = ENGINES   

    # LOG RESULTS
    log_file = open(log_name, 'w')
                   
    # RW MIX
    for rw_mix  in rw_mixes:
        # SKEW FACTOR
        for skew_factor  in skew_factors:
            ostr = ("--------------------------------------------------- \n")
            print (ostr, end="")
            log_file.write(ostr)
            ostr = ("RW MIX :: %.1f SKEW :: %.2f \n" % (rw_mix, skew_factor))
            print (ostr, end="")
            log_file.write(ostr)                    
            log_file.flush()
    
            for eng in engines:
                cleanup()
                subprocess.call([PERF, PERF_STAT, PERF_STAT_FLAGS, NUMACTL, NUMACTL_FLAGS, NSTORE,
                                 '-k', str(keys), '-x', str(txns), '-p', str(rw_mix), '-q', str(skew_factor), eng],
                                stdout=log_file, stderr=log_file)
                              
    log_file.close()   
    log_file = open(log_name, "r")    

    # CLEAN UP RESULT DIR
    subprocess.call(['rm', '-rf', YCSB_NVM_DIR])          
 
    rw_mix = 0.0
    skew = 0.0    
    
    for line in log_file:                    
        if "RW MIX" in line:
            entry = line.strip().split(' ');
            print("RW MIX :: " + str(entry))
            rw_mix = entry[3]
            skew = entry[6]
                        
            if(rw_mix == '0.0'):
                workload_type = 'read-only'
            elif(rw_mix == '0.1'):
                workload_type = 'read-heavy'
            elif(rw_mix == '0.5'):
                workload_type = 'write-heavy'  
                       
        if "Throughput" in line:
            entry = line.strip().split(':');
            etypes = entry[0].split(' ');
            
            if(etypes[0] == "WAL"):
                engine_type = "wal"                
            elif(etypes[0] == "SP"):
                engine_type = "sp"
            elif(etypes[0] == "LSM"):
                engine_type = "lsm"
            elif(etypes[0] == "OPT_WAL"):
                engine_type = "opt_wal"
            elif(etypes[0] == "OPT_SP"):
                engine_type = "opt_sp"
            elif(etypes[0] == "OPT_LSM"):
                engine_type = "opt_lsm"
                                                        
        if "LLC-load-misses" in line:
            entry = line.strip().split(' ');
            if(entry[0] == '<not'):
                llc_l_miss = "0"
            else:    
                llc_l_miss = str(entry[0])
            llc_l_miss = llc_l_miss.replace(",", "")    
                
            print(engine_type + ", " + str(rw_mix) + " , " + str(skew) + " :: " + str(llc_l_miss) + "\n")


        if "LLC-store-misses" in line:
            entry = line.strip().split(' ');
            if(entry[0] == '<not'):
                llc_s_miss = "0"
            else:    
                llc_s_miss = str(entry[0])
            llc_s_miss = llc_s_miss.replace(",", "")    
                
            print(engine_type + ", " + str(rw_mix) + " , " + str(skew) + " :: " + str(llc_s_miss) + "\n")
                                                                
            result_directory = YCSB_NVM_DIR + engine_type + "/" + workload_type + "/";
            if not os.path.exists(result_directory):
                os.makedirs(result_directory)

            result_file_name = result_directory + "nvm.csv"
            result_file = open(result_file_name, "a")
            result_file.write(str(skew) + " , " + str(llc_l_miss) + " , " + str(llc_s_miss) + "\n")
            result_file.close()    

# TPCC PERF -- EVAL
def tpcc_perf_eval(enable_sdv, enable_trials, log_name):        
    dram_latency = 100
    txns = TPCC_TXNS
                    
     # CLEANUP
    def cleanup():
        subprocess.call(["rm -f " + FS_PATH + "./*"], shell=True)        
    
    num_trials = 1 
    if enable_trials: 
        num_trials = 3
    
    nvm_latencies = LATENCIES
    engines = ENGINES
    rw_mixes = TPCC_RW_MIXES

    # LOG RESULTS
    log_file = open(log_name, 'w')
    
    for nvm_latency in nvm_latencies:

        ostr = ("LATENCY %s \n" % nvm_latency)    
        print (ostr, end="")
        log_file.write(ostr)
        log_file.flush()
        
        if enable_sdv :
            cwd = os.getcwd()
            os.chdir(SDV_DIR)
            subprocess.call(['sudo', SDV_SCRIPT, '--enable', '--pm-latency', str(nvm_latency)], stdout=log_file)
            os.chdir(cwd)
                   
        for trial in range(num_trials):
            # RW MIX
            for rw_mix  in rw_mixes:            
                ostr = ("--------------------------------------------------- \n")
                print (ostr, end="")
                log_file.write(ostr)
                ostr = ("TRIAL :: %d RW MIX :: %.1f \n" % (trial, rw_mix))
                print (ostr, end="")
                log_file.write(ostr)                    
                log_file.flush()
                           
                for eng in engines:
                    cleanup()

                    if rw_mix == 0.0:
                        subprocess.call([NUMACTL, NUMACTL_FLAGS, NSTORE, '-x', str(txns), '-t', '-o', eng], stdout=log_file)
                    else:    
                        subprocess.call([NUMACTL, NUMACTL_FLAGS, NSTORE, '-x', str(txns), '-t', eng], stdout=log_file)

    # RESET
    if enable_sdv :
        cwd = os.getcwd()
        os.chdir(SDV_DIR)
        subprocess.call(['sudo', SDV_SCRIPT, '--enable', '--pm-latency', "200"], stdout=log_file)
        os.chdir(cwd)

    # PARSE LOG
    log_file.close()   
    log_file = open(log_name, "r")    

    tput = {}
    mean = {}
    sdev = {}
    latency = 0
    
    nvm_latencies = []
    engine_types = []
    
    for line in log_file:
        if "LATENCY" in line:
            entry = line.strip().split(' ');
            if entry[0] == "LATENCY":
                latency = entry[1]
            if latency not in nvm_latencies:
                nvm_latencies.append(latency)
                    
        if "TRIAL" in line:
            entry = line.strip().split(' ');
            trial = entry[2]
            rw_mix = entry[6]
                   
        if "Throughput" in line:
            entry = line.strip().split(':');
            engine_type = entry[0].split(' ');
            val = float(entry[4]);
            
            if(engine_type[0] == "WAL"):
                engine_type[0] = "wal"                
            elif(engine_type[0] == "SP"):
                engine_type[0] = "sp"
            elif(engine_type[0] == "LSM"):
                engine_type[0] = "lsm"
            elif(engine_type[0] == "OPT_WAL"):
                engine_type[0] = "opt_wal"
            elif(engine_type[0] == "OPT_SP"):
                engine_type[0] = "opt_sp"
            elif(engine_type[0] == "OPT_LSM"):
                engine_type[0] = "opt_lsm"
                                       
            key = (rw_mix, latency, engine_type[0]);
            if key in tput:
                tput[key].append(val)
            else:
                tput[key] = [ val ]
                            

    # CLEAN UP RESULT DIR
    subprocess.call(['rm', '-rf', TPCC_PERF_DIR])          
    
    for key in sorted(tput.keys()):
        mean[key] = round(numpy.mean(tput[key]), 2)
        mean[key] = str(mean[key]).rjust(10)
            
        sdev[key] = numpy.std(tput[key])
        sdev[key] /= float(mean[key])
        sdev[key] = round(sdev[key], 3)
        sdev[key] = str(sdev[key]).rjust(10)
        
        nvm_latency = str(key[1]);
        engine_type = str(key[2]);            
              
        if(key[0] == '0.0'):
            workload_type = 'stock-level'
        elif(key[0] == '0.5'):
            workload_type = 'all'
            
        result_directory = TPCC_PERF_DIR + engine_type + "/" + nvm_latency + "/";
        if not os.path.exists(result_directory):
            os.makedirs(result_directory)

        result_file_name = result_directory + "performance.csv"
        result_file = open(result_file_name, "a")
        result_file.write(key[0] + " , " + mean[key] + "\n")
        result_file.close()    

# TPCC STORAGE -- EVAL
def tpcc_storage_eval(log_name):            
    subprocess.call(['rm', '-rf', TPCC_STORAGE_DIR])          
    txns = TPCC_TXNS
    
     # CLEANUP
    def cleanup():
        subprocess.call(["rm -f " + FS_PATH + "./*"], shell=True)        
        
    # GET STATS
    def get_stats(engine_type, rw_mix):
        print ("eng : %s rw_mix : %lf" % (engine_type, rw_mix))
                
        subprocess.call(['ls', '-larth', FS_PATH ], stdout=log_file)
        find_cmd = subprocess.Popen(['find', FS_PATH , '-name', '*.nvm', '-exec', 'ls', '-lart', '{}', ';'], stdout=subprocess.PIPE)
        log_file.write("FS STORAGE :: ")
        log_file.flush()

        fs_st = subprocess.check_output(['awk', '{ sum += $5 } END { printf "%.2f", sum }'], stdin=find_cmd.stdout)
        fs_st = fs_st.replace(" ", "").strip()
        if not fs_st:
            fs_st = "0"
        print("FS STORAGE :: " + fs_st)
        log_file.write(fs_st + "\n")
        log_file.flush()

        subprocess.call([PMEM_CHECK, FS_PATH + "./zfile" ], stdout=log_file)           
        pmem_cmd = subprocess.Popen([PMEM_CHECK, FS_PATH + "./zfile" ], stdout=subprocess.PIPE)
        grep_cmd = subprocess.Popen(['grep', 'Active'], stdin=pmem_cmd.stdout, stdout=subprocess.PIPE)
        log_file.write("PM STORAGE :: ")
        log_file.flush()

        pm_st = subprocess.check_output(['awk', '{ print $2 }'], stdin=grep_cmd.stdout)
        pm_st = pm_st.replace(" ", "").strip()
        if not pm_st:
            pm_st = "0"
        print("PM STORAGE :: " + pm_st)
        log_file.write(pm_st + "\n")
        log_file.flush()
    
        if(engine_type == "-a"):
            engine_type = "wal"                
        elif(engine_type == "-s"):
            engine_type = "sp"
        elif(engine_type == "-m"):
            engine_type = "lsm"
        elif(engine_type == "-w"):
            engine_type = "opt_wal"
        elif(engine_type == "-c"):
            engine_type = "opt_sp"
        elif(engine_type == "-l"):
            engine_type = "opt_lsm"
      
        print("rw_mix :: --" + str(rw_mix) + "-- ")    
      
        if(rw_mix == 0):
            workload_type = 'stock-level'
        elif(rw_mix == 0.5):
            workload_type = 'all'    
            
        result_directory = TPCC_STORAGE_DIR + engine_type + "/";
        if not os.path.exists(result_directory):
            os.makedirs(result_directory)

        result_file_name = result_directory + "storage.csv"
        result_file = open(result_file_name, "a")
        print(workload_type + " , " + str(fs_st) + " , " + str(pm_st))
        result_file.write(str(rw_mix) + " , " + str(fs_st) + " , " + str(pm_st) + "\n")
        result_file.close()    

        
    rw_mixes = TPCC_RW_MIXES
    engines = ENGINES   

    # LOG RESULTS
    log_file = open(log_name, 'w')
                   
    # RW MIX
    for rw_mix  in rw_mixes:
        ostr = ("--------------------------------------------------- \n")
        print (ostr, end="")
        log_file.write(ostr)
        ostr = ("RW MIX :: %.1f \n" % (rw_mix))
        print (ostr, end="")
        log_file.write(ostr)                    
        log_file.flush()

        for eng in engines:
            cleanup()
            
            if rw_mix == 0.0:
                subprocess.call([NUMACTL, NUMACTL_FLAGS, NSTORE, '-x', str(txns), '-t', '-z', '-o', eng], stdout=log_file)
            else:
                subprocess.call([NUMACTL, NUMACTL_FLAGS, NSTORE, '-x', str(txns), '-t', '-z', eng], stdout=log_file)
                
            
            get_stats(eng, rw_mix)


# TPCC NVM -- EVAL
def tpcc_nvm_eval(log_name):            
    subprocess.call(['rm', '-rf', TPCC_NVM_DIR])          
    txns = TPCC_TXNS

    PERF_STAT = "stat"    
    PERF_STAT_FLAGS = "-e LLC-load-misses,LLC-store-misses"

     # CLEANUP
    def cleanup():
        subprocess.call(["rm -f " + FS_PATH + "./*"], shell=True)        

    rw_mixes = TPCC_RW_MIXES
    engines = ENGINES   

    # LOG RESULTS
    log_file = open(log_name, 'w')
               
    # RW MIX
    for rw_mix  in rw_mixes:
        ostr = ("--------------------------------------------------- \n")
        print (ostr, end="")
        log_file.write(ostr)
        ostr = ("RW MIX :: %.1f \n" % (rw_mix))
        print (ostr, end="")
        log_file.write(ostr)                    
        log_file.flush()

        for eng in engines:
            cleanup()
            
            if rw_mix == 0.0:
                subprocess.call([NUMACTL, NUMACTL_FLAGS, PERF, PERF_STAT, PERF_STAT_FLAGS, NSTORE, '-x', str(txns), '-o', eng],
                            stdout=log_file, stderr=log_file)
            else:   
                subprocess.call([NUMACTL, NUMACTL_FLAGS, PERF, PERF_STAT, PERF_STAT_FLAGS, NSTORE, '-x', str(txns), eng],
                            stdout=log_file, stderr=log_file)
                                  
    log_file.close()   
    log_file = open(log_name, "r")    

    # CLEAN UP RESULT DIR
    subprocess.call(['rm', '-rf', TPCC_NVM_DIR])          
 
    latency = 0
    rw_mix = 0.0
    skew = 0.0    
    
    for line in log_file:                    
        if "RW MIX" in line:
            entry = line.strip().split(' ');
            print("RW MIX :: " + str(entry))
            rw_mix = entry[3]
                        
            if(rw_mix == '0.0'):
                workload_type = 'read-only'
            elif(rw_mix == '0.1'):
                workload_type = 'read-heavy'
            elif(rw_mix == '0.5'):
                workload_type = 'write-heavy'  
                       
        if "Throughput" in line:
            entry = line.strip().split(':');
            etypes = entry[0].split(' ');
            
            if(etypes[0] == "WAL"):
                engine_type = "wal"                
            elif(etypes[0] == "SP"):
                engine_type = "sp"
            elif(etypes[0] == "LSM"):
                engine_type = "lsm"
            elif(etypes[0] == "OPT_WAL"):
                engine_type = "opt_wal"
            elif(etypes[0] == "OPT_SP"):
                engine_type = "opt_sp"
            elif(etypes[0] == "OPT_LSM"):
                engine_type = "opt_lsm"
                                                        
        if "LLC-load-misses" in line:
            entry = line.strip().split(' ');
            if(entry[0] == '<not'):
                llc_l_miss = "0"
            else:    
                llc_l_miss = str(entry[0])
            llc_l_miss = llc_l_miss.replace(",", "")    
                
            print(engine_type + ", " + str(rw_mix) + " , " + str(skew) + " :: " + str(llc_l_miss) + "\n")


        if "LLC-store-misses" in line:
            entry = line.strip().split(' ');
            if(entry[0] == '<not'):
                llc_s_miss = "0"
            else:    
                llc_s_miss = str(entry[0])
            llc_s_miss = llc_s_miss.replace(",", "")    
                
            print(engine_type + ", " + str(rw_mix) + " , " + str(skew)  + " :: " + str(llc_s_miss) + "\n")
                                                                
            result_directory = TPCC_NVM_DIR + engine_type + "/";
            if not os.path.exists(result_directory):
                os.makedirs(result_directory)

            result_file_name = result_directory + "nvm.csv"
            result_file = open(result_file_name, "a")
            result_file.write(str(rw_mix) + " , " + str(llc_l_miss) + " , " + str(llc_s_miss) + "\n")
            result_file.close()    

                          
                
## ==============================================
# # main
## ==============================================
if __name__ == '__main__':
    
    enable_sdv = False
    enable_trials = False
    
    parser = argparse.ArgumentParser(description='Run experiments')
    parser.add_argument("-x", "--enable-sdv", help='enable sdv', action='store_true')
    parser.add_argument("-u", "--enable-trials", help='enable trials', action='store_true')
    parser.add_argument("-y", "--ycsb_perf_eval", help='eval ycsb perf', action='store_true')
    parser.add_argument("-s", "--ycsb_storage_eval", help='eval ycsb storage', action='store_true')
    parser.add_argument("-n", "--ycsb_nvm_eval", help='eval ycsb nvm', action='store_true')
    parser.add_argument("-t", "--tpcc_perf_eval", help='eval tpcc perf', action='store_true')
    parser.add_argument("-q", "--tpcc_storage_eval", help='eval tpcc storage', action='store_true')
    parser.add_argument("-r", "--tpcc_nvm_eval", help='eval tpcc nvm', action='store_true')
    parser.add_argument("-a", "--ycsb_perf_plot", help='plot ycsb perf', action='store_true')
    parser.add_argument("-b", "--ycsb_storage_plot", help='plot ycsb storage', action='store_true')
    parser.add_argument("-c", "--ycsb_nvm_plot", help='plot ycsb nvm', action='store_true')
    parser.add_argument("-d", "--tpcc_perf_plot", help='plot tpcc perf', action='store_true')
    parser.add_argument("-e", "--tpcc_storage_plot", help='plot tpcc storage', action='store_true')
    parser.add_argument("-f", "--tpcc_nvm_plot", help='plot tpcc nvm', action='store_true')
    parser.add_argument("-l", "--enable-local", help='local mode', action='store_true')
    
    args = parser.parse_args()
    
    if args.enable_sdv:
        enable_sdv = True
    if args.enable_trials:
        enable_trials = True

    if args.enable_local:
        NUMACTL_FLAGS="--membind=0"

    ycsb_perf_log_name = "ycsb_perf.log"
    ycsb_storage_log_name = "ycsb_storage.log"
    ycsb_nvm_log_name = "ycsb_nvm.log"
    tpcc_perf_log_name = "tpcc_perf.log"
    tpcc_storage_log_name = "tpcc_storage.log"
    tpcc_nvm_log_name = "tpcc_nvm.log"
    
    ################################ YCSB
    
    # YCSB PERF -- EVAL
    if args.ycsb_perf_eval:
        ycsb_perf_eval(enable_sdv, enable_trials, ycsb_perf_log_name)
    
    # YCSB STORAGE -- EVAL
    if args.ycsb_storage_eval:
        ycsb_storage_eval(ycsb_storage_log_name);
             
    # YCSB NVM -- EVAL
    if args.ycsb_nvm_eval:
        ycsb_nvm_eval(ycsb_nvm_log_name);             
             
    # YCSB PERF -- PLOT
    if args.ycsb_perf_plot:      
        ycsb_perf_plot();          
                           
    # YCSB STORAGE -- PLOT               
    if args.ycsb_storage_plot:                
       ycsb_storage_plot();
       
    # YCSB NVM -- PLOT               
    if args.ycsb_nvm_plot:                
       ycsb_nvm_plot();                          

    ################################ TPCC

    # TPCC PERF -- EVAL
    if args.tpcc_perf_eval:
        tpcc_perf_eval(enable_sdv, enable_trials, tpcc_perf_log_name);             

    # TPCC STORAGE -- EVAL
    if args.tpcc_storage_eval:
        tpcc_storage_eval(tpcc_storage_log_name);
             
    # TPCC NVM -- EVAL
    if args.tpcc_nvm_eval:
        tpcc_nvm_eval(tpcc_nvm_log_name);             

    # TPCC PERF -- PLOT               
    if args.tpcc_perf_plot:                
       tpcc_perf_plot();                          

    # TPCC STORAGE -- PLOT               
    if args.tpcc_storage_plot:                
       tpcc_storage_plot();
       
    # TPCC NVM -- PLOT               
    if args.tpcc_nvm_plot:                
       tpcc_nvm_plot();                          



