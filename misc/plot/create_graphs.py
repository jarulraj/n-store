#!/usr/bin/env python

# Example execution 
# export b=tpcc ; export dataDir=/home/pavlo/Documents/H-Store/Papers/anticache/data/evictions
# ant compile hstore-benchmark -Dproject=$b -Dclient.interval=500 \
#    -Dsite.anticache_enable=true -Dsite.anticache_profiling=true \
#    -Dclient.output_memory=$dataDir/$b-memory.csv \
#    -Dclient.output_csv=$dataDir/$b-throughput.csv \
#    -Dclient.output_anticache_history=$dataDir/$b-evictions.csv

import os
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
from pprint import pprint,pformat

import csv

from options import *
import graphutil

## ==============================================
## LOGGING CONFIGURATION
## ==============================================

LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(
    fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
    datefmt='%m-%d-%Y %H:%M:%S'
)
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.INFO)

## ==============================================
## CONFIGURATION
## ==============================================

BASE_DIR = os.path.dirname(__file__)

SYSTEMS = ("anticache", "mysql")
WORKLOAD_MIX = ("read-only", "read-heavy", "write-heavy")
#DATASIZES = ("2", "4", "8")
DATASIZES = ("8")
LATENCIES = ("200", "800")

## ==============================================
## CREATE WORKLOAD SKEW THROUGHPUT GRAPH
## ==============================================
def createYCSBGraphs(anticache_fast, anticache_slow, mysql_fast, mysql_slow, workload_mix, is_nvm_only=False):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
     
    datasets = (anticache_fast, anticache_slow, mysql_fast, mysql_slow)

    if is_nvm_only == True:
        labels = (OPT_LABEL_HSTORE_FAST, OPT_LABEL_HSTORE_SLOW, OPT_LABEL_MYSQL_FAST, OPT_LABEL_MYSQL_SLOW)
    else:
        labels = (OPT_LABEL_ANTICACHE_FAST, OPT_LABEL_ANTICACHE_SLOW, OPT_LABEL_MYSQL_FAST, OPT_LABEL_MYSQL_SLOW)

    x_values = [1.5, 1.25, 1.0, 0.75, 0.5]
    x_labels = ["1.5", "1.25", "1.0", ".75", ".5"]

    for i in xrange(len(datasets)):
        LOG.info("%s y_values = %s", labels[i], datasets[i])
        ax1.plot(x_values, datasets[i],
                 label=labels[i],
                 color=OPT_COLORS[i],
                 linewidth=OPT_LINE_WIDTH,
                 marker=OPT_MARKERS[i],
                 markersize=OPT_MARKER_SIZE,
                 )
    ## FOR
    
    # GRID
    axes = ax1.get_axes()
    if workload_mix == "read-only":
        axes.set_ylim(0, 200000)
    elif workload_mix == "read-heavy":
        axes.set_ylim(0, 150000)
    elif workload_mix == "write-heavy":
        axes.set_ylim(0, 50000)
        
    graphutil.makeGrid(ax1)
    axes.set_xlim(.4, 1.6)

    # LEGEND
    fp = FontProperties(family=OPT_FONT_NAME, size=OPT_LEGEND_FONT_SIZE)
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
    #ax1.legend(loc='center', shadow=OPT_LEGEND_SHADOW, prop=fp)
    
    # Y-AXIS
    fp = FontProperties(family=OPT_FONT_NAME+' Condensed', size=OPT_YLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_ylabel(OPT_Y_LABEL_THROUGHPUT, fontproperties=fp)
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    ax1.minorticks_on()
    for tick in ax1.yaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    
    # X-AXIS
    fp = FontProperties(family=OPT_FONT_NAME+' Condensed', size=OPT_XLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_xlabel(OPT_X_LABEL_WORKLOAD_SKEW, fontproperties=fp)
    ax1.xaxis.set_major_locator(MaxNLocator(len(x_values)))
    xlim = ax1.get_xlim()[::-1]
    xlim = (xlim[0], xlim[1])
    ax1.set_xlim(xlim)
    ax1.minorticks_off()
    ax1.set_xticklabels(x_labels)
    ax1.set_xticks(x_values)
    print(x_labels)
    pprint(ax1.xaxis.get_majorticklocs())
    for tick in ax1.xaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    ## FOR
    
    return (fig)
## DEF

def createTPCCGraphs(anticache, mysql, is_nvm_only=False):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
    
    anticache = anticache[0:4]
    mysql = mysql[0:4]
    
    if is_nvm_only == True: 
        labels = (OPT_LABEL_HSTORE, OPT_LABEL_MYSQL)
    else:
        labels = (OPT_LABEL_ANTICACHE, OPT_LABEL_MYSQL)
        
    x_values = [1, 2, 4, 8]
    x_labels = ["1X", "2X", "4X", "8X"]
    
    ind = np.arange(4)  # the x locations for the groups
    width = 0.3       # the width of the bars
    
    print(anticache)
    print(mysql)
    rects1 = ax1.bar(ind-width/2, anticache, width,  align='center', color=OPT_COLORS[1])
    rects2 = ax1.bar(ind + width/2, mysql, width,  align='center', color=OPT_COLORS[2])
        
    ax1.set_ylim(0, 40000)
    
    graphutil.makeGrid(ax1)
    
    fp = FontProperties(family=OPT_FONT_NAME, size=OPT_LEGEND_FONT_SIZE)
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
    fp = FontProperties(family=OPT_FONT_NAME+' Condensed', size=OPT_YLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_ylabel(OPT_Y_LABEL_THROUGHPUT, fontproperties=fp)
    ax1.yaxis.set_major_locator(MaxNLocator(4))
    ax1.minorticks_on()
    
    for tick in ax1.yaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    
    # X-AXIS
    fp = FontProperties(family=OPT_FONT_NAME+' Condensed', size=OPT_XLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_xlabel(OPT_X_LABEL_NVM_LATENCY, fontproperties=fp)
    ax1.minorticks_off()
    
    for tick in ax1.xaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE+6)
        tick.label.set_fontname(OPT_FONT_NAME)
    
    ax1.set_xticks(ind)
    ax1.set_xticklabels( x_labels )
    ## FOR
    
    return (fig)

def createRecoveryThroughputGraphs(physical, logical):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
    
    x_values = [1, 2, 4, 8]
    x_labels = ["1X", "2X", "4X", "8X"]
    
    physical = physical[0:4]
    logical = logical[0:4]
    
    labels = (OPT_LABEL_LOGICAL_LOGGING, OPT_LABEL_PHYSICAL_LOGGING)
    
    
    ind = np.arange(4)  # the x locations for the groups
    width = 0.3       # the width of the bars
    
    print(logical)
    print(physical)
    rects1 = ax1.bar(ind-width/2, logical, width,  align='center', color=OPT_COLORS[1])
    rects2 = ax1.bar(ind + width/2, physical, width,  align='center', color=OPT_COLORS[2])
    
    ax1.set_ylim(0, 40000)
    
    graphutil.makeGrid(ax1)
    
    fp = FontProperties(family=OPT_FONT_NAME, size=OPT_LEGEND_FONT_SIZE)
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
    fp = FontProperties(family=OPT_FONT_NAME+' Condensed', size=OPT_YLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_ylabel(OPT_Y_LABEL_THROUGHPUT, fontproperties=fp)
    ax1.yaxis.set_major_locator(MaxNLocator(4))
    ax1.minorticks_on()
    
    for tick in ax1.yaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    
    # X-AXIS
    fp = FontProperties(family=OPT_FONT_NAME+' Condensed', size=OPT_XLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_xlabel(OPT_X_LABEL_NVM_LATENCY, fontproperties=fp)
    ax1.minorticks_off()
    
    for tick in ax1.xaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE+6)
        tick.label.set_fontname(OPT_FONT_NAME)
    
    ax1.set_xticks(ind)
    ax1.set_xticklabels( x_labels )
    ## FOR
    
    return (fig)
    
def createRecoveryElapsedTimeGraphs(physical, logical):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)

    x_values = [1, 2, 4, 8]
    x_labels = ["1X", "2X", "4X", "8X"]
    
    physical = physical[0:4]
    logical = logical[0:4]

    ind = np.arange(4)
    width = 0.3

    print(physical)
    print(logical)

    labels = (OPT_LABEL_LOGICAL_LOGGING, OPT_LABEL_PHYSICAL_LOGGING)

    rects1 = ax1.bar(ind -width/2, logical, width,  align='center', color=OPT_COLORS[1])
    rects2 = ax1.bar(ind + width/2, physical, width,  align='center', color=OPT_COLORS[2])

    ax1.set_ylim(0, 25)
    
    graphutil.makeGrid(ax1)

    fp = FontProperties(family=OPT_FONT_NAME, size=OPT_LEGEND_FONT_SIZE)
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
    fp = FontProperties(family=OPT_FONT_NAME+' Condensed', size=OPT_YLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_ylabel(OPT_Y_LABEL_TIME_SEC, fontproperties=fp)
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    ax1.minorticks_on()

    ax1.yaxis.labelpad = 35
    for tick in ax1.yaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)

    # X-AXIS
    fp = FontProperties(family=OPT_FONT_NAME+' Condensed', size=OPT_XLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_xlabel(OPT_X_LABEL_NVM_LATENCY, fontproperties=fp)
    ax1.minorticks_off()
    
    for tick in ax1.xaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE+6)
        tick.label.set_fontname(OPT_FONT_NAME)

    ax1.set_xticks(ind)
    ax1.set_xticklabels( x_labels )
    ## FOR

    return (fig)

def loadDataFile(path):
    file = open(path, "r")
    reader = csv.reader(file)
    
    data = [None]*5
    
    row_num = 0
    for row in reader:
        column_num = 0
        for col in row:
            if column_num == 1:
                data[row_num] = float(col)
            column_num += 1
        row_num += 1
        
    return data
                
## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Throughput Graphs')
    aparser.add_argument('--powerpoint', action='store_true',
                         help='PowerPoint Formatted Graphs')
    args = vars(aparser.parse_args())
    
    OPT_FONT_NAME = 'Proxima Nova'
    OPT_GRAPH_HEIGHT = 300
    OPT_LABEL_WEIGHT = 'bold'
    OPT_COLORS = ['#0A8345']
    OPT_LINE_WIDTH = 5.0
    OPT_MARKER_SIZE = 12.0
    OPT_GRAPH_HEIGHT = 400
    
    graph_id = 1
    for workload in WORKLOAD_MIX:
        for datasize in DATASIZES: 
            anticache_fast = loadDataFile(os.path.realpath(os.path.join(baseDir, "nvm-dram/ycsb/anticache/" + workload + "/" + datasize + "/200/results.csv")))
            anticache_slow = loadDataFile(os.path.realpath(os.path.join(baseDir, "nvm-dram/ycsb/anticache/" + workload + "/" + datasize + "/800/results.csv")))
            mysql_fast = loadDataFile(os.path.realpath(os.path.join(baseDir, "nvm-dram/ycsb/mysql/" + workload + "/" + datasize + "/200/results.csv")))
            mysql_slow = loadDataFile(os.path.realpath(os.path.join(baseDir, "nvm-dram/ycsb/mysql/" + workload + "/" + datasize + "/800/results.csv")))
            fig = createYCSBGraphs(anticache_fast, anticache_slow, mysql_fast, mysql_slow, workload, False)
            fileName = "nvm-dram-ycsb-%d.pdf" % (graph_id)
            graphutil.saveGraph(fig, fileName, height=OPT_GRAPH_HEIGHT)
            graph_id += 1
    
    graph_id = 1
    for workload in WORKLOAD_MIX:
        for datasize in DATASIZES: 
            hstore_fast = loadDataFile(os.path.realpath(os.path.join(baseDir, "nvm-only/ycsb/hstore/" + workload + "/" + datasize + "/200/results.csv")))
            hstore_slow = loadDataFile(os.path.realpath(os.path.join(baseDir, "nvm-only/ycsb/hstore/" + workload + "/" + datasize + "/800/results.csv")))
            mysql_fast = loadDataFile(os.path.realpath(os.path.join(baseDir, "nvm-only/ycsb/mysql/" + workload + "/" + datasize + "/200/results.csv")))
            mysql_slow = loadDataFile(os.path.realpath(os.path.join(baseDir, "nvm-only/ycsb/mysql/" + workload + "/" + datasize + "/800/results.csv")))
            fig = createYCSBGraphs(hstore_fast, hstore_slow, mysql_fast, mysql_slow, workload, True)
            fileName = "nvm-only-ycsb-%d.pdf" % (graph_id)
            graphutil.saveGraph(fig, fileName, height=OPT_GRAPH_HEIGHT)
            graph_id += 1
    
    anticache_tpcc = loadDataFile(os.path.realpath(os.path.join(baseDir, "nvm-dram/tpcc/anticache/results.csv")))
    mysql_tpcc = loadDataFile(os.path.realpath(os.path.join(baseDir, "nvm-dram/tpcc/mysql/results.csv")))
    fig = createTPCCGraphs(anticache_tpcc, mysql_tpcc, False)
    fileName = "nvm-dram-tpcc.pdf"
    graphutil.saveGraph(fig, fileName, height=OPT_GRAPH_HEIGHT)
    
    hstore_tpcc = loadDataFile(os.path.realpath(os.path.join(baseDir, "nvm-only/tpcc/hstore/results.csv")))
    mysql_tpcc = loadDataFile(os.path.realpath(os.path.join(baseDir, "nvm-only/tpcc/mysql/results.csv")))
    fig = createTPCCGraphs(hstore_tpcc, mysql_tpcc, True)
    fileName = "nvm-only-tpcc.pdf"
    graphutil.saveGraph(fig, fileName, height=OPT_GRAPH_HEIGHT)
    
    logical = loadDataFile(os.path.realpath(os.path.join(baseDir, "recovery/throughput/logical/results.csv")))
    physical = loadDataFile(os.path.realpath(os.path.join(baseDir, "recovery/throughput/physical/results.csv")))
    fig = createRecoveryThroughputGraphs(physical, logical)
    fileName = "recovery-throughput.pdf"
    graphutil.saveGraph(fig, fileName, height=OPT_GRAPH_HEIGHT)
    
    logical = loadDataFile(os.path.realpath(os.path.join(baseDir, "recovery/recovery-time/logical/results.csv")))
    physical = loadDataFile(os.path.realpath(os.path.join(baseDir, "recovery/recovery-time/physical/results.csv")))
    fig = createRecoveryElapsedTimeGraphs(physical, logical)
    fileName = "recovery-time.pdf"
    graphutil.saveGraph(fig, fileName, height=OPT_GRAPH_HEIGHT)
    

## MAIN
