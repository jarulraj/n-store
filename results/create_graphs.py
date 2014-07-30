#!/usr/bin/env python

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
from pprint import pprint, pformat
from matplotlib.backends.backend_pdf import PdfPages

import csv
import brewer2mpl

from options import *
from functools import wraps

## ==============================================
# # LOGGING CONFIGURATION
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
# # CONFIGURATION
## ==============================================

BASE_DIR = os.path.dirname(__file__)

SYSTEMS = ("wal","sp","lsm","opt_wal","opt_sp","opt_lsm")
WORKLOAD_MIX = ("read-only", "read-heavy", "write-heavy")
LATENCIES = ("200", "800")

OPT_FONT_NAME = 'Droid Sans'
OPT_GRAPH_HEIGHT = 300
OPT_GRAPH_WIDTH = 400
OPT_LABEL_WEIGHT = 'bold'
#OPT_COLORS = brewer2mpl.get_map('Set2', 'qualitative', 8).mpl_colors
OPT_COLORS = brewer2mpl.get_map('Set1', 'qualitative', 9).mpl_colors
OPT_GRID_COLOR = 'gray'
OPT_LEGEND_SHADOW=False
OPT_MARKERS = (['o','s','v',">", "h", "v","^", "x", "d","<", "|","8","|", "_"])

## ==============================================
# # SAVE GRAPH
## ==============================================
def saveGraph(fig, output, width, height):
    size = fig.get_size_inches()
    dpi = fig.get_dpi()
    LOG.debug("Current Size Inches: %s, DPI: %d" % (str(size), dpi))

    new_size = (width / float(dpi), height / float(dpi))
    fig.set_size_inches(new_size)
    new_size = fig.get_size_inches()
    new_dpi = fig.get_dpi()
    LOG.debug("New Size Inches: %s, DPI: %d" % (str(new_size), new_dpi))

    #fig.savefig(os.path.join(dataDir, 'motivation-%s.png' % dataSet), format='png')
    
    pp = PdfPages(output)
    fig.savefig(pp, format='pdf', bbox_inches='tight')
    pp.close()
    LOG.info("OUTPUT: %s", output)
# # DEF

## ==============================================
# # MAKE GRID
## ==============================================
def makeGrid(ax):
    axes = ax.get_axes()
    axes.yaxis.grid(True, color=OPT_GRID_COLOR)
    ax.set_axisbelow(True)
# # DEF

def create_ycsb_storage_bar_chart(datasets, workload_mix):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
     
    labels = ("WAL-2X", "SP-2X", "LSM-2X",
              "PM-WAL-2X", "PM-SP-2X", "PM-LSM-2X")

    N = 2
    x_values = [0.1, 1.0]
    x_labels = ["Low", "High"]

    ind = np.arange(N)  
    width = 0.05  # the width of the bars
    offset = 0.15
    
    for group in xrange(len(datasets)):
        # GROUP
        fs_data = []       
        pm_data = [] 

        # LINE
        for line in  xrange(len(datasets[group])):
            for col in  xrange(len(datasets[group][line])):
                if col == 1:
                    fs_data.append(datasets[group][line][col]/(1024))
                if col == 2:
                    pm_data.append(datasets[group][line][col]/(1024))
  
        LOG.info("%s fs_data = %s pm_data = %s ", labels[group], str(fs_data), str(pm_data))
                
        ax1.bar(ind+group*width, fs_data, width, color=OPT_COLORS[group])
        ax1.bar(ind+group*width, pm_data, width, bottom = fs_data, color=OPT_COLORS[group], hatch='/')

    # # FOR
        
    # GRID
    axes = ax1.get_axes()
    if workload_mix == "read-only":
        axes.set_ylim(0, 400000)
    elif workload_mix == "read-heavy":
        axes.set_ylim(0, 400000)
    elif workload_mix == "write-heavy":
        axes.set_ylim(0, 400000)
        
    makeGrid(ax1)
    
    # LEGEND
    fp = FontProperties(family=OPT_FONT_NAME, weight=OPT_LABEL_WEIGHT)
    #fp = FontProperties(family=OPT_FONT_NAME, size=OPT_LEGEND_FONT_SIZE)
    ax1.legend(labels,
                prop=fp,
                bbox_to_anchor=(0.0, 1.1, 1.0, 0.10),
                loc=1,
                ncol=2,
                mode="expand",
                shadow=OPT_LEGEND_SHADOW,
                borderaxespad=0.0,
    )
    
    # Y-AXIS
    #fp = FontProperties(family=OPT_FONT_NAME, size=OPT_YLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_ylabel("Throughput", fontproperties=fp)
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    ax1.minorticks_on()
    for tick in ax1.yaxis.get_major_ticks():
        #tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
        
    # X-AXIS
    #fp = FontProperties(family=OPT_FONT_NAME, size=OPT_XLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_xlabel("Skew", fontproperties=fp)
    ax1.minorticks_on()
    ax1.set_xticklabels(x_labels)
    print (x_values)
    ax1.set_xticks(ind + width*len(x_labels))
    print(x_labels)
    #pprint(ax1.xaxis.get_majorticklocs())
    for tick in ax1.xaxis.get_major_ticks():
        #tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    # # FOR
        
    return (fig)
# # DEF

def create_ycsb_perf_bar_chart(datasets, workload_mix):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
     
    labels = ("WAL-2X", "SP-2X", "LSM-2X",
              "PM-WAL-2X", "PM-SP-2X", "PM-LSM-2X")

    N = 2
    x_values = [0.1, 1.0]
    x_labels = ["Low", "High"]

    ind = np.arange(N)  
    width = 0.05  # the width of the bars
    offset = 0.15

    for i in xrange(len(datasets)):
        LOG.info("%s y_values = %s", labels[i], datasets[i])
        ax1.bar(ind + width*i + offset, datasets[i], width, color=OPT_COLORS[i])

    # # FOR
    
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
    #fp = FontProperties(family=OPT_FONT_NAME, size=OPT_LEGEND_FONT_SIZE)
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
    #fp = FontProperties(family=OPT_FONT_NAME, size=OPT_YLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_ylabel("Throughput", fontproperties=fp)
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    ax1.minorticks_on()
    for tick in ax1.yaxis.get_major_ticks():
        #tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
        
    # X-AXIS
    #fp = FontProperties(family=OPT_FONT_NAME, size=OPT_XLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_xlabel("Skew", fontproperties=fp)
    ax1.minorticks_on()
    ax1.set_xticklabels(x_labels)
    print (x_values)
    ax1.set_xticks(ind + width*len(datasets))
    print(x_labels)
    #pprint(ax1.xaxis.get_majorticklocs())
    for tick in ax1.xaxis.get_major_ticks():
        #tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    # # FOR
        
    return (fig)
# # DEF


def create_ycsb_perf_line_chart(datasets, workload_mix):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
     
    labels = ("WAL-2X", "SP-2X", "LSM-2X",
              "PM-WAL-2X", "PM-SP-2X", "PM-LSM-2X")

    x_values = [0.1, 1.0]
    x_labels = ["Low", "High"]

    for i in xrange(len(datasets)):
        LOG.info("%s y_values = %s", labels[i], datasets[i])
        ax1.plot(x_values, datasets[i], label=labels[i], color=OPT_COLORS[i], marker=OPT_MARKERS[i])
    # # FOR
    
    # GRID
    axes = ax1.get_axes()
    if workload_mix == "read-only":
        axes.set_ylim(0, 25000)
    elif workload_mix == "read-heavy":
        axes.set_ylim(0, 25000)
    elif workload_mix == "write-heavy":
        axes.set_ylim(0, 25000)
        
    makeGrid(ax1)

    # LEGEND
    fp = FontProperties(family=OPT_FONT_NAME, weight=OPT_LABEL_WEIGHT)
    #fp = FontProperties(family=OPT_FONT_NAME, size=OPT_LEGEND_FONT_SIZE)
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
    #fp = FontProperties(family=OPT_FONT_NAME, size=OPT_YLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_ylabel("Throughput", fontproperties=fp)
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    ax1.minorticks_on()
    for tick in ax1.yaxis.get_major_ticks():
        #tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    
    # X-AXIS
    #fp = FontProperties(family=OPT_FONT_NAME, size=OPT_XLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_xscale('log');
    ax1.set_xlabel("Skew", fontproperties=fp)
    ax1.xaxis.set_major_locator(MaxNLocator(len(x_values)))
    axes.set_xlim(0.09, 1.1)
    ax1.minorticks_off()
    ax1.set_xticklabels(x_labels)
    print (x_values)
    ax1.set_xticks(x_values)
    print(x_labels)
    pprint(ax1.xaxis.get_majorticklocs())
    for tick in ax1.xaxis.get_major_ticks():
        #tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    # # FOR
    
    return (fig)
# # DEF

def loadDataFile(n_rows, n_cols, path):
    file = open(path, "r")
    reader = csv.reader(file)
    
    data = [[0 for x in xrange(n_cols)] for y in xrange(n_rows)]
    
    row_num = 0
    for row in reader:
        print row
        column_num = 0
        for col in row:
            data[row_num][column_num] = float(col)
            column_num += 1
        row_num += 1
                
    return data
                
## ==============================================
# # main
## ==============================================
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Plot graphs')
    
    parser.add_argument('--powerpoint', action='store_true', help='PowerPoint Formatted Graphs')
    parser.add_argument("-y", "--ycsb_perf", help='ycsb throughput', action='store_true')
    parser.add_argument("-s", "--ycsb_storage", help='ycsb storage', action='store_true')
    
    args = parser.parse_args()
    
    # YCSB PERF
    if args.ycsb_perf:                
        for workload in WORKLOAD_MIX:    
            for lat in LATENCIES:
                    datasets = []
    
                    for sy in SYSTEMS:    
                        dataFile = loadDataFile(2, 2, os.path.realpath(os.path.join(BASE_DIR, "ycsb/performance/" + sy + "/" + workload + "/" + lat + "/performance.csv")))
                        datasets.append(dataFile)
                               
                    fig = create_ycsb_perf_bar_chart(datasets, workload)
                    
                    fileName = "ycsb-perf-%s-%s.pdf" % (workload, lat)
                    saveGraph(fig, fileName, width=OPT_GRAPH_WIDTH, height=OPT_GRAPH_HEIGHT)
                    
    WORKLOAD_MIX = ("read-only", "write-heavy")
                    
    # YCSB STORAGE                
    if args.ycsb_storage:                
        for workload in WORKLOAD_MIX:    
                datasets = []

                for sy in SYSTEMS:    
                    dataFile = loadDataFile(2, 3, os.path.realpath(os.path.join(BASE_DIR, "ycsb/storage/" + sy + "/" + workload + "/storage.csv")))
                    datasets.append(dataFile)
                           
                fig = create_ycsb_storage_bar_chart(datasets, workload)
                                
                fileName = "ycsb-storage-%s.pdf" % (workload)
                saveGraph(fig, fileName, width=OPT_GRAPH_WIDTH, height=OPT_GRAPH_HEIGHT)
               
    
