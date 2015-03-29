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
import datetime
import math

import numpy as np
import matplotlib.pyplot as plot

from matplotlib.font_manager import FontProperties
from matplotlib.ticker import MaxNLocator
from matplotlib.ticker import LogLocator
from pprint import pprint, pformat
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib import rc
from operator import add

import csv
import brewer2mpl
import matplotlib

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


# # CONFIGURATION

BASE_DIR = os.path.dirname(__file__)
OPT_FONT_NAME = 'Helvetica'
OPT_GRAPH_HEIGHT = 300
OPT_GRAPH_WIDTH = 400

#COLOR_MAP = ('#F15854', '#9C9F84', '#F7DCB4', '#991809', '#5C755E', '#A97D5D')
COLOR_MAP = ( '#F58A87', '#80CA86', '#9EC9E9', "#F15854", "#66A26B", "#5DA5DA")
OPT_COLORS = COLOR_MAP

OPT_GRID_COLOR = 'gray'
OPT_LEGEND_SHADOW = False
OPT_MARKERS = (['o', 's', 'v', "^", "h", "v", ">", "x", "d", "<", "|", "", "|", "_"])
OPT_PATTERNS = ([ "////", "////", "o", "o", "\\\\" , "\\\\" , "//////", "//////", ".", "." , "\\\\\\" , "\\\\\\" ])

OPT_LABEL_WEIGHT = 'bold'
OPT_LINE_COLORS = ('#fdc086', '#b3e2cd', '#fc8d62', '#a6cee3', '#e41a1c')
OPT_LINE_WIDTH = 6.0
OPT_MARKER_SIZE = 10.0
DATA_LABELS = []

OPT_STACK_COLORS = ('#AFAFAF', '#F15854', '#5DA5DA', '#60BD68',  '#B276B2', '#DECF3F', '#F17CB0', '#B2912F', '#FAA43A')

# # NSTORE
SDV_DIR = "/data/devel/sdv-tools/sdv-release"
SDV_SCRIPT = SDV_DIR + "/ivt_pm_sdv.sh"    
NSTORE = "./src/nstore"
FS_PATH = "/mnt/pmfs/n-store/"
PMEM_CHECK = "./src/pmem_check"
PERF_LOCAL = "/usr/bin/perf"
PERF = "/usr/lib/linux-tools/3.11.0-12-generic/perf"
NUMACTL = "numactl"
NUMACTL_FLAGS = "--membind=2"

SYSTEMS = ("wal", "sp", "lsm", "opt_wal", "opt_sp", "opt_lsm")
RECOVERY_SYSTEMS = ("wal", "lsm", "opt_wal", "opt_lsm")
LATENCIES = ("160", "320", "1280")

ENGINES = ['-a', '-s', '-m', '-w', '-c', '-l']

YCSB_KEYS = 2000000
YCSB_TXNS = 8000000
YCSB_WORKLOAD_MIX = ("read-only", "read-heavy", "balanced", "write-heavy")
YCSB_SKEW_FACTORS = [0.1, 0.5]
YCSB_RW_MIXES = [0, 0.1, 0.5, 0.9]
YCSB_RECOVERY_TXNS = [1000, 10000, 100000]
YCSB_STACK_LATENCIES = ["320"]
YCSB_STACK_SKEW_FACTORS = [0.1]

TPCC_WORKLOAD_MIX = ("all", "stock-level")
TPCC_RW_MIXES = [0.5]
TPCC_RECOVERY_TXNS = [1000, 10000, 100000]

YCSB_PERF_DIR = "../results/ycsb/performance/"
YCSB_STORAGE_DIR = "../results/ycsb/storage/"
YCSB_NVM_DIR = "../results/ycsb/nvm/"
YCSB_RECOVERY_DIR = "../results/ycsb/recovery/"
YCSB_STACK_DIR = "../results/ycsb/stack/"

TPCC_PERF_DIR = "../results/tpcc/performance/"
TPCC_STORAGE_DIR = "../results/tpcc/storage/"
TPCC_NVM_DIR = "../results/tpcc/nvm/"
TPCC_RECOVERY_DIR = "../results/tpcc/recovery/"

TEST_NVM_DIR = "../results/test/nvm/"
BTREE_DIR = "../results/btree/"
ISE_DIR = "../results/ise/"
NVM_BW_DIR = "../results/nvm_bw/"

# XXX These should match default values in "pbtree.h" and "cow_pbtree.h"
BTREE_SIZES = ["64", "128", "256", "512", "1024", "2048", "4096", "8192", "16384"]
MISC_LATENCIES = ("320",)

BTREE_NODE_SIZE_DEFAULT = "512"
BTREE_HEADER_FILE = "../src/common/pbtree.h"

COW_BTREE_NODE_SIZE_DEFAULT = "4096"
COW_BTREE_HEADER_FILE = "../src/common/cow_pbtree.h"

# XXX These should match default values in "libpm.h"
PCOMMIT_LATENCIES = ("1", "10", "100", "1000", "10000")
PCOMMIT_LATENCY_DEFAULT = "100"
PCOMMIT_WORKLOAD_MIX = ("read-heavy", "balanced", "write-heavy")

# XXX These should match default values in "libpm.h"
ISE_MODES = ("default", "ise", "ise", "ise", "ise")
ISE_MODE_DEFAULT = "default"
ISE_HEADER_FILE = "../src/common/libpm.h"

LABELS = ("InP", "CoW", "Log", "NVM-InP", "NVM-CoW", "NVM-Log")

TPCC_TXNS = 1000000
TEST_TXNS = 500000

# SET FONT

LABEL_FONT_SIZE = 16
TICK_FONT_SIZE = 14
TINY_FONT_SIZE = 8

AXIS_LINEWIDTH = 1.3
BAR_LINEWIDTH = 1.2

#rc('font',**{'family':'sans-serif','sans-serif':['Helvetica']})
#fontProperties = {'family':'sans-serif','sans-serif':['Arial'], 'weight' : 'normal', 'size' : LABEL_FONT_SIZE}
#rc('text', usetex=True)
#rc('font',**fontProperties)

# SET TYPE1 FONTS

matplotlib.rcParams['ps.useafm'] = True
matplotlib.rcParams['pdf.use14corefonts'] = True
matplotlib.rcParams['text.usetex'] = True
#matplotlib.rcParams['text.latex.preamble']=[r'\usepackage{euler}']

LABEL_FP = FontProperties(family=OPT_FONT_NAME, style='normal', size=LABEL_FONT_SIZE, weight='bold')
TICK_FP = FontProperties(family=OPT_FONT_NAME, style='normal', size=TICK_FONT_SIZE)
TINY_FP = FontProperties(family=OPT_FONT_NAME, style='normal', size=TINY_FONT_SIZE)

###################################################################################                   
# UTILS
###################################################################################                   

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
    for axis in ['top','bottom','left','right']:
            ax.spines[axis].set_linewidth(AXIS_LINEWIDTH)
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

# # RATIO
def get_ratio(datasets, invert=False):
    #LOG.info("Dataset :: %s", datasets);
        
    t1 = datasets[0][1]/datasets[3][1] 
    
    if datasets[4][1] == 0:
        t2 = 0
    else:
        t2 = datasets[1][1]/datasets[4][1] 

    t3 = datasets[2][1]/datasets[5][1] 

    if invert:
        t1 = 1.0/t1        
        if t2 != 0:
            t2 = 1.0/t2
        t3 = 1.0/t3

    if len(datasets) == 6:    
        LOG.info("INP :: %.2f", t1);
        LOG.info("COW :: %.2f", t2);
        LOG.info("LOG :: %.2f", t3);

###################################################################################                   
# PLOT
###################################################################################                   

def create_legend():
    fig = pylab.figure()
    ax1 = fig.add_subplot(111)

    figlegend = pylab.figure(figsize=(11, 0.5))

    num_items = len(ENGINES);   
    ind = np.arange(1)  
    margin = 0.10
    width = (1.0 - 2 * margin) / num_items      
      
    bars = [None] * len(LABELS) * 2

    for group in xrange(len(ENGINES)):        
        data = [1]
        bars[group] = ax1.bar(ind + margin + (group * width), data, width, color=OPT_COLORS[group], hatch=OPT_PATTERNS[group * 2], linewidth=BAR_LINEWIDTH)
        
    # LEGEND
    figlegend.legend(bars, LABELS, prop=LABEL_FP, loc=1, ncol=6, mode="expand", shadow=OPT_LEGEND_SHADOW, 
                     frameon=False, borderaxespad=0.0, handleheight=2, handlelength=3.5)

    figlegend.savefig('legend.pdf')

def create_storage_legend():
    fig = pylab.figure()
    ax1 = fig.add_subplot(111)

    figlegend = pylab.figure(figsize=(10, 0.5))

    num_items = 5;   
    ind = np.arange(1)  
    margin = 0.10
    width = (1.0 - 2 * margin) / num_items      
   
    STORAGE_LABELS = ("Table", "Index", "Log", "Checkpoint", "Other")
   
    bars = [None] * len(STORAGE_LABELS) * 2

    for group in xrange(len(STORAGE_LABELS)):        
        data = [1]
        bars[group] = ax1.bar(ind + margin + (group * width), data, width, color=OPT_STACK_COLORS[group], linewidth=BAR_LINEWIDTH)
        
    # LEGEND
    figlegend.legend(bars, STORAGE_LABELS, prop=LABEL_FP, loc=1, ncol=len(STORAGE_LABELS), mode="expand", shadow=OPT_LEGEND_SHADOW, 
                     frameon=False, borderaxespad=0.0, handleheight=2, handlelength=3.5)

    figlegend.savefig('storage_legend.pdf')    

def create_stack_legend():
    fig = pylab.figure()
    ax1 = fig.add_subplot(111)

    figlegend = pylab.figure(figsize=(11, 0.5))

    num_items = 4;   
    ind = np.arange(1)  
    margin = 0.10
    width = (1.0 - 2 * margin) / num_items      
   
    STACK_LABELS = ("Storage", "Recovery", "Index", "Other")
   
    bars = [None] * len(STACK_LABELS) * 2

    for group in xrange(len(ENGINES)):        
        data = [1]
        bars[group] = ax1.bar(ind + margin + (group * width), data, width, color=OPT_STACK_COLORS[group], linewidth=BAR_LINEWIDTH)
        
    # LEGEND
    figlegend.legend(bars, STACK_LABELS, prop=LABEL_FP, loc=1, ncol=6, mode="expand", shadow=OPT_LEGEND_SHADOW, 
                     frameon=False, borderaxespad=0.0, handleheight=2, handlelength=3.5)

    figlegend.savefig('stack_legend.pdf')  

def create_line_legend():
    fig = pylab.figure()
    ax1 = fig.add_subplot(111)

    figlegend = pylab.figure(figsize=(9, 0.5))
    idx = 0
    lines = [None] * len(YCSB_WORKLOAD_MIX)

    workload_mix = ("Read-Only", "Read-Heavy", "Balanced", "Write-Heavy")
    workload_subset_mix = workload_mix[1:4]
             
    for group in xrange(len(YCSB_WORKLOAD_MIX)):        
        data = [1]
        x_values = [1]
        
        lines[idx], = ax1.plot(x_values, data, color=OPT_LINE_COLORS[idx], linewidth=OPT_LINE_WIDTH, 
                 marker=OPT_MARKERS[idx], markersize=OPT_MARKER_SIZE, label=str(group))        
        
        idx = idx + 1
                
    # LEGEND
    figlegend.legend(lines,  workload_mix, prop=LABEL_FP, loc=1, ncol=4, mode="expand", shadow=OPT_LEGEND_SHADOW, 
                     frameon=False, borderaxespad=0.0, handleheight=2, handlelength=3.5)

    figlegend.savefig('line_legend.pdf')

    figlegend = pylab.figure(figsize=(7, 0.5))
    lines_subset = lines[1:4]

    figlegend.legend(lines_subset, workload_subset_mix, prop=LABEL_FP, loc=1, ncol=3, mode="expand", shadow=OPT_LEGEND_SHADOW, 
                     frameon=False, borderaxespad=0.0, handleheight=2, handlelength=3.5)

    figlegend.savefig('line_subset_legend.pdf')


def create_nvm_bw_chart(datasets):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
         
    CHUNK_SIZES = [1, 2, 4, 8, 16, 32, 64, 128, 256]     
    x_values = CHUNK_SIZES
    N = len(x_values)
    x_labels = x_values 
    
    nvm_data = []
    fs_data = []
    
    for group in xrange(len(datasets)):    
        nvm_data.append(datasets[group][1]/(1024*1024))
        fs_data.append(datasets[group][2]/(1024*1024))

    LOG.info("x_values : %s", x_values)         
    LOG.info("nvm_data : %s", nvm_data)        
    LOG.info("fs_data : %s", fs_data)        

    ax1.plot(x_values, nvm_data, color=OPT_COLORS[3], linewidth=OPT_LINE_WIDTH, marker=OPT_MARKERS[0], markersize=OPT_MARKER_SIZE, label='Allocator')
    ax1.plot(x_values, fs_data, color=OPT_COLORS[5], linewidth=OPT_LINE_WIDTH, marker=OPT_MARKERS[1], markersize=OPT_MARKER_SIZE, label='Filesystem')
    
    # GRID
    axes = ax1.get_axes()
    makeGrid(ax1)
      
    # Y-AXIS
    ax1.set_yscale('log', basey=2)
    ax1.minorticks_on()
    ax1.yaxis.set_major_locator(LogLocator(base = 4.0))
    ax1.set_ylim(0.71, 4096)
    ax1.set_ylabel("Bandwidth (MB/s)", fontproperties=LABEL_FP)        
        
    # X-AXIS
    ax1.minorticks_on()
    ax1.set_xlim(0.71, 362)
    ax1.set_xscale('log', basex=2)
    ax1.set_xlabel("Chunk Size (B)", fontproperties=LABEL_FP)        
            
    # LEGEND
    legend = ax1.legend(loc='upper left')

    for label in ax1.get_yticklabels() :
        label.set_fontproperties(TICK_FP)
    for label in ax1.get_xticklabels() :
        label.set_fontproperties(TICK_FP)
            
    return (fig)    
        

def create_ycsb_perf_bar_chart(datasets):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
         
    x_values = YCSB_SKEW_FACTORS
    N = len(x_values)
    x_labels = ["Low Skew", "High Skew"]

    num_items = len(ENGINES);   
    ind = np.arange(N)  
    margin = 0.10
    width = (1.0 - 2 * margin) / num_items      
    bars = [None] * len(LABELS) * 2

    YLIMIT = 2000000

    def autolabel(rects):
        # attach some text labels
        for rect in rects:
            height = rect.get_height()
            if height > YLIMIT:
                label = '%.1f'%(height/1000000) + 'M'
                ax1.text(rect.get_x()+rect.get_width()/2., 1.05*YLIMIT, label,
                        ha='center', va='bottom', fontproperties=TINY_FP)

    # GROUP
    for group in xrange(len(datasets)):
        perf_data = []               

        # LINE
        for line in  xrange(len(datasets[group])):
            for col in  xrange(len(datasets[group][line])):
                if col == 1:
                    perf_data.append(datasets[group][line][col])
  
        LOG.info("%s perf_data = %s ", LABELS[group], str(perf_data))
        
        bars[group] = ax1.bar(ind + margin + (group * width), perf_data, width, color=OPT_COLORS[group], hatch=OPT_PATTERNS[group * 2])        
        autolabel(bars[group])

    # RATIO
    transposed_datasets = map(list,map(None,*datasets))
    for type in xrange(N):
        LOG.info("type = %f ", x_values[type])
        get_ratio(transposed_datasets[type], True)
        
    # GRID
    axes = ax1.get_axes()
    makeGrid(ax1)
      
    # Y-AXIS
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    ax1.minorticks_on()
    ax1.set_ylim([0,YLIMIT])
        
    # X-AXIS
    ax1.minorticks_on()
    ax1.set_xticklabels(x_labels)
    ax1.set_xticks(ind + 0.5)              
    ax1.set_ylabel("Throughput (txn/sec)", fontproperties=LABEL_FP)
    ax1.tick_params(axis='x', which='both', bottom='off', top='off')
        
    for label in ax1.get_yticklabels() :
        label.set_fontproperties(TICK_FP)
    for label in ax1.get_xticklabels() :
        label.set_fontproperties(TICK_FP)
            
    return (fig)


def create_ycsb_stack_bar_chart(datasets, abs_time):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
         
    x_values = YCSB_STACK_SKEW_FACTORS
    N = len(x_values)
    #x_labels = ["Low Skew", "High Skew"]

    num_items = len(ENGINES);   
    ind = np.arange(num_items)  
    pprint(ind)
    margin = 0.40
    width = (2.0 - 2 * margin) 
    col_offset = 0.15 
    col_width = width - 0.15    
    bars = [None] * len(LABELS) 
    label_loc = []

    # ABS TIME HACK
    abs_mode = False
    idx = 0
    abs_time_subset = []
    
    if abs_mode == False:
        YLIMIT = 100.0
    
    # GROUP
    for group in xrange(len(abs_time)):
        idx = 0                
        # LINE        
        for line in  xrange(len(abs_time[group])):        
            if(idx % 2 == 0):
                abs_time_subset.append(abs_time[group][line][1])
            idx = idx + 1

    idx = 0    
    if(abs_mode):
        print(abs_time_subset)
    
    # GROUP
    for group in xrange(len(datasets)):
        # ABS TIME
        if(abs_mode):
            max_num = YCSB_TXNS/abs_time_subset[idx]
        # NORMALIZE TIME
        else:
            max_num = 100.0
        
        # LINE
        for line in  xrange(len(datasets[group])):
            LOG.info("DATA = %s ", str(datasets[group][line]))

            bottom_num = 0.0                
                        
            for col in  xrange(len(datasets[group][line])):                   
                bars[group] = ax1.bar(line + margin + (group * width) + col_offset, datasets[group][line][col]/100.0 * max_num, col_width, 
                                      color=OPT_STACK_COLORS[col-1],
                                      bottom = bottom_num)
                
                bottom_num = bottom_num +  (datasets[group][line][col]/100.0 * max_num)

            col = 4
            bars[group] = ax1.bar(line + margin + (group * width) + col_offset, max_num - bottom_num, col_width, 
                                      color=OPT_STACK_COLORS[col-1],
                                      bottom = bottom_num)

            label_loc.append(line + margin + (group * width))
                
        idx = idx + 1        

    # RATIO
    #transposed_datasets = map(list,map(None,*datasets))
    #for type in xrange(N):
    #    LOG.info("type = %f ", x_values[type])
    #    get_ratio(transposed_datasets[type], True)
        
    # GRID
    axes = ax1.get_axes()
    makeGrid(ax1)
      
    # Y-AXIS
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    ax1.minorticks_on()
    #ax1.set_ylim([0,YLIMIT])
        
    # X-AXIS
    ax1.minorticks_on()
    #ax1.set_xticklabels(x_labels)
    ax1.set_xticks(label_loc)              

    if abs_mode:
        ax1.set_ylabel("Absolute Time (s)", fontproperties=LABEL_FP)
    else:
        ax1.set_ylabel("Time (\%)", fontproperties=LABEL_FP)
        
    ax1.tick_params(axis='x', which='both', bottom='off', top='off')
    ax1.set_xticklabels(LABELS, rotation=60)
        
    for label in ax1.get_yticklabels() :
        label.set_fontproperties(TICK_FP)
    for label in ax1.get_xticklabels() :
        label.set_fontproperties(TICK_FP)
            
    return (fig)

def create_ycsb_storage_bar_chart(datasets):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
    
    num_items = len(ENGINES);

    ind = np.arange(num_items)  
    margin = 0.5
    width = 1.0

    col_offset = 0.15      
    col_width = width - col_offset

    bars = [None] * len(LABELS) * 2

    YLIMIT = 6.0

    for line in xrange(len(datasets)): 
        datasets[line] = datasets[line][1:]
        list_sum = sum(datasets[line])
        LOG.info("SUM :: %f", list_sum)
        #datasets[line] = [x / list_sum for x in datasets[line]]
    
    datasets = map(list,map(None,*datasets))
    
    # TYPE      
    bottom_list = [0] * len(datasets[0])    
    for type in  xrange(len(datasets)):        
        LOG.info("TYPE :: %s", datasets[type])

        bars[type] = ax1.bar(ind + margin + col_offset, datasets[type], col_width, 
                             color=OPT_STACK_COLORS[type], linewidth=BAR_LINEWIDTH,
                             bottom = bottom_list)
        bottom_list = map(add, bottom_list, datasets[type])
        
    # GRID
    axes = ax1.get_axes()
    makeGrid(ax1)
        
    # Y-AXIS
    #ax1.set_yscale('log', nonposy='clip')
    ax1.set_ylabel("Storage (GB)", fontproperties=LABEL_FP)
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    axes.set_ylim(0,YLIMIT)
        
    # X-AXIS
    #ax1.set_xlabel("Engine", fontproperties=LABEL_FP)
    ax1.tick_params(axis='x', which='both', top='off', bottom='off')
    ax1.set_xticks(ind + 0.5)
    ax1.set_xticklabels(LABELS, rotation=60)
        
    for label in ax1.get_yticklabels() :
        label.set_fontproperties(TICK_FP)
    for label in ax1.get_xticklabels() :
        label.set_fontproperties(TICK_FP)
    
    return (fig)

def create_ycsb_nvm_bar_chart(datasets,type):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)   
            
    x_values = YCSB_SKEW_FACTORS
    N = len(x_values)
    x_labels = ["Low Skew", "High Skew"]
    num_items = len(ENGINES);

    ind = np.arange(N)  
    margin = 0.15
    width = (1.0 - 2 * margin) / num_items      
    bars = [None] * len(LABELS) * 2
    graph_type = type

    if type == 0:
        YLIMIT = 1200
    else:
        YLIMIT = 500

    def autolabel(rects):
        # attach some text labels
        for rect in rects:
            height = rect.get_height()
            if height > YLIMIT:
                label = '%.1f'%(height/1000) + 'B'
                ax1.text(rect.get_x()+rect.get_width()/2., 1.05*YLIMIT, label,
                        ha='center', va='bottom', fontproperties=TINY_FP)
    
    for group in xrange(len(datasets)):
        # GROUP
        l_misses = []       
        s_misses = [] 

        for line in  xrange(len(datasets[group])):
            for col in  xrange(len(datasets[group][line])):
                if col == 1:
                    l_misses.append(1 + datasets[group][line][col] / (1024 * 1024))
                if col == 2:
                    s_misses.append(1 + datasets[group][line][col] / (1024 * 1024))
  
        LOG.info("%s l_misses = %s s_misses = %s ", LABELS[group], str(l_misses), str(s_misses))
    
        # LOADS
        if type == 0:            
            bars[group * 2] = ax1.bar(ind + margin + (group * width), l_misses, width, color=COLOR_MAP[group], hatch=OPT_PATTERNS[group * 2], linewidth=BAR_LINEWIDTH)
            autolabel(bars[group*2])
        # STORES
        else:
            bars[group * 2 + 1] = ax1.bar(ind + margin + (group * width), s_misses, width, color=COLOR_MAP[group], hatch=OPT_PATTERNS[group * 2 + 1], linewidth=BAR_LINEWIDTH)
            autolabel(bars[group*2 + 1])
#           bars[group * 2 + 1] = ax1.bar(ind + margin + (group * width), s_misses, width, bottom=l_misses, color=COLOR_MAP[group], hatch=OPT_PATTERNS[group * 2 + 1], linewidth=BAR_LINEWIDTH)
    
    # RATIO
    if graph_type == 0:
        LOG.info("LOADS")
        tmp_datasets = map(list,map(None,*datasets))
        for type in xrange(N):
            LOG.info("type = %f ", x_values[type])
            for line in  xrange(len(ENGINES)):
                tmp_datasets[type][line][1] = tmp_datasets[type][line][1]
            get_ratio(tmp_datasets[type], False)
    else:
        LOG.info("STORES")
        tmp_datasets = map(list,map(None,*datasets))
        for type in xrange(N):
            LOG.info("type = %f ", x_values[type])
            for line in  xrange(len(ENGINES)):
                tmp_datasets[type][line][1] = tmp_datasets[type][line][2]
            get_ratio(tmp_datasets[type], False)

    # GRID
    axes = ax1.get_axes()      
    makeGrid(ax1)
        
    # Y-AXIS    
    #ax1.set_yscale('log', nonposy='clip')
    ax1.minorticks_off()
    ax1.set_ylim(0,YLIMIT)
    ax1.yaxis.set_major_locator(MaxNLocator(6))
        
    # X-AXIS
    ax1.minorticks_off()
    ax1.set_xticklabels(x_labels)
    ax1.tick_params(axis='x', which='both', bottom='off', top='off')
    ax1.set_xticks(ind + 0.5)
        
    if graph_type == 0:
        ax1.set_ylabel("NVM Loads (M)", fontproperties=LABEL_FP)
    else:
        ax1.set_ylabel("NVM Stores (M)", fontproperties=LABEL_FP)
        
    for label in ax1.get_yticklabels() :
        label.set_fontproperties(TICK_FP)
    for label in ax1.get_xticklabels() :
        label.set_fontproperties(TICK_FP)
         
    return (fig)

def create_ycsb_recovery_bar_chart(datasets):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
     
    x_values = YCSB_RECOVERY_TXNS
    N = len(x_values)
    x_labels = YCSB_RECOVERY_TXNS
    
    num_items = len(RECOVERY_SYSTEMS);

    ind = np.arange(N)  
    margin = 0.15
    width = (1.0 - 2 * margin) / num_items      
    bars = [None] * len(LABELS) * 2
        
    for group in xrange(len(datasets)):
        # GROUP
        durations = []   

        for line in  xrange(len(datasets[group])):
            for col in  xrange(len(datasets[group][line])):
                if col == 1:
                    durations.append(datasets[group][line][col])
  
        LOG.info("%s duration = %s ", LABELS[group], str(durations))
        
        color_group = group
        if group == 1:
            color_group = 2
        elif group == 2:
            color_group = 3
        elif group == 3:
            color_group = 5
        
        bars[group] = ax1.bar(ind + margin + (group * width), durations, width, color=OPT_COLORS[color_group], hatch=OPT_PATTERNS[color_group*2], linewidth=BAR_LINEWIDTH)

                
    # RATIO
    #transposed_dataset = map(list,map(None,*datasets))
    #for type in xrange(N):
    #    get_ratio(transposed_dataset[type], False)
        
    # GRID
    axes = ax1.get_axes()
    axes.set_ylim(0.01, 1000000)        
    makeGrid(ax1)
    
    # Y-AXIS
    ax1.set_ylabel("Recovery Latency (ms)", fontproperties=LABEL_FP)
    ax1.set_yscale('log', nonposy='clip')
    ax1.yaxis.set_major_locator(LogLocator(base = 100.0))
    ax1.minorticks_off()
       
    # X-AXIS
    ax1.set_xlabel("Number of transactions", fontproperties=LABEL_FP)
    #ax1.set_xticklabels([r'$10^{3}$', r'$10^{4}$', r'$10^{5}$'])
    ax1.set_xticklabels(x_labels)
    ax1.set_xticks(ind + 0.5)
    ax1.tick_params(axis='x', which='both', bottom='off', top='off')

    for label in ax1.get_yticklabels() :
        label.set_fontproperties(TICK_FP)
    for label in ax1.get_xticklabels() :
        label.set_fontproperties(TICK_FP)
        
    return (fig)

def create_tpcc_perf_bar_chart(datasets):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
         
    x_values = LATENCIES
    N = len(x_values)
    x_labels = ["DRAM Latency", "Low NVM Latency", "High NVM Latency"]

    num_items = len(ENGINES);   
    ind = np.arange(N)  
    margin = 0.10
    width = (1.0 - 2 * margin) / num_items      
    bars = [None] * len(LABELS) * 2
            
    # GROUP    
    for group in xrange(len(datasets)):
        perf_data = []               
        LOG.info("GROUP :: %s", datasets[group])
        
        for line in  xrange(len(datasets[group])):
            for col in  xrange(len(datasets[group][line])):
                if col == 1:
                    perf_data.append(datasets[group][line][col])

        LOG.info("%s perf_data = %s ", LABELS[group], str(perf_data))

        bars[group] = ax1.bar(ind + margin + (group * width), perf_data, width, color=OPT_COLORS[group], hatch=OPT_PATTERNS[group * 2], linewidth=BAR_LINEWIDTH)

    # RATIO
    transposed_dataset = map(list,map(None,*datasets))
    for type in xrange(N):
        get_ratio(transposed_dataset[type], True)
                     
    # GRID    
    axes = ax1.get_axes()
    makeGrid(ax1)    

    # Y-AXIS
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    ax1.minorticks_on()
        
    # X-AXIS
    #ax1.set_xlabel("Workload", fontproperties=LABEL_FP)
    ax1.minorticks_on()
    ax1.set_xticklabels(x_labels)
    ax1.set_xticks(ind + 0.5)
    ax1.tick_params(axis='x', which='both', bottom='off', top='off')
        
    ax1.set_ylabel("Throughput (txn/sec)", fontproperties=LABEL_FP)        
    
    for label in ax1.get_yticklabels() :
        label.set_fontproperties(TICK_FP)
    for label in ax1.get_xticklabels() :
        label.set_fontproperties(TICK_FP)
        
    return (fig)

def create_tpcc_storage_bar_chart(datasets):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
    
    x_values = TPCC_RW_MIXES
    N = len(x_values)
    num_items = len(ENGINES);

    ind = np.arange(num_items)  
    margin = 0.5
    width = 1.0      
    col_offset = 0.15      
    col_width = width - col_offset

    bars = [None] * len(LABELS) * 2

    YLIMIT = 6.0

    for line in xrange(len(datasets)): 
        datasets[line] = datasets[line][1:]
        list_sum = sum(datasets[line])
        LOG.info("SUM :: %f", list_sum)
        #datasets[line] = [x / list_sum for x in datasets[line]]
    
    datasets = map(list,map(None,*datasets))
    
    # TYPE      
    bottom_list = [0] * len(datasets[0])    
    for type in  xrange(len(datasets)):        
        LOG.info("TYPE :: %s", datasets[type])

        bars[type] = ax1.bar(ind + margin + col_offset, datasets[type], col_width, 
                             color=OPT_STACK_COLORS[type], linewidth=BAR_LINEWIDTH,
                             bottom = bottom_list)
        bottom_list = map(add, bottom_list, datasets[type])
                           
    # GRID
    axes = ax1.get_axes()
    makeGrid(ax1)

    # Y-AXIS
    #ax1.set_yscale('log', nonposy='clip')
    ax1.set_ylabel("Storage (GB)", fontproperties=LABEL_FP)
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    axes.set_ylim(0, YLIMIT)
        
    # X-AXIS
    #ax1.set_xlabel("Engine", fontproperties=LABEL_FP)
    ax1.set_xticks(ind + 0.5)
    ax1.set_xticklabels(LABELS, rotation=60)
    ax1.tick_params(axis='x', which='both', bottom='off', top='off')


    for label in ax1.get_yticklabels() :
        label.set_fontproperties(TICK_FP)
    for label in ax1.get_xticklabels() :
        label.set_fontproperties(TICK_FP)
                    
    return (fig)

def create_tpcc_nvm_bar_chart(datasets, type):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
     
    x_values = TPCC_RW_MIXES
    N = len(x_values)
    num_items = len(ENGINES);

    ind = np.arange(N)  
    margin = 0.25
    width = (1.0 - 2 * margin) / num_items      
    bars = [None] * len(LABELS) * 2
        
    if type == 0:
        YLIMIT = 1200
    else:
        YLIMIT = 600
           
    # LINE    
    for line in  xrange(len(ENGINES)):
        l_misses = []       
        s_misses = [] 

        LOG.info("LINE :: %s", datasets[line])

        for col in  xrange(len(datasets[line])):
            if col == 1:
                l_misses.append(datasets[line][col] / (1024 * 1024))
            if col == 2:
                s_misses.append(datasets[line][col] / (1024 * 1024))
    
        # LOADS
        if type == 0:
            bars[line * 2] = ax1.bar(ind + margin + (line * width), l_misses, width, color=COLOR_MAP[line], hatch=OPT_PATTERNS[line * 2], linewidth=BAR_LINEWIDTH)    
        # STORES
        else:
            bars[line * 2 + 1] = ax1.bar(ind + margin + (line * width), s_misses, width, color=COLOR_MAP[line], hatch=OPT_PATTERNS[line * 2 + 1], linewidth=BAR_LINEWIDTH)
#            bars[line * 2 + 1] = ax1.bar(ind + margin + (line * width), s_misses, width, bottom=l_misses, color=COLOR_MAP[line], hatch=OPT_PATTERNS[line * 2 + 1], linewidth=BAR_LINEWIDTH)

    # RATIO 
    if type == 0:
        LOG.info("LOADS")
        tmp_datasets = datasets;   
        for line in  xrange(len(ENGINES)):
            tmp_datasets[line][1] = tmp_datasets[line][1]
        get_ratio(tmp_datasets, False)
    else:
        LOG.info("STORES")
        tmp_datasets = datasets;   
        for line in  xrange(len(ENGINES)):
            tmp_datasets[line][1] = tmp_datasets[line][2]
        get_ratio(tmp_datasets, False)
        
    # GRID
    axes = ax1.get_axes()
    axes.set_ylim(0, YLIMIT)        
    makeGrid(ax1)
        
    # Y-AXIS
    #ax1.set_yscale('log', nonposy='clip')
    ax1.minorticks_off()
    ax1.yaxis.set_major_locator(MaxNLocator(6))

    if type == 0:
        ax1.set_ylabel("NVM Loads (M)", fontproperties=LABEL_FP)
    else:
        ax1.set_ylabel("NVM Stores (M)", fontproperties=LABEL_FP)
        
    # X-AXIS
    #ax1.minorticks_on()
    ax1.tick_params(axis='x', which='both', bottom='off', top='off', labelbottom='off')
    
    for label in ax1.get_yticklabels() :
        label.set_fontproperties(TICK_FP)
    for label in ax1.get_xticklabels() :
        label.set_fontproperties(TICK_FP)
        
    return (fig)

def create_tpcc_recovery_bar_chart(datasets):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
     
    x_values = TPCC_RECOVERY_TXNS
    N = len(x_values)
    x_labels = TPCC_RECOVERY_TXNS
    
    num_items = len(RECOVERY_SYSTEMS);

    ind = np.arange(N)  
    margin = 0.15
    width = (1.0 - 2 * margin) / num_items      
    bars = [None] * len(LABELS) * 2
        
    for group in xrange(len(datasets)):
        # GROUP
        durations = []   

        for line in  xrange(len(datasets[group])):
            for col in  xrange(len(datasets[group][line])):
                if col == 1:
                    durations.append(datasets[group][line][col])
  
        LOG.info("%s duration = %s ", LABELS[group], str(durations))
        
        color_group = group
        if group == 1:
            color_group = 2
        elif group == 2:
            color_group = 3
        elif group == 3:
            color_group = 5
        
        bars[group] = ax1.bar(ind + margin + (group * width), durations, width, color=OPT_COLORS[color_group], hatch=OPT_PATTERNS[color_group*2], linewidth=BAR_LINEWIDTH)
    
    # RATIO
    #transposed_dataset = map(list,map(None,*datasets))
    #for type in xrange(N):
    #    get_ratio(transposed_dataset[type], False)
    
    # GRID
    axes = ax1.get_axes()
    makeGrid(ax1)
    axes.set_ylim(0.01, 1000000)        

    
    # Y-AXIS
    ax1.set_ylabel("Recovery Latency (ms)", fontproperties=LABEL_FP)
    ax1.set_yscale('log', nonposy='clip')
    ax1.yaxis.set_major_locator(LogLocator(base = 100.0))
    ax1.minorticks_off()
    ax1.tick_params(axis='x', which='both', bottom='off', top='off')
        
    # X-AXIS
    ax1.set_xlabel("Number of transactions", fontproperties=LABEL_FP)
    ax1.set_xticklabels(x_labels)
    ax1.set_xticks(ind + 0.5)

    for label in ax1.get_yticklabels() :
        label.set_fontproperties(TICK_FP)
    for label in ax1.get_xticklabels() :
        label.set_fontproperties(TICK_FP)
        
    return (fig)

def create_btree_line_chart(datasets, sy):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
         
    # X-AXIS
    btree_node_sizes = BTREE_SIZES
    if sy in ["wal", "opt_wal", "lsm", "opt_lsm"]:
        btree_node_sizes = ["64", "128", "256", "512", "1024", "2048"]
        x_axis_min = math.pow(2, 5.75)
        x_axis_max =  math.pow(2, 11.25)        
    else:   
        btree_node_sizes = ["512", "1024", "2048", "4096", "8192", "16384"]
        x_axis_min = math.pow(2, 8.75)
        x_axis_max =  math.pow(2, 14.25)        

    x_values = btree_node_sizes
    N = len(x_values)
    x_labels = x_values

    num_items = len(ENGINES);   
    ind = np.arange(N)  
    idx = 0
    
    YLIMIT = 2200000
            
    # GROUP
    for group in YCSB_WORKLOAD_MIX:
        perf_data = []             
        
        # LINE
        for line in btree_node_sizes:
            #print(datasets[group][line][0])
            
            for col in  xrange(len(datasets[group][line][0][0])):
                if col == 1:
                    perf_data.append(datasets[group][line][0][0][1])
  
        LOG.info("%s perf_data = %s ", group, str(perf_data))
        
        ax1.plot(x_values, perf_data, color=OPT_LINE_COLORS[idx], linewidth=OPT_LINE_WIDTH, 
                 marker=OPT_MARKERS[idx], markersize=OPT_MARKER_SIZE, label=str(group))        
        
        idx = idx + 1  

    # GRID
    axes = ax1.get_axes()
    makeGrid(ax1)
      
    # Y-AXIS    
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    ax1.minorticks_on()
    ax1.set_ylabel("Throughput (txn/sec)", fontproperties=LABEL_FP)
    ax1.set_ylim([0, YLIMIT])
        
    # X-AXIS
    ax1.minorticks_on()
    #ax1.set_xticklabels(x_labels)    
    ax1.set_xticks(ind + 0.5)              
    ax1.set_xscale('log', basex=2)
    ax1.set_xlabel("B+tree node size (bytes)", fontproperties=LABEL_FP)
    #ax1.tick_params(axis='x', which='both', bottom='off', top='off')
    ax1.set_xlim([x_axis_min, x_axis_max])

    # LEGEND
    #legend = ax1.legend(loc='lower left', ncol=2, mode="expand", bbox_to_anchor=(0., 1.0, 1, 1))
        
    for label in ax1.get_yticklabels() :
        label.set_fontproperties(TICK_FP)
    for label in ax1.get_xticklabels() :
        label.set_fontproperties(TICK_FP)
            
    return (fig)

def create_ise_line_chart(datasets, sy):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
         
    x_values = ("1", "2", "3", "4", "5")
    N = len(x_values)
    x_labels = x_values

    num_items = len(ENGINES);   
    ind = np.arange(N)  
    idx = 0
    
    YLIMIT = 1800000
            
    # GROUP
    for group in PCOMMIT_WORKLOAD_MIX:
        perf_data = []             
        idx = idx + 1  

        # LINE
        for line in PCOMMIT_LATENCIES:
            print(datasets[group][line][0])
            
            for col in  xrange(len(datasets[group][line][0][0])):
                if col == 1:
                    perf_data.append(datasets[group][line][0][0][1])
  
        LOG.info("%s perf_data = %s ", group, str(perf_data))
        
        ax1.plot(x_values, perf_data, color=OPT_LINE_COLORS[idx], linewidth=OPT_LINE_WIDTH, 
                 marker=OPT_MARKERS[idx], markersize=OPT_MARKER_SIZE, label=str(group))        
        
    # GRID
    axes = ax1.get_axes()
    makeGrid(ax1)
      
    # Y-AXIS    
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    ax1.set_ylabel("Throughput (txn/sec)", fontproperties=LABEL_FP)
    ax1.set_ylim([0, YLIMIT])
        
    # X-AXIS
    #ax1.minorticks_on()
    ax1.set_xlim(0.75, 5.25)
    ax1.set_xticklabels(("Current", "10", "100", "1000", "10000"))    
    ax1.set_xticks(ind + 1)           
    ax1.set_xlabel("Sync primitive latency (ns)", fontproperties=LABEL_FP)
    ax1.tick_params(axis='x', which='both', bottom='off', top='off')
    
    # LEGEND
    #legend = ax1.legend(loc='lower left', ncol=2, mode="expand", bbox_to_anchor=(0., 1.0, 1, 1))
        
    for label in ax1.get_yticklabels() :
        label.set_fontproperties(TICK_FP)
    for label in ax1.get_xticklabels() :
        label.set_fontproperties(TICK_FP)
            
    return (fig)


###################################################################################                   
# PLOT HELPERS                  
###################################################################################                   


# YCSB PERF -- PLOT
def ycsb_perf_plot(result_dir, latency_list, prefix):
    for workload in YCSB_WORKLOAD_MIX:    

        for lat in latency_list:
            datasets = []
        
            for sy in SYSTEMS:    
                dataFile = loadDataFile(2, 2, os.path.realpath(os.path.join(result_dir, sy + "/" + workload + "/" + lat + "/performance.csv")))
                datasets.append(dataFile)
                                   
            fig = create_ycsb_perf_bar_chart(datasets)
            
            fileName = prefix + "ycsb-perf-%s-%s.pdf" % (workload, lat)
            saveGraph(fig, fileName, width= OPT_GRAPH_WIDTH, height=OPT_GRAPH_HEIGHT/1.5)

# YCSB STACK -- PLOT
def ycsb_stack_plot():
    for workload in YCSB_WORKLOAD_MIX:    

        for lat in YCSB_STACK_LATENCIES:
            datasets = []
            abs_time = []
        
            for sy in SYSTEMS:    
                dataFile = loadDataFile(1, 4, os.path.realpath(os.path.join(YCSB_STACK_DIR, sy + "/" + workload + "/" + lat + "/stack.csv")))
                datasets.append(dataFile)

                dataFile = loadDataFile(2, 2, os.path.realpath(os.path.join(YCSB_PERF_DIR, sy + "/" + workload + "/" + lat + "/performance.csv")))                
                abs_time.append(dataFile)        
                                   
            fig = create_ycsb_stack_bar_chart(datasets, abs_time)
            
            fileName = "ycsb-stack-%s-%s.pdf" % (workload, lat)
            saveGraph(fig, fileName, width= OPT_GRAPH_WIDTH/2.0, height=OPT_GRAPH_HEIGHT/2.0)
                   
# YCSB STORAGE -- PLOT               
def ycsb_storage_plot():    
    datasets = []

    dataFile = loadDataFile(6, 6, os.path.realpath(os.path.join(YCSB_STORAGE_DIR, "storage.csv")))
    datasets = dataFile
                                      
    fig = create_ycsb_storage_bar_chart(datasets)
    fileName = "ycsb-storage.pdf"
    saveGraph(fig, fileName, width= OPT_GRAPH_WIDTH/2.0, height=OPT_GRAPH_HEIGHT/2.0)

# YCSB NVM -- PLOT               
def ycsb_nvm_plot():    
    for workload in YCSB_WORKLOAD_MIX: 
        LOG.info("Workload : %s ", workload)
        datasets = []   
    
        for sy in SYSTEMS:    
            dataFile = loadDataFile(2, 3, os.path.realpath(os.path.join(YCSB_NVM_DIR, sy + "/" + workload + "/nvm.csv")))
            datasets.append(dataFile)            
                       
        # LOADS                       
        fig = create_ycsb_nvm_bar_chart(datasets, 0)                         
        fileName = "ycsb-nvm-loads-%s.pdf" % (workload)
        saveGraph(fig, fileName, width= OPT_GRAPH_WIDTH, height=OPT_GRAPH_HEIGHT/1.5)

        # STORES                       create_ycsb_nvm_bar_chart
        fig = create_ycsb_nvm_bar_chart(datasets, 1)                         
        fileName = "ycsb-nvm-stores-%s.pdf" % (workload)
        saveGraph(fig, fileName, width= OPT_GRAPH_WIDTH, height=OPT_GRAPH_HEIGHT/1.5)

# YCSB RECOVERY -- PLOT
def  ycsb_recovery_plot():   
    for txn in YCSB_RECOVERY_TXNS:    
        datasets = []
    
        for sy in RECOVERY_SYSTEMS:    
            dataFile = loadDataFile(3, 2, os.path.realpath(os.path.join(YCSB_RECOVERY_DIR, sy + "/recovery.csv")))
            datasets.append(dataFile)
                                      
    fig = create_ycsb_recovery_bar_chart(datasets)
                        
    fileName = "ycsb-recovery.pdf"
    saveGraph(fig, fileName, width=OPT_GRAPH_WIDTH, height=OPT_GRAPH_HEIGHT/1.5) 

# TPCC PERF -- PLOT
def tpcc_perf_plot():

    datasets = []   

    for sy in SYSTEMS:    
        dataFile = loadDataFile(3, 2, os.path.realpath(os.path.join(TPCC_PERF_DIR, sy + "/performance.csv")))
        datasets.append(dataFile)
                       
    fig = create_tpcc_perf_bar_chart(datasets)
                
    fileName = "tpcc-perf.pdf"
    saveGraph(fig, fileName, width=len(LATENCIES)*OPT_GRAPH_WIDTH/1.5, height=OPT_GRAPH_HEIGHT)

# TPCC STORAGE -- PLOT               
def tpcc_storage_plot():    
    datasets = []

    dataFile = loadDataFile(6, 6, os.path.realpath(os.path.join(TPCC_STORAGE_DIR, "storage.csv")))
    datasets = dataFile
                                      
    fig = create_tpcc_storage_bar_chart(datasets)
                        
    fileName = "tpcc-storage.pdf"
    saveGraph(fig, fileName, width=OPT_GRAPH_WIDTH/2.0, height=OPT_GRAPH_HEIGHT/2.0) 

# TPCC NVM -- PLOT               
def tpcc_nvm_plot():    
    datasets = []
    
    dataFile = loadDataFile(6, 3, os.path.realpath(os.path.join(TPCC_NVM_DIR, "nvm.csv")))
    datasets = dataFile

    # LOADS                                                               
    fig = create_tpcc_nvm_bar_chart(datasets,0)                        
    fileName = "tpcc-nvm-loads.pdf"
    saveGraph(fig, fileName, width=OPT_GRAPH_WIDTH/2.0, height=OPT_GRAPH_HEIGHT/2) 

    # STORES                                                               
    fig = create_tpcc_nvm_bar_chart(datasets,1)                        
    fileName = "tpcc-nvm-stores.pdf"
    saveGraph(fig, fileName, width=OPT_GRAPH_WIDTH/2.0, height=OPT_GRAPH_HEIGHT/2) 

# TPCC RECOVERY -- PLOT
def  tpcc_recovery_plot():   
    for txn in TPCC_RECOVERY_TXNS:    
        datasets = []
    
        for sy in RECOVERY_SYSTEMS:    
            dataFile = loadDataFile(3, 2, os.path.realpath(os.path.join(TPCC_RECOVERY_DIR, sy + "/recovery.csv")))
            datasets.append(dataFile)
                                      
    fig = create_tpcc_recovery_bar_chart(datasets)
                        
    fileName = "tpcc-recovery.pdf"
    saveGraph(fig, fileName, width=OPT_GRAPH_WIDTH, height=OPT_GRAPH_HEIGHT/1.5) 


# NVM SYNC LANTENCY -- PLOT
def nvm_bw_plot():
    RANDOM_ACCESS = ["sequential", "random"]

    for access in RANDOM_ACCESS:            
        datasets = loadDataFile(9, 3, os.path.realpath(os.path.join(NVM_BW_DIR, access + "/bw.csv")))
                           
        fig = create_nvm_bw_chart(datasets)
    
        fileName = "nvm-bw-%s.pdf" % (access)
        saveGraph(fig, fileName, width= OPT_GRAPH_WIDTH, height=OPT_GRAPH_HEIGHT)


# BTREE -- PLOT
def btree_plot(log_name):

    # LOG RESULTS
    log_file = open(log_name, 'w')
    log_file.write('Start :: %s \n' % datetime.datetime.now())

    latency_list = MISC_LATENCIES
    result_dir = BTREE_DIR
    
    # Go over all engines
    for sy in SYSTEMS:    
        
        for lat in latency_list:
            datasets = {}

            for workload in YCSB_WORKLOAD_MIX:    
                datasets[workload] = {}
                
                for btree_size in BTREE_SIZES:    
                    datasets[workload][btree_size] = []

                    # Get results in relevant subdir            
                    dataFile = loadDataFile(2, 2, os.path.realpath(os.path.join(result_dir, btree_size  + "/" + sy + "/" + workload + "/" + lat + "/performance.csv")))
                    datasets[workload][btree_size].append(dataFile)
        
            print(datasets)                                        
            fig = create_btree_line_chart(datasets, sy)
                        
            fileName = "btree-%s-%s.pdf" % (sy, lat)
            saveGraph(fig, fileName, width= OPT_GRAPH_WIDTH, height=OPT_GRAPH_HEIGHT/1.5)
            
            
# ISE -- PLOT
def ise_plot(log_name):

    # LOG RESULTS
    log_file = open(log_name, 'w')
    log_file.write('Start :: %s \n' % datetime.datetime.now())

    latency_list = MISC_LATENCIES
    result_dir = ISE_DIR
        
    # Go over all engines
    for sy in SYSTEMS:    
         
        for lat in latency_list:
            datasets = {}
 
            for workload in PCOMMIT_WORKLOAD_MIX:    
                datasets[workload] = {}
                 
                for pcommit_latency in PCOMMIT_LATENCIES:    
                    datasets[workload][pcommit_latency] = []
 
                    # Get results in relevant subdir            
                    dataFile = loadDataFile(2, 2, os.path.realpath(os.path.join(result_dir, pcommit_latency  + "/" + sy + "/" + workload + "/" + lat + "/performance.csv")))
                    datasets[workload][pcommit_latency].append(dataFile)
         
            print(datasets)                                        
            fig = create_ise_line_chart(datasets, sy)
                         
            fileName = "ise-%s-%s.pdf" % (sy, lat)
            saveGraph(fig, fileName, width= OPT_GRAPH_WIDTH, height=OPT_GRAPH_HEIGHT/1.5)            
                        
           
###################################################################################                   
# EVAL                   
###################################################################################                   

# CLEANUP PMFS

SDV_DEVEL = "/data/devel/sdv-tools/"
FS_ABS_PATH = "/mnt/pmfs/"

def cleanup(log_file):
    # LOCAL
    if enable_local:        
        subprocess.call(["rm -f " + FS_PATH + "./*"], shell=True)    
           
    # PMFS            
    else:
        cwd = os.getcwd()
        os.chdir(SDV_DEVEL)
        subprocess.call(['sudo', 'umount', '/mnt/pmfs'], stdout=log_file)
        subprocess.call(['sudo', 'bash', 'mount_pmfs.sh'], stdout=log_file)        
        os.chdir(FS_ABS_PATH)
        subprocess.call(['sudo', 'mkdir', 'n-store'], stdout=log_file)
        subprocess.call(['sudo', 'chown', 'user', 'n-store'], stdout=log_file)
        os.chdir(cwd)    


# YCSB PERF -- EVAL
def ycsb_perf_eval(enable_sdv, enable_trials, log_name, result_dir, latency_list):        
    dram_latency = 100
    keys = YCSB_KEYS
    txns = YCSB_TXNS
                            
    num_trials = 1 
    if enable_trials: 
        num_trials = 3
    
    nvm_latencies = latency_list
    rw_mixes = YCSB_RW_MIXES
    skew_factors = YCSB_SKEW_FACTORS
    engines = ENGINES
    
    # LOG RESULTS
    log_file = open(log_name, 'w')
    log_file.write('Start :: %s \n' % datetime.datetime.now())
    
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
                        cleanup(log_file)
                        subprocess.call([NUMACTL, NUMACTL_FLAGS, NSTORE, '-k', str(keys), '-x', str(txns), '-p', str(rw_mix), '-q', str(skew_factor), eng], stdout=log_file)

    # RESET
    if enable_sdv :
        cwd = os.getcwd()
        os.chdir(SDV_DIR)
        subprocess.call(['sudo', SDV_SCRIPT, '--enable', '--pm-latency', "200"], stdout=log_file)
        os.chdir(cwd)
 
    # PARSE LOG
    log_file.write('End :: %s \n' % datetime.datetime.now())
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
    subprocess.call(['rm', '-rf', result_dir])          
    
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
            workload_type = 'balanced'
        elif(key[0] == '0.9'):
            workload_type = 'write-heavy'
    
        nvm_latency = str(key[2]);
        
        result_directory = result_dir + engine_type + "/" + workload_type + "/" + nvm_latency + "/";
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


# YCSB STACK -- EVAL
def ycsb_stack_eval(log_name):     
    keys = YCSB_KEYS
    txns = YCSB_TXNS

    PERF_RECORD = "record"    
    PERF_REPORT = "report"  
    PERF_FILE = "perf.summary"  

    if enable_local:
        PERF_RECORD_FLAGS = "-e cpu-cycles:u"
        PERF = PERF_LOCAL
        NUMACTL_FLAGS = "--membind=0"
    else:
        PERF_RECORD_FLAGS = "-e instructions:u"
                            
    #nvm_latencies = LATENCIES
    nvm_latencies = YCSB_STACK_LATENCIES

    rw_mixes = YCSB_RW_MIXES
    skew_factors = YCSB_STACK_SKEW_FACTORS
    #skew_factors = YCSB_SKEW_FACTORS
    engines = ENGINES
    
    # CLEAN UP RESULT DIR
    subprocess.call(['rm', '-rf', YCSB_STACK_DIR])          

    recovery_dict = ['serialize','update','stream','file','libc','write','fp','fsync','0x','push_back']
    index_dict = ['select','btree', 'Hash']
    storage_dict = ['insert','load','delete', 'string', 'alloc', 'free', 'mem', 'cow_btree']

    def round_sigfigs(num, sig_figs):
        if num != 0:
            return round(num, -int(math.floor(math.log10(abs(num))) - (sig_figs - 1)))
        else:
            return 0  # Can't take the log of 0

    def parse_perf_log(log_file):        
        num_types = 3        
        per_list = [0] * num_types;
        # Types : Storage, Recovery, Index, Others

        perf_file = open(PERF_FILE, 'w')
        subprocess.call([PERF, PERF_REPORT], stdout=perf_file, stderr=perf_file)
        perf_file.close()
        
        perf_file = open(PERF_FILE, 'r')
                
        for line in perf_file:                    
            if "%" in line:
                label = line.strip().split('.]')[1];
                fraction = line.strip().split('.]')[0];
                entry = filter(None, fraction)                

                func = str(label)
                fraction = float(fraction.split('%')[0])
                                        
                if any(x in func for x in recovery_dict):
                    per_list[1] = per_list[1] + fraction
                    #print("ENTRY :: R :: " + str(fraction) + " :: " + func)
                elif any(x in func for x in index_dict):
                    per_list[2] = per_list[2] + fraction 
                    #print("ENTRY :: I :: " + str(fraction) + " :: " + func)
                elif any(x in func for x in storage_dict):
                    per_list[0] = per_list[0] + fraction
                    #print("ENTRY :: S :: " + str(fraction) + " :: " + func)
                #else:
                    #print("ENTRY :: X :: " + str(fraction) + " :: " + func)                               
                    

        for elem in range(0, num_types):
            per_list[elem] = round_sigfigs(per_list[elem], 2)
                        
        
        print("LIST :: ")
        pprint(per_list)
        
        perf_file.close()

        return per_list;

    def generate_report(etype, rw_mix, nvm_latency, skew_factor, log_file):        
        if(etype == "-a"):
            engine_type = "wal"                
        elif(etype == "-s"):
            engine_type = "sp"
        elif(etype == "-m"):
            engine_type = "lsm"
        elif(etype == "-w"):
            engine_type = "opt_wal"
        elif(etype == "-c"):
            engine_type = "opt_sp"
        elif(etype == "-l"):
            engine_type = "opt_lsm"
           
        if(rw_mix == 0.0):
            workload_type = 'read-only'
        elif(rw_mix == 0.1):
            workload_type = 'read-heavy'
        elif(rw_mix == 0.5):
            workload_type = 'balanced'
        elif(rw_mix == 0.9):
            workload_type = 'write-heavy'
            
        result_directory = YCSB_STACK_DIR + engine_type + "/" + workload_type + "/" + nvm_latency + "/";
        if not os.path.exists(result_directory):
            os.makedirs(result_directory)

        per_list = parse_perf_log(log_file);
        per_list_string =  ' , '.join(map(str, per_list)) 
        
        result_file_name = result_directory + "stack.csv"
        result_file = open(result_file_name, "a")
        result_file.write(str(skew_factor) + " , "  + per_list_string + "\n")
        result_file.close()            

    # LOG RESULTS
    log_file = open(log_name, 'w')
    log_file.write('Start :: %s \n' % datetime.datetime.now())
    
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
                    cleanup(log_file)

                    subprocess.call([PERF, PERF_RECORD, PERF_RECORD_FLAGS, NUMACTL, NUMACTL_FLAGS, 
                                     NSTORE, '-k', str(keys), '-x', str(txns), '-p', str(rw_mix), '-q', str(skew_factor), eng],
                                     stdout=log_file, stderr=log_file)

                    generate_report(eng, rw_mix, nvm_latency, skew_factor, log_file)                                        

    # RESET
    if enable_sdv :
        cwd = os.getcwd()
        os.chdir(SDV_DIR)
        subprocess.call(['sudo', SDV_SCRIPT, '--enable', '--pm-latency', "200"], stdout=log_file)
        os.chdir(cwd)
 
    log_file.write('End :: %s \n' % datetime.datetime.now())
    log_file.close()   
                                     
     
# YCSB STORAGE -- EVAL
def ycsb_storage_eval(log_name):            
    subprocess.call(['rm', '-rf', YCSB_STORAGE_DIR])          
    keys = YCSB_KEYS 
    txns = YCSB_TXNS
            
    # GET STATS
    def get_stats(engine_type):                
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
            e_type = 0                
        elif(engine_type == "-s"):
            e_type = 1
        elif(engine_type == "-m"):
            e_type = 2
        elif(engine_type == "-w"):
            e_type = 3
        elif(engine_type == "-c"):
            e_type = 4
        elif(engine_type == "-l"):
            e_type = 5
          
        result_directory = YCSB_STORAGE_DIR ;
        if not os.path.exists(result_directory):
            os.makedirs(result_directory)

        result_file_name = result_directory + "storage.csv"
        result_file = open(result_file_name, "a")
        print(str(engine_type) + " , " + str(fs_st) + " , " + str(pm_st))
        result_file.write(str(e_type) + " , " + str(fs_st) + " , " + str(pm_st) + "\n")
        result_file.close()    

        
    engines = ENGINES   

    # LOG RESULTS
    log_file = open(log_name, 'w')
    log_file.write('Start :: %s \n' % datetime.datetime.now())
                   
    for eng in engines:
        cleanup(log_file)
        subprocess.call([NUMACTL, NUMACTL_FLAGS, NSTORE, '-k', str(keys), '-x', str(txns), '-p', '0.5', '-q', '0.1', '-z', eng], stdout=log_file)
        get_stats(eng)


# YCSB NVM -- EVAL
def ycsb_nvm_eval(log_name):            
    subprocess.call(['rm', '-rf', YCSB_NVM_DIR])          
    keys = YCSB_KEYS 
    txns = YCSB_TXNS

    PERF_STAT = "stat"    
    PERF_STAT_FLAGS = "-e LLC-load-misses,LLC-store-misses"

    rw_mixes = YCSB_RW_MIXES
    skew_factors = YCSB_SKEW_FACTORS
    engines = ENGINES   

    # LOG RESULTS
    log_file = open(log_name, 'w')
    log_file.write('Start :: %s \n' % datetime.datetime.now())

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
                cleanup(log_file)
                subprocess.call([PERF, PERF_STAT, PERF_STAT_FLAGS, NUMACTL, NUMACTL_FLAGS, NSTORE,
                                 '-k', str(keys), '-x', str(txns), '-p', str(rw_mix), '-q', str(skew_factor), eng],
                                stdout=log_file, stderr=log_file)

                subprocess.call([PERF, PERF_STAT, PERF_STAT_FLAGS, NUMACTL, NUMACTL_FLAGS, NSTORE,
                                 '-k', str(keys), '-x', '0', '-p', str(rw_mix), '-q', str(skew_factor), eng],
                                stdout=log_file, stderr=log_file)
                              
    log_file.close()   
    log_file = open(log_name, "r")    

    # CLEAN UP RESULT DIR
    subprocess.call(['rm', '-rf', YCSB_NVM_DIR])          
 
    rw_mix = 0.0
    skew = 0.0    
    llc_l_miss = -1
    llc_s_miss = -1

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
                workload_type = 'balanced'  
            elif(rw_mix == '0.9'):
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
            elif llc_l_miss == -1:    
                llc_l_miss = str(entry[0])            
                llc_l_miss = llc_l_miss.replace(",", "")    
            else:
                llc_l_miss_only_load = str(entry[0]) 
                llc_l_miss_only_load = llc_l_miss_only_load.replace(",", "")    

                llc_l_miss = max(0.0,float(llc_l_miss) - float(llc_l_miss_only_load))     
                
                print(engine_type + ", " + str(rw_mix) + " , " + str(skew) + " :: " + str(llc_l_miss))


        if "LLC-store-misses" in line:
            entry = line.strip().split(' ');
            if(entry[0] == '<not'):
                llc_s_miss = "0"
            elif llc_s_miss == -1:    
                llc_s_miss = str(entry[0])            
                llc_s_miss = llc_s_miss.replace(",", "")    
            else:
                llc_s_miss_only_load = str(entry[0]) 
                llc_s_miss_only_load = llc_s_miss_only_load.replace(",", "")    

                llc_s_miss = max(0.0,float(llc_s_miss) - float(llc_s_miss_only_load))     
                
                print(engine_type + ", " + str(rw_mix) + " , " + str(skew) + " :: " + str(llc_s_miss))
                                                                                   
                result_directory = YCSB_NVM_DIR + engine_type + "/" + workload_type + "/";
                if not os.path.exists(result_directory):
                    os.makedirs(result_directory)
    
                result_file_name = result_directory + "nvm.csv"
                result_file = open(result_file_name, "a")
                result_file.write(str(skew) + " , " + str(llc_l_miss) + " , " + str(llc_s_miss) + "\n")
                result_file.close()    
                
                llc_l_miss = -1
                llc_s_miss = -1


# YCSB RECOVERY -- EVAL
def ycsb_recovery_eval(log_name):            
    subprocess.call(['rm', '-rf', YCSB_RECOVERY_DIR])          
    
    txns = YCSB_RECOVERY_TXNS
    engines = ENGINES   

    # LOG RESULTS
    log_file = open(log_name, 'w')
               
    # RW MIX
    for txn  in txns:
        ostr = ("--------------------------------------------------- \n")
        print (ostr, end="")
        log_file.write(ostr)
        ostr = ("TXN :: %.lf \n" % (txn))
        print (ostr, end="")
        log_file.write(ostr)                    
        log_file.flush()

        for eng in engines:
            cleanup(log_file)
            
            subprocess.call([NUMACTL, NUMACTL_FLAGS, NSTORE, '-x', str(txn), '-y', '-r', eng],
                            stdout=log_file, stderr=log_file)
                                  
    log_file.close()   
    log_file = open(log_name, "r")    

    # CLEAN UP RESULT DIR
    subprocess.call(['rm', '-rf', YCSB_RECOVERY_DIR])          
 
    txn = 0.0
    
    for line in log_file:                    
        if "TXN" in line:
            entry = line.strip().split(' ');
            txn = entry[2]
     
        if "Recovery" in line:
            entry = line.strip().split(' ');
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
      
            duration = str(entry[6])
                
            print(engine_type + ", " + str(txn) + " :: " + str(duration))
                                                                
            result_directory = YCSB_RECOVERY_DIR + engine_type + "/";
            if not os.path.exists(result_directory):
                os.makedirs(result_directory)

            result_file_name = result_directory + "recovery.csv"
            result_file = open(result_file_name, "a")
            result_file.write(str(txn) + " , " + str(duration) + "\n")
            result_file.close()    


# TPCC PERF -- EVAL
def tpcc_perf_eval(enable_sdv, enable_trials, log_name):        
    dram_latency = 100
    txns = TPCC_TXNS
                        
    num_trials = 1 
    if enable_trials: 
        num_trials = 3
    
    nvm_latencies = LATENCIES
    engines = ENGINES

    # LOG RESULTS
    log_file = open(log_name, 'w')
    log_file.write('Start :: %s \n' % datetime.datetime.now())
 
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
            
            for eng in engines:
                cleanup(log_file)

                subprocess.call([NUMACTL, NUMACTL_FLAGS, NSTORE, '-x', str(txns), '-t', eng], stdout=log_file)

    # RESET
    if enable_sdv :
        cwd = os.getcwd()
        os.chdir(SDV_DIR)
        subprocess.call(['sudo', SDV_SCRIPT, '--enable', '--pm-latency', "200"], stdout=log_file)
        os.chdir(cwd)

    # PARSE LOG
    log_file.write('End :: %s \n' % datetime.datetime.now())
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
                                       
            key = (latency, engine_type[0]);
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
        
        nvm_latency = str(key[0]);
        engine_type = str(key[1]);            
              
        result_directory = TPCC_PERF_DIR + engine_type + "/";
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
            
    # GET STATS
    def get_stats(engine_type):
        print ("eng : %s " % (engine_type))
                
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
            e_type = 0                
        elif(engine_type == "-s"):
            e_type = 1
        elif(engine_type == "-m"):
            e_type = 2
        elif(engine_type == "-w"):
            e_type = 3
        elif(engine_type == "-c"):
            e_type = 4
        elif(engine_type == "-l"):
            e_type = 5
                  
        result_directory = TPCC_STORAGE_DIR ;
        if not os.path.exists(result_directory):
            os.makedirs(result_directory)

        result_file_name = result_directory + "storage.csv"
        result_file = open(result_file_name, "a")
        print(str(engine_type) + " , " + str(fs_st) + " , " + str(pm_st))
        result_file.write(str(e_type) + " , " + str(fs_st) + " , " + str(pm_st) + "\n")
        result_file.close()    

        
    engines = ENGINES   

    # LOG RESULTS
    log_file = open(log_name, 'w')
    log_file.write('Start :: %s \n' % datetime.datetime.now())
                   

    for eng in engines:
        cleanup(log_file)        
        subprocess.call([NUMACTL, NUMACTL_FLAGS, NSTORE, '-x', str(txns), '-t', '-z', eng], stdout=log_file)
            
        get_stats(eng)


# TPCC NVM -- EVAL
def tpcc_nvm_eval(log_name):            
    subprocess.call(['rm', '-rf', TPCC_NVM_DIR])          
    txns = TPCC_TXNS

    PERF_STAT = "stat"    
    PERF_STAT_FLAGS = "-e LLC-load-misses,LLC-store-misses"

    engines = ENGINES   

    # LOG RESULTS
    log_file = open(log_name, 'w')
    log_file.write('Start :: %s \n' % datetime.datetime.now())
               

    for eng in engines:
        cleanup(log_file)
        
        subprocess.call([NUMACTL, NUMACTL_FLAGS, PERF, PERF_STAT, PERF_STAT_FLAGS, NSTORE, '-x', str(txns), '-t', eng],
                    stdout=log_file, stderr=log_file)
        
        subprocess.call([NUMACTL, NUMACTL_FLAGS, PERF, PERF_STAT, PERF_STAT_FLAGS, NSTORE, '-x', '0', '-t', eng],
                    stdout=log_file, stderr=log_file)
                                 
    log_file.close()   
    log_file = open(log_name, "r")    

    # CLEAN UP RESULT DIR
    subprocess.call(['rm', '-rf', TPCC_NVM_DIR])          
 
    latency = 0
    rw_mix = 0.0
    skew = 0.0    
    llc_l_miss = -1
    llc_s_miss = -1
    
    for line in log_file:                                           
        if "Throughput" in line:
            entry = line.strip().split(':');
            etypes = entry[0].split(' ');
            
            if(etypes[0] == "WAL"):
                engine_type = 0                
            elif(etypes[0] == "SP"):
                engine_type = 1
            elif(etypes[0] == "LSM"):
                engine_type = 2
            elif(etypes[0] == "OPT_WAL"):
                engine_type = 3
            elif(etypes[0] == "OPT_SP"):
                engine_type = 4
            elif(etypes[0] == "OPT_LSM"):
                engine_type = 5

        if "LLC-load-misses" in line:
            entry = line.strip().split(' ');
            if(entry[0] == '<not'):
                llc_l_miss = "0"
            elif llc_l_miss == -1:    
                llc_l_miss = str(entry[0])            
                llc_l_miss = llc_l_miss.replace(",", "")    
            else:
                llc_l_miss_only_load = str(entry[0]) 
                llc_l_miss_only_load = llc_l_miss_only_load.replace(",", "")    

                llc_l_miss = max(0.0, float(llc_l_miss) - float(llc_l_miss_only_load))     
                
                print(str(engine_type) + ", " + str(rw_mix) + " , " + str(skew) + " :: " + str(llc_l_miss))


        if "LLC-store-misses" in line:
            entry = line.strip().split(' ');
            if(entry[0] == '<not'):
                llc_s_miss = "0"
            elif llc_s_miss == -1:    
                llc_s_miss = str(entry[0])            
                llc_s_miss = llc_s_miss.replace(",", "")    
            else:
                llc_s_miss_only_load = str(entry[0]) 
                llc_s_miss_only_load = llc_s_miss_only_load.replace(",", "")    

                llc_s_miss = max(0.0, float(llc_s_miss) - float(llc_s_miss_only_load))     
                
                print(str(engine_type) + ", " + str(rw_mix) + " , " + str(skew) + " :: " + str(llc_s_miss))
                                                                                   
                result_directory = TPCC_NVM_DIR;
                if not os.path.exists(result_directory):
                    os.makedirs(result_directory)
                    
                result_file_name = result_directory + "nvm.csv"
                result_file = open(result_file_name, "a")
                result_file.write(str(engine_type) + " , " + str(llc_l_miss) + " , " + str(llc_s_miss) + "\n")
                result_file.close()    
    
                llc_l_miss = -1
                llc_s_miss = -1
     

# TPCC RECOVERY -- EVAL
def tpcc_recovery_eval(log_name):            
    subprocess.call(['rm', '-rf', TPCC_RECOVERY_DIR])          
    
    txns = TPCC_RECOVERY_TXNS
    engines = ENGINES   

    # LOG RESULTS
    log_file = open(log_name, 'w')
               
    # RW MIX
    for txn  in txns:
        ostr = ("--------------------------------------------------- \n")
        print (ostr, end="")
        log_file.write(ostr)
        ostr = ("TXN :: %.lf \n" % (txn))
        print (ostr, end="")
        log_file.write(ostr)                    
        log_file.flush()

        for eng in engines:
            cleanup(log_file)
            
            subprocess.call([NUMACTL, NUMACTL_FLAGS, NSTORE, '-x', str(txn), '-t', '-r', eng],
                            stdout=log_file, stderr=log_file)
                                  
    log_file.close()   
    log_file = open(log_name, "r")    

    # CLEAN UP RESULT DIR
    subprocess.call(['rm', '-rf', TPCC_RECOVERY_DIR])          
 
    txn = 0.0
    
    for line in log_file:                    
        if "TXN" in line:
            entry = line.strip().split(' ');
            txn = entry[2]
                                               
        if "Recovery" in line:
            entry = line.strip().split(' ');
            duration = str(entry[6])
 
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
                
            print(engine_type + ", " + str(txn) + " :: " + str(duration))
                                                                
            result_directory = TPCC_RECOVERY_DIR + engine_type + "/";
            if not os.path.exists(result_directory):
                os.makedirs(result_directory)

            result_file_name = result_directory + "recovery.csv"
            result_file = open(result_file_name, "a")
            result_file.write(str(txn) + " , " + str(duration) + "\n")
            result_file.close()    
                          

# TEST NVM -- EVAL
def test_nvm_eval(log_name):            
    subprocess.call(['rm', '-rf', TEST_NVM_DIR])          
    txns = TEST_TXNS

    #if enable_local:
    #    PERF = PERF_LOCAL
    #    NUMACTL_FLAGS = "--membind=0"

    PERF_STAT = "stat"    
    PERF_STAT_FLAGS = "-e LLC-stores,LLC-store-misses"

    engines = ENGINES   

    # LOG RESULTS
    log_file = open(log_name, 'w')
    log_file.write('Start :: %s \n' % datetime.datetime.now())
               
    # 3 modes
    for mode in range(1, 2):
        for eng in engines:
    
            print("Mode : " + str(mode) + " Engine : " + str(eng));

            cleanup(log_file)
            
            subprocess.call([NUMACTL, NUMACTL_FLAGS, PERF, PERF_STAT, PERF_STAT_FLAGS, NSTORE, '-k', str(txns), '-x', str(txns), '-j', str(mode), '-d', eng],
                        stdout=log_file, stderr=log_file)
                                         
            subprocess.call([NUMACTL, NUMACTL_FLAGS, PERF, PERF_STAT, PERF_STAT_FLAGS, NSTORE, '-k', str(txns), '-x', '0', '-j', str(mode), '-d', eng],
                        stdout=log_file, stderr=log_file)
    
    log_file.close()   
    log_file = open(log_name, "r")    

    # CLEAN UP RESULT DIR
    subprocess.call(['rm', '-rf', TEST_NVM_DIR])          
 
    latency = 0
    rw_mix = 0.0
    skew = 0.0    
    llc_l_miss = -1
    llc_s_miss = -1
    llc_l = -1
    llc_s = -1
    
    for line in log_file:                                           
        if "Throughput" in line:
            entry = line.strip().split(':');
            etypes = entry[0].split(' ');
            engine_name = etypes[0]
            
            if(etypes[0] == "WAL"):
                engine_type = 0                
            elif(etypes[0] == "SP"):
                engine_type = 1
            elif(etypes[0] == "LSM"):
                engine_type = 2
            elif(etypes[0] == "OPT_WAL"):
                engine_type = 3
            elif(etypes[0] == "OPT_SP"):
                engine_type = 4
            elif(etypes[0] == "OPT_LSM"):
                engine_type = 5
 
        if "TYPE" in line:
            entry = line.strip().split(' ');
            optype = entry[2]

        if "LLC-stores" in line:
            entry = line.strip().split(' ');
            if(entry[0] == '<not'):
                llc_s = "0"
            elif llc_s == -1:    
                llc_s = str(entry[0])            
                llc_s = llc_s.replace(",", "")    
            else:
                llc_s_only_load = str(entry[0]) 
                llc_s_only_load = llc_s_only_load.replace(",", "")    

                llc_s = max(0.0, float(llc_s) - float(llc_s_only_load))     
                
                #print(str(engine_type) + " :: " + str(llc_s))
 
        if "LLC-store-misses" in line:
            entry = line.strip().split(' ');
            if(entry[0] == '<not'):
                llc_s_miss = "0"
            elif llc_s_miss == -1:    
                llc_s_miss = str(entry[0])            
                llc_s_miss = llc_s_miss.replace(",", "")    
            else:
                llc_s_miss_only_load = str(entry[0]) 
                llc_s_miss_only_load = llc_s_miss_only_load.replace(",", "")    

                llc_s_miss = max(0.0, float(llc_s_miss) - float(llc_s_miss_only_load))     
                llc_s_miss += llc_s

                print(str(engine_type) + " :: " + str(llc_s_miss))
                                                                                   
                result_directory = TEST_NVM_DIR;
                if not os.path.exists(result_directory):
                    os.makedirs(result_directory)
                    
                result_file_name = result_directory + "nvm.csv"
                result_file = open(result_file_name, "a")
                result_file.write(str(engine_type) + " , " + str(optype) +  " , " + str(llc_s_miss) + "\n")
                result_file.close()    
    
                llc_l_miss = -1
                llc_s_miss = -1
                llc_l = -1
                llc_s = -1
                


# BTREE -- EVAL
def btree_eval(log_name):            
    subprocess.call(['rm', '-rf', BTREE_DIR])          

    engines = ENGINES   

    # LOG RESULTS
    log_file = open(log_name, 'w')
    log_file.write('Start :: %s \n' % datetime.datetime.now())
    itr_count = 0;

    # Go over all sizes
    for btree_size in BTREE_SIZES:    
        print("Size : " + btree_size);
                
        # Update header files
        if itr_count == 0:
            btree_cmd = 's/BTREE_NODE_SIZE ' + BTREE_NODE_SIZE_DEFAULT +  "/BTREE_NODE_SIZE " + BTREE_SIZES[0] + "/g";    
            cow_btree_cmd = 's/PAGESIZE ' + COW_BTREE_NODE_SIZE_DEFAULT +  "/PAGESIZE " + BTREE_SIZES[0] + "/g";        
        else:
            btree_cmd = 's/BTREE_NODE_SIZE ' + BTREE_SIZES[itr_count-1] +  "/BTREE_NODE_SIZE " + BTREE_SIZES[itr_count] + "/g";    
            cow_btree_cmd = 's/PAGESIZE ' + BTREE_SIZES[itr_count-1] +  "/PAGESIZE " + BTREE_SIZES[itr_count] + "/g";        

        print(btree_cmd)
        print(cow_btree_cmd)

        subprocess.call(['sed', '-i', btree_cmd, BTREE_HEADER_FILE], stdout=log_file)
        subprocess.call(['sed', '-i', cow_btree_cmd, COW_BTREE_HEADER_FILE], stdout=log_file)            
        
        # Build
        subprocess.call(['make', '-j'], stdout=log_file)
                
        # Store results in relevant subdir
        btree_subdir = BTREE_DIR + btree_size + "/";

        ycsb_perf_eval(False, False, log_name, btree_subdir, MISC_LATENCIES)      
        
        # Next iteration
        itr_count += 1;


    # Reset header files
    if itr_count != 0:
            btree_cmd = 's/BTREE_NODE_SIZE ' + BTREE_SIZES[itr_count-1] +  "/BTREE_NODE_SIZE " + BTREE_NODE_SIZE_DEFAULT + "/g";    
            cow_btree_cmd = 's/PAGESIZE ' + BTREE_SIZES[itr_count-1] +  "/PAGESIZE " + COW_BTREE_NODE_SIZE_DEFAULT + "/g";        

            print(btree_cmd)
            print(cow_btree_cmd)
            subprocess.call(['sed', '-i', btree_cmd, BTREE_HEADER_FILE], stdout=log_file)
            subprocess.call(['sed', '-i', cow_btree_cmd, COW_BTREE_HEADER_FILE], stdout=log_file)            

            # Build
            subprocess.call(['make', '-j'], stdout=log_file)

# ISE -- EVAL
def ise_eval(log_name):            
    subprocess.call(['rm', '-rf', ISE_DIR])          

    engines = ENGINES   

    # LOG RESULTS
    log_file = open(log_name, 'w')
    log_file.write('Start :: %s \n' % datetime.datetime.now())
    itr_count = 0;

    def get_ise_str(mode):
        if mode == "default":
            mode_str = "#undef ISE"
        else:
            mode_str = "#define ISE"            
        return mode_str

    # Go over all sizes
    for pcommit_latency in PCOMMIT_LATENCIES:    
        print("PCOMMIT latency : " + pcommit_latency);
        
        # Pick ISE mode
        if pcommit_latency == "1":
            mode = "default"
        else:
            mode = "ise"
                                
        mode_str = get_ise_str(mode)
                            
        # Update header file
        if itr_count == 0:
            clwb_cmd = 's/' + get_ise_str(ISE_MODE_DEFAULT) +  "/" + get_ise_str(ISE_MODES[0]) + "/g";                
            pcommit_cmd = 's/PCOMMIT_LATENCY ' + PCOMMIT_LATENCY_DEFAULT +  "/PCOMMIT_LATENCY " + PCOMMIT_LATENCIES[0] + "/g";    
        else:
            clwb_cmd = 's/' + get_ise_str(ISE_MODES[itr_count-1]) +  "/" + get_ise_str(ISE_MODES[itr_count]) + "/g";    
            pcommit_cmd = 's/PCOMMIT_LATENCY ' + PCOMMIT_LATENCIES[itr_count-1] +  "/PCOMMIT_LATENCY " + PCOMMIT_LATENCIES[itr_count] + "/g";    

        print(clwb_cmd)
        print(pcommit_cmd)

        subprocess.call(['sed', '-i', clwb_cmd, ISE_HEADER_FILE], stdout=log_file)
        subprocess.call(['sed', '-i', pcommit_cmd, ISE_HEADER_FILE], stdout=log_file)
        
        # Build
        subprocess.call(['make', '-j'], stdout=log_file)
                
        # Store results in relevant subdir
        ise_subdir = ISE_DIR + pcommit_latency + "/";

        ycsb_perf_eval(False, False, log_name, ise_subdir, MISC_LATENCIES)      
        
        # Next iteration
        itr_count += 1;


    # Reset header files
    if itr_count != 0:
            pcommit_cmd = 's/PCOMMIT_LATENCY ' + PCOMMIT_LATENCIES[itr_count-1] +  "/PCOMMIT_LATENCY " + PCOMMIT_LATENCY_DEFAULT + "/g";    
            clwb_cmd = 's/' + get_ise_str(ISE_MODES[itr_count-1]) +  "/" + get_ise_str(ISE_MODE_DEFAULT) + "/g";    
         
            print(clwb_cmd)
            print(pcommit_cmd)
            
            subprocess.call(['sed', '-i', clwb_cmd, ISE_HEADER_FILE], stdout=log_file)
            subprocess.call(['sed', '-i', pcommit_cmd, ISE_HEADER_FILE], stdout=log_file)
         
            # Build
            subprocess.call(['make', '-j'], stdout=log_file)


## ==============================================
# # main
## ==============================================
if __name__ == '__main__':
    
    enable_sdv = False
    enable_trials = False
    enable_local = False
    
    parser = argparse.ArgumentParser(description='Run experiments')
    parser.add_argument("-x", "--enable-sdv", help='enable sdv', action='store_true')
    parser.add_argument("-u", "--enable-trials", help='enable trials', action='store_true')
    parser.add_argument("-l", "--enable-local", help='local mode', action='store_true')
    
    parser.add_argument("-y", "--ycsb_perf_eval", help='eval ycsb perf', action='store_true')
    parser.add_argument("-s", "--ycsb_storage_eval", help='eval ycsb storage', action='store_true')
    parser.add_argument("-n", "--ycsb_nvm_eval", help='eval ycsb nvm', action='store_true')
    parser.add_argument("-i", "--ycsb_recovery_eval", help='ycsb_recovery_eval', action='store_true')
    
    parser.add_argument("-t", "--tpcc_perf_eval", help='eval tpcc perf', action='store_true')
    parser.add_argument("-q", "--tpcc_storage_eval", help='eval tpcc storage', action='store_true')
    parser.add_argument("-r", "--tpcc_nvm_eval", help='eval tpcc nvm', action='store_true')
    parser.add_argument("-j", "--ycsb_recovery_plot", help='ycsb_recovery_plot', action='store_true')
    
    parser.add_argument("-d", "--tpcc_perf_plot", help='plot tpcc perf', action='store_true')
    parser.add_argument("-e", "--tpcc_storage_plot", help='plot tpcc storage', action='store_true')
    parser.add_argument("-f", "--tpcc_nvm_plot", help='plot tpcc nvm', action='store_true')
    parser.add_argument("-o", "--tpcc_recovery_eval", help='tpcc_recovery_eval', action='store_true')
    
    parser.add_argument("-a", "--ycsb_perf_plot", help='plot ycsb perf', action='store_true')
    parser.add_argument("-b", "--ycsb_storage_plot", help='plot ycsb storage', action='store_true')
    parser.add_argument("-c", "--ycsb_nvm_plot", help='plot ycsb nvm', action='store_true')    
    parser.add_argument("-g", "--tpcc_recovery_plot", help='tpcc_recovery_plot', action='store_true')

    parser.add_argument("-w", "--nvm_bw_plot", help='nvm_bw_plot', action='store_true')

    parser.add_argument("-m", "--ycsb_stack_eval", help='ycsb_stack_eval', action='store_true')
    parser.add_argument("-z", "--ycsb_stack_plot", help='ycsb_stack_plot', action='store_true')

    #parser.add_argument("-g", "--btree_eval", help='btree_eval', action='store_true')
    parser.add_argument("-k", "--btree_plot", help='btree_plot', action='store_true')

    #parser.add_argument("-j", "--ise_eval", help='ise_eval', action='store_true')
    parser.add_argument("-p", "--ise_plot", help='ise_plot', action='store_true')
    
    args = parser.parse_args()
    
    if args.enable_sdv:
        enable_sdv = True
    if args.enable_trials:
        enable_trials = True

    if args.enable_local:
        enable_local = True
        NUMACTL_FLAGS = "--membind=0"

    ycsb_perf_log_name = "ycsb_perf.log"
    ycsb_storage_log_name = "ycsb_storage.log"
    ycsb_nvm_log_name = "ycsb_nvm.log"
    ycsb_recovery_log_name = "ycsb_recovery.log"
    ycsb_stack_log_name = "ycsb_stack.log"
    
    tpcc_perf_log_name = "tpcc_perf.log"
    tpcc_storage_log_name = "tpcc_storage.log"
    tpcc_nvm_log_name = "tpcc_nvm.log"   
    tpcc_recovery_log_name = "tpcc_recovery.log"

    test_nvm_log_name = "test_nvm.log"
    btree_log_name = "btree.log"
    ise_log_name = "ise.log"
            
    ################################ YCSB
    
    if args.ycsb_perf_eval:
        ycsb_perf_eval(enable_sdv, enable_trials, ycsb_perf_log_name, YCSB_PERF_DIR, LATENCIES)
    
    if args.ycsb_storage_eval:
        ycsb_storage_eval(ycsb_storage_log_name);
             
    if args.ycsb_nvm_eval:
        ycsb_nvm_eval(ycsb_nvm_log_name);             

    if args.ycsb_recovery_eval:             
        ycsb_recovery_eval(ycsb_recovery_log_name);             

    if args.ycsb_stack_eval:
        ycsb_stack_eval(ycsb_stack_log_name);                    
                          
    if args.ycsb_perf_plot:      
        ycsb_perf_plot(YCSB_PERF_DIR, LATENCIES, "");
        
    if args.ycsb_storage_plot:                
       ycsb_storage_plot();
       
    if args.ycsb_nvm_plot:                
       ycsb_nvm_plot();                          

    if args.ycsb_recovery_plot:                
       ycsb_recovery_plot();        
       
    if args.ycsb_stack_plot:
        ycsb_stack_plot();                    

    ################################ TPCC

    if args.tpcc_perf_eval:
        tpcc_perf_eval(enable_sdv, enable_trials, tpcc_perf_log_name);             

    if args.tpcc_storage_eval:
        tpcc_storage_eval(tpcc_storage_log_name);
             
    if args.tpcc_nvm_eval:
        tpcc_nvm_eval(tpcc_nvm_log_name);             

    if args.tpcc_recovery_eval:             
        tpcc_recovery_eval(tpcc_recovery_log_name);             

    if args.tpcc_perf_plot:                
       tpcc_perf_plot();                          

    if args.tpcc_storage_plot:                
       tpcc_storage_plot();
       
    if args.tpcc_nvm_plot:                
       tpcc_nvm_plot();                          

    if args.tpcc_recovery_plot:                
       tpcc_recovery_plot();                          

    if args.nvm_bw_plot:                            
        nvm_bw_plot();   
        
    #create_legend()
    create_storage_legend()
    #create_stack_legend()

    ################################ MISC

    #if args.btree_eval:
    #    btree_eval(btree_log_name);

    if args.btree_plot:
        btree_plot(btree_log_name);

    #if args.ise_eval:
    #    ise_eval(ise_log_name);

    if args.ise_plot:
        ise_plot(ise_log_name);

    #create_line_legend()
