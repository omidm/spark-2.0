#!/usr/bin/python

# ./get_summary.py [directory_name] [number of workers]

import os
import re
import sys
import argparse
import decimal
import numpy as np

def parse_line(line):
    items = line.split()
    assert(len(items) == 6);
    return float(items[1]),  float(items[3]), float(items[5])


## Parse the command line arguments ##
parser = argparse.ArgumentParser(description='Process log files.')
parser.add_argument(
    "-d", "--dir_path",
    dest="dirpath",
    default="..",
    help="input dorectory path that has log-dir folder")
parser.add_argument(
    "-i", "--input_file",
    dest="inputfile",
    default="",
    help="input file to parse including the path name, overwrites the -d option")
parser.add_argument(
    "-wn", "--worker_num",
    dest="workernum",
    required=True,
    help="worker num")
parser.add_argument(
    "-cn", "--core_num",
    dest="corenum",
    required=True,
    help="core num per worker")
parser.add_argument(
    "-ti", "--truncate_index",
    dest="truncateindex",
    default=0,
    help="truncate index to ignore the initial iterations")
parser.add_argument(
    "-v", "--verbose",
    dest="collapse",
    action="store_true",
    help="print per iteration stats as well")


args = parser.parse_args()

D  = args.dirpath 
IF  = args.inputfile 
WN  = int(args.workernum) 
CN  = int(args.corenum) 
TI = int(args.truncateindex)



#   "Event":"SparkListenerJobStart" -> "Job ID":0
#                                   -> "Submission Time":1453843980496
#   "Event":"SparkListenerStageSubmitted" -> none
#   "Event":"SparkListenerTaskEnd" -> "Launch Time":1453843985415
#                                  -> "Finish Time":1453843988068
#                                  -> "Executor Deserialize Time":508
#                                  -> "Executor Run Time":2067
#                                  -> "JVM GC Time":524
#                                  -> "Result Serialization Time":0
#   "Event":"SparkListenerStageCompleted" -> "Submission Time":1453843980678
#                                         -> "Completion Time":1453843988079
#   "Event":"SparkListenerJobEnd" -> "Job ID":0
#                                 -> "Completion Time":1453843988082

if IF == "":
  path = D+"/log-dir/"
  assert(len(os.listdir(path)) == 1)
  filename = os.listdir(path)[0]
  f = open(os.path.join(path, filename), 'r')
else:
  f = open(IF, 'r')


iter_num     = 0
task_sum     = 0
gradient_sum = 0


job_count    = 0
task_counts  = []
stage_counts = []
task_times   = []
dser_times   = []
exe_times    = []
gc_times     = []
ser_times    = []
stage_times  = []
job_times    = []

diffs        = []


for line in f:
  if "SparkListenerJobStart" in line:
    regexp = '.*'
    regexp += '\"Job ID\":(\d+).*'
    regexp += '\"Submission Time\":(\d+).*'
    tokens = re.findall(regexp, line)
    assert(len(tokens) == 1)
    assert(len(tokens[0]) == 2)
    job_id    = decimal.Decimal(tokens[0][0])
    job_start = decimal.Decimal(tokens[0][1])
    job_count  += 1
    stage_count = 0
    stage_time  = 0
    task_count  = 0
    task_time   = 0
    dser_time   = 0
    exe_time    = 0
    gc_time     = 0
    ser_time    = 0
  elif "SparkListenerTaskEnd" in line:
    regexp = '.*'
    regexp += '\"Launch Time\":(\d+).*'
    regexp += '\"Finish Time\":(\d+).*'
    regexp += '\"Executor Deserialize Time\":(\d+).*'
    regexp += '\"Executor Run Time\":(\d+).*'
    regexp += '\"JVM GC Time\":(\d+).*'
    regexp += '\"Result Serialization Time\":(\d+).*'
    tokens = re.findall(regexp, line)
    assert(len(tokens) == 1)
    assert(len(tokens[0]) == 6)
    start = decimal.Decimal(tokens[0][0])
    end   = decimal.Decimal(tokens[0][1])
    dser  = decimal.Decimal(tokens[0][2])
    exe   = decimal.Decimal(tokens[0][3])
    gc    = decimal.Decimal(tokens[0][4])
    ser   = decimal.Decimal(tokens[0][5])
    task_count += 1
    task_time += (end - start)
    dser_time += dser
    exe_time  += exe
    gc_time   += gc
    ser_time  += ser
    diffs.append((end - start) - (dser + exe + ser));
  elif "SparkListenerStageCompleted" in line:
    regexp = '.*'
    regexp += '\"Submission Time\":(\d+).*'
    regexp += '\"Completion Time\":(\d+).*'
    tokens = re.findall(regexp, line)
    assert(len(tokens) == 1)
    assert(len(tokens[0]) == 2)
    start = decimal.Decimal(tokens[0][0])
    end   = decimal.Decimal(tokens[0][1])
    stage_count += 1
    stage_time += (end - start)
  elif "SparkListenerJobEnd" in line:
    regexp = '.*'
    regexp += '\"Job ID\":(\d+).*'
    regexp += '\"Completion Time\":(\d+).*'
    tokens = re.findall(regexp, line)
    assert(len(tokens) == 1)
    assert(len(tokens[0]) == 2)
    j_id    = decimal.Decimal(tokens[0][0])
    job_end = decimal.Decimal(tokens[0][1])
    assert(job_id == j_id)
    stage_counts.append(stage_count)
    task_counts.append(task_count)
    job_times.append(job_end - job_start)
    stage_times.append(stage_time)
    task_times.append(task_time)
    dser_times.append(dser_time)
    exe_times.append(exe_time)
    gc_times.append(gc_time)
    ser_times.append(ser_time)

# assert(job_count == len(task_counts))
# assert(job_count == len(stage_counts))

diffs       = map(lambda x: x / 1000, diffs)
job_times   = map(lambda x: x / 1000, job_times)
stage_times = map(lambda x: x / 1000, stage_times)
task_times  = map(lambda x: x / (WN*CN) / 1000, task_times)
dser_times  = map(lambda x: x / (WN*CN) / 1000, dser_times)
exe_times   = map(lambda x: x / (WN*CN) / 1000, exe_times)
gc_times    = map(lambda x: x / (WN*CN) / 1000, gc_times)
ser_times   = map(lambda x: x / (WN*CN) / 1000, ser_times)

print '--------------------------------------------------------------------------------------'
print '          Job      Stage(Num)  Task(Num)    [Exec(GC)     + DSer  + Ser  ]    Overhead  '
print '--------------------------------------------------------------------------------------'
if (args.collapse):
  for i in range(0, job_count):
    print '    {:2d} {:8.3f} {:8.3f}({:0d}) {:8.3f}({:0d})   [{:0.3f}({:0.3f}) + {:0.3f} + {:0.3f}] {:8.3f}'.format(
        i + 1,
        job_times[i],
        stage_times[i],
        stage_counts[i],
        task_times[i],
        task_counts[i],
        exe_times[i],
        gc_times[i],
        dser_times[i],
        ser_times[i],
        job_times[i] - exe_times[i])


print '--------------------------------------------------------------------------------------'
print '    {:2d} {:8.3f} {:8.3f}({:0d}) {:8.3f}({:0d})   [{:0.3f}({:0.3f}) + {:0.3f} + {:0.3f}] {:8.3f}'.format(
    job_count - TI,
    np.mean(job_times[TI:]),
    np.mean(stage_times[TI:]),
    int(np.mean(stage_counts[TI:])),
    np.mean(task_times[TI:]),
    int(np.mean(task_counts[TI:])),
    np.mean(exe_times[TI:]),
    np.mean(gc_times[TI:]),
    np.mean(dser_times[TI:]),
    np.mean(ser_times[TI:]),
    np.mean(job_times[TI:]) - np.mean(exe_times[TI:]))
print '--------------------------------------------------------------------------------------'

# print 'DIFF:  mean: {:4.2f} std: {:4.3f} '.format(np.mean(diffs), np.std(diffs))

