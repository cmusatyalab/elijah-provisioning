#!/usr/bin/env python
import sys
import os
import ast
from pprint import pprint

stage_names = ["MemoryReadProcess", "CreateMemoryDeltalist", "CreateDiskDeltalist", "DeltaDedup"]

def parse_each_experiement(lines):
    # get configuration
    config_lines = ""
    is_start_config_line = False
    for line in lines:
        if line.find("* Overlay creation configuration") != -1:
            is_start_config_line = True
            continue
        if is_start_config_line == True:
            config_lines += line
            if line.find("}") != -1:
                break
    config_dict = ast.literal_eval(config_lines)

    # filter out only profiling log
    profile_lines = list()
    for line in lines:
        # see only DEBUG message
        if line.find("DEBUG") == -1:
            continue
        if line.find("profiling") == -1:
            continue

        # see only profiling message
        profile_dic = dict()
        log = line.split("profiling")[1].strip()
        profile_lines.append(log)

    # process filtered log data
    profile_ret = dict()
    profile_ret['conf'] = config_dict
    profile_ret['size'] = dict.fromkeys(stage_names, 0)
    profile_ret['time'] = dict.fromkeys(stage_names, 0)
    for line in profile_lines:
        log = line.split("\t")
        stage_name = log[0]
        profile_type = str(log[1])
        if profile_type == "size":
            in_size = long(log[2])
            out_size = long(log[3])
            profile_ret['size'][stage_name] = (float(in_size)/out_size)
        if profile_type == "time":
            duration = float(log[-1])
            profile_ret['time'][stage_name] = duration
    return profile_ret


def parsing(inputfile):
    lines = open(inputfile, "r").read().split("\n")
    test_list = list()
    new_log = list()
    for line in lines:
        if line.find("==========================================") != -1:
            if len(new_log) > 0:
                test_list.append(new_log)
            new_log = list()
        else:
            new_log.append(line)

    #parse_each_experiement(test_list[0])
    test_ret_list = list()
    for each_exp_log in test_list:
        test_ret = parse_each_experiement(each_exp_log)
        test_ret_list.append(test_ret)
        pprint(test_ret)
        print ""



if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.stderr.write("Need input filename")
        sys.exit(1)
    inputfile = sys.argv[1]
    parsing(inputfile)
