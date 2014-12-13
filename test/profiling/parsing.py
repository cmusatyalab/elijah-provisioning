#!/usr/bin/env python
import sys
import os
import ast
from pprint import pprint
from collections import defaultdict

stage_names = ["CreateMemoryDeltalist", "CreateDiskDeltalist", "DeltaDedup", "CompressProc"]


class ProfilingError(Exception):
    pass


class Experiment(object):
    def __init__(self):
        pass

    def __repr__(self):
        return "%s,R:%s,P:%s" % (self.workload, self.get_total_R(), self.get_total_P())

    def get_total_P(self):
        # total P: max(disk_diff, memory_diff)+delta+compression
        disk_diff = self.stage_time['CreateMemoryDeltalist']
        memory_diff = self.stage_time['CreateDiskDeltalist']
        delta = self.stage_time['DeltaDedup']
        comp = self.stage_time['CompressProc']
        total_p = max(disk_diff, memory_diff)+delta+comp
        return total_p

    def get_total_R(self):
        # R: (disk_in + memor_in)/compressed_out
        disk_diff = self.stage_size_in['CreateMemoryDeltalist']
        memory_diff = self.stage_size_in['CreateDiskDeltalist']
        comp = self.stage_size_out['CompressProc']
        total_R = float(comp)/(disk_diff+memory_diff)
        return round(total_R, 4)

    @staticmethod
    def mode_diff_str(exp1, exp2):
        mode1 = exp1.mode
        mode2 = exp2.mode
        set_mode1, set_mode2= set(mode1.keys()), set(mode2.keys())
        intersect = set_mode1.intersection(set_mode2)
        changed_keys = [o for o in intersect if mode1[o] != mode2[o]]
        changed_list = list()
        for key in changed_keys:
            value1 = mode1[key]
            value2 = mode2[key]
            changed = "%s: %s->%s" % (key, value1, value2)
            changed_list.append(changed)
        changed_str = ", ".join(changed_list)
        return changed_str




def parse_each_experiement(lines):
    # get configuration
    config_lines = ""
    is_start_config_line = False
    workload = lines[0].split(" ")[-1]
    migration_total_time = 0
    for line in lines[1:]:
        if line.find("* Overlay creation mode start") != -1:
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
        if line.find("profiling") != -1:
            # see only profiling message
            profile_dic = dict()
            log = line.split("profiling")[1].strip()
            profile_lines.append(log)
        elif line.find("ime for finishing transferring") != -1:
            log = line.split(":")[-1]
            migration_total_time = float(log.strip())

    # process filtered log data
    exp = Experiment()
    workload = lines[0].split(" ")[-1]
    setattr(exp, 'workload', os.path.basename(workload))
    setattr(exp, 'migration_total_time', migration_total_time)
    setattr(exp, 'mode', config_dict)
    setattr(exp, 'stage_size_in', dict.fromkeys(stage_names, 0))
    setattr(exp, 'stage_size_out', dict.fromkeys(stage_names, 0))
    setattr(exp, 'stage_size_ratio', dict.fromkeys(stage_names, 0))
    setattr(exp, 'stage_time', dict.fromkeys(stage_names, 0))
    setattr(exp, 'block', dict.fromkeys(stage_names, 0))
    setattr(exp, 'block_size_in', dict.fromkeys(stage_names, 0))
    setattr(exp, 'block_size_ratio', dict.fromkeys(stage_names, 0))
    setattr(exp, 'block_size_out', dict.fromkeys(stage_names, 0))
    setattr(exp, 'block_time', dict.fromkeys(stage_names, 0))
    for line in profile_lines:
        log = line.split("\t")
        stage_name = log[0]
        profile_type = str(log[1])
        if stage_name not in stage_names:
            continue
        if profile_type == "size":
            in_size = long(log[2])
            out_size = long(log[3])
            ratio = float(log[4])
            exp.stage_size_in[stage_name] = in_size
            exp.stage_size_out[stage_name] = out_size
            exp.stage_size_ratio[stage_name] = ratio
        if profile_type == "block-size":
            in_size = float(log[2])
            out_size = float(log[3])
            block_count = log[4]
            exp.block[stage_name] = block_count
            exp.block_size_in[stage_name] = in_size
            exp.block_size_out[stage_name] = out_size
            exp.block_size_ratio[stage_name] = float(in_size)/out_size
        if profile_type == "time":
            duration = float(log[-1])
            exp.stage_time[stage_name] = duration
        if profile_type == "block-time":
            duration = round(float(log[-1])*1000, 6)
            exp.block_time[stage_name] = duration
    #print "%s: %s, %s" % (exp.workload, exp.stage_time, exp.get_total_P())
    #print "%s: %s, %s" % (exp.workload, exp.stage_size_in, exp.get_total_R())
    return exp


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
    test_list.append(new_log)

    test_ret_list = list()
    for each_exp_log in test_list:
        test_ret = parse_each_experiement(each_exp_log)
        test_ret_list.append(test_ret)
    return test_ret_list


def profile_each_exp(each_exp_dict):
    # total execution time from processing time

    pass


def _split_experiment(test_ret_list):
    moped_exps = list()
    speech_exps = list()
    fluid_exps = list()
    face_exps = list()
    mar_exps = list()
    for each_exp in test_ret_list:
        if each_exp.workload.find("moped") !=  -1:
            moped_exps.append(each_exp)
        elif each_exp.workload.find("fluid") !=  -1:
            fluid_exps.append(each_exp)
        elif each_exp.workload.find("face") !=  -1:
            face_exps.append(each_exp)
        elif each_exp.workload.find("mar") !=  -1:
            mar_exps.append(each_exp)
        elif each_exp.workload.find("speech") !=  -1:
            speech_exps.append(each_exp)
        else:
            msg = "Invalid workload %s" % each_exp['workload']
            print msg
            sys.exit(1)
            raise ProfilingError(msg)
    #if (len(moped_exps) == len(fluid_exps) == len(face_exps) == len(mar_exps)) == False:
    #    msg = "workloads have different experiement size"
    #    print msg
    #    sys.exit(1)
    #    raise ProfilingError(msg)
    return moped_exps, speech_exps, fluid_exps, face_exps, mar_exps

def multikeysort(items, columns):
    from operator import itemgetter
    comparers = [ ((itemgetter(col[1:].strip()), -1) if col.startswith('-') else (itemgetter(col.strip()), 1)) for col in columns]  
    def comparer(left, right):
        for fn, mult in comparers:
            result = cmp(fn(left), fn(right))
            if result:
                return mult * result
            else:
                return 0
    return sorted(items, cmp=comparer)


def profiling(test_ret_list):
    # how change in mode will affect system performance?
    moped_exps, speech_exps, fluid_exps, face_exps, mar_exps = _split_experiment(test_ret_list)
    comparison = defaultdict(list)

    exps = moped_exps

    # sort by compression algorithm gzip 1, .., gzip9, .., lzma1, .., lzma9
    def compare_comp_algorithm(a):
        d = {"xdelta3":2,
             "bsdiff":3,
             "none":1}
        return (d[a.mode['DISK_DIFF_ALGORITHM']], -a.mode['COMPRESSION_ALGORITHM_TYPE'], a.mode['COMPRESSION_ALGORITHM_SPEED'])
    exps.sort(key=compare_comp_algorithm)
    for each_exp in exps:
        in_data_size = each_exp.stage_size_in['CreateMemoryDeltalist'] + each_exp.stage_size_in['CreateDiskDeltalist']
        in_data_disk = each_exp.stage_size_in['CreateDiskDeltalist']
        in_data_mem = each_exp.stage_size_in['CreateMemoryDeltalist']
        alpha = float(in_data_mem)/in_data_size
        out_data_size = each_exp.stage_size_out['CompressProc']
        duration = each_exp.migration_total_time
        est_duration1 = each_exp.stage_time['CreateMemoryDeltalist'] + each_exp.stage_time['CreateDiskDeltalist']+\
            each_exp.stage_time['DeltaDedup'] + each_exp.stage_time['CompressProc']
        print "(%d,%d,%s)\tin_size:%ld\tout_size:%ld\tduration:%f,%f,%f\tthroughput(Mbps):%f" %\
            (each_exp.mode['COMPRESSION_ALGORITHM_TYPE'],
             each_exp.mode['COMPRESSION_ALGORITHM_SPEED'],\
             each_exp.mode['DISK_DIFF_ALGORITHM'],\
             in_data_size, out_data_size, duration, est_duration1, float(duration)/est_duration1,
             8*float(out_data_size)/1024.0/1024/duration)


    '''
    pivot_mode = moped_exps[0]
    pivot_R = pivot_mode.get_total_R()
    pivot_P = pivot_mode.get_total_P()
    for other_mode in moped_exps:
        other_r = other_mode.get_total_R()
        other_p = other_mode.get_total_P()
        ratio_r = round(other_r/pivot_R, 4)
        ratio_p = round(other_p/pivot_P, 4)
        mode_diff_str = Experiment.mode_diff_str(pivot_mode, other_mode)
        if len(mode_diff_str) == 0:
            mode_diff_str = "original"
        comparison[mode_diff_str].append((ratio_r, ratio_p))
        #print "%s\t%s %s" % (mode_diff_str, ratio_r, ratio_p)

    pivot_mode = mar_exps[0]
    pivot_R = pivot_mode.get_total_R()
    pivot_P = pivot_mode.get_total_P()
    for other_mode in mar_exps:
        other_r = other_mode.get_total_R()
        other_p = other_mode.get_total_P()
        ratio_r = round(other_r/pivot_R, 4)
        ratio_p = round(other_p/pivot_P, 4)
        mode_diff_str = Experiment.mode_diff_str(pivot_mode, other_mode)
        if len(mode_diff_str) == 0:
            mode_diff_str = "original"
        (o_r, o_p) = comparison[mode_diff_str][0]
        comparison[mode_diff_str].append((ratio_r, ratio_p))
        r_diff = abs(o_r-ratio_r)/o_r*100
        p_diff = abs(o_p-ratio_p)/o_p*100
        print "%s\t%f %f" % (mode_diff_str, round(r_diff,0), round(p_diff, 0))
        #print "%s\t%f %f" % (mode_diff_str, ratio_r, ratio_p)

    pivot_mode = face_exps[0]
    pivot_R = pivot_mode.get_total_R()
    pivot_P = pivot_mode.get_total_P()
    for other_mode in face_exps:
        other_r = other_mode.get_total_R()
        other_p = other_mode.get_total_P()
        ratio_r = round(other_r/pivot_R, 4)
        ratio_p = round(other_p/pivot_P, 4)
        mode_diff_str = Experiment.mode_diff_str(pivot_mode, other_mode)
        if len(mode_diff_str) == 0:
            mode_diff_str = "original"
        (o_r, o_p) = comparison[mode_diff_str][0]
        comparison[mode_diff_str].append((ratio_r, ratio_p))
        r_diff = abs(o_r-ratio_r)/o_r*100
        p_diff = abs(o_p-ratio_p)/o_p*100
        #print "%s\t%f %f" % (mode_diff_str, round(r_diff, 0), round(p_diff, 0))
        #print "%s\t%f %f" % (mode_diff_str, ratio_r, ratio_p)
    '''


def get_ratio(in_config, out_configs):
    pass



if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.stderr.write("Need input filename")
        sys.exit(1)
    inputfile = sys.argv[1]
    test_ret_list = parsing(inputfile)
    profiling(test_ret_list)
