#!/usr/bin/env python
import sys
import os
import ast
import json
from pprint import pprint
from collections import defaultdict

stage_names = ["CreateMemoryDeltalist", "CreateDiskDeltalist", "DeltaDedup", "CompressProc"]


class ProfilingError(Exception):
    pass


class Experiment(object):
    def __init__(self):
        self.workload = ''
        self.mode = dict()
        self.stage_size_in = dict()
        self.stage_size_out = dict()
        self.stage_size_ratio = dict()
        self.stage_time = dict()
        self.block = dict()
        self.block_size_in = dict()
        self.block_size_ratio = dict()
        self.block_size_out = dict()
        self.block_time = dict()

    def get_id(self):
        mode_str = ["%s:%s" % (key, value) for key, value in self.mode.iteritems()]
        return "|".join(mode_str)


    def __repr__(self):
        return "%s(%s)" % (self.workload, self.get_id())

    def get_total_P(self):
        # total P: max(disk_diff, memory_diff)+delta+compression

        # validation: from values of each stage
        memory_in_blocks = long(self.block['CreateMemoryDeltalist'])
        disk_in_blocks = long(self.block['CreateDiskDeltalist'])
        diff_in_blocks = long(self.block['DeltaDedup'])
        comp_in_blocks = long(self.block['CompressProc'])
        p_dict = self.block_time
        diff_stage = 0
        if memory_in_blocks*p_dict['CreateDiskDeltalist'] > disk_in_blocks*p_dict['CreateMemoryDeltalist']:
            diff_stage = p_dict['CreateDiskDeltalist']
        else:
            diff_stage = p_dict['CreateMemoryDeltalist']

        total_P_from_each_stage = diff_stage + p_dict['DeltaDedup'] + p_dict['CompressProc']
        #total_P_from_each_stage = max(p_dict['CreateDiskDeltalist'], p_dict['CreateMemoryDeltalist']) + p_dict['DeltaDedup'] + p_dict['CompressProc']
        total_time_per_core = total_P_from_each_stage/1000 * (disk_in_blocks+memory_in_blocks)
        total_time = total_time_per_core/self.mode['NUM_PROC_COMPRESSION']
        #print "%f, %f, %f" % (total_P_from_each_stage, total_time_per_core, total_time)
        return total_P_from_each_stage

    def get_system_throughput(self):
        # cpu throughput for input data
        p_dict = self.block_time
        time_per_block_per_core = max(p_dict['CreateDiskDeltalist'], p_dict['CreateMemoryDeltalist']) + p_dict['DeltaDedup'] + p_dict['CompressProc']
        throughput_per_cpu_per_block = 1/(time_per_block_per_core/1000)
        throughput_per_cpu_MBps = (4096+11)*throughput_per_cpu_per_block/1024.0/1024
        throughput_cpus_MBps = self.mode['NUM_PROC_COMPRESSION']*throughput_per_cpu_MBps

        # required network throughput
        ratio = self.get_total_R()
        throughput_network_MBps = throughput_cpus_MBps*ratio
        return throughput_network_MBps, ratio

    def get_network_throughput(self):
        pass

    def get_total_R(self):
        # R: (disk_in + memor_in)/compressed_out

        # from total output
        memory_diff = self.stage_size_in['CreateMemoryDeltalist']
        disk_diff = self.stage_size_in['CreateDiskDeltalist']
        #comp = self.stage_size_out['CompressProc']
        #total_R = float(comp)/(disk_diff+memory_diff)

        # validation: from caclulcation of each stage
        disk_memory_ratio = float(disk_diff)/(disk_diff+memory_diff)
        memory_disk_ratio = float(memory_diff)/(disk_diff+memory_diff)
        memory_r = self.stage_size_ratio['CreateMemoryDeltalist']
        disk_r = self.stage_size_ratio['CreateDiskDeltalist']
        delta_r = self.stage_size_ratio['DeltaDedup']
        comp_r = self.stage_size_ratio['CompressProc']
        total_R_from_each_stage = (disk_r*disk_memory_ratio+memory_r*memory_disk_ratio)*delta_r*comp_r
        #print "%f == %f --> %f" % (total_R, total_R_from_each_stage, (total_R-total_R_from_each_stage))

        return total_R_from_each_stage

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

    @staticmethod
    def to_file(exp, fd):
        fd.write(exp.workload + "\n")
        fd.write(json.dumps(exp.mode) + "\n")
        fd.write(json.dumps(exp.stage_size_in) + "\n")
        fd.write(json.dumps(exp.stage_size_out) + "\n")
        fd.write(json.dumps(exp.stage_size_ratio) + "\n")
        fd.write(json.dumps(exp.stage_time) + "\n")
        fd.write(json.dumps(exp.block) + "\n")
        fd.write(json.dumps(exp.block_size_in) + "\n")
        fd.write(json.dumps(exp.block_size_out) + "\n")
        fd.write(json.dumps(exp.block_size_ratio) + "\n")
        fd.write(json.dumps(exp.block_time) + "\n")

    @staticmethod
    def from_file(fd):
        exp = Experiment()
        exp.workload = fd.readline()
        exp.mode = json.loads(fd.readline())
        exp.stage_size_in = json.loads(fd.readline())
        exp.stage_size_out = json.loads(fd.readline())
        exp.stage_size_ratio = json.loads(fd.readline())
        exp.stage_time = json.loads(fd.readline())
        exp.block = json.loads(fd.readline())
        exp.block_size_in = json.loads(fd.readline())
        exp.block_size_out = json.loads(fd.readline())
        exp.block_size_ratio = json.loads(fd.readline())
        exp.block_time = json.loads(fd.readline())
        return exp




def parse_each_experiement(lines):
    # get configuration
    config_lines = ""
    is_start_config_line = False
    workload = lines[0].split(" ")[-1]
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
        if line.find("profiling") == -1:
            continue

        # see only profiling message
        profile_dic = dict()
        log = line.split("profiling")[1].strip()
        profile_lines.append(log)

    # process filtered log data
    exp = Experiment()
    setattr(exp, 'workload', os.path.basename(workload))
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
        else:
            import pdb;pdb.set_trace()
            msg = "Invalid workload %s" % each_exp['work']
            print msg
            sys.exit(1)
            raise ProfilingError(msg)
    #if (len(moped_exps) == len(fluid_exps) == len(face_exps) == len(mar_exps)) == False:
    #    msg = "workloads have different experiement size"
    #    print msg
    #    sys.exit(1)
    #    raise ProfilingError(msg)
    return moped_exps, fluid_exps, face_exps, mar_exps


def save_load():
    pivot_mode = moped_exps[0]
    fd = open("test", "rw+")
    Experiment.to_file(pivot_mode, fd)
    fd.close()

    fd = open("test", "rw+")
    aa = Experiment.from_file(fd)
    fd.close()
    import pdb;pdb.set_trace()


def profiling(test_ret_list):
    # how change in mode will affect system performance?
    moped_exps, fluid_exps, face_exps, mar_exps = _split_experiment(test_ret_list)
    comp_list = list()
    pivot_P = pivot_mode.get_total_P()
    network_bw, pivot_r = pivot_mode.get_system_throughput()

    for other_mode in moped_exps:
        other_p = other_mode.get_total_P()
        other_network_bw, other_r = other_mode.get_system_throughput()
        ratio_p = round(other_p/pivot_P, 4)
        ratio_network_bw = round(other_network_bw/network_bw, 4)
        ratio_r = round(other_r/pivot_r, 4)
        mode_diff_str = Experiment.mode_diff_str(pivot_mode, other_mode)
        if len(mode_diff_str) == 0:
            mode_diff_str = "original"
        comp_list.append((other_mode, ratio_r, ratio_network_bw))
        #print "%s\t(%s %s)/(%s %s) --> (%s, %s)" % (mode_diff_str[:], network_bw, pivot_r, other_network_bw, other_r, ratio_network_bw, ratio_r)

    # networkBW:5 mbps, actual BW: 10 mps
    # speed up computation, but do not loose R
    selected_config = list()
    for item in comp_list:
        (mode_str, ratio_r, ratio_network_bw) = item
        if ratio_network_bw > 1:
            selected_config.append(item)

    from operator import itemgetter, attrgetter, methodcaller
    selected_config = sorted(selected_config, key=itemgetter(1))
    for item in selected_config:
        (each_mode, ratio_r, ratio_network_bw) = item
        mode_diff_str = Experiment.mode_diff_str(pivot_mode, each_mode)
        print "%s\t%s,%s" % (mode_diff_str, ratio_r, ratio_network_bw)


def get_new_mode(overlay_mode, current_p, current_r, network_bw):
    new_mode = ''
    return new_mode



if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.stderr.write("Need input filename")
        sys.exit(1)
    inputfile = sys.argv[1]
    test_ret_list = parsing(inputfile)
    profiling(test_ret_list)
