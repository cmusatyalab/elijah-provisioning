#!/usr/bin/env python
import sys
import os
import ast
import json
from pprint import pprint
from collections import defaultdict
from Configuration import VMOverlayCreationMode
from operator import itemgetter, attrgetter, methodcaller

stage_names = ["CreateMemoryDeltalist", "CreateDiskDeltalist", "DeltaDedup", "CompressProc"]



class MigrationMode(object):
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

    def get_mode_id(self):
        sorted_key = self.mode.keys()
        sorted_key.sort()
        mode_str = list()
        for key in sorted_key:
            value = self.mode[key]
            mode_str.append("%s:%s" % (key, value))
        return "|".join(mode_str)

    def __repr__(self):
        return "%s(%s)" % (self.workload, self.get_mode_id())

    @staticmethod
    def get_total_P(p_dict):
        # total P: max(disk_diff, memory_diff)+delta+compression

        # get total P considering input data
        #memory_in_blocks = long(self.block['CreateMemoryDeltalist'])
        #disk_in_blocks = long(self.block['CreateDiskDeltalist'])
        #diff_in_blocks = long(self.block['DeltaDedup'])
        #comp_in_blocks = long(self.block['CompressProc'])
        #p_dict = self.block_time
        #diff_stage = 0
        #if memory_in_blocks*p_dict['CreateDiskDeltalist'] > disk_in_blocks*p_dict['CreateMemoryDeltalist']:
        #    diff_stage = p_dict['CreateDiskDeltalist']
        #else:
        #    diff_stage = p_dict['CreateMemoryDeltalist']
        #total_P_from_each_stage = diff_stage + p_dict['DeltaDedup'] + p_dict['CompressProc']

        total_P_from_each_stage = max(p_dict['CreateDiskDeltalist'],
                                      p_dict['CreateMemoryDeltalist']) + \
                                p_dict['DeltaDedup'] + p_dict['CompressProc']
        #print "%f, %f, %f" % (total_P_from_each_stage, total_time_per_core, total_time)
        return total_P_from_each_stage


    @staticmethod
    def get_total_R(r_dict):
        # weight using input size
        #disk_memory_ratio = float(disk_diff)/(disk_diff+memory_diff)
        #memory_disk_ratio = float(memory_diff)/(disk_diff+memory_diff)
        #memory_r = self.stage_size_ratio['CreateMemoryDeltalist']
        #disk_r = self.stage_size_ratio['CreateDiskDeltalist']
        #delta_r = self.stage_size_ratio['DeltaDedup']
        #comp_r = self.stage_size_ratio['CompressProc']
        #total_R = (disk_r*disk_memory_ratio+memory_r*memory_disk_ratio)*delta_r*comp_r

        total_R_from_each_stage = (r_dict['CreateMemoryDeltalist']/2+
                                   r_dict['CreateDiskDeltalist']/2)\
                                * r_dict['DeltaDedup']\
                                * r_dict['CompressProc']
        #print "%f == %f --> %f" % (total_R, total_R_from_each_stage, (total_R-total_R_from_each_stage))

        return total_R_from_each_stage

    @staticmethod
    def get_system_throughput(num_cores, total_p, total_r):
        system_bw_per_block = 1/(total_p*1000) * num_cores
        system_bw_per_bits = system_bw_per_block*(4096+11)*8
        system_out_bw = system_bw_per_bits * total_r # mbps
        return system_out_bw

    @staticmethod
    def mode_diff(mode1, mode2):
        set_mode1, set_mode2= set(mode1.keys()), set(mode2.keys())
        intersect = set_mode1.intersection(set_mode2)
        changed_keys = [o for o in intersect if mode1[o] != mode2[o]]
        changed_dict = dict()
        for key in changed_keys:
            value1 = mode1[key]
            value2 = mode2[key]
            changed_dict[key] = value2
        return changed_dict

    @staticmethod
    def mode_diff_str(mode1, mode2):
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
        fd.write(exp.workload.strip() + "\n")
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
        exp = MigrationMode()
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


class ModeProfileError(Exception):
    pass


class ModeProfile(object):
    def __init__(self, overlay_mode_list):
        self.overlay_mode_list = overlay_mode_list


    def predict_new_mode(self, cur_mode, cur_p, cur_r, system_out_bw, network_bw):
        overlay_mode = self.find_same_mode(cur_mode)
        if overlay_mode == None:
            msg = "Cannot find matching mode with %s" % str(cur_mode.__dict__)
            raise ModeProfileError(msg)
        new_mode = self.find_matching_mode(overlay_mode,
                                           cur_mode,
                                           cur_p, cur_r,
                                           system_out_bw,
                                           network_bw)
        return VMOverlayCreationMode.from_dictionary(new_mode.mode)

    def find_same_mode(self, in_mode):
        for overlay_mode in self.overlay_mode_list:
            id1 = overlay_mode.get_mode_id()
            id2 = in_mode.get_mode_id()
            if id1 == id2:
                return overlay_mode
            else:
                pass
                #print "not identical: %s" % MigrationMode.mode_diff_str(in_mode.__dict__, overlay_mode.mode)
        return None

    def find_matching_mode(self, new_mode, cur_mode, cur_p, cur_r, system_out_bw, network_bw):
        # to be deleted
        for key, in_size in new_mode.block_size_in.iteritems():
            out_size = new_mode.block_size_out[key] 
            new_mode.block_size_ratio[key] = float(out_size)/float(in_size)

        # get scaling factor between current workload and profiled data
        new_total_P = new_mode.get_total_P(new_mode.block_time)
        new_total_R = new_mode.get_total_R(new_mode.block_size_ratio)
        cur_total_P = new_mode.get_total_P(cur_p)
        cur_total_R = new_mode.get_total_R(cur_r)
        scale_p = cur_total_P/new_total_P
        scale_r = cur_total_R/new_total_R

        #print "scaling p: %f, r: %f" % (scale_p, scale_r)
        # apply scaling and get expected system out bw for each mode
        scaled_mode_list = list()
        for each_mode in self.overlay_mode_list:
            each_p = new_mode.get_total_P(each_mode.block_time)
            each_r = new_mode.get_total_R(each_mode.block_size_ratio)
            scaled_each_p = each_p * scale_p
            scaled_each_r = each_r * scale_r
            new_system_bw = MigrationMode.get_system_throughput(cur_mode.NUM_PROC_COMPRESSION,
                                                                scaled_each_p,
                                                                scaled_each_r)
            diff_str = MigrationMode.mode_diff_str(cur_mode.__dict__, each_mode.mode)
            scaled_mode_list.append((each_mode, scaled_each_p, scaled_each_r, new_system_bw))
            #print "%f %f --> (%s) %f %f, %f" % (system_out_bw, network_bw, diff_str, scaled_each_p, scaled_each_r, new_system_bw)

        # find candidate
        candidate_mode_list = list()
        for margin in [0.1, 0.2, 0.3, 0.4, 0.6]:
            for item  in scaled_mode_list:
                (each_mode, scaled_p, scaled_r, new_system_bw) = item
                if network_bw*(1-margin) < new_system_bw and new_system_bw < network_bw*(1+margin):
                    candidate_mode_list.append(item)
            if len(candidate_mode_list) > 0:
                break

        sorted_candidate = list()
        desirable_bw_increase = network_bw - system_out_bw
        if len(candidate_mode_list) == 0:
            # nothing meets the requirement
            if len(candidate_mode_list) == 0:
                if desirable_bw_increase > 0:
                    # chose the most fastest one
                    selected_item = sorted(scaled_mode_list, key=itemgetter(3), reverse=True)[0]
                    return selected_item[0]
                else:
                    # choose the slowest one
                    selected_item = sorted(scaled_mode_list, key=itemgetter(3))[0]
                    return selected_item[0]
        elif len(candidate_mode_list) == 1:
            return candidate_mode_list[0][0]
        else:
            if desirable_bw_increase > 0:
                # increase system speed to use more network BW
                # choose the one that has the biggest compression ratio (max R)
                # --> more thoughput, but little lost in compression
                sorted_candidate = sorted(candidate_mode_list, key=itemgetter(2), reverse=True)
            else:
                # decreasing system speed to work with limited network
                # choose the one that has the shortest speed (minimal P)
                # --> more compression, but little cpu usage
                sorted_candidate = sorted(candidate_mode_list, key=itemgetter(1))
            return sorted_candidate[0][0]

        #for item in sorted_candidate:
        #    (each_mode, scaled_p, scaled_r, new_system_bw) = item
        #    diff_str = MigrationMode.mode_diff_str(cur_mode.__dict__, each_mode.mode)
        #    print "%s\t%s,%s --> (%s, %s) %s" % (diff_str, network_bw, system_out_bw, scaled_p, scaled_r, new_system_bw)
        #import pdb;pdb.set_trace()

    def profile(self):
        pivot_mode = self.overlay_mode_list[0]
        comp_list = list()
        pivot_P = pivot_mode.get_total_P()
        network_bw, pivot_r = pivot_mode.get_system_throughput()

        for other_mode in self.overlay_mode_list:
            other_p = other_mode.get_total_P()
            other_network_bw, other_r = other_mode.get_system_throughput()
            ratio_p = round(other_p/pivot_P, 4)
            ratio_network_bw = round(other_network_bw/network_bw, 4)
            ratio_r = round(other_r/pivot_r, 4)
            mode_diff_str = MigrationMode.mode_diff_str(pivot_mode.mode, other_mode.mode)
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

        selected_config = sorted(selected_config, key=itemgetter(1))
        for item in selected_config:
            (each_mode, ratio_r, ratio_network_bw) = item
            mode_diff_str = MigrationMode.mode_diff_str(pivot_mode.mode, each_mode.mode)
            print "%s\t%s,%s" % (mode_diff_str, ratio_r, ratio_network_bw)

    @staticmethod
    def load_from_file(profile_path):
        exp_list = list()
        try:
            with open(profile_path, "r") as fd:
                while True:
                    exp = MigrationMode.from_file(fd)
                    exp_list.append(exp)
        except ValueError as e:
            pass
        return ModeProfile(exp_list)

    @staticmethod
    def save_to_file(profile_path, exp_list):
        with open(profile_path, "w+") as fd:
            for each_exp in exp_list:
                MigrationMode.to_file(each_exp, fd)


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
    exp = MigrationMode()
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
            exp.block_size_ratio[stage_name] = float(out_size)/in_size
        if profile_type == "time":
            duration = float(log[-1])
            exp.stage_time[stage_name] = duration
        if profile_type == "block-time":
            duration = round(float(log[-1])*1000, 6)
            exp.block_time[stage_name] = duration
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
            msg = "Invalid workload %s" % each_exp['work']
            print msg
            sys.exit(1)
            raise ModeProfileError(msg)
    #if (len(moped_exps) == len(fluid_exps) == len(face_exps) == len(mar_exps)) == False:
    #    msg = "workloads have different experiement size"
    #    print msg
    #    sys.exit(1)
    #    raise ModeProfileError(msg)
    return moped_exps, fluid_exps, face_exps, mar_exps



def profiling(test_ret_list):
    # how change in mode will affect system performance?
    moped_exps, fluid_exps, face_exps, mar_exps = _split_experiment(test_ret_list)
    comp_list = list()
    if moped_exps:
        ModeProfile.save_to_file("moped-profile", moped_exps)
    if face_exps:
        ModeProfile.save_to_file("face-profile", face_exps)
    if mar_exps:
        ModeProfile.save_to_file("mar-profile", mar_exps)
    if fluid_exps:
        ModeProfile.save_to_file("fluid-profile", fluid_exps)



if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.stderr.write("%prog [parse|test] filename\n")
        sys.exit(1)
    command = sys.argv[1]
    inputfile = sys.argv[2]

    if command == "parse":
        test_ret_list = parsing(inputfile)
        profiling(test_ret_list)
    elif command == "test":
        mode_profile = ModeProfile.load_from_file(inputfile)

        # measured information
        cur_p = {'CreateMemoryDeltalist': 0.5540335827711419, 'CreateDiskDeltalist': 0.5118202023133814, 'DeltaDedup': 0.0024018974990242282, 'CompressProc': 0.45448795749054516}
        cur_r = {'CreateMemoryDeltalist': 0.48741149800555605, 'CreateDiskDeltalist': 0.3919707506873821, 'DeltaDedup': 0.6054054544199231, 'CompressProc': 0.7024070026179592}
        cur_mode = VMOverlayCreationMode.get_pipelined_multi_process_finite_queue()

        # get system throughput using P and R
        total_p = MigrationMode.get_total_P(cur_p)
        total_r= MigrationMode.get_total_R(cur_r)

        system_out_bw = MigrationMode.get_system_throughput(cur_mode.NUM_PROC_COMPRESSION, total_p, total_r)
        network_bw = 100 # mbps

        # get new mode
        new_mode = mode_profile.predict_new_mode(cur_mode, cur_p, cur_r, system_out_bw, network_bw)
        diff_str = MigrationMode.mode_diff_str(cur_mode.__dict__, new_mode.__dict__)
        diff = MigrationMode.mode_diff(cur_mode.__dict__, new_mode.__dict__)
        print "%s\n%s\n%s" % (new_mode, diff_str, diff)
    else:
        sys.stderr.write("Invalid command\n")
        sys.exit(1)
