#!/usr/bin/env python
import sys
import os
import ast
import json
import math
from collections import defaultdict
from collections import OrderedDict
from pprint import pprint
from collections import defaultdict
from Configuration import VMOverlayCreationMode
from operator import itemgetter, attrgetter, methodcaller
import log as logging


LOG = logging.getLogger(__name__)
_process_controller = None


stage_names = ["CreateMemoryDeltalist", "CreateDiskDeltalist", "DeltaDedup", "CompressProc"]
BIT_PER_BLOCK = (4096+11)*8


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
            # ignore numer of process for each stage since this is a maximum
            # number of thread. Also this does not be consider to get P and R
            # since we get per block P and per block R
            if key in VMOverlayCreationMode.VARYING_PARAMETERS:
                value = self.mode[key]
                mode_str.append("%s:%s" % (key, value))
        return "|".join(mode_str)

    def __repr__(self):
        return "%s(%s)" % (self.workload, self.get_mode_id())

    @staticmethod
    def get_total_P(p_dict, alpha):
        # calculate using p of all stages --> use alpha
        # calculate using time at all stage --> do not use alpha
        total_P_from_each_stage = (p_dict['CreateMemoryDeltalist']*alpha +\
                                   p_dict['CreateDiskDeltalist']*(1-alpha)) +\
            p_dict['DeltaDedup'] +\
            p_dict['CompressProc']
        return total_P_from_each_stage

    @staticmethod
    def get_total_R(r_dict, alpha):
        # weight using input size
        total_R_from_each_stage = (r_dict['CreateMemoryDeltalist']*alpha +\
                                   r_dict['CreateDiskDeltalist']*(1-alpha))\
                                * r_dict['DeltaDedup']\
                                * r_dict['CompressProc']
        return total_R_from_each_stage

    @staticmethod
    def get_system_throughput(num_cores, total_p, total_r):
        #LOG.debug("num_core\t%d" % num_cores)
        system_block_per_sec = (1/total_p*1000) * num_cores*0.7
        system_in_mbps = system_block_per_sec*BIT_PER_BLOCK/1024.0/1024
        system_out_mbps = system_in_mbps * total_r# mbps
        return system_block_per_sec, system_in_mbps, system_out_mbps

    @staticmethod
    def mode_diff(mode1, mode2):
        set_mode1, set_mode2 = set(mode1.keys()), set(mode2.keys())
        set_mode1 = set_mode1.intersection(set(VMOverlayCreationMode.VARYING_PARAMETERS))
        set_mode2 = set_mode2.intersection(set(VMOverlayCreationMode.VARYING_PARAMETERS))

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
        set_mode1 = set_mode1.intersection(set(VMOverlayCreationMode.VARYING_PARAMETERS))
        set_mode2 = set_mode2.intersection(set(VMOverlayCreationMode.VARYING_PARAMETERS))

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

        memory_in_size = (exp.block_size_in['CreateMemoryDeltalist'])
        disk_in_size = (exp.block_size_in['CreateDiskDeltalist'])
        alpha = float(memory_in_size)/(memory_in_size+disk_in_size)
        exp.total_p = MigrationMode.get_total_P(exp.block_time, alpha)
        exp.total_r = MigrationMode.get_total_R(exp.block_size_ratio, alpha)
        return exp


class ModeProfileError(Exception):
    pass


class ModeProfile(object):
    MATCHING_BEST_EFFORT = 1
    MATCHING_ONE = 2
    MATCHING_MULTIPLE = 3

    def __init__(self, overlay_mode_list):
        self.overlay_mode_list = overlay_mode_list


    def predict_new_mode(self, cur_mode, cur_p, cur_r, cur_block_size, network_bw):
        overlay_mode = ModeProfile.find_same_mode(self.overlay_mode_list, cur_mode)
        if overlay_mode == None:
            msg = "Cannot find matching mode : %s" % str(cur_mode.get_mode_id())
            raise ModeProfileError(msg)
        item = self.find_matching_mode(overlay_mode,
                                       cur_mode,
                                       cur_p, cur_r, cur_block_size,
                                       network_bw)
        return item

    @staticmethod
    def find_same_mode(overlay_mode_list, in_mode):
        for overlay_mode in overlay_mode_list:
            id1 = overlay_mode.get_mode_id()
            id2 = in_mode.get_mode_id()
            if id1 == id2:
                return overlay_mode
            else:
                pass
                #print "not identical: %s" % MigrationMode.mode_diff_str(in_mode.__dict__, overlay_mode.mode)
        return None

    def find_matching_mode(self, profiled_mode_obj, cur_mode, cur_p, cur_r, cur_block_size, network_bw):
        # get scaling factor between current workload and profiled data
        profiled_mode_total_p = profiled_mode_obj.total_p
        profiled_mode_total_r = profiled_mode_obj.total_r

        memory_in_size = (cur_block_size['CreateMemoryDeltalist'])
        disk_in_size = (cur_block_size['CreateDiskDeltalist'])
        alpha = float(memory_in_size)/(memory_in_size+disk_in_size)
        cur_total_p = profiled_mode_obj.get_total_P(cur_p, alpha)
        cur_total_r = profiled_mode_obj.get_total_R(cur_r, alpha)
        scale_p = cur_total_p/profiled_mode_total_p
        scale_r = cur_total_r/profiled_mode_total_r

        #LOG.debug("mode-predict\tcur_p:%f\tcur_r:%f\tprofile_p:%f\tprofile_r:%f\tscaling_p:%f\tscaling_r:%f" % \
        #          (cur_total_p, cur_total_r, profiled_mode_total_p, profiled_mode_total_r, scale_p, scale_r))
        scaled_mode_list, current_block_per_sec = self.list_scaled_modes(cur_mode, scale_p, scale_r, network_bw)

        sorted_mode_list = sorted(scaled_mode_list, key=itemgetter(1), reverse=True)
        selected_item = sorted_mode_list[0]
        selected_mode_obj = selected_item[0]
        selected_block_per_sec = selected_item[1]

        #for sorted_item in sorted_mode_list:
        #    (each_mode, actual_block_per_sec,\
        #    (bottleneck, system_block_per_sec, system_in_mbps, system_out_mbps,\
        #    network_block_per_sec, network_bw)) = sorted_item
        #    diff_str = MigrationMode.mode_diff_str(cur_mode.__dict__, each_mode.mode)
        #    LOG.debug("mode-predict\t(%s) %f %f %s --> %f %f, %f" % (each_mode.mode,
        #                                                             network_bw,
        #                                                             system_out_mbps,
        #                                                             bottleneck,
        #                                                             network_block_per_sec,
        #                                                             system_block_per_sec,
        #                                                             actual_block_per_sec))

        if selected_block_per_sec <= current_block_per_sec:
            return None
        else:
            return selected_item

    def list_scaled_modes(self, cur_mode, scale_p, scale_r, network_bw):
        scaled_mode_list = list()
        for each_mode in self.overlay_mode_list:
            each_p = each_mode.total_p
            each_r = each_mode.total_r
            scaled_each_p = each_p * scale_p
            scaled_each_r = each_r * scale_r

            num_cores = VMOverlayCreationMode.get_num_cores()
            system_block_per_sec, system_in_mbps, system_out_mbps = MigrationMode.get_system_throughput(num_cores, scaled_each_p, scaled_each_r)

            # find the bottleneck
            network_block_per_sec = network_bw*1024*1024/(scaled_each_r*BIT_PER_BLOCK)
            bottleneck = None
            if network_bw < system_out_mbps:
                bottleneck = "network"
                actual_block_per_sec = network_block_per_sec
            else:
                bottleneck = "compute"
                actual_block_per_sec = system_block_per_sec
            id1 = each_mode.get_mode_id()
            id2 = cur_mode.get_mode_id()
            if id1 == id2:
                current_block_per_sec = actual_block_per_sec

            data = (each_mode, actual_block_per_sec,
                    (bottleneck, system_block_per_sec, system_in_mbps, system_out_mbps,
                     network_block_per_sec, network_bw))
            scaled_mode_list.append(data)
            #LOG.debug("mode-predict\t(%s) %f %f %s --> %f %f, %f" % (each_mode.mode,
            #                                                         network_bw,
            #                                                         system_out_mbps,
            #                                                         bottleneck,
            #                                                         network_block_per_sec,
            #                                                         system_block_per_sec,
            #                                                         actual_block_per_sec))
        return scaled_mode_list, current_block_per_sec


    def show_relative_ratio(self, input_mode):
        pivot_mode = self.find_same_mode(self.overlay_mode_list, input_mode)
        comp_list = dict()
        pivot_p = MigrationMode.get_total_P(pivot_mode.block_time)
        pivot_r = MigrationMode.get_total_R(pivot_mode.block_size_ratio)

        for index, other_mode in enumerate(self.overlay_mode_list):
            other_p = MigrationMode.get_total_P(other_mode.block_time)
            other_r = MigrationMode.get_total_R(other_mode.block_size_ratio)
            ratio_p = round(other_p/pivot_p, 4)
            ratio_r = round(other_r/pivot_r, 4)
            mode_diff_str = MigrationMode.mode_diff_str(pivot_mode.mode, other_mode.mode)
            if len(mode_diff_str) == 0:
                mode_diff_str = "original"
            comp_list[other_mode.get_mode_id()] = (ratio_p, ratio_r)
            print "%d: %s\t(%s %s)/(%s %s) --> (%s, %s)" % (index, mode_diff_str[:],  pivot_p, pivot_r, other_p, other_r, ratio_p, ratio_r)
        return comp_list


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
    migration_total_time = 0
    migration_downtime = 0
    for line in lines:
        # see only DEBUG message
        if line.find("DEBUG") == -1:
            continue
        if line.find("profiling") != -1:
            log = line.split("profiling")[1].strip()
            profile_lines.append(log)
        elif line.find("Time for finishing transferring") != -1:
            log = line.split(":")[-1]
            migration_total_time = float(log.strip())
        elif line.find("Finish migration") != -1:
            log = line.split(":")[-1]
            migration_total_time = float(log.strip())
        elif line.find("migration downtime") != -1:
            log = line.split(":")[-1]
            migration_downtime = float(log.strip())

    # process filtered log data
    exp = MigrationMode()
    setattr(exp, 'workload', os.path.basename(workload))
    setattr(exp, 'migration_total_time', migration_total_time)
    setattr(exp, 'migration_downtime', migration_downtime)
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
    speech_exps = list()
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
            msg = "Invalid workload %s" % each_exp.workload
            print msg
            sys.exit(1)
            raise ModeProfileError(msg)
    #if (len(moped_exps) == len(fluid_exps) == len(face_exps) == len(mar_exps)) == False:
    #    msg = "workloads have different experiement size"
    #    print msg
    #    sys.exit(1)
    #    raise ModeProfileError(msg)
    return moped_exps, fluid_exps, face_exps, mar_exps, speech_exps


def select_mediam_exp(exps):
    # sort by compression algorithm gzip 1, .., gzip9, .., lzma1, .., lzma9
    def compare_comp_algorithm(a):
        d = {"xdelta3":3,
             "bsdiff":4,
             "xor":2,
             "none":1}
        return (d[a.mode['DISK_DIFF_ALGORITHM']], -a.mode['COMPRESSION_ALGORITHM_TYPE'], a.mode['COMPRESSION_ALGORITHM_SPEED'])
    selected_exp_list = list()
    exps.sort(key=compare_comp_algorithm)
    result_dict = OrderedDict()
    for each_exp in exps:
        in_data_size = each_exp.stage_size_in['CreateMemoryDeltalist'] + each_exp.stage_size_in['CreateDiskDeltalist']
        in_data_disk = each_exp.stage_size_in['CreateDiskDeltalist']
        in_data_mem = each_exp.stage_size_in['CreateMemoryDeltalist']
        alpha = float(in_data_mem)/in_data_size
        total_p = MigrationMode.get_total_P(each_exp.block_time, alpha)
        total_r = MigrationMode.get_total_R(each_exp.block_size_ratio, alpha)
        out_data_size = each_exp.stage_size_out['CompressProc']
        duration = each_exp.migration_total_time
        est_duration1 = each_exp.stage_time['CreateMemoryDeltalist'] + each_exp.stage_time['CreateDiskDeltalist']+\
            each_exp.stage_time['DeltaDedup'] + each_exp.stage_time['CompressProc']
        key = "%s,%d,%d" % (each_exp.mode['DISK_DIFF_ALGORITHM'], each_exp.mode['COMPRESSION_ALGORITHM_TYPE'],each_exp.mode['COMPRESSION_ALGORITHM_SPEED'])
        value = (in_data_size, out_data_size, duration, 8*float(out_data_size)/1024.0/1024/duration, total_p, total_r, each_exp)
        item_list = result_dict.get(key, list())
        item_list.append(value)
        result_dict[key] = item_list

        #print "%s,%d,%d\t%ld\t%ld\t%f,%f,%f\t%f" %\
        #    (each_exp.mode['DISK_DIFF_ALGORITHM'],\
        #    each_exp.mode['COMPRESSION_ALGORITHM_TYPE'],\
        #     each_exp.mode['COMPRESSION_ALGORITHM_SPEED'],\
        #     in_data_size, out_data_size, duration, est_duration1, float(duration)/est_duration1,
        #     8*float(out_data_size)/1024.0/1024/duration) 

    # chose the median throughput value
    for (key, value_list) in result_dict.iteritems():
        value_list.sort(key=itemgetter(3))
        value_len = len(value_list)
        value = value_list[value_len/2]
        print "%s\t%s\t%s\t%s\t%s\t%s\t%s" % ("\t".join(key.split(",")), value[0], value[1], value[2], value[3], value[4], value[5])
        selected_exp = value[-1]
        selected_exp_list.append(selected_exp)
    return selected_exp_list


def profiling(test_ret_list):
    # how change in mode will affect system performance?
    moped_exps, fluid_exps, face_exps, mar_exps, speech_exps = _split_experiment(test_ret_list)
    comp_list = list()
    filename = "profile.json"
    if moped_exps:
        selected_exp_list = select_mediam_exp(moped_exps)
        ModeProfile.save_to_file(filename, selected_exp_list)
        print "saved at %s" % filename
    if face_exps:
        ModeProfile.save_to_file(filename, face_exps)
        print "saved at %s" % filename
    if mar_exps:
        ModeProfile.save_to_file(filename, mar_exps)
        print "saved at %s" % filename
    if fluid_exps:
        ModeProfile.save_to_file(filename, fluid_exps)
        print "saved at %s" % filename
    if speech_exps:
        ModeProfile.save_to_file(filename, speech_exps)
        print "saved at %s" % filename



if __name__ == "__main__":
    if len(sys.argv) < 3:
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
        cur_block_size = {'CreateMemoryDeltalist': 1, 'CreateDiskDeltalist': 1}
        cur_mode = VMOverlayCreationMode.get_pipelined_multi_process_finite_queue()

        network_bw = 15# mbps

        # get new mode
        item = mode_profile.predict_new_mode(cur_mode, cur_p, cur_r, cur_block_size, network_bw)
        if item is not None:
            (new_mode_obj, actual_block_per_sec, misc) = item
            (bottleneck, system_block_per_sec, system_in_mbps, system_out_mbps, network_block_per_sec, network_bw) = misc
            # print result
            diff_str = MigrationMode.mode_diff_str(cur_mode.__dict__, new_mode_obj.mode)
            diff = MigrationMode.mode_diff(cur_mode.__dict__, new_mode_obj.mode)
            print "change conf: %s\toutput bandwidth(%f, %f), block_per_sec:(%f, %f)" % (diff_str, system_out_mbps, network_bw, actual_block_per_sec, network_block_per_sec)
        else:
            print "do not change"
    elif command == "show":
        mode_profile = ModeProfile.load_from_file(inputfile)
        pivot_mode = VMOverlayCreationMode.get_pipelined_multi_process_finite_queue()
        mode_profile.show_relative_ratio(pivot_mode)
    elif command == "compare":
        another_inputfile = sys.argv[3]
        mode_profile1 = ModeProfile.load_from_file(inputfile)
        mode_profile2 = ModeProfile.load_from_file(another_inputfile)
        pivot_mode = VMOverlayCreationMode.get_pipelined_multi_process_finite_queue()

        mode_ratio_dict1 = mode_profile1.show_relative_ratio(pivot_mode)
        mode_ratio_dict2 = mode_profile2.show_relative_ratio(pivot_mode)
        count_under_threashold = 0
        threshold_percent = 20
        counter = 0
        for mode_id, (ratio_p1, ratio_r1) in mode_ratio_dict1.iteritems():
            item = mode_ratio_dict2.get(mode_id, None)
            if item is None:
                sys.stderr.write("Failed to find matching mode with %s\n" % mode)
            (ratio_p2, ratio_r2) = item
            p_diff = math.fabs(ratio_p1 - ratio_p2)
            r_diff = math.fabs(ratio_r1 - ratio_r2)
            p_diff_percent = (p_diff/ratio_p1*100)
            r_diff_percent = (r_diff/ratio_r1*100)
            if p_diff_percent < threshold_percent and r_diff_percent < threshold_percent:
                count_under_threashold += 1
            print "%d: (%f, %f) - (%f, %f) == %f, %f (%f %f)" % (counter, ratio_p1, ratio_r1, ratio_p2, ratio_r2, p_diff, r_diff, p_diff_percent, r_diff_percent)
            counter += 1
        print "number of item below threashold %d percent: %d out of %d (%f)" % (threshold_percent, count_under_threashold, len(mode_ratio_dict1), (float(count_under_threashold)/len(mode_ratio_dict1)))
    else:
        sys.stderr.write("Invalid command\n")

        sys.exit(1)
