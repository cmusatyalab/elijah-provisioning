#!/usr/bin/env python
import sys
import os
import ast
from pprint import pprint
from collections import defaultdict
from collections import OrderedDict
from operator import itemgetter
import matplotlib.pyplot as plt


stage_names = ["CreateMemoryDeltalist", "CreateDiskDeltalist", "DeltaDedup", "CompressProc"]


class ProfilingError(Exception):
    pass


class Experiment(object):
    def __init__(self):
        pass

    def __repr__(self):
        return "%s,R:%s,P:%s" % (self.workload, self.get_total_R(), self.get_total_P())

    def get_total_P(self):
        # get processing time per block using total time of stages

        #memory_in_size = self.stage_size_in['CreateMemoryDeltalist']
        #disk_in_size = self.stage_size_in['CreateDiskDeltalist']
        #alpha = float(memory_in_size)/(memory_in_size+disk_in_size)

        disk_diff = self.stage_time['CreateMemoryDeltalist']
        memory_diff = self.stage_time['CreateDiskDeltalist']
        delta = self.stage_time['DeltaDedup']
        comp = self.stage_time['CompressProc']

        # should not weight using alpha
        # calculate using p of all stages --> use alpha
        # calculate using time at all stage --> do not use alpha
        total_processing_time = memory_diff+disk_diff+delta+comp 
        total_block = self.block['CreateDiskDeltalist'] + self.block['CreateMemoryDeltalist']
        total_p_block = float(total_processing_time) / total_block
        return total_p_block*1000

    def estimate_total_P(self):
        # estimate processing time per block using block processing time of each stage
        # calculate using p of all stages --> use alpha
        # calculate using time at all stage --> do not use alpha
        memory_in_size = self.stage_size_in['CreateMemoryDeltalist']
        disk_in_size = self.stage_size_in['CreateDiskDeltalist']
        alpha = float(memory_in_size)/(memory_in_size+disk_in_size)
        total_P_from_each_stage = (self.block_time['CreateMemoryDeltalist']*alpha + self.block_time['CreateDiskDeltalist']*(1-alpha))\
            + self.block_time['DeltaDedup'] + self.block_time['CompressProc']
        return total_P_from_each_stage

    def get_total_R(self):
        disk_diff = self.stage_size_in['CreateMemoryDeltalist']
        memory_diff = self.stage_size_in['CreateDiskDeltalist']
        comp = self.stage_size_out['CompressProc']
        total_R = float(comp)/(disk_diff+memory_diff)
        return round(total_R, 4)

    def estimate_total_R(self):
        # weight using input size
        memory_in_size = self.stage_size_in['CreateMemoryDeltalist']
        disk_in_size = self.stage_size_in['CreateDiskDeltalist']
        alpha = float(memory_in_size)/(memory_in_size+disk_in_size)
        total_R_from_each_stage = (self.block_size_ratio['CreateMemoryDeltalist']*alpha +\
                                   self.block_size_ratio['CreateDiskDeltalist']*(1-alpha))\
                                * self.block_size_ratio['DeltaDedup']\
                                * self.block_size_ratio['CompressProc']
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
            log = line.split("profiling")[1].strip()
            profile_lines.append(log)
        elif line.find("Time for finishing transferring") != -1:
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
            block_count = long(log[4])
            exp.block[stage_name] = block_count
            exp.block_size_in[stage_name] = in_size
            exp.block_size_out[stage_name] = out_size
            exp.block_size_ratio[stage_name] = out_size/float(in_size)
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


def _split_experiment(test_ret_list):
    moped_exps = list()
    speech_exps = list()
    fluid_exps = list()
    face_exps = list()
    mar_exps = list()
    random_exps = list()
    delta_exps = list()
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
        elif each_exp.workload.find("random") !=  -1:
            random_exps.append(each_exp)
        elif each_exp.workload.find("delta") !=  -1:
            delta_exps.append(each_exp)
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
    return moped_exps, speech_exps, fluid_exps, face_exps, mar_exps, random_exps, delta_exps

def multikeysort(items, columns):
    comparers = [ ((itemgetter(col[1:].strip()), -1) if col.startswith('-') else (itemgetter(col.strip()), 1)) for col in columns]  
    def comparer(left, right):
        for fn, mult in comparers:
            result = cmp(fn(left), fn(right))
            if result:
                return mult * result
            else:
                return 0
    return sorted(items, cmp=comparer)


def print_bw(exps):
    # sort by compression algorithm gzip 1, .., gzip9, .., lzma1, .., lzma9
    def compare_comp_algorithm(a):
        d = {"xdelta3":3,
             "bsdiff":4,
             "xor":2,
             "none":1}
        return (d[a.mode['DISK_DIFF_ALGORITHM']], -a.mode['COMPRESSION_ALGORITHM_TYPE'], a.mode['COMPRESSION_ALGORITHM_SPEED'])
    exps.sort(key=compare_comp_algorithm)
    result_dict = OrderedDict()
    for each_exp in exps:
        in_data_size = each_exp.stage_size_in['CreateMemoryDeltalist'] + each_exp.stage_size_in['CreateDiskDeltalist']
        in_data_disk = each_exp.stage_size_in['CreateDiskDeltalist']
        in_data_mem = each_exp.stage_size_in['CreateMemoryDeltalist']
        alpha = float(in_data_mem)/in_data_size
        out_data_size = each_exp.stage_size_out['CompressProc']
        duration = each_exp.migration_total_time
        est_duration1 = each_exp.stage_time['CreateMemoryDeltalist'] + each_exp.stage_time['CreateDiskDeltalist']+\
            each_exp.stage_time['DeltaDedup'] + each_exp.stage_time['CompressProc']
        key = "%s,%d,%d" % (each_exp.mode['DISK_DIFF_ALGORITHM'], each_exp.mode['COMPRESSION_ALGORITHM_TYPE'],each_exp.mode['COMPRESSION_ALGORITHM_SPEED'])
        value = (in_data_size, out_data_size, duration, 8*float(out_data_size)/1024.0/1024/duration)
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
        print "%s\t%s\t%s\t%s\t%s" % ("\t".join(key.split(",")), value[0], value[1], value[2], value[3])


def sort_experiment_by_diff_compress(exps):
    def compare_comp_algorithm(a):
        d = {"xdelta3":3,
             "bsdiff":4,
             "xor":2,
             "none":1}
        return (d[a.mode['DISK_DIFF_ALGORITHM']], -a.mode['COMPRESSION_ALGORITHM_TYPE'], a.mode['COMPRESSION_ALGORITHM_SPEED'])
    exps.sort(key=compare_comp_algorithm)
    return exps


def print_bw_block(exps):
    # sort by compression algorithm gzip 1, .., gzip9, .., lzma1, .., lzma9
    exps = sort_experiment_by_diff_compress(exps)
    result_dict = OrderedDict()
    for each_exp in exps:
        in_data_size = each_exp.stage_size_in['CreateMemoryDeltalist'] + each_exp.stage_size_in['CreateDiskDeltalist']
        in_data_disk = each_exp.stage_size_in['CreateDiskDeltalist']
        in_data_mem = each_exp.stage_size_in['CreateMemoryDeltalist']
        alpha = float(in_data_mem)/in_data_size
        out_data_size = each_exp.stage_size_out['CompressProc']
        duration = each_exp.migration_total_time
        est_duration = each_exp.stage_time['CreateMemoryDeltalist'] + each_exp.stage_time['CreateDiskDeltalist']+\
            each_exp.stage_time['DeltaDedup'] + each_exp.stage_time['CompressProc']
        est_duration += 14# serial part
        total_r = each_exp.get_total_R()
        total_p = each_exp.get_total_P()
        total_r_est = each_exp.estimate_total_R()
        total_p_est = each_exp.estimate_total_P()
        key = "%s,%d,%d" % (each_exp.mode['DISK_DIFF_ALGORITHM'], each_exp.mode['COMPRESSION_ALGORITHM_TYPE'],each_exp.mode['COMPRESSION_ALGORITHM_SPEED'])
        value = (in_data_size, out_data_size, duration, est_duration,
                 8*float(out_data_size)/1024.0/1024/duration,
                 total_p, total_r, total_p_est, total_r_est)
        item_list = result_dict.get(key, list())
        item_list.append(value)
        result_dict[key] = item_list

        #print "%s,%d,%d\t%ld\t%ld\t%f,%f,%f\t%f\t%f,%f\t%f,%f" %\
        #    (each_exp.mode['DISK_DIFF_ALGORITHM'],\
        #    each_exp.mode['COMPRESSION_ALGORITHM_TYPE'],\
        #     each_exp.mode['COMPRESSION_ALGORITHM_SPEED'],\
        #     in_data_size, out_data_size, duration, est_duration, float(duration)/est_duration1,
        #     8*float(out_data_size)/1024.0/1024/duration,\
        #     total_p, total_p_est,
        #     total_r, total_r_est)

    # chose the median throughput value
    for (key, value_list) in result_dict.iteritems():
        value_list.sort(key=itemgetter(3))
        value_len = len(value_list)
        (insize, outsize, duration, est_duration, bw, total_p, total_r, total_p_est, total_r_est) = value_list[value_len/2]
        print "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % ("\t".join(key.split(",")),
                                                      insize, outsize, duration,
                                                      est_duration,
                                                      float(duration)/float(est_duration),
                                                      bw, total_p, total_r)

def averaged_value(measure_history, duration):
    avg_p = float(0)
    avg_r = float(0)
    counter = 0
    (cur_time, cur_p, cur_r) = measure_history[-1]
    for (measured_time, p, r) in reversed(measure_history):
        if cur_time - measured_time > duration:
            break
        avg_p += p
        avg_r += r
        counter += 1
    #print "%f measure last %d/%d" % (cur_time, counter, len(measure_history))
    return avg_p/counter, avg_r/counter


def print_p_r_over_time(inputfile):
    lines = open(inputfile, "r").read().split("\n")
    adaptation_log_lines = list()
    mode_test_log_lines = list()
    mode_change_log_lines = list()
    for line in lines:
        # see only DEBUG message
        if line.find("DEBUG") == -1:
            continue
        if line.find("adaptation") != -1:
            # see only profiling message
            log = line.split("adaptation")[1].strip()
            adaptation_log_lines.append(log)
        if line.find("mode-change-test") != -1:
            log = line.split("mode-change-test")[1].strip()
            mode_test_log_lines.append(log)
        if line.find("mode-change") != -1 and line.find("mode-change-test") == -1:
            log = line.split("mode-change")[1].strip()
            mode_change_log_lines.append(log)

    migration_start_time = 0
    iteration_time_list = list()
    p_and_r_list = list()
    measure_history = list()
    time_list = list()
    p_list = list()
    p_list_cur = list()
    r_list = list()
    r_list_cur = list()
    system_in_bw_list = list()
    system_out_bw_list = list()
    system_out_bw_potential_list = list()
    for line in adaptation_log_lines:
        if line.startswith("start time"):
            migration_start_time = float(line.split(":")[-1])
        elif line.startswith("new iteration"):
            iter_time, iter_seq_old, iter_seq_new, iter_mem_size = line.split("\t")[1:]
            iteration_time_list.append((iter_seq_old, iter_time))
        else:
            (time_measured, duration_measured, network_bw_mbps,\
             system_out_bw_actual, system_in_bw_actual,\
             system_out_bw_cur_est, system_in_bw_cur_est,\
             total_p, total_r,\
             total_p_cur, total_r_cur) = line.split("\t")
            p_and_r_list.append((duration_measured, total_p, total_p_cur, total_r, total_r_cur))
            measure_history.append((float(duration_measured), float(total_p_cur), float(total_r_cur)))

            # data for plot
            #time_list.append(float(time_measured)-float(migration_start_time))
            time_list.append(duration_measured)
            p_list.append(float(total_p))
            p_list_cur.append(float(total_p_cur))
            r_list.append(float(total_r))
            r_list_cur.append(float(total_r_cur))
            system_in_bw_list.append(float(system_in_bw_actual))
            system_out_bw_potential_list.append(float(system_out_bw_cur_est))
            system_out_bw_list.append(float(system_out_bw_actual))

    # averaged p and r over time window
    p_list_avg1 = list()
    p_list_avg2 = list()
    r_list_avg1 = list()
    r_list_avg2 = list()
    for index, (duration_measured, total_p_cur, total_r_cur) in enumerate(measure_history):
        avg_p1, avg_r1 = averaged_value(measure_history[0:index+1], 1)
        avg_p2, avg_r2 = averaged_value(measure_history[0:index+1], 5)
        p_list_avg1.append(avg_p1)
        p_list_avg2.append(avg_p2)

    # print result
    print "iter #\tduration\ttime"
    for index, (iter_seq_old, iter_time) in enumerate(iteration_time_list):
        print "%s\t%f\t%s" % (iter_seq_old, (float(iter_time)-float(migration_start_time)), iter_time)
    print "\n\n"

    print "duration\ttotal_p\ttotal_r\tcur_p\tcur_r"
    for values in p_and_r_list:
        (duration_measured, total_p, total_p_cur, total_r, total_r_cur) = values
        print "%s\t%s\t%s\t%s\t%s" % (duration_measured, total_p, total_p_cur, total_r, total_r_cur)

    # plot
    f, (p_plot, r_plot, bw_plot)= plt.subplots(3, 1, sharex=True)
    p_plot.set_title("P - " + inputfile)
    r_plot.set_title("R - " + inputfile)
    bw_plot.set_title("BW - " + inputfile)

    p_plot.set_ylim([0, 1])
    r_plot.set_ylim([0, 1])
    p_plot.plot(time_list, p_list, 'r-', time_list, p_list_cur, 'b-')
    r_plot.plot(time_list, r_list, 'r-', time_list, r_list_cur, 'b-')
    bw_plot.plot(time_list, system_in_bw_list, 'r-', label="System in BW")
    bw_plot.plot(time_list, system_out_bw_list, 'b-', label="System out BW")
    bw_plot.plot(time_list, system_out_bw_potential_list, 'b--', label="System out BW potential")
    #bw_plot.legend(shadow=True, bbox_to_anchor=(0., 1.02, 1., .102), loc=3)
    bw_plot.legend(shadow=True, loc="best", prop={'size':6})
    #plt.show()
    cur_xlim = f.gca().get_xlim()[1]
    p_plot.set_xlim([0, cur_xlim])

    # plot mode change if it exists
    for each_mode_change in mode_change_log_lines:
        if each_mode_change.lower().find("current mode is the best") != -1:
            continue
        mode_change_time = float(each_mode_change.split("\t")[1])
        bw_plot.axvline(x=mode_change_time, linewidth=2, color='k')

    plt.savefig(inputfile+ '.pdf')


if __name__ == "__main__":
    command_list = ["profiling", "over-time"]
    if len(sys.argv) != 3:
        sys.stderr.write("Need command(%s) and input filename\n" % (','.join(command_list)))
        sys.exit(1)
    command = sys.argv[1]
    inputfile = sys.argv[2]

    if command == "profiling":
        test_ret_list = parsing(inputfile)
        moped_exps, speech_exps, fluid_exps, face_exps, mar_exps, random_exps, delta_exps = _split_experiment(test_ret_list)
        #print_bw(face_exps)
        print_bw_block(moped_exps)
    elif command == "over-time":
        print_p_r_over_time(inputfile)
    else:
        pass
        '''
        # how change in mode will affect system performance?
        comparison = defaultdict(list)
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
        '''
