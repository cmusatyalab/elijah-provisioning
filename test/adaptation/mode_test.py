#!/usr/bin/env python

import os
import sys
sys.path.insert(0, "../../")

from elijah.provisioning.Configuration import VMOverlayCreationMode
from elijah.provisioning.migration_profile import *
from elijah.provisioning import log as logging
LOG = logging.getLogger(__name__)



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
            print "conf: %s    output bw(%0.2f, %0.2f), block_per_sec:(%0.2f, %0.2f)" % (diff_str, system_out_mbps, network_bw, actual_block_per_sec, network_block_per_sec)
        else:
            print "do not change"
    elif command == "list-all":
        mode_profile = ModeProfile.load_from_file(inputfile)
        #cur_mode = VMOverlayCreationMode.get_pipelined_multi_process_finite_queue()
        cur_mode = VMOverlayCreationMode.get_serial_single_process()
        print cur_mode
        scale_p = 1
        scale_r = 1
        print "conf\t\t\t\tbottleneck by\tsystem BW\tnetwork BW\tblock processing/s"
        for network_bw in xrange(1, 50, 1):
            scaled_mode_list, current_block_per_sec = mode_profile.list_scaled_modes(cur_mode, scale_p, scale_r, network_bw)
            scaled_mode_list.sort(key=lambda item: item[1], reverse=True)
            selected_item = scaled_mode_list[0]
            selected_mode_obj = selected_item[0]
            selected_block_per_sec = selected_item[1]
            if selected_block_per_sec <= current_block_per_sec:
                print "no changes"
            else:
                (new_mode_obj, actual_block_per_sec, misc) = selected_item
                (bottleneck, system_block_per_sec, system_in_mbps, system_out_mbps, network_block_per_sec, network_bw) = misc
                # print result
                diff_str = MigrationMode.mode_diff_str(cur_mode.__dict__, new_mode_obj.mode)
                diff = MigrationMode.mode_diff(cur_mode.__dict__, new_mode_obj.mode)
                print "%s|%s|%0.2f|%0.2f|%0.2f" % (diff_str, bottleneck, system_out_mbps, network_bw, actual_block_per_sec)
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
