#!/usr/bin/env python 

import os
import sys
import json
import matplotlib.pyplot as plt



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


def plot_values(measure_history, title, p_plot, r_plot):
    avg_p = 0.0
    avg_r = 0.0

    t = list()
    cur_p_list = list()
    avg_p_list = list()
    avg1_p_list = list()
    avg2_p_list = list()

    cur_r_list = list()
    avg_r_list = list()
    avg1_r_list = list()
    avg2_r_list = list()

    for index, (time_measure, cur_p, cur_r) in enumerate(measure_history):
        avg_p1, avg_r1 = averaged_value(measure_history[0:index+1], 1)
        avg_p2, avg_r2 = averaged_value(measure_history[0:index+1], 2)
        avg_p += cur_p
        avg_r += cur_r
        print "%s\t%0.4f\t%0.4f\t%0.4f\t%0.4f" % (time_measure, cur_p, avg_p1, avg_p2, (avg_p/(index+1)))

        t.append(time_measure)
        avg_p_list.append(avg_p/(index+1))
        cur_p_list.append(cur_p)
        avg1_p_list.append(avg_p1)
        avg2_p_list.append(avg_p2)

        avg_r_list.append(avg_r/(index+1))
        cur_r_list.append(cur_r)
        avg1_r_list.append(avg_r1)
        avg2_r_list.append(avg_r2)

    p_plot.set_title("P - " + title)
    r_plot.set_title("R - " + title)
    p_plot.plot(t, avg_p_list, 'r-', t, avg1_p_list, 'b-', t, avg2_p_list, 'y-')
    r_plot.plot(t, avg_r_list, 'r-', t, avg1_r_list, 'b-', t, avg2_r_list, 'y-')


def plot_workload(dirname):
    memory_filename = "%s/pr-history-memory" % dirname
    disk_filename = "%s/pr-history-disk" % dirname
    comp_filename = "%s/pr-history-comp" % dirname
    memory_list = json.loads(open(memory_filename).read())
    disk_list = json.loads(open(disk_filename).read())
    comp_list = json.loads(open(comp_filename).read())

    f, axarr = plt.subplots(3, 2, sharex=True)
    axarr[0, 0].set_ylim([0, 1])
    axarr[1, 0].set_ylim([0, 1])
    axarr[2, 0].set_ylim([0, 1])
    axarr[0, 1].set_ylim([0, 1])
    axarr[1, 1].set_ylim([0, 1])
    axarr[2, 1].set_ylim([0, 1])
    plot_values(memory_list, memory_filename, axarr[0, 0], axarr[0, 1])
    plot_values(disk_list, disk_filename, axarr[1, 0], axarr[1, 1])
    plot_values(comp_list, comp_filename, axarr[2, 0], axarr[2, 1])
    plt.show()


if __name__ == "__main__":
    #plot_workload("moped-pr-history")
    #plot_workload("speech-pr-history")
    plot_workload("face-pr-history")
    #plot_workload("mar-pr-history")

