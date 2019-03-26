import sys
import time
from random import randint
import zmq
import argparse
import collections

class Stage():
    def __init__(self, bw=0, len=0):
        self.bw = bw #Mbps
        self.len = len #seconds

def parse_schedule(schedule):
    stages = []
    for s in schedule.strip().split(","):
        tokens = s.split(":")
        stages.append(Stage(int(tokens[0]), int(tokens[1])))
    return stages


def publish(args):
    ctx = zmq.Context.instance()
    publisher = ctx.socket(zmq.PUB)
    publisher.bind("tcp://*:5556")
    # Ensure subscriber connection has time to complete
    time.sleep(1)

    stages = parse_schedule(args.schedule)
    try:
        for stage in stages:
            stage_start = time.time()
            #publish bandwidth of nth stage and then wait for duration
            if(args.verbose):
                print("[PUB] %d Mbps\t- %d seconds" % (stage.bw, stage.len))
            publisher.send_string("bandwidth %d" % stage.bw)
            time.sleep(stage.len)
    except KeyboardInterrupt:
        print "Keyboard interrupt acknowledged."


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", help="Display bandwidth changes.", action="store_true")
    parser.add_argument("-s", "--schedule", default="10:20,100:10,25:5,10:5,25:5,10:15",
        help="Bandwidth schedule in the following format: b1:l1,b2:b2,...bn:ln where bn is bandwidth (Mbps) and ln is length (sec) of the nth stage. Default schedule: 10:20,100:10,25:5,10:5,25:5,10:15")

    args = parser.parse_args()
    print("Publishing using the following schedule: %s" % args.schedule)
    print("[CTRL-C] to stop publishing.")
    publish(args=args)
