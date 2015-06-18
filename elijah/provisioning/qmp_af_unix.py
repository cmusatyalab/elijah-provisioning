#!/usr/bin/env python
#
# Cloudlet Infrastructure for Mobile Computing
#
#   Author: Kiryong Ha <krha@cmu.edu>
#
#   Copyright (C) 2011-2013 Carnegie Mellon University
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

import sys
import socket
import json
import time
import select


class QmpAfUnix:

    def __init__(self, s_name):
        self.s_name = s_name

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self.s_name)

    def disconnect(self):
        self.sock.close()

    # first we need to negotiate qmp capabilities before
    # issuing commands.
    # returns True on success, False otherwise
    def qmp_negotiate(self):
        # qemu provides capabilities information first
        select.select([self.sock], [], [])
        capabilities = json.loads(self.sock.recv(1024))

        json_cmd = json.dumps({"execute": "qmp_capabilities"})
        self.sock.sendall(json_cmd)
        response = json.loads(self.sock.recv(1024))
        if "return" in response:
            return True
        else:
            return False

    # returns True on success, False otherwise
    def stop_raw_live(self):
        json_cmd = json.dumps({"execute": "stop-raw-live"})
        self.sock.sendall(json_cmd)
        select.select([self.sock], [], [])
        response = json.loads(self.sock.recv(1024))
        if "return" not in response:
            return True

        # wait for QEVENT_STOP in next 10 responses
        try:
            for i in range(10):
                select.select([self.sock], [], [])
                recved_data = self.sock.recv(1024)
                response = json.loads(recved_data)
                if "event" in response and response["event"] == "STOP":
                    timestamp = response["timestamp"]
                    ts = float(timestamp["seconds"]) + \
                        float(timestamp["microseconds"]) / 1000000
                    return ts
        except ValueError as e:
            sys.stderr.write(str(e) + "\n")
            pass

        return None

    # returns True on success, False otherwise
    def iterate_raw_live(self):
        try:
            json_cmd = json.dumps({"execute": "iterate-raw-live"})
            self.sock.sendall(json_cmd)
            select.select([self.sock], [], [])
            recved_data = self.sock.recv(1024)
            response = json.loads(recved_data)
            if "return" in response:
                return True
            else:
                return False
        except Exception as e:
            sys.stderr.write(str(e) + "\n")
            sys.stderr.write(recved_data)
            pass

    # returns True on success, False otherwise
    def randomize_raw_live(self):
        json_cmd = json.dumps({"execute": "randomize-raw-live"})
        self.sock.sendall(json_cmd)
        try:
            recved_data = self.sock.recv(1024)
            response = json.loads(recved_data)
            if "return" in response:
                return True
            else:
                return False
        except Exception as e:
            sys.stderr.write(str(e) + "\n")
            sys.stderr.write(recved_data)
            pass

    # returns True on success, False otherwise
    def unrandomize_raw_live(self):
        json_cmd = json.dumps({"execute": "unrandomize-raw-live"})
        self.sock.sendall(json_cmd)
        select.select([self.sock], [], [])
        response = json.loads(self.sock.recv(1024))
        if "return" in response:
            return True
        else:
            return False

    def stop_raw_live_once(self):
        self.connect()
        ret = self.qmp_negotiate()
        if ret:
            ret = self.stop_raw_live()
        self.disconnect()

        return ret

    def iterate_raw_live_once(self):
        self.connect()
        ret = self.qmp_negotiate()
        ret = self.randomize_raw_live()  # randomize page output order
        if ret:
            print "Randomized page output order"
        else:
            print "Failed to randomize page output order"
        # self.unrandomize_raw_live()  # make page output order sequential
        time.sleep(20)
        if ret:
            print "iterating"
            ret = self.iterate_raw_live()
        if ret:
            time.sleep(10)
            print "iterating"
            ret = self.iterate_raw_live()
        if ret:
            time.sleep(10)
            print "stopping"
            ret = self.stop_raw_live()

        self.disconnect()

        return ret
