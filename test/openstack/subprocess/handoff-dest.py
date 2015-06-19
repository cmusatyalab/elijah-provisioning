#!/usr/bin/env python


import os
import subprocess
import sys
import select
import StringIO

sys.path.insert(0, "../../../")
from elijah.provisioning import synthesis
from elijah.provisioning import handoff
from elijah.provisioning.configuration import Const as Cloudlet_Const


def wait_for_proc(proc, print_log=True):
    stdout_buf = StringIO.StringIO()
    while True:
        returncode = proc.poll()
        if returncode is None:
            # keep record stdout
            print("waiting for finishing handoff recv")
            in_ready, _, _ = select.select([proc.stdout], [], [])
            try:
                buf = os.read(proc.stdout.fileno(), 1024*100)
                if print_log:
                    sys.stdout.write(buf)
                stdout_buf.write(buf)
            except OSError as e:
                if e.errno == errno.EAGAIN or e.errno == errno.EWOULDBLOCK:
                    continue
        else:
            # handoff finishes. Read reamining stdout
            in_ready, _, _ = select.select([proc.stdout], [], [], 0.1)
            buf = proc.stdout.read()
            stdout_buf.write(buf)
            break
    returncode = proc.poll()
    if returncode is not 0:
        print("Handoff recv finishes with error: %d" % returncode)
    else:
        print("Handoff recv finishes")
    return stdout_buf.getvalue()


def handoff_launch_vm(base_diskpath, base_mempath,
                      launch_disk, launch_memory,
                      launch_disk_size, launch_memory_size,
                      disk_overlay_map, memory_overlay_map):
    fuse = synthesis.run_fuse(
        Cloudlet_Const.CLOUDLETFS_PATH, Cloudlet_Const.CHUNK_SIZE,
        base_diskpath, launch_disk_size, base_mempath, launch_memory_size,
        resumed_disk=launch_disk,  disk_overlay_map=disk_overlay_map,
        resumed_memory=launch_memory, memory_overlay_map=memory_overlay_map
    )
    synthesized_vm = synthesis.SynthesizedVM(
        launch_disk, launch_memory, fuse,
        disk_only=False, qemu_args=None,
    )

    synthesized_vm.resume()
    synthesis.connect_vnc(synthesized_vm.machine)
    # terminate
    synthesized_vm.monitor.terminate()
    synthesized_vm.monitor.join()
    synthesized_vm.terminate()

    #import pdb;pdb.set_trace()
    #synthesized_vm.start()
    #synthesized_vm.join()

    return synthesized_vm


def main():
    # predefine configuration
    basedisk_path = "/home/stack/.cloudlet/abda52a61692094b3b7d45c9647d022f5e297d1b788679eb93735374007576b8/precise.raw"
    basemem_path = "/home/stack/.cloudlet/abda52a61692094b3b7d45c9647d022f5e297d1b788679eb93735374007576b8/precise.base-mem"
    diskhash_path = "/home/stack/.cloudlet/abda52a61692094b3b7d45c9647d022f5e297d1b788679eb93735374007576b8/precise.base-img-meta"
    memhash_path = "/home/stack/.cloudlet/abda52a61692094b3b7d45c9647d022f5e297d1b788679eb93735374007576b8/precise.base-mem-meta"
    base_vm_paths = [basedisk_path, basemem_path, diskhash_path, memhash_path]
    base_hashvalue = "abda52a61692094b3b7d45c9647d022f5e297d1b788679eb93735374007576b8"
    tmpdir= "/tmp/cloudlet-tmp/"
    if os.path.exists(tmpdir):
        import shutil
        shutil.rmtree(tmpdir)
    os.mkdir(tmpdir)
    launch_diskpath = os.path.join(tmpdir, "launch-disk")
    launch_memorypath = os.path.join(tmpdir, "launch-memory")
    handoff_recv_datafile = os.path.join(tmpdir, "handoff-data")

    # data structure
    handoff_ds_recv = handoff.HandoffDataRecv()
    handoff_ds_recv.save_data(
        base_vm_paths, base_hashvalue,
        launch_diskpath, launch_memorypath
    )
    handoff_ds_recv.to_file(handoff_recv_datafile)

    # launch handoff-server process
    cmd = ["/usr/local/bin/handoff-server-proc", "-d", "%s" % handoff_recv_datafile]
    print("subprocess: %s" % cmd)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, close_fds=True)
    ret_str = wait_for_proc(proc)
    keyword, launch_disk_size, launch_memory_size, disk_overlay_map, memory_overlay_map =\
        ret_str.split("\n")[-1].split("\t")
    if keyword.lower() != "openstack":
        print "error, not valid return string"
        return

    # restore VM
    import pdb;pdb.set_trace()
    synthesized_vm = handoff_launch_vm(
        basedisk_path, basemem_path,
        launch_diskpath, launch_memorypath,
        int(launch_disk_size), int(launch_memory_size),
        disk_overlay_map, memory_overlay_map,
    )


if __name__ == "__main__":
    main()
