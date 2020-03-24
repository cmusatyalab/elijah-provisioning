# nephele

<description> Please visit our website at [Elijah page](http://elijah.cs.cmu.edu/).

Copyright (C) 2011-2020 Carnegie Mellon University

## License

All source code and documentation except modified-QEMU listed below are
under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).

To enable on-demand fetching of the virtual machine, we use a modified-QEMU,
which is distributed under GPLv2.
  - [Repository for modified-QEMU](https://github.com/cmusatyalab/elijah-qemu)
  - [Download link for the binaries](https://owncloud.cmusatyalab.org/owncloud/index.php/s/viEQ29YYOMlkgJm/download)

A copy of this license is reproduced in the [LICENSE](LICENSE) file.

## Tested Platforms

We have tested the nephele server components on __Ubuntu 16.04 LTS 64-bit__.
The nephele client has been tested on __Ubuntu 16.04 LTS 64-bit__ and __Ubuntu 18.04 LTS 64-bit__.
Both client and server require __Python 2.7.x__.

## Client Installation

__Your client's ssh key must reside in root's authorized_keys file on each nephele node. You can use `ssh-keygen` to create a public key if you need to and then use `ssh-copy-id root@<node>` to push it to the server nodes, assuming you can authenticate to them.__

```bash
sudo apt install git ansible
git clone -b nephele https://github.com/cmusatyalab/elijah-provisioning
cd elijah-provisioning/ansible
ansible-playbook client.yml --ask-sudo-pass
```

## Server Installation

## Usage

[virtman-main]: http://virt-manager.org/wp-content/uploads/2013/04/virt-manager-vm-list.png "virt-manager"
[virtman-add]: http://virt-manager.org/wp-content/uploads/2013/04/virt-manager-new-connection.png "virt-manager"

[virt-manager](http://virt-manager.org) is used for display/keyboard/mouse interaction with virtual machine instances.  You should run virt-manager and add __remote__ connections to each nephele server node in order to see what instances are running there and interact with them.

![Adding a remote connection][virtman-add]

While nephele VMs can be viewed/interacted with via virt-manager just like normal KVM/QEMU instances...

![virt manager][virtman-main]

...they have to be managed by using nephele-client which has the following command-line interface:

```bash
$ nephele-client
usage: nephele [-h] host {handoff,image,images,ps,run,snapshot,snapshots} ...

Manage VMs across cloudlets.

positional arguments:
  host                  Execute commands on specified remote host.

optional arguments:
  -h, --help            show this help message and exit

command:
  {handoff,image,images,ps,run,snapshot,snapshots}
    handoff             Handoff a running instance to another nephele node
    image               Management of base VM images
    images              List base images tracked by nephele
    ps                  List running nephele instances
    run                 Instantiate a VM from an existing snapshot
    snapshot            Management of VM snapshots
    snapshots           List snapshots known to nephele
usage: nephele [-h] host {handoff,image,images,ps,run,snapshot,snapshots} ...
nephele: error: too few arguments
```

### Image Management

#### Building Images

To launch virtual machines using nephele, you first need to have images to work with. To build a nephele compatible image, you use the `nephele <host> image build` command. This will lauch and instance of the image, boot into the OS, and then allow you to customize it to your liking. When you have reached the point at which you want to generate a nephele image, just pause the VM in virt-manager using the menu or the pause icon. After the VM is paused, nephele will take the precise memory and disk state and copy it to /var/nephele/images.

```bash
$ nephele-client localhost image build -h
usage: nephele host image build [-h] [-t TITLE] [-c CPU] [-m MEM] path

positional arguments:
  path                  Path to an existing base image (will be converted to
                        raw format)

optional arguments:
  -h, --help            show this help message and exit
  -t TITLE, --title TITLE
                        Title of the VM instance to display in virt-manager
  -c CPU, --cpu CPU     Override number of vCPUs. By default, the number
                        specified in the VM_TEMPLATE.xml will be used.
  -m MEM, --mem MEM     Override amount of memory (in KiB). By default, the
                        number specified in the VM_TEMPLATE.xml will be used.
```

You can use the -c and -m option to explicitly set the size the of the VM. By default, the values in [VM_TEMPLATE.xml](elijah/provisioning/config/VM_TEMPLATE.xml) will be used. If you do not provide a title with the -t option, the VM will be entitled __nephele-(libvirt UUID)__.

#### Listing Images

You can list the images nephele is aware of with `nephele-client host image ls` or `nephele-client host images`.

```bash
./nephele-client localhost images
IMAGE ID            NAME (/var/nephele/images/)                        SOURCE
edbe05049453        win10.qcow2                                        /root/win10_base.zip
ee4720c0ca33        Win7.img                                           /root/win7horizon.zip
d7d501d19f06        ubuntu1804-12vcpu.img                              /root/ubuntu1804-12cpu.zip
```

#### Export/Import

To migrate an instance from one node to another using VM handoff, you will need to have the base image installed on each nephele server. To do this, you can export the image from the node that it was built on, and import it on the other nodes.

For example:

```bash
$ nephele-client nodeA image export edbe05049453 /root/gold_image.zip
(scp /root/gold_image.zip nodeB:/root)
$ nephele-client nodeB image import /root/gold_image.zip
```

#### Deleting Images

You can remove nephele images by using `nephele-client <host> image rm <id>`

__NOTE: Removing an image will render any snapshots that have been built atop that image useless. You should also remember to remove the image from ALL nephele nodes so that handoffs do not fail because the image exists in some places but not others.__

### Snapshots

#### Building Snapshot

Snapshots are customized memory/disk deltas with respect to a base image. You must build a snapshot before you can launch (and subsequently handoff) a VM instance. Building a snapshot follows a similar process to building an image: you launch a base image using `nephele-client <host> snapshot build <image id> <destination path>`, customize it until you are satisfied, and then you pause the VM in virt-manager using the menu or the pause icon. You can specify a title with the -t option so it is easier to find in virt-manager.

```bash
$ ./nephele-client localhost snapshot build -h
usage: nephele host snapshot build [-h] [-t TITLE] [-d] [-m] [-n] [-z] id dest

positional arguments:
  id                    an existing base image to start with
  dest                  Destination path for snapshot

optional arguments:
  -h, --help            show this help message and exit
  -t TITLE, --title TITLE
                        Title of the VM instance to display in virt-manager
  -d, --disk-only       Ignore memory, create delta from disk blocks only.
                        (default=False)
  -m, --free-mem        Extract free memory (default=False)
  -n, --no-trim         Disable TRIM support. (default=False)
  -z, --zip             Package snapshot files into a single zip. (default=True)
  ```

#### Listing Snapshots

You can list the snapshots that a nephele node is aware of with `nephele-client host snapshot ls` or `nephele-client host snapshots`.

__NOTE: Snapshots only reside on the nephele node in which they were built. If you wish to launch an instance of a snapshot from another host, you will need to scp that snapshot first.__

```bash
$ ./nephele-client localhost snapshots
SNAPSHOT                                           IMAGE ID            CREATED
/root/t2.zip                                       edbe05049453        2020-02-28 15:23:42.956169
/root/rpcbuild.zip                                 edbe05049453        2020-03-03 08:49:12.872102
/root/rpcbuild_1.zip                               edbe05049453        2020-03-03 14:22:10.750467
```

#### Deleting Snapshots

You can remove nephele snapshots by using `nephele-client <host> snapshot rm <path>`.

__NOTE: This will not affect the base image that the snapshot was built upon.__

#### Iteratively Building Snapshots

You can create iterative snapshots by launching an instance of an existing snapshot, interacting with the VM, and then pausing it in virt-manager using the menu or the pause icon. This will create a snapshot in the form *original_snapshot_name_#* where # is the next sequential number on disk, starting with 1. For example, if you launched an instance with `nephele-client localhost run /root/my_snapshot.zip mysnapshot` and installed some security updates and then paused the VM, a snapshot would be created called *__*/root/my_snapshot_1.zip*.

__NOTE: Keep in mind that both my_snapshot.zip and my_snapshot1.zip are built upon the same base image; snapshots are flat deltas and are not hierarchical.__

### Running VM Instances

Once nephele has some images and snapshots to work with, we can launch virtual machine instances that are restored to the exact state we left them in when those snapshots were created. This is done with `nephele-client <host> run <snapshot path> <title>`. The -p option can be used to expose host ports to the VM instance. For instance, if you needed to be able to remote into the VM over the RDP protocol, you could pass -p 3389 to forward the host 3389 traffic onto the VM.

__NOTE: A port can only be bound to a single VM instance, so any attempts to launch a subsequent VM that needs any port that is already being forwarded to an existing instance will fail. This applies whether the VM is launched initially on that node, or was the result of a handoff from another nephele node.__

```bash
$ ./nephele-client localhost run -h
usage: nephele host run [-h] [-d] [-p PORTS] snapshot title

positional arguments:
  snapshot              Disk/memory snapshot to instantiate from
  title                 Title of the VM instance to display in virt-manager

optional arguments:
  -h, --help            show this help message and exit
  -d, --disk-only       Ignore memory, create delta from disk blocks only.
                        (default=False)
  -p PORTS, --ports PORTS
                        Comma separated list of ports to forward from host to
                        guest. -p 80,443,8080,8443
```

The run command is an asynchronous operation. After a few seconds, barring any errors (such as trying to launch a snapshot that doesn't exist), the client will return some information about the VM that is about to be instantiated. There will be a delay between the time the information is reported and the instance actually appears inside of virt-manager so that it can be interacted with. This is due to the snapshot being decompressed and the delta being applied to the base image which returns the VM instance to the previous memory/disk state.

```bash
$ ./nephele-client host run /root/demo.zip test
HOST            host
TITLE           test
SNAPSHOT        /root/demo.zip
FWDPORTS        None
VM should be resumed shortly; check the virt-manager window.
```

If you are finished interacting with the VM, you can shut it down from within the OS, or by using virt-manager (either via the menu or by using the power icon).

As mentioned above, if you want to generate a snapshot, simply pause the VM in virt-manager.

If you want to continue working, but wish to migrate the VM to another nephele node, you can use the handoff command...

### VM Handoff

VM Handoff is similar to classical VM (KVM/QEMU) migration , but it is optimized for migration across WAN connected nodes rather than at typical datacenter bandwidths. While in the datacenter, we can expect 10-40Gpbs, the average WAN bandwidth in the US is on the order of 20Mbps. Thus VM handoff optimizes for shortest total migration time versus shortest downtime.  We aggressively employ deduplication and compression to reduce the amount of data that needs to be shipped across the (relatively) poor links.

In order to handoff an instance, we need to supply either the title or the UUID of the running instance, as well as the destination node. If the name you supply doesn't match the title or UUID of any running instance you will get error. If your supplied title is ambigious, because there are more than one running instance with the same title, you will receive an error indicating you try the instances UUID instead. The UUID can be used by running the ps command which is outlined in the next section.

```bash
$ ./nephele-client nodeA handoff test nodeB
Handoff initiated for test [e8690455-c9e6-4c7a-9e52-e918da807eac] to the following destination: tcp://nodeB:8022
```

Even though a handoff has been initiated, you can continue interacting with the desktop until it is suspended. The suspension takes place once the amount of new dirty state that needs to be transfered to the destination is below some threshold. At this point, the VM is stopped at the source and the final bytes are sent to the destination where they will be reassembled against the base image at the destination. This period is called the downtime window. Once the VM is resumed at the destination, you can once again interact with it via virt-manager (although it is now running on a different host).

### Listing Running Instances

The `nephele-client <host> ps` command will display details about any running instances on a particular node. The PID, title, UUID (once resumed and assigned by libvirt), start time, and a handoff url (if handoff has been initiated) are displayed for each instance.

```bash
$ ./nephele-client localhost ps
PID    TITLE           PORTS        UUID                                 STARTED             HANDOFFURL
89268  test2           None         5d2bee02-48c6-4301-9dda-1618b040dcbd 2020-03-19 15:34:21 tcp://nodeB:8022
106642 test0           None         c3d52a9a-5bd1-44f8-9763-0059b71a00dc 2020-03-19 15:21:22 None
109359 test1           9000         66ea5fdb-d9ba-43e8-afcc-6ff8d6bb2ed2 2020-03-19 15:31:37 None
```

### Analysis

The main nephele log file can be found at `/var/nephele/nephele.log`. It contains the actions from any client calls as well as the machinations of the instances running on that particular nephele node.

Individual logs pertaining to handoffs can be found in the `/var/nephele/logs` directory on the destination node; the filenames follow the format `handoff_from_<ip>_at_<timestamp>.[log|stats]`. The handoff log file is a rather verbose file that details all of the memory and disk blocks that were transferred during the handoff and how they were applied. The handoff stats file presents a synopsis of migration that includes the total migration time, amount of data sent over the wire, and a breakdown of the disk/memory state that was transferred.

```bash
Iterations: 1
Elapsed Time: 129.345255 seconds
VM Disk Size: 25600 MB
VM Memory Size: 9024 MB


Blobs Received: 292
Total MBytes Received over wire: 146.63


VM STATE (MBytes)
                    DIRTIED     BY VALUE    BY REF      ZEROS
MEMORY              1876.41     514.30      397.12      964.99
DISK                186.96      77.52       101.62      7.82
```

In the above example, the handoff of a 25GB disk/8GB RAM virtual machine took just over 2 minutes. Though there was nearly 2GB of dirty state between memory and disk, about 1/4 of it was shipped by reference as it could already be found at the destination. In addition, about 1/2 of dirty state was zero blocks which can be highly compressed before transfer. As a result, the total amount of data shipped across the wire was roughly 150MB, a small fraction of the amount of total dirty state.

Additionally, you can enable a live 'heatmap' view of the handoff by setting `Const.PRODUCE_HEATMAP_IMAGES  = True` in [configuration.py](elijah/provisioning/configuration.py#L58) on each nephele server. This will expose a webpage at `http://<node>/heatmap/index.html` that will show a layout of both memory and disk regions along with some statistics. As the handoff progresses, this page on the destination host will be periodically updated to show the disk/memory regions that are being dirtied. This page uses [OpenSeadragon](https://openseadragon.github.io/) to support zooming into more granular regions within each image (memory and disk). The more times a particular block gets dirtied, the brighter (whiter) it gets.
