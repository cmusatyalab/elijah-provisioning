# nephele

<description> Please visit our website at [Elijah page](http://elijah.cs.cmu.edu/).

Copyright (C) 2011-2020 Carnegie Mellon University

## License

All source code and documentation except modified-QEMU listed below are
under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).

To enable on-demand fetching of the virtual machine, we use a modified-QEMU,
which is distributed under GPLv2.
  - [Repository for modified-QEMU](https://github.com/cmusatyalab/elijah-qemu)
  - [Download link for the binary](https://github.com/cmusatyalab/elijah-qemu/releases/download/cloudlet-v0.9.4/qemu-system-x86_64)

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
[virtman1]: ./images/virtman1.jpg
virt-manager is used for display/keyboard/mouse interaction with virtual machine instances.  You should run virt-manager and add remote connections to each server node so you will be able to see what instances are running there.

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

#### Building

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

You can use the -c and -m option to explicitly set the size the of the VM. By default, the values in [VM_TEMPLATE.xml] will be used. If you do not provide a title with the -t option, the VM will be entitled __nephele-(libvirt UUID)__.


#### Listing

You can list the images nephele is aware of with `nephele-client host image ls` or `nephele-client host images`.

```bash
./nephele-client cloudlet040.elijah.cs.cmu.edu images
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

#### Deleting

You can remove nephele images by using `nephele-client <host> image rm <id>`

__NOTE: Removing an image will render any snapshots that have been built atop that image useless. You should also remember to remove the image from ALL nephele nodes so that handoffs do not fail because the image exists in some places but not others.__

### Snapshots

