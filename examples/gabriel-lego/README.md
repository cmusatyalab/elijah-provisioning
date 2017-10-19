# Lego Wearable Cognitive Assistance

Lego a cloudlet-based wearable cognitive assistance application. Its source code
repository is [here](https://github.com/cmusatyalab/gabriel-lego).

This directory contains scripts used to create the base image and the overlay
image for the lego application. The base image and the overlay image can be used
to synthesize the demo application.

The artifacts can be downloaded from:
  * [Ubuntu 14.04 base image](https://storage.cmusatyalab.org/elijah-provisioning-lego/ubuntu-trusty-lego-base.zip)
  * [Overlay image](https://storage.cmusatyalab.org/elijah-provisioning-lego/overlay.zip)

# Steps to create the base and the overlay image
  1. Create the base image:
     1. Download the ubuntu [14.04 cloud image](https://cloud-images.ubuntu.com/trusty/current/trusty-server-cloudimg-amd64-disk1.img). Note the image is in qcow2 format.
     2. Assume the qcow2 image you downloaded is named *trusty-server-cloudimg-amd64-disk1.img* and you want to name your raw disk image *trusty-server-cloudimg-raw.img*. You can convert the qcow2 disk into raw disk with
        ````
        qemu-img convert -f qcow2 -O raw trusty-server-cloudimg-amd64-disk1.img trusty-server-cloudimg-raw.img
        ````
     3. By default, the cloud image can only be logged in though injected ssh-key. In order to boot a vm from the cloud image without being in a cloud environment, we need to modify /etc/cloud/cloud.cfg to enable ubuntu and give it a password. You can do so with following command. cloud.cfg sets the password for user "ubuntu" to be "gabriel".
        ````
        sudo virt-copy-in -a trusty-server-raw.img cloud.cfg /etc/cloud/
        ````
     4. Now we can create a base image for VM synthesis.
        ````
        cloudlet base trusty-server-cloudimg-raw.img
        ````
     4. (Optional) If you want to use start a standalone kvm for debugging purpose, you can use *vm.xml* to create a virtual machine. You need to modify the disk path to correctly point to your raw disk path.
        ````
        virsh create vm.xml
        ````
    
  2. Create the overlay image
     1. Start overlay image creation. You should get a VNC for installing packages after the command.
        ````
        cloudlet overlay <path to trusty-server-cloudimg-raw.img>
        ````
     2. Install and run lego application. In the VNC, issue following commands.
        ````
        wget install_and_run.sh
        bash install_and_run.sh
        ````
     3. When the installation has finished and the lego app has booted up, close the VNC terminal. VM synthesis will will start to create the overlay image after closing the VNC terminal.

# What's in this directory
  * install_and_run.sh: script to install and start lego application
  * vm.xml: libvirt domain file for creating standlone debug vm
  * cloud.cfg: cloud-init configuration file (/etc/cloud/cloud.cfg) to set password "gabriel" for user "ubuntu" and enable console login
