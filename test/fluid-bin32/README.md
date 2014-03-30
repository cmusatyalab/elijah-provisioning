Fluid Simulation
-------------------

* This is binary files of the backend server of the Fluid Simulation

* CREDIT: [Doyub Kim](http://www.doyub.com/) is a primary contributor of this application.

* Fluid Simulation is an interactive fluid dynamics simulation, that renders a
  liquid sloshing in a container on the screen of a phone based on
  accelerometer inputs.  The application backend runs on Linux and performs a
  [smoothed particle hydrodynamics](http://dl.acm.org/citation.cfm?id=1531346)
  physics simulation using 2218 particles, generating up to 50 frames per
  second.  The structure of this application is representative of real-time
  (i.e., not turn-based) games.

* Video demo
  - <a href=https://www.youtube.com/watch?v=f9MN-kvG_ko target="_blank">Using Cloudlet</a>
  - <a href=https://www.youtube.com/watch?v=hWc2fpejfiw target="_blank">Using Amazon EC2 West</a>
  - <a href=https://www.youtube.com/watch?v=aSjQnfkUoU8 target="_blank">Using Amazon EC2 Asia</a>



Installation & Run
---------------

Tested only on Ubuntu 12.04 32bit

  > $ wget https://storage.cmusatyalab.org/cloudlet-app/fluid-server-bin32.tag.gz  
  > $ tar xvfz fluid-server-bin32.tar.gz  
  > $ cd ./fluid-server-bin32/  
  > $ sudo apt-get install libgomp1  
  > $ export LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH  
  > $ ./cloudlet_test -j [number of cores to use]  

