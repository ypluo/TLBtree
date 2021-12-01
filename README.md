Implementation of the paper "TLBtree: A Read/Write-Optimized Tree Index for Non-Volatile Memory", 
which is to appear in ICDE 2021.

TLBtree is a read/write-optimized persistent index for NVM-only Memory System. It is composed of a read-optimized top layer and a NVM-friendly write-optimized down layer. The top layer is read frequently, so we adpot a less-mutable and cache-friendly design; The down layer is write frequently and should be carefully guarded by persistent instructions (e.g. clwb and sfence), so we employ a structure with low persistent cost on real NVM enviroment.

Our idea is motivated by the observation that B+-tree-like range indices tend to absorb 99% of writes in the bottom 2-3 levels, while reads are evenly distributed in each level. According to the observation, we propose a Two Layer Persistent Index (TLPI) achitecture first. TLPI divides a pesistent tree index into two layers, so it enables to put together specific optimizations which may be too complicated to coexist in an individual index. TLBtree is a tailored instance of TLPI, you can also add new read optimizations into the top layer and write optimizations to the down layer, for further performance potential.


#### Dependence
We test our project on ubuntu LTS 2020. 
1. This project depends on libpmemobj from [PMDK](https://pmem.io/pmdk/libpmemobj/). Install it using command
    ```shell
    sudo apt-get install make
    sudo apt-get install libpmemobj-dev
    ``` 
2. Make sure you are avaliable with Optane memory (or you can use volatile memory to simulate pmem device on latest ubuntu version). The project assumes that your pmem device is mounted at address `/mnt/pmem` and you have write permission to it.

    (a). Avaiable to real Optane DC Memory, configure it in this [way](https://software.intel.com/content/www/us/en/develop/articles/qsg-part2-linux-provisioning-with-optane-pmem.html).
    
    (b). Not avaiable to Optane, try simluate it with DRAM following this [link](https://software.intel.com/content/www/us/en/develop/articles/how-to-emulate-persistent-memory-on-an-intel-architecture-server.html).


#### Usage
1. Configure your PMEM file address and file size threshold in *include/tlbtree.h*
2. Compile the program with following commands (the same in Single or Concurrent)
    ```sh
    mkdir build
    cd build; cmake ..
    make
    ```
3. Play with TLBtree using provided test modules:
    
    (a). generate data with `datagen` (type `datagen -h` if needed)

    (b). populate the TLBtree with some inital key value pairs

    (c). doing CUID operations with `main`

#### Limitations
Currently TLBtree supports only 8-byte integer key and payload