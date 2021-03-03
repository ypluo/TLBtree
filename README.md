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
Follow these steps to play with TLBtree.
1. Compile the program use `make` command. It will generate four excutable files: *datagen, test, preload, and main*. Run each excuatable file with -h option for help info.
2. Use *datagen* to generate different types of dataset and workload.
3. Use *preload* to pre-build a persistent index, upon which we run the later CRUD operations.

4. Use *test* to do basic function test of TLBtree or use *main* to evaluate the performance of TLBtree. 

#### Dataset Scale
The default configuration hard-codes the dataset scale (LOADSCALE) and index file size (POOLSIZE, proportional to LOADSCALE) into file `include/common.h`. Change it for different scale.

We presume that your machine is not capable of issuing clwb and clflushopt instrutions. If not, change the FLUSH_FLAG variable in makefile into -DCLWB and -DCLFLUSHOPT, respectively.