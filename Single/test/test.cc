/*  
    Copyright(c) 2020 Luo Yongping. THIS SOFTWARE COMES WITH NO WARRANTIES, 
    USE AT YOUR OWN RISK!
*/
#include <iostream>
#include <fstream>
#include <cstdint>
#include <cstdlib>
#include <vector>
#include <random>
#include <algorithm>
#include <unistd.h>

#include "tlbtree.h"

using std::cout;
using std::endl;
using std::ifstream;
using std::string;

_key_t * keys;
uint32_t seed;

template <typename BTreeType>
double put_throughput(BTreeType &tree, uint32_t req_cnt) {
    auto start = seconds();
    for(int i = 0; i < req_cnt; i++) {
        _key_t key = keys[i];
        int64_t val = key;
        //cout << "insert " << key + seed << endl;
        tree.insert(key + seed, val);
    }
    auto end = seconds();
    return double(end - start);
}

template <typename BTreeType>
double get_throughput(BTreeType &tree, uint32_t req_cnt) {
    auto start = seconds();
    int64_t val;
    for(int i = 0; i < req_cnt; i++) {
        _key_t key = keys[i];
        bool foundif = tree.find(key, val);
        //cout << key << " " << val << endl;
        if(foundif == false) {
            cout << "Not Found " << key << endl;
        }
    }
    auto end = seconds();
    return double(end - start);
}

template <typename BTreeType>
double del_throughput(BTreeType &tree, uint32_t req_cnt) {
    auto start = seconds();
    int64_t val;
    for(int i = 0; i < req_cnt; i++) {
        cout << "delete " << keys[i] << endl;
        tree.remove(keys[i]);
    }
    auto end = seconds();
    return double(end - start);
}

template <typename BTreeType>
double update_throughput(BTreeType &tree, uint32_t req_cnt) {
    auto start = seconds();
    for(int i = 0; i < req_cnt; i++) {
        _key_t key = keys[i]; 
        //cout << "update " << key << endl;
        bool foundif = tree.update(key, (key * 2));
    }
    auto end = seconds();
    return double(end - start);
}

template <typename BtreeType>
void run_test(int opt_loadid, int opt_scale){
    switch(opt_loadid) {
    case 1: { // test insert
            BtreeType tree(true);
            double dur1 = put_throughput(tree, opt_scale);
            cout << dur1 << endl;
            break;
        }
    case 2: { // test read
            BtreeType tree(true);
            double dur1 = get_throughput(tree, opt_scale);
            cout << dur1 << endl;
            break;
        }
    case 3: { // test update
            BtreeType tree(true);
            double dur1 = update_throughput(tree, opt_scale);
            cout << dur1 << endl;
            break;
        }
    case 4: { // test delete
            BtreeType tree(true);
            double dur1 = del_throughput(tree, opt_scale);
            cout << dur1 << endl;
            break;
        }
    default : { // basic test
            seed = 0; // the tree is empty
            BtreeType tree(false);
            put_throughput(tree, opt_scale);
            get_throughput(tree, opt_scale);
            //del_throughput(tree, opt_scale);
            //get_throughput(tree, opt_scale);
            break;    
        }
    }
}

void print_help(const char * name) {
    cout << "USAGE: "<< name << "[option]" << endl;
    cout << "\t -h: " << "Print the USAGE" << endl;
    cout << "\t -s: " << "scale of the test" << endl;
    cout << "\t -l: " << "The workload of the test (1:input 2:get: 3:update 4:delete other: multiple test)" << endl;
}

int main(int argc, char ** argv) {
    int opt_loadid = 2;
    int opt_scale = KILO;
    
    static const char * optstr = "s:t:l:dh";
    opterr = 0;
    char opt;

    while((opt = getopt(argc, argv, optstr)) != -1) {
        switch(opt) {
        case 's':
            if(atoi(optarg) > 0)
                opt_scale = KILO * atoi(optarg);
            break;
        case 'l':
            if(atoi(optarg) > 0)
                opt_loadid = atoi(optarg);
            break;
        case '?':
        case 'h':
        default:
            print_help(argv[0]);
            exit(-1);
            break;
        }
    }

    if(argc == 1) {
        print_help(argv[0]);
        exit(-1);
    }

    // open the data file
    std::string data_name = "dataset.dat";
    std::ifstream fin(data_name.c_str(), std::ios::binary);
    if(!fin) {
        cout << "File not exists or Open error\n";
        exit(-1);
    }
    // initialization of the test
    keys = new _key_t[opt_scale];
    seed = getRandom(); 
    std::default_random_engine rd(seed);
    std::uniform_int_distribution<uint64_t> dist(0, LOADSCALE * MILLION - opt_scale);
    #ifdef DEBUG
        // seek to the start of the file
        fin.seekg(0, std::ios_base::beg);
    #else
        // seek to a random position in the file
        fin.seekg(dist(rd) * sizeof(_key_t), std::ios_base::beg);
    #endif
    // read data from data file
    fin.read((char *)keys, sizeof(_key_t) * opt_scale);

    cout << "tlbtree" << endl;
    run_test<TLBtree>("/mnt/pmem/tlbtree.pool", opt_loadid, opt_scale);

    fin.close();
    delete keys;
    return 0;
}