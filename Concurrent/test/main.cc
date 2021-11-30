#include <iostream>
#include <fstream>
#include <cstdint>
#include <cstdlib>
#include <vector>
#include <random>
#include <algorithm>
#include <omp.h>

#include "tlbtree.h"

using std::cout;
using std::endl;
using std::ifstream;
using std::string;

template<typename BtreeType>
double run_test(std::vector<QueryType> querys, int thread_cnt) {
    // construct a Btree
    BtreeType tree(true);
    
    std::atomic_int cur_pos(0);
    int small_noise = getRandom() & 0xff; // each time we run, we will insert different keys
    
    // set the timer
    #pragma omp barrier
    auto start = seconds();

    // start the section of parallel 
    #pragma omp parallel num_threads(thread_cnt)
    {
        #pragma omp for schedule(static)
        for (size_t i = 0; i < querys.size(); ++i) {
            // get a unique query from querys
            int obtain_pos = cur_pos.fetch_add(1, std::memory_order_relaxed);
            OperationType op = querys[obtain_pos].op;
            _key_t key = querys[obtain_pos].key;
            _value_t val = (_value_t)key;

            switch (op) {
                case OperationType::READ: {
                    auto val = tree.lookup(key);
                    // if(val == nullptr)
                    //     printf("%lu\n", key);
                    assert(val != nullptr);
                    break;
                }
                case OperationType::INSERT: {
                    tree.insert(key + small_noise, _value_t((uint64_t)val + small_noise));
                    break;
                }
                case OperationType::UPDATE: {
                    auto r = tree.update(key, val);
                    assert(r);
                    break;
                }
                case OperationType::DELETE: {
                    auto r = tree.remove(key);
                    assert(r);
                    break;
                }
                default:
                    std::cout << "Error: unknown operation!" << std::endl;
                    exit(0);
                    break;
            }
        }
    }

    #pragma omp barrier
    auto end = seconds();

    return end - start;
}


int main(int argc, char ** argv) {
    string opt_fname = "../build/workload.txt";
    int opt_num_thread = 1;

    static const char * optstr = "f:t:h";
    opterr = 0;
    char opt;
    while((opt = getopt(argc, argv, optstr)) != -1) {
        switch(opt) {
        case 'f':
            opt_fname = string(optarg);
            break;
        case 't':
            if(atoi(optarg) > 0)
                opt_num_thread = atoi(optarg);
            break;
        case '?':
        case 'h':
        default:
            cout << "USAGE: "<< argv[0] << "[option]" << endl;
            cout << "\t -h: " << "Print the USAGE" << endl;
            cout << "\t -f: " << "Filename of the workload" << endl;
            cout << "\t -t: " << "Number of Threads to excute the workload" << endl;
            cout << "\t -i: " << "The index tree type" << endl;
            exit(-1);
            break;
        }
    }

    ifstream fin(opt_fname.c_str());
    if(!fin) {
        cout << "workload file not openned" << endl;
        exit(-1);
    }
    std::vector<QueryType> querys;
    int op;
    _key_t key;
    while(fin >> op >> key) {
        querys.push_back({(OperationType)op, key});
    }
    double time = run_test<TLBtree>(querys, opt_num_thread);

    cout << time << endl;

    return 0;
}