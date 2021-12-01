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

#include "tlbtree.h"

using std::cout;
using std::endl;
using std::ifstream;
using std::string;

template<typename BTreeType>
double run_test(ifstream & fin) {
    int small_noise = getRandom() & 0x3ff; // each time we run, we will insert different keys
    
    auto start = seconds();

    BTreeType tree(true);
    int op_id, notfound = 0;
    _key_t key;
    _value_t val;
    for(int i = 0; fin >> op_id >> key; i++) {
        switch (op_id) {
            case OperationType::INSERT:
                tree.insert(key + small_noise, _value_t(key + small_noise));
                break;
            case OperationType::READ:
                val = tree.lookup(key);
                if(val == nullptr) 
                    notfound++; // optimizer killer
                break;
            case OperationType::UPDATE:
                tree.update(key, _value_t(key * 2));
                break;
            case OperationType::DELETE:
                tree.remove(key);
                break;
            default:
                cout << "wrong operation id" << endl;
                break;
        }
    }
    if(notfound > 0) cout << "somthing not found" << endl; // optimizer killer

    auto end = seconds();
    return double(end - start);
}

int main(int argc, char ** argv) {
    int opt_testid = 1;
    string opt_fname = "workload.txt";

    if(argc > 1) {
        if(file_exist(argv[1]))
            opt_fname = argv[1];
        else
            cout << "workload file "<< argv[1] << " not exist" << endl;
    }

    ifstream fin(opt_fname.c_str());
    double time = 0;
    time = run_test<TLBtree>(fin);
    
    cout << time << endl;
    fin.close();
    return 0;
}