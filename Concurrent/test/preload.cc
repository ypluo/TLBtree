#include <iostream>
#include <fstream>
#include <cstdint>
#include <cstdlib>
#include <vector>
#include <omp.h>

#include "tlbtree.h"

using std::cout;
using std::endl;
using std::ifstream;

typedef double mytime_t;

_key_t *keys;

template <typename BTreeType>
void preload(BTreeType &tree, int64_t load_size, ifstream & fin, int thread_cnt) {
    #pragma omp parallel num_threads(thread_cnt)
    {
        #pragma omp for schedule(static)
        for(int64_t i = 0; i < load_size; i++) {
            _key_t key = keys[i];
            tree.insert((_key_t)key, (_value_t)key);
        }
    }
    return ;
}

int main(int argc, char ** argv) {
    int num_threads = 4;

    if(argc > 1 && atoi(argv[1]) > 0) {
        num_threads = atoi(argv[1]);
    }
    // open the data file
    std::string filename = "dataset.dat";
    std::ifstream fin(filename.c_str(), std::ios::binary);
    if(!fin) {
        cout << "File not exists or Open error\n";
        exit(-1);
    }

    // read all the key into vector keys
    keys = new _key_t[sizeof(_key_t) * LOADSCALE * MILLION];
    fin.read((char *)keys, sizeof(_key_t) * LOADSCALE * MILLION);
    
    cout << "tlbtree" << endl;
    TLBtree tree(false);
    preload(tree, LOADSCALE * MILLION, fin, num_threads);

    delete keys;
    fin.close();

    return 0;
}
