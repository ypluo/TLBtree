/*  
    Copyright(c) 2020 Luo Yongping. THIS SOFTWARE COMES WITH NO WARRANTIES, 
    USE AT YOUR OWN RISK!
*/
#include <iostream>
#include <fstream>
#include <cstdint>
#include <cstdlib>
#include <vector>

#include "tlbtree.h"

using std::cout;
using std::endl;
using std::ifstream;

typedef double mytime_t;

_key_t *keys;
PMAllocator * galc;

template <typename BTreeType>
void preload(BTreeType &tree, uint64_t load_size, ifstream & fin) {
    #ifdef DEBUG
        fin.read((char *)keys, sizeof(_key_t) * MILLION);
        for(int i = 0; i < LOADSCALE * KILO; i++) {
            _key_t key = keys[i];
            cout << key << endl;
            tree.insert((_key_t)key, key);
        }
        //tree.printAll();
    #else 
        for(uint64_t t = 0; t < load_size; t++) {
            fin.read((char *)keys, sizeof(_key_t) * MILLION);

            for(int i = 0; i < MILLION; i++) {
                _key_t key = keys[i];
                tree.insert((_key_t)key, _value_t(key));
            }
        }
    #endif

    return ;
}

int main(int argc, char ** argv) {
    // open the data file
    std::string filename = "dataset.dat";
    std::ifstream fin(filename.c_str(), std::ios::binary);
    if(!fin) {
        cout << "File not exists or Open error\n";
        exit(-1);
    }

    // read all the key into vector keys
    keys = new _key_t[sizeof(_key_t) * MILLION];
    
    cout << "tlbtree" << endl;
    TLBtree tree(false);
    preload(tree, LOADSCALE, fin);

    delete keys;
    fin.close();

    return 0;
}
