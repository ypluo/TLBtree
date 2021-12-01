#ifndef __TLBTREE_H__
#define __TLBTREE_H__

#include "../src/tlbtree_impl.h"

using tlbtree::TLBtreeImpl;

// configure the PMEM file and file size
static const char * FILE_PATH = "/mnt/pmem/tlbtree.pool";
static constexpr uint64_t POOL_SIZE = 10 * 1024UL * 1024 * 1024;

class TLBtree {
public:
    TLBtree(bool recover) {
        tree_ = new TLBtreeImpl<2,2>(FILE_PATH, recover, POOL_SIZE);
    }

    ~TLBtree() {
        delete tree_;
    }

    inline void insert(_key_t key, _value_t val) {
        tree_->insert(key, val);
    }

    inline bool update(_key_t key, _value_t val) {
        return tree_->update(key, val);
    }

    inline _value_t lookup(_key_t key) {
        _value_t val;
        bool found = tree_->find(key, val);

        if(found)
            return val;
        else 
            return nullptr;
    }

    inline bool remove(_key_t key) {
        return tree_->remove(key);
    }

private:
    TLBtreeImpl <2,2> * tree_;
};

#endif //__TLBTREE_H__