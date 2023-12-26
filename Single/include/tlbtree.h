#ifndef __TLBTREE_H__
#define __TLBTREE_H__

#include "../src/tlbtree_impl.h"

using tlbtree::TLBtreeImpl;

// configure the PMEM file and file size
static constexpr uint64_t POOL_SIZE = 512UL * 1024 * 1024;

class TLBtree {
public:
    TLBtree(std::string tlbname, uint64_t poolsize = POOL_SIZE) {
        bool recover = file_exist(tlbname.c_str());
        tree_ = new TLBtreeImpl<2,2>(tlbname, recover, poolsize);
    }

    ~TLBtree() {
        delete tree_;
    }

    inline void insert(_key_t key, uint64_t val) {
        tree_->insert(key, val);
    }

    inline bool update(_key_t key, uint64_t val) {
        return tree_->update(key, val);
    }

    inline uint64_t lookup(_key_t key) {
        uint64_t val;
        bool found = tree_->find(key, val);

        if(found)
            return val;
        else 
            return 0;
    }

    inline bool remove(_key_t key) {
        return tree_->remove(key);
    }

private:
    TLBtreeImpl <2,2> * tree_;
};

#endif //__TLBTREE_H__