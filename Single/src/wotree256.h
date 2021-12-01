/*
    log free wbtree with 256B nodesize, nonclass version of wbtree_slotonly
    Copyright(c) Luo Yongping. THIS SOFTWARE COMES WITH NO WARRANTIES, 
    USE AT YOUR OWN RISK!
*/

#ifndef __WOTREE256__
#define __WOTREE256__

#include <cstdint>
#include <cstring>
#include <string>
#include <cstdio>

#include "flush.h"
#include "pmallocator.h"

namespace wotree256 {
using std::string;
constexpr int CARDINALITY = 13;
constexpr int UNDERFLOW_CARD = 4;

struct state_t {
    struct statefield_t { // totally 8 bytes
        uint64_t slotArray       : 52;
        uint64_t count           : 4;
        uint64_t sibling_version : 1;
        uint64_t latch           : 1;
        uint64_t node_version    : 6; 
    };
    // the real data field of state
    union {
        uint64_t pack;      // to do 8 bytes assignment
        statefield_t unpack;// to facilite accessing subfields
    };
    
public:
    state_t(uint64_t s = 0): pack(s) {}

    inline int8_t read(int8_t idx) const {
        uint64_t p = this->unpack.slotArray << 12;
        return (p & ((uint64_t)0xf << ((15 - idx) * 4))) >> ((15 - idx) * 4);
    }

    inline int8_t alloc() {
        int8_t occupy[CARDINALITY] = {0};
        for(int64_t i = 0; i < unpack.count; i++) {
            occupy[read(i)] = 1; 
        }
        for(int i = 0; i < CARDINALITY; i++) {
            if(occupy[i] == 0) return i;
        }
        #ifdef DEBUG
            assert(false);
        #endif
        return CARDINALITY; // never be here
    }
    
    inline uint64_t add(int8_t idx, int8_t slot) {
        state_t new_state(this->pack);

        // update bit fields
        uint64_t p      = this->unpack.slotArray << 12;
        uint64_t mask   = 0xffffffffffffffff >> (idx * 4);
        uint64_t add_value = (uint64_t)slot << ((15 - idx) * 4);
        new_state.unpack.slotArray = ((p & (~mask)) + add_value + ((p & mask) >> 4)) >> 12;
        new_state.unpack.count++;

        return new_state.pack;
    }

    inline uint64_t remove(int idx) { // delete a slot id at position idx
        state_t new_state(this->pack);
        // update bit fields
        uint64_t p      = this->unpack.slotArray << 12;
        uint64_t mask   = 0xffffffffffffffff >> (idx * 4);
        new_state.unpack.slotArray = ((p & ~mask) + ((p & (mask>>4)) << 4)) >> 12;
        new_state.unpack.count--;

        return new_state.pack;
    }

    inline uint64_t append(int8_t idx, int8_t slot) {
        // Append the record at slotid, append the slotArray entry, but DO NOT modify the count
        state_t new_state(this->pack);

        // update bit fields
        uint64_t p      = this->unpack.slotArray << 12;
        uint64_t mask   = 0xffffffffffffffff >> (idx * 4);
        uint64_t add_value = (uint64_t)slot << ((15 - idx) * 4);
        new_state.unpack.slotArray = ((p & (~mask)) + add_value + ((p & mask) >> 4)) >> 12;

        return new_state.pack;
    }
};

class Node {
public:
    // First Cache Line
    state_t state_;      // a very complex and compact state field
    char * leftmost_ptr_;// the left most child of current node
    Record siblings_[2]; // shadow sibling of current node
    // Slots 
    Record recs_[CARDINALITY];

    friend class wbtree;

public:
    Node(bool isleaf = false): state_(0), leftmost_ptr_(NULL) {
        siblings_[0] = {INT64_MAX, NULL};
        siblings_[1] = {INT64_MAX, NULL};
    }

    void *operator new(size_t size) {
        void * ret = galc->malloc(size);
        return ret;
    }

    bool store(_key_t k, _value_t v, _key_t & split_k, Node * & split_node) {
        if(state_.unpack.count == CARDINALITY) { // should split the node
            uint64_t m = state_.unpack.count / 2;
            split_k = recs_[state_.read(m)].key;

            // copy half of the records into split node
            int8_t j = 0;
            state_t new_state = state_;
            if(leftmost_ptr_ == NULL) {
                split_node = new Node();
                for(int i = m; i < state_.unpack.count; i++) {
                    int8_t slotid = state_.read(i);
                    split_node->append(recs_[slotid], j, j);
                    j += 1;
                }

                new_state.unpack.count -= j;
            } else {
                int8_t slotid = state_.read(m);
                split_node = new Node();
                split_node->leftmost_ptr_ = recs_[slotid].val;

                for(int i = m + 1; i < state_.unpack.count; i++) {
                    slotid = state_.read(i);
                    split_node->append(recs_[slotid], j, j);
                    j += 1;
                }

                new_state.unpack.count -= (j + 1);
            }
            split_node->state_.unpack.count = j;
            split_node->state_.unpack.sibling_version = 0;
            // the sibling node of current node pointed by split_node
            split_node->siblings_[0] = siblings_[state_.unpack.sibling_version];
            clwb(split_node, 64); // persist header
            clwb(&split_node->recs_[1], sizeof(Record) * (j - 1)); // persist all the inserted records
            
            // the split node is installed as the shadow sibling of current node
            siblings_[(state_.unpack.sibling_version + 1) % 2] = {split_k, (char *)galc->relative(split_node)};
            // persist_assign the state field
            new_state.unpack.sibling_version = (state_.unpack.sibling_version + 1) % 2;
            
            mfence(); // a barrier here to make sure all the update is persisted to storage

            persist_assign(&(state_.pack), new_state.pack);
            
            // go on the insertion
            if(k < split_k) {
                insertone(k, (char *)v);
            } else {
                split_node->insertone(k, (char *)v);
            }
            return true;
        } else {
            insertone(k, (char *)v);
            return false;
        }
    }

    char * get_child(_key_t k) {
        Record &sibling = siblings_[state_.unpack.sibling_version];

        if(k >= sibling.key) { // if the node has splitted and k to find is in next node 
            Node * sib_node = (Node *)galc->absolute(sibling.val);
            return sib_node->get_child(k);
        }

        if(leftmost_ptr_ == NULL) {
            int8_t slotid = 0;
            for(int i = 0; i < state_.unpack.count; i++) {
                slotid = state_.read(i);
                if(recs_[slotid].key >= k) {
                    break;
                }
            }

            if (recs_[slotid].key == k && state_.unpack.count > 0)
                return recs_[slotid].val;
            else 
                return NULL;
        } else {
            int8_t slotid = 0, pos = state_.unpack.count;
            for(int i = 0; i < state_.unpack.count; i++) {
                slotid = state_.read(i);
                if(recs_[slotid].key > k) {
                    pos = i;
                    break;
                }
            }

            if (pos == 0) // all the key is bigger than k
                return leftmost_ptr_;
            else 
                return recs_[state_.read(pos - 1)].val;
        }
    }

    bool update(_key_t k, _value_t v) {
        uint64_t slotid = 0;
        for(int i = 0; i < state_.unpack.count; i++) {
            slotid = state_.read(i);
            if(recs_[slotid].key >= k) {
                break;
            }
        }

        if (recs_[slotid].key == k) {
            recs_[slotid].val = (char *)v;
            clwb(&recs_[slotid], sizeof(Record));
            return true;
        } else {
            return false;
        }
    }

    bool remove(_key_t k) {
        // Non-SMO delete takes only one clwb 
        Record &sibling = siblings_[state_.unpack.sibling_version];
        if(k >= sibling.key) { // if the node has splitted and k to find is in next node 
            Node * sib_node = (Node *)galc->absolute(sibling.val);
            return sib_node->remove(k);
        }

        if(leftmost_ptr_ == NULL) {
            int8_t idx, slotid;
            for(idx = 0; idx < state_.unpack.count; idx++) {
                slotid = state_.read(idx);
                if(recs_[slotid].key >= k)
                    break;
            }

            if(recs_[slotid].key == k) {
                uint64_t newpack = state_.remove(idx);
                persist_assign(&(state_.pack), newpack);
                return true;
            } else {
                return false;
            }
        } else {
            int8_t idx;
            for(idx = 0; idx < state_.unpack.count; idx++) {
                int8_t slotid = state_.read(idx);
                if(recs_[slotid].key > k) 
                    break;
            }
            /* NOTICE: 
                * We will never remove the leftmost child in our wbtree design
                * So the idx here must be larger than 0
                */
            uint64_t newpack = state_.remove(idx - 1);
            persist_assign(&(state_.pack), newpack);

            return true;
        }
    }

    void print(string prefix, bool recursively) const {
        printf("%s[%lx(%ld) ", prefix.c_str(), state_.unpack.slotArray, state_.unpack.count);

        for(int i = 0; i < state_.unpack.count; i++) {
            printf("%d ", state_.read(i));
        }

        for(int i = 0; i < state_.unpack.count; i++) {
            int8_t slotid = state_.read(i);
            printf("(%ld 0x%lx) ", recs_[slotid].key, (uint64_t)recs_[slotid].val);
        }
        printf("]\n");

        if(recursively && leftmost_ptr_ != NULL) {
            Node * child = (Node *)galc->absolute(leftmost_ptr_);
            child->print(prefix + "    ", recursively);

            for(int i = 0; i < state_.unpack.count; i++) {
                Node * child = (Node *)galc->absolute(recs_[state_.read(i)].val);
                child->print(prefix + "    ", recursively);
            }
        }
    }

    void get_sibling(_key_t & k, Node ** &sibling) {
        Record &sib = siblings_[state_.unpack.sibling_version];
        k = sib.key;
        sibling = (Node **)&(sib.val);
    }

public:
    void insertone(_key_t key, char * right) {
        int8_t idx;
        for(idx = 0; idx < state_.unpack.count; idx++) {
            int8_t slotid = state_.read(idx);
            if(key < recs_[slotid].key) {
                break;
            }
        }
        
        // insert and flush the kv
        int8_t slotid = state_.alloc(); // alloc a slot in the node
        recs_[slotid] = {key, (char *) right};
        clwb(&recs_[slotid], sizeof(Record));
        mfence();

        //write_version++;

        // atomically update the state
        uint64_t new_pack = state_.add(idx, slotid);
        persist_assign(&(state_.pack), new_pack);
    }

    void append(Record r, int8_t slotid, int8_t pos) {
        recs_[slotid] = r;
        state_.pack = state_.append(pos, slotid);
    }

    static void merge(Node * left, Node * right) {
        Record & sibling = left->siblings_[left->state_.unpack.sibling_version];

        state_t new_state = left->state_;
        if(left->leftmost_ptr_ != NULL) { // insert the leftmost_ptr of the right node
            int8_t slotid = new_state.alloc();
            left->append({sibling.key, right->leftmost_ptr_}, slotid, new_state.unpack.count);
            new_state.pack = new_state.add(new_state.unpack.count, slotid);
        }
        for(int i = 0; i < right->state_.unpack.count; i++) {
            int8_t slotid = new_state.alloc();
            left->append(right->recs_[right->state_.read(i)], slotid, new_state.unpack.count);
            new_state.pack = new_state.add(new_state.unpack.count, slotid);;
        }
        
        Record tmp = right->siblings_[right->state_.unpack.sibling_version];
        left->siblings_[(left->state_.unpack.sibling_version + 1) % 2] = tmp;
        new_state.unpack.sibling_version = (left->state_.unpack.sibling_version + 1) % 2;
        
        clwb(left, sizeof(Node)); // persist the whole leaf node

        // persist_assign the state_ field
        mfence();
        left->state_.pack = new_state.pack;
        clwb(left, 64);

        galc->free(right); // WARNING: persistent memory leak here
    }

    void get_lrchild(_key_t k, Node * & left, Node * & right) {
        int16_t i = 0;
        for( ; i < state_.unpack.count; i++) {
            int8_t slotid = state_.read(i);
            if(recs_[slotid].key > k)
                break;
        }

        if(i == 0) {
            left = NULL;
        } else if(i == 1) {
            left = (Node *)galc->absolute(leftmost_ptr_);
        } else {
            left = (Node *)galc->absolute(recs_[state_.read(i - 2)].val);
        }

        if(i == state_.unpack.count) {
            right = NULL;
        } else {
            right = (Node *)galc->absolute(recs_[state_.read(i)].val);
        }
    }
};

extern bool insert_recursive(Node * n, _key_t k, _value_t v, _key_t &split_k, 
                                Node * &split_node, int8_t &level);
extern bool remove_recursive(Node * n, _key_t k);
extern bool find(Node ** rootPtr, _key_t key, _value_t &val);
extern res_t insert(Node ** rootPtr, _key_t key, _value_t val, int threshold);
extern bool update(Node ** rootPtr, _key_t key, _value_t val);
extern bool remove(Node ** rootPtr, _key_t key);
extern void printAll(Node ** rootPtr);

}
#endif // __WOTREE256__