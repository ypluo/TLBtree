/*  fixtree.h - A search-optimized fixed tree
    Copyright(c) 2020 Luo Yongping. THIS SOFTWARE COMES WITH NO WARRANTIES, 
    USE AT YOUR OWN RISK!
*/

#ifndef __FIXTREE__
#define __FIXTREE__

#include <cstdio>
#include <cstdint>
#include <cmath>
#include <vector>
#include <cassert>
#include <cstring>

#include "flush.h"
#include "pmallocator.h"

namespace fixtree {
    /*  Fixtree: a linearize tree structure which can absort moderate insertions: 
        This tree is intended for search optimized so insertion may not be good
    */
    using _key_t = int64_t;

    const int INNER_CARD = 32; // node size: 256B, the fanout of inner node is 32
    const int LEAF_CARD = 16;  // node size: 256B, the fanout of leaf node is 16
    const int LEAF_REBUILD_CARD = 4;
    const int MAX_HEIGHT = 10;

    struct entrance_t {
        void * buff;
        uint32_t height;
        uint32_t level_offset[MAX_HEIGHT];
    };

class Fixtree {
    public:
        struct INNode { // inner node is packed keys, which is very compact
            _key_t keys[INNER_CARD];
        } __attribute__((aligned(CACHE_LINE_SIZE)));

        struct LFNode { // leaf node is packed key-ptr along with a header. 
                        // leaf node has some gap to absort insert
            _key_t keys[LEAF_CARD];
            void * vals[LEAF_CARD];
        } __attribute__((aligned(CACHE_LINE_SIZE)));

    public:
        // linearized array for inner node and leaf node
        INNode * inner_nodes_;
        LFNode * leaf_nodes_;

        // Note: auxiliary structure
        uint32_t height_;
        uint32_t level_offset_[MAX_HEIGHT]; // Nodes in level L <=> inner_nodes_[ level_offset_[L] : level_offset_[L - 1]]
        /* 
            Why we need a level_offset_ array?
            We assume the inner tree to be a non-complete k-ary tree, which means that 
            we can be more flexible than a complete tree. the leaf nodes needn't be power-of-k.

            To make that possible, we need to maintain a offset array that record the base address
            of each tree level in the inner nodes array
        */
    public:
        Fixtree(entrance_t * ent, bool empty = false) { // build the tree from given entrance
            inner_nodes_ = (INNode *)galc->absolute(ent->buff);
            leaf_nodes_ = (LFNode *)(inner_nodes_ + ent->level_offset[0]);
            height_ = ent->height;
            memcpy(&(level_offset_[0]), &(ent->level_offset[0]), sizeof(uint32_t) * MAX_HEIGHT);

            if(empty) { // the tree contains no record
                inner_nodes_[0].keys[0] = 0;
                for(int i = 1; i < INNER_CARD; i++)
                    inner_nodes_[0].keys[i] = INT64_MAX;
                
                for(int i = 0; i < LEAF_CARD; i++) {
                    leaf_nodes_[0].keys[i] = INT64_MAX;
                }
            }
        }

        Fixtree(std::vector<Record> records) { // build a tree from recocds 
            height_ = 0;
            const int lfary = LEAF_REBUILD_CARD;

            // caculate the leaf node count
            int lfnode_cnt = std::ceil((float)records.size() / lfary);
            // caculate the inner node count and tree height
            int innode_cnt = 0;
            int cur_level_cnt = lfnode_cnt; // inner node count of current level
            while((float)cur_level_cnt / INNER_CARD > 1) {
                height_ += 1;
                cur_level_cnt = std::ceil((float)cur_level_cnt / INNER_CARD);
                innode_cnt += cur_level_cnt;
            }
            // along with the root node
            height_ += 1;
            innode_cnt += 1;

            // alloate the inner node and leaf node space
            void * buff = galc->malloc(std::max((size_t)4096, innode_cnt * sizeof(INNode) + lfnode_cnt * sizeof(LFNode)));
            inner_nodes_ = (INNode *)buff;
            leaf_nodes_ = (LFNode *) (inner_nodes_ + innode_cnt);
            
            // fill the leaf nodes
            for(int i = 0; i < records.size(); i++) {
                leaf_nodes_[i / lfary].keys[i % lfary] = records[i].key;
                leaf_nodes_[i / lfary].vals[i % lfary] = records[i].val;
            }
            clwb(leaf_nodes_, sizeof(LFNode) * lfnode_cnt);
            
            // fill last level inner node
            for(int i = 0; i < lfnode_cnt; i++) { // extract the first key of each leaf node
                inner_insert(i / INNER_CARD, i % INNER_CARD, leaf_nodes_[i].keys[0]);
            }
            if(lfnode_cnt % INNER_CARD != 0) // mark the end of the inner node
                inner_insert(lfnode_cnt / INNER_CARD, lfnode_cnt % INNER_CARD, INT64_MAX);
            
            // fill other inner node
            cur_level_cnt = lfnode_cnt; // inner node count of current level
            level_offset_[height_] = 0;
            for(int l = height_ - 1; l > 0; l--) {
                cur_level_cnt = std::ceil((float)cur_level_cnt / INNER_CARD);

                level_offset_[l] = level_offset_[l + 1] + cur_level_cnt;
                for(int i = 0; i < cur_level_cnt; i++) { // extract the first key of each inner node of current level
                    inner_insert(level_offset_[l] + i / INNER_CARD, i % INNER_CARD,
                        inner_nodes_[level_offset_[l + 1] + i].keys[0]);
                }
                if(cur_level_cnt % INNER_CARD != 0)
                    inner_insert(level_offset_[l] + cur_level_cnt / INNER_CARD, cur_level_cnt % INNER_CARD, INT64_MAX);
            }
            level_offset_[0] = innode_cnt;

            clwb(buff, sizeof(INNode) * innode_cnt);
            mfence();
        }

    public:
        char ** find_lower(_key_t key) { 
        /* linear search, return the position of the stored value */
            int cur_idx = level_offset_[1];
            for(int l = 1; l < height_; l++) {
                #ifdef DEBUG
                    INNode * inner = inner_nodes_ + cur_idx; 
                #endif
                cur_idx = level_offset_[l + 1] + (cur_idx - level_offset_[l]) * INNER_CARD + inner_search(cur_idx, key);
            }
            
            cur_idx = (cur_idx - level_offset_[height_]) * INNER_CARD + inner_search(cur_idx, key);

            return leaf_search(cur_idx, key);
        }

        bool insert(_key_t key, _value_t val) {
            int cur_idx = level_offset_[1];
            for(int l = 1; l < height_; l++) {
                #ifdef DEBUG
                    INNode * inner = inner_nodes_ + cur_idx; 
                #endif
                cur_idx = level_offset_[l + 1] + (cur_idx - level_offset_[l]) * INNER_CARD + inner_search(cur_idx, key);
            }
            cur_idx = (cur_idx - level_offset_[height_]) * INNER_CARD + inner_search(cur_idx, key);
            
            LFNode * cur_leaf = leaf_nodes_ + cur_idx;

            for(int i = 0; i < LEAF_CARD; i++) {
                if (cur_leaf->keys[i] == INT64_MAX) { // empty
                    leaf_insert(cur_idx, i, {key, (char *)val});
                    return true;
                }
            }
            return false;
        }

        bool try_remove(_key_t key, bool & need_rebuild) {
            int cur_idx = level_offset_[1];
            for(int l = 1; l < height_; l++) {
                #ifdef DEBUG
                    INNode * inner = inner_nodes_ + cur_idx; 
                #endif
                cur_idx = level_offset_[l + 1] + (cur_idx - level_offset_[l]) * INNER_CARD + inner_search(cur_idx, key);
            }
            cur_idx = (cur_idx - level_offset_[height_]) * INNER_CARD + inner_search(cur_idx, key);
            
            LFNode * cur_leaf = leaf_nodes_ + cur_idx;

            _key_t max_leqkey = -1;
            int8_t max_leqi = -1;
            int8_t rec_cnt = 0;
            for(int i = 0; i < LEAF_CARD; i++) {
                if(cur_leaf->keys[i] < INT64_MAX) {
                    rec_cnt += 1;
                    if (cur_leaf->keys[i] <= key && cur_leaf->keys[i] > max_leqkey) {
                        max_leqkey = cur_leaf->keys[i];
                        max_leqi = i;
                    }
                }
            }

            /* There are three cases:
                  1. | k1 | --- | kx |, delete k1, leaf is not empty if k1 is deleted (fail)
                  2. | k1 | ---      |, delete k1, leaf is empty if k1 is deleted (success)
                  3. | k1 | --- | kx |, delete kx, leaf is not empty if kx is deleted (success)
            */
            if(max_leqi == 0 && rec_cnt > 1) { // case 1
                need_rebuild = false;
                return false;
            } else if (max_leqi == 0) { // case 2
                persist_assign(&(cur_leaf->keys[0]), INT64_MAX);
                
                need_rebuild = true;
                return true;
            } else { // case 3
                persist_assign(&(cur_leaf->keys[max_leqi]), INT64_MAX);

                need_rebuild = false;
                return true;
            }
        }

        char ** find_first() {
            return (char **)&(leaf_nodes_[0].vals[0]);
        }

        void printAll() {
            for(int l = 1; l <= height_; l++) {
                printf("level: %d =>", l);
                
                for(int i = level_offset_[l]; i < level_offset_[l - 1]; i++) {
                    inner_print(i);
                }
                printf("\n");

                if(l == height_) {
                    int m = 0;
                    for(int i = level_offset_[l]; i < level_offset_[l - 1]; i++) {
                        for(int j = 0; j < INNER_CARD; j++) {
                            if(inner_nodes_[i].keys[j] == INT64_MAX) {
                                break;
                            }
                            leaf_print(m++);
                        }
                    }
                }
            }
        }

    private:
        int inner_search(int node_idx, _key_t key) const{
            INNode * cur_inner = inner_nodes_ + node_idx;
            for(int i = 0; i < INNER_CARD; i++) {
                if (cur_inner->keys[i] > key) {
                    return i - 1;
                }
            }

            return INNER_CARD - 1;
        }
        
        char **leaf_search(int node_idx, _key_t key) const {
            LFNode * cur_leaf = leaf_nodes_ + node_idx;

            _key_t max_leqkey = -1;
            int8_t max_leqi = -1;
            for(int i = 0; i < LEAF_CARD; i++) {
                if (cur_leaf->keys[i] <= key && cur_leaf->keys[i] > max_leqkey) {
                    max_leqkey = cur_leaf->keys[i];
                    max_leqi = i;
                }
            }

            return (char **) &(cur_leaf->vals[max_leqi]);
        }

        bool leaf_insert(int node_idx, int off, Record rec) { // TODO: should do it in a CAS way
            leaf_nodes_[node_idx].vals[off] = rec.val;
            clwb(&leaf_nodes_[node_idx].vals[off], 8);
            mfence();

            leaf_nodes_[node_idx].keys[off] = rec.key;
            clwb(&leaf_nodes_[node_idx].keys[off], 8);
            mfence();
            return true;
        }

        inline void inner_insert(int node_idx, int off, _key_t key) {
            inner_nodes_[node_idx].keys[off] = key;
        }

        void inner_print(int node_idx) {
            printf("(");
            for(int i = 0; i < INNER_CARD; i++) {
                printf("%lu ", inner_nodes_[node_idx].keys[i]);
            }
            printf(") ");
        }

        void leaf_print(int node_idx) {
            printf("(");
            for(int i = 0; i < LEAF_CARD; i++) {
                printf("[%lu, %lu] ", leaf_nodes_[node_idx].keys[i], (long unsigned int)leaf_nodes_[node_idx].vals[i]);
            }
            printf(") \n");
        }
};

entrance_t * build_uptree(std::vector<Record> & records, Fixtree * &new_tree) {
    new_tree = new Fixtree(records);
    entrance_t * new_upent = (entrance_t *)galc->malloc(sizeof(entrance_t));
    new_upent->buff = galc->relative(new_tree->inner_nodes_);
    new_upent->height = new_tree->height_;
    memcpy(&(new_upent->level_offset[0]), &(new_tree->level_offset_[0]), sizeof(new_tree->level_offset_));
    clwb(new_upent, sizeof(entrance_t));
    mfence();

    return new_upent;
}

void free_uptree(entrance_t * upent) {
    if (upent->buff != NULL)
        galc->free(galc->absolute(upent->buff));
    galc->free(upent);
    
    return ;
}

typedef Fixtree uptree_t;

} // namespace fixtree

#endif //__FIXTREE__