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
    const int INNER_CARD = 32; // node size: 256B, the fanout of inner node is 32
    const int LEAF_CARD = 16;  // node size: 256B, the fanout of leaf node is 16
    const int LEAF_REBUILD_CARD = 12;
    const int MAX_HEIGHT = 10;

    // the entrance of fixtree that stores its persistent tree metadata
    struct entrance_t {
        void * leaf_buff;
        void * inner_buff;
        uint32_t height; // the tree height
        uint32_t leaf_cnt; 
    };

/*  Fixtree: 
        a search-optimized linearize tree structure which can absort moderate insertions: 
*/
class Fixtree {
    public:
        struct INNode { // inner node is packed keys, which is very compact
            _key_t keys[INNER_CARD];
        } __attribute__((aligned(CACHE_LINE_SIZE)));

        struct LFNode { // leaf node is packed key-ptr along with a header. 
                        // leaf node has some gap to absort insert
            _key_t keys[LEAF_CARD];
            char * vals[LEAF_CARD];
        } __attribute__((aligned(CACHE_LINE_SIZE)));

    public:
        // volatile structures
        INNode * inner_nodes_;
        LFNode * leaf_nodes_;
        uint32_t height_;
        uint32_t leaf_cnt_;
        entrance_t * entrance_;
        uint32_t level_offset_[MAX_HEIGHT];
    
    public:
        Fixtree(entrance_t * ent) { // recovery the tree from the entrance
            inner_nodes_ = (INNode *)galc->absolute(ent->inner_buff);
            leaf_nodes_ = (LFNode *)galc->absolute(ent->leaf_buff);
            height_ = ent->height;
            leaf_cnt_ = ent->leaf_cnt;
            entrance_ = ent;

            uint32_t tmp = 0;
            for(int l = 0; l < height_; l++) {
                level_offset_[l] = tmp;
                tmp += std::pow(INNER_CARD, l);
            }
            level_offset_[height_] = tmp;
        }

        Fixtree(std::vector<Record> records) {
            const int lfary = LEAF_REBUILD_CARD;
            int record_count = records.size();
            
            uint32_t lfnode_cnt = std::ceil((float)record_count / lfary);
            leaf_nodes_ = (LFNode *) galc->malloc(std::max((size_t)4096, lfnode_cnt * sizeof(LFNode)));

            height_ = std::ceil(std::log(std::max((uint32_t)INNER_CARD, lfnode_cnt)) / std::log(INNER_CARD));
            uint32_t innode_cnt = (std::pow(INNER_CARD, height_) - 1) / (INNER_CARD - 1);
            inner_nodes_ = (INNode *) galc->malloc(std::max((size_t)4096, innode_cnt * sizeof(INNode)));

            // fill leaf nodes
            for(int i = 0; i < lfnode_cnt; i++) {
                for(int j = 0; j < lfary; j++) {
                    auto idx = i * lfary + j;
                    leaf_nodes_[i].keys[j] = idx < record_count ? records[idx].key : INT64_MAX; 
                    leaf_nodes_[i].vals[j] = idx < record_count ? records[idx].val : 0;
                }
                for(int j = lfary; j < LEAF_CARD; j++) { // intialized key
                    leaf_nodes_[i].keys[j] = INT64_MAX;
                }
                clwb(leaf_nodes_ + i, sizeof(LFNode));
            }
            
            int cur_level_cnt = lfnode_cnt;
            int cur_level_off = innode_cnt - std::pow(INNER_CARD, height_ - 1);
            int last_level_off = 0;
            
            // fill parent innodes of leaf nodes
            for(int i = 0; i < cur_level_cnt; i++)
                inner_insert(cur_level_off + i / INNER_CARD, i % INNER_CARD, leaf_nodes_[i].keys[0]);
            if(cur_level_cnt % INNER_CARD)
                inner_insert(cur_level_off + cur_level_cnt / INNER_CARD, cur_level_cnt % INNER_CARD, INT64_MAX);
            clwb(&inner_nodes_[cur_level_off], sizeof(INNode) * (cur_level_cnt / INNER_CARD + 1));
            
            cur_level_cnt = std::ceil((float)cur_level_cnt / INNER_CARD);
            last_level_off = cur_level_off;
            cur_level_off = cur_level_off - std::pow(INNER_CARD, height_ - 2);

            // fill other inner nodes
            for(int l = height_ - 2; l >= 0; l--) { // level by level
                for(int i = 0; i < cur_level_cnt; i++)
                    inner_insert(cur_level_off + i / INNER_CARD, i % INNER_CARD, inner_nodes_[last_level_off + i].keys[0]);
                if(cur_level_cnt % INNER_CARD)
                    inner_insert(cur_level_off + cur_level_cnt / INNER_CARD, cur_level_cnt % INNER_CARD, INT64_MAX);
                clwb(&inner_nodes_[cur_level_off], sizeof(INNode) * (cur_level_cnt / INNER_CARD + 1));

                cur_level_cnt = std::ceil((float)cur_level_cnt / INNER_CARD);
                last_level_off = cur_level_off;
                cur_level_off = cur_level_off - std::pow(INNER_CARD, l - 1);
            }
            
            leaf_cnt_ = lfnode_cnt;
            entrance_ = (entrance_t *)galc->malloc(4096); // the allocator is not thread_safe, allocate a large entrance
            uint32_t tmp = 0;
            for(int l = 0; l < height_; l++) {
                level_offset_[l] = tmp;
                tmp += std::pow(INNER_CARD, l);
            }
            level_offset_[height_] = tmp;

            persist_assign(&(entrance_->leaf_buff), (void *) galc->relative(leaf_nodes_));
            persist_assign(&(entrance_->inner_buff), (void *) galc->relative(inner_nodes_));
            persist_assign(&(entrance_->height), height_);
            persist_assign(&(entrance_->leaf_cnt), lfnode_cnt);

            return ;
        }

    public:
        char ** find_lower(_key_t key) { 
            /* linear search, return the position of the stored value */
            int cur_idx = level_offset_[0];
            for(int l = 0; l < height_; l++) {
                #ifdef DEBUG
                    INNode * inner = inner_nodes_ + cur_idx; 
                #endif
                cur_idx = level_offset_[l + 1] + (cur_idx - level_offset_[l]) * INNER_CARD + inner_search(cur_idx, key);
            }
            cur_idx -= level_offset_[height_];

            return leaf_search(cur_idx, key);
        }

        bool insert(_key_t key, _value_t val) {
            uint32_t cur_idx = level_offset_[0];
            for(int l = 0; l < height_; l++) {
                #ifdef DEBUG
                    INNode * inner = inner_nodes_ + cur_idx; 
                #endif
                cur_idx = level_offset_[l + 1] + (cur_idx - level_offset_[l]) * INNER_CARD + inner_search(cur_idx, key);
            }
            cur_idx -= level_offset_[height_];
            
            LFNode * cur_leaf = leaf_nodes_ + cur_idx;

            for(int i = 0; i < LEAF_CARD; i++) {
                if (cur_leaf->keys[i] == INT64_MAX) { // empty slot
                    leaf_insert(cur_idx, i, {key, (char *)val});
                    return true;
                }
            }
            return false;
        }

        bool try_remove(_key_t key) {
            int cur_idx = level_offset_[0];
            for(int l = 0; l < height_; l++) {
                #ifdef DEBUG
                    INNode * inner = inner_nodes_ + cur_idx; 
                #endif
                cur_idx = level_offset_[l + 1] + (cur_idx - level_offset_[l]) * INNER_CARD + inner_search(cur_idx, key);
            }
            cur_idx -= level_offset_[height_];
            
            LFNode * cur_leaf = leaf_nodes_ + cur_idx;

            _key_t max_leqkey = cur_leaf->keys[0];
            int8_t max_leqi = 0;
            int8_t rec_cnt = 1;
            for(int i = 1; i < LEAF_CARD; i++) {
                if(cur_leaf->keys[i] != INT64_MAX) {
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
                return false;
            } else { // case 2, 3
                persist_assign(&(cur_leaf->keys[max_leqi]), INT64_MAX);
                return true;
            }
        }

        void printAll() {
            for(int l = 0; l < height_; l++) {
                printf("level: %d =>", l);
                
                for(int i = level_offset_[l]; i < level_offset_[l + 1]; i++) {
                    inner_print(i);
                }
                printf("\n");
            }

            printf("leafs");

            for(int i = 0; i < leaf_cnt_; i++) {
                leaf_print(i);
            }
        }

        char ** find_first() {
            return (char **)&(leaf_nodes_[0].vals[0]);
        }

        void merge(std::vector<Record> & in, std::vector<Record> & out) { // merge the records with in to out
            uint32_t insize = in.size();

            uint32_t incur = 0, innode_pos = 0, cur_lfcnt = 0;
            Record tmp[LEAF_CARD];
            load_node(tmp, &leaf_nodes_[0]);
            _key_t k1 = in[0].key, k2 = tmp[0].key;
            while(incur < insize && cur_lfcnt < leaf_cnt_) {
                if(k1 == k2) { 
                    out.push_back(in[incur]);

                    incur += 1;
                    innode_pos += 1;
                    if(innode_pos == LEAF_CARD || tmp[innode_pos].key == INT64_MAX) {
                        cur_lfcnt += 1;
                        load_node(tmp, &leaf_nodes_[cur_lfcnt]);
                        innode_pos = 0;
                    }

                    k1 = in[incur].key;
                    k2 = tmp[innode_pos].key;
                } else if(k1 > k2) {
                    out.push_back(tmp[innode_pos]);

                    innode_pos += 1;
                    if(innode_pos == LEAF_CARD || tmp[innode_pos].key == INT64_MAX) {
                        cur_lfcnt += 1;
                        load_node(tmp, &leaf_nodes_[cur_lfcnt]);
                        innode_pos = 0;
                    }

                    k2 = tmp[innode_pos].key;;
                } else {
                    out.push_back(in[incur]);
                    
                    incur += 1;
                    k1 = in[incur].key;
                }
            }

            if(incur < insize) {
                for(int i = incur; i < insize; i++) 
                    out.push_back(in[i]);
            }

            if(cur_lfcnt < leaf_cnt_) {
                while(cur_lfcnt < leaf_cnt_) {
                    out.push_back(tmp[innode_pos]);

                    innode_pos += 1;
                    if(innode_pos == LEAF_CARD || tmp[innode_pos].key == INT64_MAX) {
                        cur_lfcnt += 1;
                        load_node(tmp, &leaf_nodes_[cur_lfcnt]);
                        innode_pos = 0;
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

            _key_t max_leqkey = cur_leaf->keys[0];
            int8_t max_leqi = 0;
            for(int i = 1; i < LEAF_CARD; i++) {
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

        static void load_node(Record * to, LFNode * from) {
            for(int i = 0; i < LEAF_CARD; i++) {
                to[i].key = from->keys[i];
                to[i].val = from->vals[i];
            }

            std::sort(to, to + LEAF_CARD);
        }
};

inline entrance_t * get_entrance(Fixtree * tree) {
    return tree->entrance_;
}

inline void free(Fixtree * tree) {
    entrance_t * upent = get_entrance(tree);
    delete tree;

    galc->free(galc->absolute(upent->inner_buff));
    galc->free(galc->absolute(upent->leaf_buff));
    galc->free(upent);

    return ;
}

typedef Fixtree uptree_t;

} // namespace fixtree

#endif //__FIXTREE__