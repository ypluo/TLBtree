/*
    log free wbtree with 512 nodesize, nonclass version of wbtree_logfree
    Copyright(c) Luo Yongping. THIS SOFTWARE COMES WITH NO WARRANTIES, 
    USE AT YOUR OWN RISK!
*/

#ifndef __WOTREE512__
#define __WOTREE512__

#include <cstdint>
#include <cstring>
#include <string>
#include <cstdio>

#include "flush.h"
#include "pmallocator.h"

namespace wotree512 {
using std::string;

const int PAGESIZE = 512;
const int NODE_SIZE = 24; // the max fanout is 24
const int8_t SLOT_MASK = 0x01;
const int8_t SIB_MASK = 0x02;

static int8_t allocBit(const uint32_t bitmap) {
    return __builtin_clz(~bitmap);
}

static inline uint32_t setBit(uint32_t bitmap, int8_t loc) {
    return bitmap + ((uint32_t)1 << (31 - loc));
}

static inline uint32_t clearBit(uint32_t bitmap, int8_t loc) {
    return bitmap - ((uint32_t)1 << (31 - loc));
}

union state_t{ // a 8 bytes states type
//1
uint64_t pack;
//2
struct unpack_t {
    uint32_t bitmap;
    uint8_t  count;
    uint8_t node_version;
    uint8_t shadow_version;
    uint8_t isleaf;
} unpack;
};

class Node;

class Node {
    public:
        friend class wbtree_logfree;
        /* ============== Node Header ============== */
        // first Cache line
        state_t state_;        //8 bytes state field
        char * leftmost_ptr_;  //8 bytes
        char shadow_slot_[2][NODE_SIZE];//48 bytes
        // second Cache line
        Record shadow_sibling_[2]; // two versions of siblings
    public:
        /* =============== Record slots ================ */
        Record recs_[NODE_SIZE] ; // 24 * 16 + 96 bytes
        char dummy[32]; // empty but not used

    public:
        Node(bool leaf = false) {
            state_.pack = 0;
            if (leaf) state_.unpack.isleaf = 1;

            leftmost_ptr_ = nullptr;
            shadow_sibling_[0] = {INT64_MAX, nullptr};
            shadow_sibling_[1] = {INT64_MAX, nullptr};
            memset(shadow_slot_, 0, sizeof(shadow_slot_));
        }

        void * operator new(size_t size) {
            return galc->malloc(size);
        }

        bool store(_key_t k, _value_t v, _key_t & split_k, Node * & split_node) {
            if(state_.unpack.count == NODE_SIZE) { // should split the node
                char *slot = get_cur_slots();

                uint64_t m = state_.unpack.count / 2;
                split_k = recs_[slot[m]].key;

                // copy half of the records into split node
                int8_t j = 0;
                state_t new_state = state_;
                new_state.unpack.count = m;
                if(state_.unpack.isleaf == 1) {
                    split_node = new Node(true);
                    for(int i = m; i < state_.unpack.count; i++) {
                        split_node->append(recs_[slot[i]].key, recs_[slot[i]].val, j, j);
                        new_state.unpack.bitmap = clearBit(new_state.unpack.bitmap, slot[i]);
                        j += 1;
                    }
                } else {
                    split_node = new Node(false);
                    split_node->leftmost_ptr_ = recs_[slot[m]].val;
                    new_state.unpack.bitmap = clearBit(new_state.unpack.bitmap, slot[m]);

                    for(int i = m + 1; i < state_.unpack.count; i++) {
                        split_node->append(recs_[slot[i]].key, recs_[slot[i]].val, j, j);
                        new_state.unpack.bitmap = clearBit(new_state.unpack.bitmap, slot[i]);
                        j += 1;
                    }
                }

                split_node->state_.unpack.bitmap = UINT32_MAX << (32 - j);
                split_node->state_.unpack.count = j;
                split_node->state_.unpack.shadow_version = 0;
                // the sibling node of current node pointed by split_node
                split_node->shadow_sibling_[0] = shadow_sibling_[(state_.unpack.shadow_version & SIB_MASK) > 0 ? 1 : 0];
                // the split node is installed as the shadow sibling of current node
                shadow_sibling_[(state_.unpack.shadow_version & SIB_MASK) > 0 ? 0 : 1] = {split_k, (char *)galc->relative(split_node)};
                clwb(split_node, 96); // persist header
                clwb(&split_node->recs_[0], sizeof(Record) * j); // persist all the inserted records
                clwb(&this->shadow_sibling_[0], 32); // persist the new sibling pointer
                
                mfence(); // a barrier here to make sure all the update is persisted to storage

                new_state.unpack.shadow_version = state_.unpack.shadow_version ^ SIB_MASK;
                // persist_assign the state_ field
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
            char *slot = get_cur_slots();
            Record sibling = shadow_sibling_[(state_.unpack.shadow_version & SIB_MASK) > 0 ? 1 : 0];

            if(k >= sibling.key) { // if the node has splitted and k to find is in next node 
                Node * sib_node = (Node *)galc->absolute(sibling.val);
                return sib_node->get_child(k);
            }

            if(state_.unpack.isleaf == 1) {
                uint64_t slotid = 0;
                for(int i = 0; i < state_.unpack.count; i++) {
                    slotid = slot[i];
                    if(recs_[slotid].key >= k) {
                        break;
                    }
                }

                if (recs_[slotid].key == k)
                    return recs_[slotid].val;
                else 
                    return NULL;
            } else {
                uint64_t pos = state_.unpack.count;
                for(int i = 0; i < state_.unpack.count; i++) {
                    if(recs_[slot[i]].key > k) {
                        pos = i;
                        break;
                    }
                }

                if (pos == 0) // all the key is bigger than k
                    return leftmost_ptr_;
                else 
                    return recs_[slot[pos - 1]].val;
            }
        }

        bool update(_key_t k, _value_t v) {
            // each update needs 1 clwb 
            char *slot = get_cur_slots();

            uint64_t slotid = 0;
            for(int i = 0; i < state_.unpack.count; i++) {
                slotid = slot[i];
                if(recs_[slotid].key >= k) {
                    break;
                }
            }

            if (recs_[slotid].key == k) {
                recs_[slotid].val = (char *)v;
                clwb(&recs_[slotid], sizeof(Record));

                return true;
            }
            else 
                return false;
        }

        bool remove_leaf(_key_t k) {
            // Non-SMO delete takes only one clwb 
            char * slot = get_cur_slots();
            char * next_slot = get_next_slots();

            Record sibling = shadow_sibling_[(state_.unpack.shadow_version & SIB_MASK) > 0 ? 1 : 0];
            if(k >= sibling.key) { // if the node has splitted and k to find is in next node 
                Node * sib_node = (Node *)galc->absolute(sibling.val);
                return sib_node->remove_leaf(k);
            }

            int8_t i, j = 0, del_slot;
            for(i = 0; i < state_.unpack.count && recs_[slot[i]].key < k; i++) {
                next_slot[j++] = slot[i];
            }
            
            if(recs_[slot[i]].key > k || i == state_.unpack.count) { // not found
                return false;
            } else {
                del_slot = i;
            }

            memcpy(&next_slot[j], &slot[del_slot + 1], state_.unpack.count - del_slot - 1);

            state_t new_state = state_;
            new_state.unpack.bitmap = clearBit(state_.unpack.bitmap, slot[del_slot]);
            new_state.unpack.count = state_.unpack.count - 1;
            new_state.unpack.shadow_version = (state_.unpack.shadow_version) ^ SLOT_MASK;

            // persist the first cache line
            mfence();
            state_.pack = new_state.pack;
            clwb(this, 64);
            return true; 
        }

        void remove_inner(_key_t k) {
            // Non-SMO delete takes only one clwb 
            char * slot = get_cur_slots();
            char * next_slot = get_next_slots();
            
            Record sibling = shadow_sibling_[(state_.unpack.shadow_version & SIB_MASK) > 0 ? 1 : 0];
            if(k >= sibling.key) { // if the node has splitted and k to find is in next node 
                Node * sib_node = (Node *)galc->absolute(sibling.val);
                sib_node->remove_inner(k);
                return ;
            }

            int8_t i, j = 0;
            for (i = 0; i < state_.unpack.count && recs_[slot[i]].key <= k; i++) {
                next_slot[j++] = slot[i];
            }

            memcpy(&next_slot[j - 1], &slot[i], (state_.unpack.count - i));

            state_t new_state = state_;
            new_state.unpack.count = state_.unpack.count - 1;
            new_state.unpack.bitmap = setBit(state_.unpack.bitmap, slot[i - 1]);
            new_state.unpack.shadow_version = state_.unpack.shadow_version ^ SLOT_MASK;
            
            // CAS the state_ and persist the first cache line
            mfence();
            state_.pack = new_state.pack;
            clwb(this, 64);
        }

        void print(std::string prefix, bool recursively = true) {
            char * slot = get_cur_slots();
            
            printf("%s[%x(%d) ", prefix.c_str(), state_.unpack.bitmap, state_.unpack.count);

            for(int i = 0; i < state_.unpack.count; i++) {
                printf("%d ", slot[i]);
            }

            for(int i = 0; i < state_.unpack.count; i++) {
                printf("(%ld 0x%lx) ", recs_[slot[i]].key, (uint64_t)recs_[slot[i]].val);
            }
            printf("]\n");

            if(recursively && state_.unpack.isleaf == 0) {
                Node * child = (Node *)galc->absolute(leftmost_ptr_);
                child->print(prefix + "    ", recursively);

                for(int i = 0; i < state_.unpack.count; i++) {
                    Node * child = (Node *)galc->absolute(recs_[slot[i]].val);
                    child->print(prefix + "    ", recursively);
                }
            }
        }

        void get_sibling(_key_t & k, Node ** &sibling) {
            Record &sib = shadow_sibling_[(state_.unpack.shadow_version & SIB_MASK) > 0 ? 1 : 0];
            k = sib.key;
            sibling = (Node **)&(sib.val);
        }
    public:
        static void merge(Node * left, Node * right) {
            char * slot = right->get_cur_slots();
            Record sibling = left->shadow_sibling_[(left->state_.unpack.shadow_version & SIB_MASK) > 0 ? 1 : 0];

            state_t new_state = left->state_;
            if(left->state_.unpack.isleaf == 0) {
                // insert the leftmost ptr
                int8_t slotid = allocBit(new_state.unpack.bitmap);
                left->append(sibling.key, right->leftmost_ptr_, slotid, new_state.unpack.count++);
                new_state.unpack.bitmap = setBit(new_state.unpack.bitmap, slotid);
            }
            for(int i = 0; i < right->state_.unpack.count; i++) {
                int8_t slotid = allocBit(new_state.unpack.bitmap);
                left->append(right->recs_[slot[i]].key, right->recs_[slot[i]].val, slotid, new_state.unpack.count++);
                new_state.unpack.bitmap = setBit(new_state.unpack.bitmap, slotid);
            }
            
            Record tmp = right->shadow_sibling_[(right->state_.unpack.shadow_version & SIB_MASK) > 0 ? 1 : 0];
            left->shadow_sibling_[(left->state_.unpack.shadow_version & SIB_MASK) > 0 ? 1 : 0] = tmp;
            new_state.unpack.shadow_version = left->state_.unpack.shadow_version ^ SIB_MASK;
            
            // persist the whole leaf node
            clwb(left, sizeof(Node));

            mfence(); // a barrier to make sure all the movement is persisted

            // persist_assign the state_ field
            left->state_.pack = new_state.pack;
            clwb(&left->state_, 64);

            galc->free(right); // WARNING: persistent memory leak here
        }

    public:
        inline char * get_cur_slots() {
            return shadow_slot_[(state_.unpack.shadow_version & SLOT_MASK) > 0 ? 1 : 0];
        }

        inline char * get_next_slots() {
            return shadow_slot_[(state_.unpack.shadow_version & SLOT_MASK) > 0 ? 0 : 1];
        }

    public:
        void append(_key_t k, char * v, int8_t idx, int8_t pos) {
            char * slot = get_cur_slots();
            recs_[idx] = {k, v};
            slot[pos] = idx;
        }

        void insertone(_key_t k, void *v) {
            // each Non-SMO insert needs 2 clwb
            int8_t alloc_slot = allocBit(state_.unpack.bitmap);
            recs_[alloc_slot] = {k, (char *) v};
            clwb(&recs_[alloc_slot], sizeof(Record));

            char * slot = get_cur_slots();
            char * next_slot = get_next_slots();
            int8_t i, j = 0;
            for(i = 0; i < state_.unpack.count && recs_[slot[i]].key <= k; i++) {
                next_slot[j++] = slot[i];
            }
            next_slot[j++] = alloc_slot;
            memcpy(&next_slot[j], &slot[i], state_.unpack.count - i);

            state_t new_state = state_;
            new_state.unpack.bitmap = setBit(state_.unpack.bitmap, alloc_slot);
            new_state.unpack.count = state_.unpack.count + 1;
            new_state.unpack.shadow_version = state_.unpack.shadow_version ^ SLOT_MASK;

            // not only to persist the state_, but also the next slotArray
            mfence();
            state_.pack = new_state.pack;
            clwb(this, 64); // All in one cache line
        }
   
        void get_lrchild(_key_t k, Node * & left, Node * & right) {
            char * slot = get_cur_slots();

            int16_t i = 0;
            for( ; i < state_.unpack.count; i++) {
                if(recs_[slot[i]].key > k) {
                    break;
                }
            }

            if(i == 0) {
                left = NULL;
            } else if(i == 1) {
                left = (Node *)galc->absolute(leftmost_ptr_);
            } else {
                left = (Node *)galc->absolute(recs_[slot[i - 2]].val);
            }

            if(i == state_.unpack.count) {
                right = NULL;
            } else {
                right = (Node *)galc->absolute(recs_[slot[i]].val);
            }
        }
};

// private interfaces of downtree
static bool insert_recursive(Node * n, _key_t k, _value_t v, _key_t &split_k, Node * &split_node, int8_t &level) {
    if(n->state_.unpack.isleaf == 1) {
        return n->store(k, v, split_k, split_node);
    } else {
        level++;
        Node * child = (Node *) galc->absolute(n->get_child(k));
        
        _key_t split_k_child;
        Node * split_node_child;
        bool splitIf = insert_recursive(child, k, v, split_k_child, split_node_child, level);

        if(splitIf) { 
            return n->store(split_k_child, (_value_t)galc->relative(split_node_child), split_k, split_node);
        } 
        return false;
    }
}

static bool remove_recursive(Node * n, _key_t k) {
    if(n->state_.unpack.isleaf == 1) {
        n->remove_leaf(k);
        return n->state_.unpack.count < NODE_SIZE / 4;
    }
    else {
        Node * child = (Node *) galc->absolute(n->get_child(k));

        bool shouldMrg = remove_recursive(child, k);

        if(shouldMrg) {
            Node *leftsib = NULL, *rightsib = NULL;
            n->get_lrchild(k, leftsib, rightsib);

            if(leftsib != NULL && (child->state_.unpack.count + leftsib->state_.unpack.count) < NODE_SIZE) {
                // merge with left node
                char * slot = child->get_cur_slots();
                n->remove_inner(child->recs_[slot[0]].key);
                Node::merge(leftsib, child);

                return n->state_.unpack.count < NODE_SIZE / 4;
            } else if (rightsib != NULL && (child->state_.unpack.count + rightsib->state_.unpack.count) < NODE_SIZE) {
                // merge with right node
                char * slot = rightsib->get_cur_slots();
                n->remove_inner(rightsib->recs_[slot[0]].key);
                Node::merge(child, rightsib);

                return n->state_.unpack.count < NODE_SIZE / 4;
            }
        }
        return false;
    }
}

// public interface2 of downtree
bool find(Node **root_ptr, uint64_t key, _value_t &val) {
    Node * cur = galc->absolute(*root_ptr);;
    while(cur->state_.unpack.isleaf == 0) { // no prefetch here
        char * child_ptr = cur->get_child(key);
        cur = (Node *)galc->absolute(child_ptr);
    }

    val = (_value_t) cur->get_child(key);

    if((char *)val == NULL)
        return false;
    else
        return true;
}

res_t insert(Node **root_ptr, _key_t key, _value_t val, int threshold) {
    Node * root = galc->absolute(*root_ptr);
    int8_t level = 1;
    _key_t split_k;
    Node * split_node;
    bool splitIf = insert_recursive(root, key, val, split_k, split_node, level);

    if(splitIf) {
        if(level < threshold) {
            Node *new_root = new Node;
            new_root->leftmost_ptr_ = (char *)galc->relative(root);
            new_root->append(split_k, (char *)galc->relative(split_node), 0, 0);
            new_root->state_.unpack.bitmap = (uint32_t)1 << 31;
            new_root->state_.unpack.count = 1;

            clwb(&(new_root->state_), 64);
            clwb(&(new_root->shadow_sibling_[0]), 64);

            mfence(); // a barrier to make sure the new node is persisted

            persist_assign(root_ptr, (Node *)galc->relative(new_root));

            root = new_root;

            return res_t(false, {0, NULL});
        } else{
            return res_t(true, {split_k, (char *)split_node});
        }
    }
    else {
        return res_t(false, {0, NULL});
    }
}

bool update(Node **root_ptr, _key_t key, _value_t val) {
    Node * cur = galc->absolute(*root_ptr);;
    while(cur->state_.unpack.isleaf == 0) { // no prefetch here
        char * child_ptr = cur->get_child(key);
        cur = (Node *)galc->absolute(child_ptr);
    }

    val = (_value_t) cur->update(key, val);
    return true;
}

bool remove(Node **root_ptr, _key_t key) {
    Node * root = galc->absolute(*root_ptr);

    if(root->state_.unpack.isleaf == 1) {
        root->remove_leaf(key);

        return root->state_.unpack.count == 0;
    }
    else {
        Node * child = (Node *) galc->absolute(root->get_child(key));

        bool shouldMrg = remove_recursive(child, key);

        if(shouldMrg) {
            Node *leftsib = NULL, *rightsib = NULL;
            root->get_lrchild(key, leftsib, rightsib);

            if(leftsib != NULL && (child->state_.unpack.count + leftsib->state_.unpack.count) < NODE_SIZE) {
                // merge with left node
                char * slot = child->get_cur_slots();
                root->remove_inner(child->recs_[slot[0]].key);
                Node::merge(leftsib, child);
            } 
            else if (rightsib != NULL && (child->state_.unpack.count + rightsib->state_.unpack.count) < NODE_SIZE) {
                // merge with right node
                char * slot = rightsib->get_cur_slots();
                root->remove_inner(rightsib->recs_[slot[0]].key);
                Node::merge(child, rightsib);
            }
            
            if(root->state_.unpack.count == 0) { // the root is empty
                Node * old_root = root;

                persist_assign(root_ptr, (Node *)root->leftmost_ptr_);

                galc->free(old_root);
            }
        }

        return false;
    } 
}

Node * get_lastnode(Node ** root_ptr) {
    Node * cur = galc->absolute(*root_ptr);;
    while(cur->state_.unpack.isleaf == 0) {
        char *slot = cur->get_cur_slots();

        int8_t slotid = slot[cur->state_.unpack.count - 1];
        cur = (Node *)galc->absolute(cur->recs_[slotid].val);
    }

    return cur;
}

} // namespace logfree

#endif // __WOTREE512__