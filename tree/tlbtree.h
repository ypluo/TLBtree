/*  tlbtree.h - A two level btree for persistent memory
    Copyright(c) 2020 Luo Yongping. THIS SOFTWARE COMES WITH NO WARRANTIES, 
    USE AT YOUR OWN RISK!
*/

#ifndef __TLBTREE_H__
#define __TLBTREE_H__

#include <string>
#include <thread>
#include <unistd.h>

#include "pmallocator.h"
#include "fixtree.h"
#include "wotree256.h"
#include "wotree512.h"

// choose downtree type, providing the same interfaces: insert, find_lower, remove_lower, build_uptree, free_uptree
#define DOWNTREE_NS wotree256
// choose uptree type, providing the same interfaces: insert, remove, update, find
#define UPTREE_NS   fixtree

namespace tlbtree {
using std::string;
using Node = DOWNTREE_NS::Node;

// functions to build/free an upindex, aliases of uptree functions
static inline UPTREE_NS::entrance_t * build_upindex(std::vector<Record> &recs, UPTREE_NS::uptree_t * &new_uptree) {
    return UPTREE_NS::build_uptree(recs, new_uptree);
}
static inline void free_upindex(UPTREE_NS::entrance_t * up_ent) {
    return UPTREE_NS::free_uptree(up_ent);
}

template<int DOWNLEVEL, int REBUILD_THRESHOLD=2>
class TLBtree {
private:
    typedef TLBtree<DOWNLEVEL, REBUILD_THRESHOLD> SelfType;
    struct tlbtree_entrance_t{
        UPTREE_NS::entrance_t * upent; // current entrance_ of fixtree
    };
    // volatile domain
    UPTREE_NS::uptree_t *uptree_;
    tlbtree_entrance_t * entrance_;

public:
    TLBtree(string path, bool recover=true, string id = "tlbtree") {
        if(recover == false) {
            galc = new PMAllocator(path.c_str(), false, id.c_str());
            entrance_ = (tlbtree_entrance_t *) galc->get_root(sizeof(tlbtree_entrance_t));
            entrance_->upent = NULL;
            clwb(entrance_, sizeof(tlbtree_entrance_t));
            
            //allocate a entrance_ to the fixtree
            std::vector<Record> init = {Record(0, (char *)galc->relative(new Node(true)))}; 
            UPTREE_NS::entrance_t * uptree_entrance = build_upindex(init, uptree_);

            UPTREE_NS::entrance_t *a = entrance_->upent;
            persist_assign(&(entrance_->upent), galc->relative(uptree_entrance));
        } else {
            galc = new PMAllocator(path.c_str(), true, id.c_str());

            entrance_ = (tlbtree_entrance_t *) galc->get_root(sizeof(tlbtree_entrance_t));
            if(entrance_->upent == NULL) { // empty tree
                printf("the tree is empty\n");
                exit(-1);
            }

            uptree_ = new UPTREE_NS::uptree_t (galc->absolute(entrance_->upent));
        }
    }

    ~TLBtree() {
        delete uptree_;
        delete galc;
    }

public: // public interface
    void insert(const _key_t & k, _value_t v) {  
        Node ** root_ptr = (Node **)uptree_->find_lower(k);
        Node * downroot = (Node *)galc->absolute(*root_ptr);
        /* ================ NOTE HERE WITH CAUTIOUS ===========================
            This is very IMPORTANT, for reducing the rebuild cost!
            The two level tree adopt mechanism of skiplist, which means
            new DOWNTREE_NS will not be forced to install into the uptree
            we rather do it in this way: 
                if we can install upward, then install it.
                if not, we just do nothing (installed as a sibling by default)
            Note that this will make the DOWNTREE_NS search more time-consuming
            as it should go right like skiplist to find the correct DOWNTREE_NS

            We rebuild the uptree if we found that the search goes too far
        ======================================================================= */ 
        // travese in sibling chain
        int8_t goes_steps = 0;
        _key_t splitkey; Node ** sibling_ptr;
        downroot->get_sibling(splitkey, sibling_ptr);
        while(splitkey < k) { // the splitkey 
            root_ptr = sibling_ptr; // where is current root store
            downroot = (Node *)galc->absolute(*root_ptr);
            downroot->get_sibling(splitkey, sibling_ptr);
            goes_steps += 1;
        }
        //DOWNTREE_NS::printAll(root_ptr);
        res_t insert_res = DOWNTREE_NS::insert(root_ptr, k, v, DOWNLEVEL);

        // we rebuild if the search in the root chain take too long 
        if(goes_steps > REBUILD_THRESHOLD) {
            #ifdef BACKGROUND_REBUILD
                std::thread rebuild_thread(&SelfType::rebuild, this);
                rebuild_thread.detach();
            #else
                rebuild();
            #endif
        }

        if(insert_res.flag == true) {
            // Note that the fixtree can absort some insertion, do it anyway
            uptree_->insert(insert_res.rec.key, (_value_t)galc->relative(insert_res.rec.val));
        }
    }

    bool find(const _key_t & k, _value_t & v) {
        Node ** root_ptr = (Node **)uptree_->find_lower(k);
        Node * downroot = (Node *)galc->absolute(*root_ptr);

        // traverse in sibling chain
        _key_t splitkey; Node ** sibling_ptr;
        downroot->get_sibling(splitkey, sibling_ptr);
        while(splitkey <= k) { // the splitkey 
            root_ptr = sibling_ptr; // where is current root store
            downroot = (Node *)galc->absolute(*root_ptr);
            downroot->get_sibling(splitkey, sibling_ptr);
        }

        return DOWNTREE_NS::find(root_ptr, k, v);
    }

    bool remove(const _key_t & k) {
        Node ** root_ptr = (Node **)uptree_->find_lower(k);
        Node ** last_root_ptr = NULL; // record the last root ptr for laster use
        Node *downroot = (Node *)galc->absolute(*root_ptr);

        // travese in sibling chain
        _key_t splitkey; Node ** sibling_ptr;
        downroot->get_sibling(splitkey, sibling_ptr);
        while(splitkey < k) { // the splitkey 
            root_ptr = sibling_ptr; // where is current root store
            downroot = (Node *)galc->absolute(*root_ptr);
            downroot->get_sibling(splitkey, sibling_ptr);
        }
        
        bool emptyif = DOWNTREE_NS::remove(root_ptr, k);
        
        if(emptyif) { // the DOWNTREE_NS is empty now, eliminate the root from the downroot linklist
            bool need_rebuild, success;
            // the downroot is installed in the uptree, find it and try remove it from uptree
            success = uptree_->try_remove(k, need_rebuild);
            
            // rebuild if needed
            if(need_rebuild == true) {
                #ifdef BACKGROUND_REBUILD
                    std::thread rebuild_thread(&SelfType::rebuild, this);
                    rebuild_thread.detach();
                #else
                    rebuild();
                #endif
            }
        }
        return true;
    }

    bool update(const _key_t & k, const _value_t & v) {
        Node ** root_ptr = (Node **)uptree_->find_lower(k);
        Node * downroot = (Node *)galc->absolute(*root_ptr);

        // travese in sibling chain
        _key_t splitkey; Node ** sibling_ptr;
        downroot->get_sibling(splitkey, sibling_ptr);
        while(splitkey < k) { // the splitkey 
            root_ptr = sibling_ptr; // where is current root store
            downroot = (Node *)galc->absolute(*root_ptr);
            downroot->get_sibling(splitkey, sibling_ptr);
        }

        return DOWNTREE_NS::update(root_ptr, k, v);
    }

    void printAll() {
        uptree_->printAll();
    }

    public:
    void rebuild() {
        std::vector<Record> subindex_roots;
        subindex_roots.reserve(0xffff);

        // get all the root nodes of subindices
        _key_t split_key = 0; 
        Node ** sibling_ptr = (Node **)uptree_->find_first();
        Node * cur_root = (Node *)galc->absolute(*sibling_ptr);
        while (cur_root != NULL) {
            subindex_roots.emplace_back(split_key, (char *)(*sibling_ptr));
            // get next sibling
            cur_root->get_sibling(split_key, sibling_ptr);
            cur_root = galc->absolute(*sibling_ptr);
        }

        /*  rebuild the uptree */  
        // build a new tree, get the entrance_ of it
        UPTREE_NS::uptree_t * old_tree = uptree_;
        UPTREE_NS::uptree_t * new_tree;
        UPTREE_NS::entrance_t * old_upent = galc->absolute(entrance_->upent);
        UPTREE_NS::entrance_t * new_upent = build_upindex(subindex_roots, new_tree);
        
        // install it to fixtree entrance_
        persist_assign(&(entrance_->upent), galc->relative(new_upent));
        uptree_ = new_tree;
        
        /* free the old uptree: make sure no one is using it now */
        #ifdef BACKGROUND_REBUILD
            usleep(50);
        #endif
        delete old_tree;
        free_upindex(old_upent);
    }
}; // tlbtree with fixtree as uptree

} // tlbtree namespace

#endif //__TLBTREE_H__