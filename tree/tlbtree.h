/*  tlbtree.h - A two level btree for persistent memory
    Copyright(c) 2020 Luo Yongping. THIS SOFTWARE COMES WITH NO WARRANTIES, 
    USE AT YOUR OWN RISK!
*/

#ifndef __TLBTREE_H__
#define __TLBTREE_H__

#include <string>
#include <thread>
#include <algorithm>
#include <unistd.h>

#include "pmallocator.h"
#include "fixtree.h"
#include "spinlock.h"
#include "wotree256.h"
#include "wotree512.h"

// choose uptree type, providing interfaces: insert, remove, update, find
#define UPTREE_NS   fixtree
// choose downtree type, providing interfaces: insert, find_lower, remove_lower, merge, free_uptree
#define DOWNTREE_NS wotree256

namespace tlbtree {
using std::string;
using std::vector;
using Node = DOWNTREE_NS::Node;

template<int DOWNLEVEL, int REBUILD_THRESHOLD=2>
class TLBtree {
private:
    typedef TLBtree<DOWNLEVEL, REBUILD_THRESHOLD> SelfType;
    
    // the entrance of TLBtree that stores its persistent tree metadata
    struct tlbtree_entrance_t{
        UPTREE_NS::entrance_t * upent; // the entrance of the top layer
        bool is_rebuilding;          // a mutex for exclusive rebuilding
        bool is_clean;                  // is TLBtree shutdown expectedly
    };
    
    // volatile domain
    UPTREE_NS::uptree_t * uptree_;
    tlbtree_entrance_t * entrance_;
    vector<Record> mutable_;
    vector<Record> immutable_;
    Spinlock rebuild_mtx_;

public:
    TLBtree(string path, bool recover=true, string id = "tlbtree") {
        if(recover == false) {
            galc = new PMAllocator(path.c_str(), false, id.c_str());
            entrance_ = (tlbtree_entrance_t *) galc->get_root(sizeof(tlbtree_entrance_t));
            entrance_->upent = NULL;
            entrance_->is_clean =  false;
            entrance_->is_rebuilding = false;
            clwb(entrance_, sizeof(tlbtree_entrance_t));
            
            //allocate a entrance_ to the fixtree
            std::vector<Record> init = {Record(0, (char *)galc->relative(new Node(true)))}; 

            uptree_ = new UPTREE_NS::uptree_t(init);
            UPTREE_NS::entrance_t * uptree_entrance = UPTREE_NS::get_entrance(uptree_);

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

            if(entrance_->is_clean == false && entrance_->is_rebuilding == true) {
                rebuild_recover();
            }   
        }

        persist_assign(&(entrance_->is_clean), false); // set the TLBtree state to be dirty
    }

    ~TLBtree() {
        persist_assign(&(entrance_->is_clean), true); // a intended shutdown
        delete uptree_;
        delete galc;
    }

public: // public interface
    void insert(const _key_t & k, _value_t v) {  
        Node ** root_ptr = (Node **)uptree_->find_lower(k);
        Node * downroot = (Node *)galc->absolute(*root_ptr);
 
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
        res_t insert_res = DOWNTREE_NS::insert(root_ptr, k, v, DOWNLEVEL);

        // we rebuild if the search in the root chain take too long 
        if(goes_steps > REBUILD_THRESHOLD && rebuild_mtx_.trylock()) {
            immutable_.swap(mutable_); // make the mutable vector be immutable
            
            #ifdef BACKGROUND_REBUILD
                std::thread rebuild_thread(&SelfType::rebuild, this);
                rebuild_thread.detach();
            #else
                rebuild();
            #endif
            rebuild_mtx_.unlock();
        }

        if(insert_res.flag == true) { // a sub-index tree is splitted
            // try save the sub-indices root into the top layer
            bool succ = uptree_->insert(insert_res.rec.key, (_value_t)galc->relative(insert_res.rec.val));
            
            // save these records into mutable_
            if(entrance_->is_rebuilding == true || succ == false) {
                mutable_.push_back({insert_res.rec.key, (char *)galc->relative(insert_res.rec.val)}); // TODO: data race
            }
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
        if(emptyif) { // the DOWNTREE_NS is empty now
           uptree_->try_remove(k); // TODO: rebuilding should also be triggered when the top layer is too empty
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

private:
    void rebuild() { // rebuilding function one
        persist_assign(&(entrance_->is_rebuilding), true); 

        std::sort(immutable_.begin(), immutable_.end());
        // get the snapshot of all sub-index trees by combining the top layer with immutable
        std::vector<Record> subroots;
        subroots.reserve(0x2fffff);
        uptree_->merge(immutable_, subroots);

        /* rebuild the top layer with immutable_ */  
        UPTREE_NS::uptree_t * old_tree = uptree_;
        UPTREE_NS::entrance_t * old_upent = galc->absolute(entrance_->upent);
        UPTREE_NS::uptree_t * new_tree = new UPTREE_NS::uptree_t(subroots);
        UPTREE_NS::entrance_t * new_upent = UPTREE_NS::get_entrance(new_tree);
        
        // install the new top layer
        persist_assign(&(entrance_->upent), galc->relative(new_upent));
        uptree_ = new_tree;
        
        /* free the old top layer */
        #ifdef BACKGROUND_REBUILD
            usleep(100); // TODO: wait until on-going readers finish
        #endif
        delete old_tree;            // free the volatile object
        UPTREE_NS::free(old_upent); // free the PM space
        
        // clear the immutable_ and finish rebuilding
        immutable_.clear();
        
        persist_assign(&(entrance_->is_rebuilding), false);
    }

    void rebuild_recover() { // rebuilding function two
        // get the snapshot of all sub-index trees by traverse in the down layer
        std::vector<Record> subroots;
        subroots.reserve(0x2fffff);
        _key_t split_key = 0; 
        Node ** sibling_ptr = (Node **)uptree_->find_first();
        Node * cur_root = (Node *)galc->absolute(*sibling_ptr);
        while (cur_root != NULL) {
            subroots.emplace_back(split_key, (char *)(*sibling_ptr));
            // get next sibling
            cur_root->get_sibling(split_key, sibling_ptr);
            cur_root = galc->absolute(*sibling_ptr);
        }

        /* rebuild the top layer with immutable_ */  
        UPTREE_NS::uptree_t * old_tree = uptree_;
        UPTREE_NS::entrance_t * old_upent = galc->absolute(entrance_->upent);
        UPTREE_NS::uptree_t * new_tree = new UPTREE_NS::uptree_t(subroots);
        UPTREE_NS::entrance_t * new_upent = UPTREE_NS::get_entrance(new_tree);
        
        // install the new top layer
        persist_assign(&(entrance_->upent), galc->relative(new_upent));
        uptree_ = new_tree;
        
        /* free the old top layer */
        delete old_tree;            // free the volatile object
        UPTREE_NS::free(old_upent); // free the PM space
    }
};

} // tlbtree namespace

#endif //__TLBTREE_H__