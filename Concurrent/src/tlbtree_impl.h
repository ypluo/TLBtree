/*  tlbtree.h - A two level btree for persistent memory
    Copyright(c) 2020 Luo Yongping. THIS SOFTWARE COMES WITH NO WARRANTIES, 
    USE AT YOUR OWN RISK!
*/

#ifndef __TLBTREEIMPL_H__
#define __TLBTREEIMPL_H__

#include <string>
#include <thread>
#include <algorithm>
#include <unistd.h>

#include "pmallocator.h"
#include "fixtree.h"
#include "spinlock.h"
#include "wotree256.h"

extern PMAllocator * galc;

#define BACKGROUND_REBUILD
// choose uptree type, providing interfaces: insert, remove, update, find, merge, free_uptree
#define UPTREE_NS   fixtree
// choose downtree type, providing interfaces: insert, find_lower, remove_lower
#define DOWNTREE_NS wotree256

namespace tlbtree {

using std::string;
using std::vector;
using Node = DOWNTREE_NS::Node;

template<int DOWNLEVEL, int REBUILD_THRESHOLD=2>
class TLBtreeImpl {
private:
    typedef TLBtreeImpl<DOWNLEVEL, REBUILD_THRESHOLD> SelfType;
    
    // the entrance of TLBtree that stores its persistent tree metadata
    struct tlbtree_entrance_t {
        UPTREE_NS::entrance_t * upent; // the entrance of the top layer
        Record * restore;              // restore subroots that fails to insert into the top layer
        int restore_size;
        bool is_clean;                 // is TLBtree shutdown expectedly
        bool use_rebuild_recover;      // whether to use recover rebuilding next time
    };
    
    // volatile domain
    UPTREE_NS::uptree_t * uptree_;
    tlbtree_entrance_t * entrance_;
    vector<Record> * mutable_;
    Spinlock rebuild_mtx_;
    Spinlock mutable_mtx_;
    bool is_rebuilding_;

public:
    TLBtreeImpl(string path, bool recover=true, uint64_t pool_size=10 * (1024UL * 1024 * 1024));

    ~TLBtreeImpl();

    void insert(const _key_t & k, _value_t v);

    bool find(const _key_t & k, _value_t & v) const ;

    bool update(const _key_t & k, const _value_t & v);

    bool remove(const _key_t & k);

    inline void printAll() { uptree_->printAll();}

private:
    void rebuild_fast();

    void rebuild_recover();
};

template<int DOWNLEVEL, int REBUILD_THRESHOLD>
TLBtreeImpl<DOWNLEVEL, REBUILD_THRESHOLD>::TLBtreeImpl(string path, bool recover, uint64_t pool_size) {
    mutable_ = new vector<Record>();
    mutable_->reserve(0xfff);
    bool is_rebuilding_ = false;
    
    if(recover == false) {
        galc = new PMAllocator(path.c_str(), false, "tlbtree", pool_size);
        // initialize entrance_
        entrance_ = (tlbtree_entrance_t *) galc->get_root(sizeof(tlbtree_entrance_t));
        entrance_->upent = NULL;
        entrance_->restore = NULL;
        entrance_->restore_size = 0;
        entrance_->is_clean = false;
        entrance_->use_rebuild_recover = true;
        clwb(entrance_, sizeof(tlbtree_entrance_t));
        
        //allocate a entrance_ to the fixtree
        std::vector<Record> init = {Record(INT64_MIN, (char *)galc->relative(new Node()))}; 
        uptree_ = new UPTREE_NS::uptree_t(init);
        persist_assign(&(entrance_->upent), galc->relative(UPTREE_NS::get_entrance(uptree_)));
        persist_assign(&(entrance_->use_rebuild_recover), false); // use fast rebuilding next time
    } else {
        galc = new PMAllocator(path.c_str(), true, "tlbtree");

        entrance_ = (tlbtree_entrance_t *) galc->get_root(sizeof(tlbtree_entrance_t));
        if(entrance_ == NULL || entrance_->upent == NULL) { // empty tree
            printf("the tree is empty\n");
            exit(-1);
        }

        if(entrance_->is_clean == false) { // TLBtree crashed at last usage
            persist_assign(&(entrance_->use_rebuild_recover), true); // use recover rebuilding next time
        } else { // normal shutdown
            // recover all subroots from PM back to mutable_, within miliseconds
            if(entrance_->restore != NULL) {
                Record * rec = galc->absolute(entrance_->restore);
                for(int i = 0; i < entrance_->restore_size; i++) {
                    mutable_->push_back(rec[i]);
                }
                entrance_->restore = NULL;
                entrance_->restore_size = 0;
                clwb(&entrance_->restore, 16);
                galc->free(rec);
            }
        }

        uptree_ = new UPTREE_NS::uptree_t (galc->absolute(entrance_->upent));
    }

    persist_assign(&(entrance_->is_clean), false); // set the TLBtree state to be dirty
}

template<int DOWNLEVEL, int REBUILD_THRESHOLD>
TLBtreeImpl<DOWNLEVEL, REBUILD_THRESHOLD>::~TLBtreeImpl() {
    if(entrance_->use_rebuild_recover == false) { // fast rebuilding next time
        // save all subroots in mutable_ into PM
        Record * rec = (Record *) galc->malloc(std::max((size_t)4096, mutable_->size() * sizeof(Record)));
        for(int i = 0; i < mutable_->size(); i++) {
            rec[i] = (*mutable_)[i];
        }
        clwb(rec, mutable_->size() * sizeof(Record));
        mfence();
        entrance_->restore = galc->relative(rec);
        entrance_->restore_size = mutable_->size();
        clwb(&entrance_->restore, 16);
    }

    //printf("%lx %d\n", entrance_->restore, entrance_->restore_size);

    persist_assign(&(entrance_->is_clean), true); // a intended shutdown

    delete uptree_;
    delete mutable_;
    delete galc;
}

template<int DOWNLEVEL, int REBUILD_THRESHOLD>
void TLBtreeImpl<DOWNLEVEL, REBUILD_THRESHOLD>::insert(const _key_t & k, _value_t v) { 
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

    // we rebuild if the searching in the linklist is too long 
    if(goes_steps > REBUILD_THRESHOLD && rebuild_mtx_.trylock()) {
        if(entrance_->use_rebuild_recover == true) {
            #ifdef BACKGROUND_REBUILD
                std::thread rebuild_thread(&SelfType::rebuild_recover, this);
                rebuild_thread.detach();
            #else
                rebuild_recover();
            #endif
        } else {
            #ifdef BACKGROUND_REBUILD
                std::thread rebuild_thread(&SelfType::rebuild_fast, this);
                rebuild_thread.detach();
            #else
                rebuild_fast();
            #endif
        } 
    }

    if(insert_res.flag == true) { // a sub-index tree is splitted
        // try save the sub-indices root into the top layer
        bool succ = uptree_->insert(insert_res.rec.key, (_value_t)galc->relative(insert_res.rec.val));
        
        // save these records into mutable_
        if(is_rebuilding_ == true || succ == false) {
            mutable_mtx_.lock();
                mutable_->push_back({insert_res.rec.key, (char *)galc->relative(insert_res.rec.val)});
            mutable_mtx_.unlock();
        }
    }
}

template<int DOWNLEVEL, int REBUILD_THRESHOLD>
bool TLBtreeImpl<DOWNLEVEL, REBUILD_THRESHOLD>::find(const _key_t & k, _value_t & v) const {
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

template<int DOWNLEVEL, int REBUILD_THRESHOLD>
bool TLBtreeImpl<DOWNLEVEL, REBUILD_THRESHOLD>::remove(const _key_t & k) {
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

template<int DOWNLEVEL, int REBUILD_THRESHOLD>
bool TLBtreeImpl<DOWNLEVEL, REBUILD_THRESHOLD>::update(const _key_t & k, const _value_t & v) {
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

template<int DOWNLEVEL, int REBUILD_THRESHOLD>
void TLBtreeImpl<DOWNLEVEL, REBUILD_THRESHOLD>::rebuild_fast() { // fast rebuilding function
    // switch the restore to be immutable
    vector<Record> * new_mutable = new vector<Record>;
    new_mutable->reserve(0xffff);
    
    mutable_mtx_.lock();
        vector<Record> * immutable = mutable_; // make the restore vector be immutable
        mutable_ = new_mutable; // before this line, the mutable_ is still the old one
    mutable_mtx_.unlock();

    is_rebuilding_ = true;

    std::sort(immutable->begin(), immutable->end());
    // get the snapshot of all sub-index trees by combining the top layer with immutable
    std::vector<Record> subroots;
    subroots.reserve(0x2ffff);
    uptree_->merge(*immutable, subroots);

    /* rebuild the top layer with immutable */  
    UPTREE_NS::uptree_t * old_tree = uptree_;
    UPTREE_NS::entrance_t * old_upent = galc->absolute(entrance_->upent);
    UPTREE_NS::uptree_t * new_tree = new UPTREE_NS::uptree_t(subroots);
    UPTREE_NS::entrance_t * new_upent = UPTREE_NS::get_entrance(new_tree);
    
    // install the new top layer
    persist_assign(&(entrance_->upent), galc->relative(new_upent));
    uptree_ = new_tree;
    
    /* free the old top layer */
    #ifdef BACKGROUND_REBUILD
        usleep(50); // TODO: unsafe memory reclamation
    #endif
    UPTREE_NS::free(old_tree); // free the old_tree

    is_rebuilding_ = false;
    asm volatile("" ::: "memory");
    rebuild_mtx_.unlock();

    delete immutable;
}

template<int DOWNLEVEL, int REBUILD_THRESHOLD>
void TLBtreeImpl<DOWNLEVEL, REBUILD_THRESHOLD>::rebuild_recover() { // slow rebuilding function 
    is_rebuilding_ = true;
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

    /* rebuild the top layer with immutable */  
    UPTREE_NS::uptree_t * old_tree = uptree_;
    UPTREE_NS::entrance_t * old_upent = galc->absolute(entrance_->upent);
    UPTREE_NS::uptree_t * new_tree = new UPTREE_NS::uptree_t(subroots);
    UPTREE_NS::entrance_t * new_upent = UPTREE_NS::get_entrance(new_tree);
    
    // install the new top layer
    persist_assign(&(entrance_->upent), galc->relative(new_upent));
    uptree_ = new_tree;
    
    /* free the old top layer */
    #ifdef BACKGROUND_REBUILD
        usleep(50); // TODO: unsafe memory reclamation
    #endif
    UPTREE_NS::free(old_tree); // free the old_tree

    is_rebuilding_ = false;
    asm volatile("" ::: "memory");
    rebuild_mtx_.unlock();

    persist_assign(&(entrance_->use_rebuild_recover), false); // use fast rebuilding next time
}

} // tlbtree namespace

#endif //__TLBTREEIMPL_H__