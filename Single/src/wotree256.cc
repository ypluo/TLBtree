#include "wotree256.h"

namespace wotree256 {

bool insert_recursive(Node * n, _key_t k, _value_t v, _key_t &split_k, Node * &split_node, int8_t &level) {
    if(n->leftmost_ptr_ == NULL) {
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

bool remove_recursive(Node * n, _key_t k) {
    if(n->leftmost_ptr_ == NULL) {
        n->remove(k);
        return n->state_.unpack.count < UNDERFLOW_CARD;
    }
    else {
        Node * child = (Node *) galc->absolute(n->get_child(k));

        bool shouldMrg = remove_recursive(child, k);

        if(shouldMrg) {
            Node *leftsib = NULL, *rightsib = NULL;
            n->get_lrchild(k, leftsib, rightsib);

            if(leftsib != NULL && (child->state_.unpack.count + leftsib->state_.unpack.count) < CARDINALITY) {
                // merge with left node
                int8_t slotid = child->state_.read(0);
                n->remove(child->recs_[slotid].key);
                Node::merge(leftsib, child);

                return n->state_.unpack.count < UNDERFLOW_CARD;
            } else if (rightsib != NULL && (child->state_.unpack.count + rightsib->state_.unpack.count) < CARDINALITY) {
                // merge with right node
                int8_t slotid = rightsib->state_.read(0);
                n->remove(rightsib->recs_[slotid].key);
                Node::merge(child, rightsib);

                return n->state_.unpack.count < UNDERFLOW_CARD;
            }
        }
        return false;
    }
}

bool find(Node ** rootPtr, _key_t key, _value_t &val) {
    Node * cur = galc->absolute(*rootPtr);
    while(cur->leftmost_ptr_ != NULL) { // no prefetch here
        char * child_ptr = cur->get_child(key);
        cur = (Node *)galc->absolute(child_ptr);
    }

    val = (_value_t) cur->get_child(key);

    if((char *)val == NULL)
        return false;
    else
        return true;
}

res_t insert(Node ** rootPtr, _key_t key, _value_t val, int threshold) {
    Node *root_= galc->absolute(*rootPtr);
    
    int8_t level = 1;
    _key_t split_k;
    Node * split_node;
    bool splitIf = insert_recursive(root_, key, val, split_k, split_node, level);

    if(splitIf) {
        if(level < threshold) {
            Node *new_root = new Node;
            new_root->leftmost_ptr_ = (char *)galc->relative(root_);
            new_root->append({split_k, (char *)galc->relative(split_node)}, 0, 0);
            new_root->state_.unpack.count = 1;

            clwb(new_root, 64);

            mfence(); // a barrier to make sure the new node is persisted
            persist_assign(rootPtr, (Node *)galc->relative(new_root));

            return res_t(false, {0, NULL});
        } else {
            return res_t(true, {split_k, (char *)split_node});
        }   
    }
    else {
        return res_t(false, {0, NULL});
    }
}

bool update(Node ** rootPtr, _key_t key, _value_t val) {
    Node * cur = galc->absolute(*rootPtr);
    while(cur->leftmost_ptr_ != NULL) { // no prefetch here
        char * child_ptr = cur->get_child(key);
        cur = (Node *)galc->absolute(child_ptr);
    }

    val = (_value_t) cur->update(key, val);
    return true;
}

bool remove(Node ** rootPtr, _key_t key) {   
    Node *root_= galc->absolute(*rootPtr);
    if(root_->leftmost_ptr_ == NULL) {
        root_->remove(key);

        return root_->state_.unpack.count == 0;
    }
    else {
        Node * child = (Node *) galc->absolute(root_->get_child(key));

        bool shouldMrg = remove_recursive(child, key);

        if(shouldMrg) {
            Node *leftsib = NULL, *rightsib = NULL;
            root_->get_lrchild(key, leftsib, rightsib);

            if(leftsib != NULL && (child->state_.unpack.count + leftsib->state_.unpack.count) < CARDINALITY) {
                // merge with left node
                int8_t slotid = child->state_.read(0);
                root_->remove(child->recs_[slotid].key);
                Node::merge(leftsib, child);
            } 
            else if (rightsib != NULL && (child->state_.unpack.count + rightsib->state_.unpack.count) < CARDINALITY) {
                // merge with right node
                int8_t slotid = rightsib->state_.read(0);
                root_->remove(rightsib->recs_[slotid].key);
                Node::merge(child, rightsib);
            }
            
            if(root_->state_.unpack.count == 0) { // the root is empty
                Node * old_root = root_;

                persist_assign(rootPtr, (Node *)root_->leftmost_ptr_);

                galc->free(old_root);
            }
        }

        return false;
    } 
}

void printAll(Node ** rootPtr) {
    Node *root= galc->absolute(*rootPtr);
    root->print("", true);
}

} // namespace wotree256