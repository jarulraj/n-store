#ifndef PMEM_TREE_H_
#define PMEM_TREE_H_

#include "libpm.h"

using namespace std;

template<typename K, typename V>
class ptree {
 public:

  struct node {
    node(const K& _key, const V& _val)
        : parent(NULL),
          left(NULL),
          right(NULL),
          left_max_depth(0),
          right_max_depth(0),
          key(_key),
          val(_val) {
    }

    node* parent;
    node* left;
    node* right;
    int left_max_depth;
    int right_max_depth;
    K key;
    V val;
  };

  node** root;
  int size;

  ptree()
      : root(NULL),
        size(0) {
  }

  ptree(void** _root)
      : size(0) {
    root = (struct node**) _root;
  }

  virtual ~ptree(void) {
    clear();
  }

  void clear() {
    clear_node((*root));
  }

  void clear_node(node* np) {
    if (np == NULL) {
      return;
    }

    clear_node(np->left);
    clear_node(np->right);
    size--;

    if (np->parent != NULL) {
      if ((np->parent)->left == np) {
        (np->parent)->left = 0;
        (np->parent)->left_max_depth--;
      } else {
        (np->parent)->right = 0;
        (np->parent)->right_max_depth--;
      }
    } else {
      (*root) = NULL;
    }

    delete np;
  }

  int get_balance(node* np) const {
    return np->right_max_depth - np->left_max_depth;
  }

  // single right rotate
  void srr(node* np) {
    bool newRoot = (np == (*root));

    (np->left)->parent = np->parent;
    node* old_right = (np->left)->right;

    (np->left)->right = np;
    np->parent = np->left;
    (np->parent)->right = np;
    np->left = old_right;
    np->left_max_depth = (old_right ? get_max_children_size(old_right) : 0);

    if (old_right) {
      old_right->parent = np;
      np->left_max_depth++;
    }
    (np->parent)->right_max_depth++;

    if (newRoot) {
      (*root) = np->parent;
    } else {
      if (np->parent->parent->left == np) {
        np->parent->parent->left = np->parent;
      } else {
        np->parent->parent->right = np->parent;
      }
    }
  }

  // single left rotate
  void slr(node* np) {
    bool newRoot = (np == (*root));

    (np->right)->parent = np->parent;
    node* old_left = (np->right)->left;

    (np->right)->left = np;
    np->parent = np->right;
    (np->parent)->left = np;
    np->right = old_left;
    np->right_max_depth = (old_left ? get_max_children_size(old_left) : 0);

    if (old_left) {
      old_left->parent = np;
      np->right_max_depth++;
    }
    (np->parent)->left_max_depth++;

    if (newRoot) {
      (*root) = np->parent;
    } else {
      if (np->parent->parent->left == np) {
        np->parent->parent->left = np->parent;
      } else {
        np->parent->parent->right = np->parent;
      }
    }
  }

  // balance a node, invoke rotations on demand
  bool balance(node* parent) {
    bool didBalance = false;

    int balancing = get_balance(parent);

    if (balancing < -1) {
      int balanceRightHeavy = get_balance(parent->left);
      if (balanceRightHeavy >= 1) {
        slr(parent->left);
        srr(parent);
      } else {
        srr(parent);
      }
      didBalance = true;

    } else if (balancing > 1) {
      int balanceLeftHeavy = get_balance(parent->right);
      if (balanceLeftHeavy <= -1) {
        srr(parent->right);
        slr(parent);
      } else {
        slr(parent);
      }
      didBalance = true;

    }

    return didBalance;
  }

  int get_max_children_size(node* np) const {
    return std::max(np->left_max_depth, np->right_max_depth);
  }

  // Propagates new max children size to parents, does balancing on demand.
  void propagate_max_children_size(node* notified_node, node* sender_node,
                                   int child_maxsize, bool is_deletion) {

    bool isRight = sender_node == notified_node->right;
    int maxsize = child_maxsize + 1;

    int old_maxsize = get_max_children_size(notified_node);
    if (isRight) {
      notified_node->right_max_depth = maxsize;
    } else {
      notified_node->left_max_depth = maxsize;
    }

    int new_maxsize = get_max_children_size(notified_node);
    if (balance(notified_node)) {
      if (is_deletion) {
        notified_node = notified_node->parent;  // our notified_node moved, readjust
      } else {
        return;
      }
    }

    if (notified_node->parent) {
      if (is_deletion ?
          new_maxsize != old_maxsize : new_maxsize > old_maxsize) {
        propagate_max_children_size(notified_node->parent, notified_node,
                                    new_maxsize, is_deletion);
      }
    }
  }

  void insert(const K& key, const V& val) {
    node* current_node = (*root);

    if (current_node == NULL) {
      node* np = new node(key, val);

      (*root) = np;
      pmemalloc_activate(np);

      size++;
      return;
    }

    for (;;) {
      if (current_node->key == key) {
        current_node->val = val;
        break;
      } else if (current_node->key > key) {
        if (current_node->left == NULL) {
          node* np = new node(key, val);
          np->parent = current_node;
          current_node->left = np;
          pmemalloc_activate(np);

          size++;
          propagate_max_children_size(current_node, current_node->left, 0,
                                      false);
          break;
        }

        current_node = current_node->left;
      } else {
        if (current_node->right == NULL) {
          node* np = new node(key, val);
          np->parent = current_node;
          current_node->right = np;
          pmemalloc_activate(np);

          size++;
          propagate_max_children_size(current_node, current_node->right, 0,
                                      false);
          break;
        }
        current_node = current_node->right;
      }
    }

  }

  node* find(const K& key) const {
    node* current_node = (*root);

    if (current_node == NULL) {
      return NULL;
    }

    for (;;) {
      if (current_node->key == key) {
        return current_node;
      }

      if (current_node->key > key) {
        if (current_node->left) {
          current_node = current_node->left;
        } else {
          return NULL;
        }

      } else {
        if (current_node->right) {
          current_node = current_node->right;
        } else {
          return NULL;
        }
      }
    }

    return NULL;
  }

  V at(const K& key) const {
    node* ret = find(key);
    return ret ? ret->val : 0;
  }

  bool contains(const K& key) const {
    return find(key) ? true : false;
  }

  // retrieves the leftmost child
  node* getLeftMostNode(node* np) const {
    node* current_node = np;

    if (!current_node->left) {
      return current_node;
    } else {
      return getLeftMostNode(current_node->left);
    }
  }

  // erase a key from the tree
  bool erase(const K& key) {
    node* np = find(key);
    if (np == NULL) {
      return false;
    }

    size--;

    node* parent = np->parent;
    bool is_root_node = np->parent ? false : true;
    bool parent_pos_right = !is_root_node ? parent->right == np : false;
    bool has_left = np->left ? true : false;
    bool has_right = np->right ? true : false;

    // deleted node has no leaves
    if (!has_left && !has_right) {
      if (!is_root_node) {
        if (!parent_pos_right) {
          parent->left = 0;
          parent->left_max_depth--;
        } else {
          parent->right = 0;
          parent->right_max_depth--;
        }
      }

      delete np;

      if (is_root_node) {
        (*root) = NULL;
        return true;
      }
    }
    // deleted node has exactly one leaf
    else if ((has_left && !has_right) || (has_right && !has_left)) {
      bool detachRight = np->right ? true : false;

      if ((!parent_pos_right) && (!is_root_node)) {
        parent->left = detachRight ? np->right : np->left;
        parent->left_max_depth = (
            detachRight ?
                get_max_children_size(np->right) :
                get_max_children_size(np->left)) + 1;
      } else if (!is_root_node) {
        parent->right = detachRight ? np->right : np->left;
        parent->right_max_depth = (
            detachRight ?
                get_max_children_size(np->right) :
                get_max_children_size(np->left)) + 1;
      }

      if (detachRight) {
        (np->right)->parent = parent;
      } else {
        (np->left)->parent = parent;
      }

      if (is_root_node) {
        (*root) = detachRight ? np->right : np->left;
        parent = (*root);
      }

      delete np;
    }
    // deleted node has 2 leaves
    else if (has_left && has_right) {
      node* replace_node = getLeftMostNode(np->right);
      node* propagation_root_node = replace_node->parent;

      np->key = replace_node->key;
      np->val = replace_node->val;

      if (replace_node != np->right) {
        (replace_node->parent)->left = 0;
        (replace_node->parent)->left_max_depth = 0;
        parent = propagation_root_node;
      } else {
        node* old_right = replace_node->right;
        np->right = old_right;
        np->right_max_depth = old_right ? (old_right->right_max_depth + 1) : 0;
        if (old_right) {
          old_right->parent = np;
        }
        parent = np;
      }

      delete replace_node;
    }

    bool didBalance = balance(parent);
    if (didBalance) {
      parent = parent->parent;
    }

    if (parent && parent->parent) {
      propagate_max_children_size(parent->parent, parent,
                                  get_max_children_size(parent), true);
    }

    return true;
  }

  void display() {
    node* current_node = (*root);

    if (current_node == NULL) {
      cout << "Empty tree" << endl;
      return;
    }

    display_node(current_node);
  }

  void display_node(node* np) {
    if (np != NULL) {
      cout << " key: " << np->key << " val: " << np->val << "  bal: "
          << get_balance(np) << "\n";
      display_node(np->left);
      display_node(np->right);
    }
  }

};

#endif /* PMEM_TREE_H_ */
