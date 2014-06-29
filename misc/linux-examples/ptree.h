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
  bool doBalancing;

  ptree()
      : root(NULL),
        size(0),
        doBalancing(true) {
  }

  ptree(void** _root)
      : size(0),
        doBalancing(true) {

    root = (struct node**) OFF(_root);
    cout << "root : " << root << "\n";
  }

  virtual ~ptree(void) {
    clear();
  }

  void clear() {
    clear_node((*PMEM(root)));
  }

  void clear_node(node* np) {
    if (np == NULL) {
      return;
    }

    clear_node(PMEM(np)->left);
    clear_node(PMEM(np)->right);
    size--;

    if (PMEM(np)->parent != NULL) {
      if (((node*) PMEM(PMEM(np)->parent))->left == np) {
        ((node*) PMEM(PMEM(np)->parent))->left = 0;
        ((node*) PMEM(PMEM(np)->parent))->left_max_depth--;
      } else {
        ((node*) PMEM(PMEM(np)->parent))->right = 0;
        ((node*) PMEM(PMEM(np)->parent))->right_max_depth--;
      }
    } else {
      (*PMEM(root)) = NULL;
    }

    delete PMEM(np);
  }

  int get_balance(node* np) const {
    return PMEM(np)->right_max_depth - PMEM(np)->left_max_depth;
  }

  // single right rotate
  void srr(node* np) {
    bool newRoot = (np == (*PMEM(root)));

    ((node*) PMEM(PMEM(np)->left))->parent = PMEM(np)->parent;
    node* old_right = ((node*) PMEM(PMEM(np)->left))->right;

    ((node*) PMEM(PMEM(np)->left))->right = np;
    PMEM(np)->parent = PMEM(np)->left;
    ((node*) PMEM(PMEM(np)->parent))->right = np;
    PMEM(np)->left = old_right;
    PMEM(np)->left_max_depth =
        (old_right ? get_max_children_size(old_right) : 0);

    if (old_right) {
      PMEM(old_right)->parent = np;
      PMEM(np)->left_max_depth++;
    }
    ((node*) PMEM(PMEM(np)->parent))->right_max_depth++;

    if (newRoot) {
      (*PMEM(root)) = PMEM(np)->parent;
    } else {
      if (((node*) PMEM(((node*)PMEM(PMEM(np)->parent))->parent))->left == np) {
        ((node*) PMEM(((node*)PMEM(PMEM(np)->parent))->parent))->left = PMEM(np)
            ->parent;
      } else {
        ((node*) PMEM(((node*)PMEM(PMEM(np)->parent))->parent))->right =
        PMEM(np)->parent;
      }
    }
  }

  // single left rotate
  void slr(node* np) {
    bool newRoot = (np == (*PMEM(root)));

    ((node*) PMEM(PMEM(np)->right))->parent = PMEM(np)->parent;
    node* old_left = ((node*) PMEM(PMEM(np)->right))->left;

    ((node*) PMEM(PMEM(np)->right))->left = np;
    PMEM(np)->parent = PMEM(np)->right;
    ((node*) PMEM(PMEM(np)->parent))->left = np;
    PMEM(np)->right = old_left;
    PMEM(np)->right_max_depth =
        (old_left ? get_max_children_size(old_left) : 0);

    if (old_left) {
      PMEM(old_left)->parent = np;
      PMEM(np)->right_max_depth++;
    }
    ((node*) PMEM(PMEM(np)->parent))->left_max_depth++;

    if (newRoot) {
      (*PMEM(root)) = PMEM(np)->parent;
    } else {
      if (((node*) PMEM(((node*)PMEM(PMEM(np)->parent))->parent))->left == np) {
        ((node*) PMEM(((node*)PMEM(PMEM(np)->parent))->parent))->left = PMEM(np)
            ->parent;
      } else {
        ((node*) PMEM(((node*)PMEM(PMEM(np)->parent))->parent))->right =
        PMEM(np)->parent;
      }
    }
  }

  // balance a node, invoke rotations on demand
  bool balance(node* parent) {
    bool didBalance = false;

    if (doBalancing) {
      int balancing = get_balance(parent);

      if (balancing < -1) {
        int balanceRightHeavy = get_balance(PMEM(parent)->left);
        if (balanceRightHeavy >= 1) {
          slr(PMEM(parent)->left);
          srr(parent);
        } else {
          srr(parent);
        }
        didBalance = true;

      } else if (balancing > 1) {
        int balanceLeftHeavy = get_balance(PMEM(parent)->right);
        if (balanceLeftHeavy <= -1) {
          srr(PMEM(parent)->right);
          slr(parent);
        } else {
          slr(parent);
        }
        didBalance = true;

      }
    }

    return didBalance;
  }

  int get_max_children_size(node* np) const {
    return std::max(PMEM(np)->left_max_depth, PMEM(np)->right_max_depth);
  }

  // Propagates new max children size to parents, does balancing on demand.
  void propagate_max_children_size(node* notified_node, node* sender_node,
                                   int child_maxsize, bool is_deletion) {

    bool isRight = sender_node == PMEM(notified_node)->right;
    int maxsize = child_maxsize + 1;

    int old_maxsize = get_max_children_size(notified_node);
    if (isRight) {
      PMEM(notified_node)->right_max_depth = maxsize;
    } else {
      PMEM(notified_node)->left_max_depth = maxsize;
    }

    int new_maxsize = get_max_children_size(notified_node);
    if (balance(notified_node)) {
      if (is_deletion) {
        notified_node = PMEM(notified_node)->parent;  // our notified_node moved, readjust
      } else {
        return;
      }
    }

    if (PMEM(notified_node)->parent) {
      if (is_deletion ?
          new_maxsize != old_maxsize : new_maxsize > old_maxsize) {
        propagate_max_children_size(PMEM(notified_node)->parent, notified_node,
                                    new_maxsize, is_deletion);
      }
    }
  }

  void insert(const K& key, const V& val) {
    node* current_node = (*PMEM(root));

    if (current_node == NULL) {
      node* np = OFF(new node(key, val));

      (*PMEM(root)) = np;
      pmemalloc_activate(pmp, np);

      size++;
      return;
    }

    for (;;) {
      if (PMEM(current_node)->key == key) {
        PMEM(current_node)->val = val;
        break;
      } else if (PMEM(current_node)->key > key) {
        if (PMEM(current_node)->left == NULL) {
          node* np = OFF(new node(key, val));
          PMEM(np)->parent = current_node;
          PMEM(current_node)->left = np;
          pmemalloc_activate(pmp, np);

          size++;
          propagate_max_children_size(current_node, PMEM(current_node)->left, 0,
                                      false);
          break;
        }

        current_node = PMEM(current_node)->left;
      } else {
        if (PMEM(current_node)->right == NULL) {
          node* np = OFF(new node(key, val));
          PMEM(np)->parent = current_node;
          PMEM(current_node)->right = np;
          pmemalloc_activate(pmp, np);

          size++;
          propagate_max_children_size(current_node, PMEM(current_node)->right,
                                      0, false);
          break;
        }
        current_node = PMEM(current_node)->right;
      }
    }

  }

  node* find(const K& key) const {
    node* current_node = (*PMEM(root));

    if (current_node == NULL) {
      return NULL;
    }

    for (;;) {
      if (PMEM(current_node)->key == key) {
        return current_node;
      }

      if (PMEM(current_node)->key > key) {
        if (PMEM(current_node)->left) {
          current_node = PMEM(current_node)->left;
        } else {
          return NULL;
        }

      } else {
        if (PMEM(current_node)->right) {
          current_node = PMEM(current_node)->right;
        } else {
          return NULL;
        }
      }
    }

    return NULL;
  }

  V at(const K& key) const {
    node* ret = find(key);
    return ret ? PMEM(ret)->val : 0;
  }

  bool contains(const K& key) const {
    return find(key) ? true : false;
  }

  // retrieves the leftmost child
  node* getLeftMostNode(node* np) const {
    node* current_node = np;

    if (!PMEM(current_node)->left) {
      return current_node;
    } else {
      return getLeftMostNode(PMEM(current_node)->left);
    }
  }

  // erase a key from the tree
  bool erase(const K& key) {
    node* np = find(key);
    if (np == NULL) {
      return false;
    }

    size--;

    node* parent = PMEM(np)->parent;
    bool is_root_node = PMEM(np)->parent ? false : true;
    bool parent_pos_right = !is_root_node ? PMEM(parent)->right == np : false;
    bool has_left = PMEM(np)->left ? true : false;
    bool has_right = PMEM(np)->right ? true : false;

    // deleted node has no leaves
    if (!has_left && !has_right) {
      if (!is_root_node) {
        if (!parent_pos_right) {
          PMEM(parent)->left = 0;
          PMEM(parent)->left_max_depth--;
        } else {
          PMEM(parent)->right = 0;
          PMEM(parent)->right_max_depth--;
        }
      }

      delete PMEM(np);

      if (is_root_node) {
        (*PMEM(root)) = NULL;
        return true;
      }
    }
    // deleted node has exactly one leaf
    else if ((has_left && !has_right) || (has_right && !has_left)) {
      bool detachRight = PMEM(np)->right ? true : false;

      if ((!parent_pos_right) && (!is_root_node)) {
        PMEM(parent)->left = detachRight ? PMEM(np)->right : PMEM(np)->left;
        PMEM(parent)->left_max_depth = (
            detachRight ?
                get_max_children_size(PMEM(np)->right) :
                get_max_children_size(PMEM(np)->left)) + 1;
      } else if (!is_root_node) {
        PMEM(parent)->right = detachRight ? PMEM(np)->right : PMEM(np)->left;
        PMEM(parent)->right_max_depth = (
            detachRight ?
                get_max_children_size(PMEM(np)->right) :
                get_max_children_size(PMEM(np)->left)) + 1;
      }

      if (detachRight) {
        ((node*) PMEM(PMEM(np)->right))->parent = parent;
      } else {
        ((node*) PMEM(PMEM(np)->left))->parent = parent;
      }

      if (is_root_node) {
        (*PMEM(root)) = detachRight ? PMEM(np)->right : PMEM(np)->left;
        parent = (*PMEM(root));
      }

      delete PMEM(np);
    }
    // deleted node has 2 leaves
    else if (has_left && has_right) {
      node* replace_node = getLeftMostNode(PMEM(np)->right);
      node* propagation_root_node = PMEM(replace_node)->parent;

      PMEM(np)->key = PMEM(replace_node)->key;
      PMEM(np)->val = PMEM(replace_node)->val;

      if (replace_node != PMEM(np)->right) {
        ((node*) PMEM(PMEM(replace_node)->parent))->left = 0;
        ((node*) PMEM(PMEM(replace_node)->parent))->left_max_depth = 0;
        parent = propagation_root_node;
      } else {
        node* old_right = PMEM(replace_node)->right;
        PMEM(np)->right = old_right;
        PMEM(np)->right_max_depth =
            old_right ? (PMEM(old_right)->right_max_depth + 1) : 0;
        if (old_right) {
          PMEM(old_right)->parent = np;
        }
        parent = np;
      }

      delete PMEM(replace_node);
    }

    bool didBalance = balance(parent);
    if (didBalance) {
      parent = PMEM(parent)->parent;
    }

    if (parent && PMEM(parent)->parent) {
      propagate_max_children_size(PMEM(parent)->parent, parent,
                                  get_max_children_size(parent), true);
    }

    return true;
  }

  void display() {
    node* current_node = (*PMEM(root));

    if (current_node == NULL) {
      cout << "Empty tree" << endl;
      return;
    }

    display_node(current_node);
  }

  void display_node(node* np) {
    if (np != NULL) {
      cout << " key: " << PMEM(np)->key << " val: " << PMEM(np)->val
          << "  bal: " << get_balance(np) << "\n";
      display_node(PMEM(np)->left);
      display_node(PMEM(np)->right);
    }
  }

};

#endif /* PMEM_TREE_H_ */
