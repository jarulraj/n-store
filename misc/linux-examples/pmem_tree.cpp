#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <string>

#include <thread>
#include <mutex>

#include <vector>
#include <map>
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <exception>

#include "libpm.h"

using namespace std;

void* pmp;
std::mutex pmp_mutex;

#define MAX_PTRS 128

struct static_info {
  void* ptrs[MAX_PTRS];
};

struct static_info *sp;

/**
 * An AVL tree
 */
class ptree {
 public:

  struct node {
    node(const int& _key, const int& _val)
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
    int key;
    int val;
  };

  node* root_node;
  int size;
  bool doBalancing;

  ptree()
      : root_node(0),
        size(0),
        doBalancing(true) {
  }

  virtual ~ptree(void) {
    clear();
  }

  void clear() {
    clear_node(root_node);
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

  void insert(const int& key, const int& val) {
    node* current_node = this->root_node;

    if (!current_node) {
      this->root_node = new node(key, val);
      size++;
      return;
    }

    for (;;) {
      if (current_node->key == key) {
        current_node->val = val;
        return;
      }

      if (current_node->key > key) {
        if (!current_node->left) {
          current_node->left = new_node(current_node, key, val);
          size++;
          propagate_max_children_size(current_node, current_node->left, 0,
                                      false);
          return;
        }
        current_node = current_node->left;

      } else {
        if (!current_node->right) {
          current_node->right = new_node(current_node, key, val);
          size++;
          propagate_max_children_size(current_node, current_node->right, 0,
                                      false);
          return;
        }
        current_node = current_node->right;

      }

    }

  }

  int at(const int& key) const {
    node* ret = getNode(key);
    return ret ? ret->val : 0;
  }

  bool contains(const int& key) const {
    return getNode(key) ? true : false;
  }

  // balance a node, invoke rotations on demand
  bool balance(node* parent) {
    bool didBalance = false;

    if (doBalancing) {
      int balancing = getBalance_(parent);

      if (balancing < -1) {
        int balanceRightHeavy = getBalance_(parent->left);
        if (balanceRightHeavy >= 1) {
          slr(parent->left);
          srr(parent);
        } else {
          srr(parent);
        }
        didBalance = true;

      } else if (balancing > 1) {
        int balanceLeftHeavy = getBalance_(parent->right);
        if (balanceLeftHeavy <= -1) {
          srr(parent->right);
          slr(parent);
        } else {
          slr(parent);
        }
        didBalance = true;

      }
    }

    return didBalance;
  }

  // erase a key from the tree
  bool erase(const int& key) {
    node* np = getNode(key);
    if (!np) {
      return false;
    }

    this->size--;

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
        root_node = 0;
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
                getMaxChildrenSize_(np->right) : getMaxChildrenSize_(np->left))
            + 1;
      } else if (!is_root_node) {
        parent->right = detachRight ? np->right : np->left;
        parent->right_max_depth = (
            detachRight ?
                getMaxChildrenSize_(np->right) : getMaxChildrenSize_(np->left))
            + 1;
      }

      if (detachRight) {
        np->right->parent = parent;
      } else {
        np->left->parent = parent;
      }

      if (is_root_node) {
        root_node = detachRight ? np->right : np->left;
        parent = root_node;
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
        replace_node->parent->left = 0;
        replace_node->parent->left_max_depth = 0;
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

      delete (replace_node);
    }

    bool didBalance = balance(parent);
    if (didBalance) {
      parent = parent->parent;
    }

    if (parent && parent->parent) {
      propagate_max_children_size(parent->parent, parent,
                                  getMaxChildrenSize_(parent), true);
    }

    return true;
  }

  node* getNode(const int& key) const {
    node* current_node = this->root_node;

    if (!current_node) {
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

  // create a new node
  node* new_node(node* parent, const int& key, const int& val) {
    node* newNode = new node(key, val);
    newNode->parent = parent;
    return newNode;
  }

  // Propagates new max children size to parents, does balancing on demand.
  void propagate_max_children_size(node* notified_node, node* sender_node,
                                   int child_maxsize, bool is_deletion) {
    bool isRight = sender_node == notified_node->right;
    int maxsize = child_maxsize + 1;
    int old_maxsize = getMaxChildrenSize_(notified_node);
    if (isRight) {
      notified_node->right_max_depth = maxsize;
    } else {
      notified_node->left_max_depth = maxsize;
    }
    int new_maxsize = getMaxChildrenSize_(notified_node);

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

  int getBalance_(node* np) const {
    return np->right_max_depth - np->left_max_depth;
  }

  int getMaxChildrenSize_(node* np) const {
    return std::max(np->left_max_depth, np->right_max_depth);
  }

  void clear_node(node* np) {
    if (!np) {
      return;
    }

    if (np->left) {
      clear_node(np->left);
    }
    if (np->right) {
      clear_node(np->right);
    }

    this->size--;

    if (np->parent) {
      if (np->parent->left == np) {
        np->parent->left = 0;
        np->parent->left_max_depth--;
      } else {
        np->parent->right = 0;
        np->parent->right_max_depth--;
      }
    } else {
      root_node = 0;
    }

    delete np;
  }

  // set tree balancing mode (useful for debugging purposes)
  void setBalancing(bool mode) {
    doBalancing = mode;
  }

  // single right rotate
  void srr(node* np) {
    bool newRoot = (np == root_node);
    np->left->parent = np->parent;
    node* old_right = np->left->right;
    np->left->right = np;
    np->parent = np->left;
    np->parent->right = np;
    np->left = old_right;
    np->left_max_depth = old_right ? getMaxChildrenSize_(old_right) : 0;
    if (old_right) {
      old_right->parent = np;
      np->left_max_depth++;
    }
    np->parent->right_max_depth++;

    if (newRoot) {
      root_node = np->parent;
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
    bool newRoot = (np == root_node);
    np->right->parent = np->parent;
    node* old_left = np->right->left;
    np->right->left = np;
    np->parent = np->right;
    np->parent->left = np;
    np->right = old_left;
    np->right_max_depth = old_left ? getMaxChildrenSize_(old_left) : 0;
    if (old_left) {
      old_left->parent = np;
      np->right_max_depth++;
    }
    np->parent->left_max_depth++;

    if (newRoot) {
      root_node = np->parent;
    } else {
      if (np->parent->parent->left == np) {
        np->parent->parent->left = np->parent;
      } else {
        np->parent->parent->right = np->parent;
      }
    }
  }

  void display() {
    display_node(root_node);
  }

  void display_node(node* pCurr) {
    if (!pCurr) {
      return;
    }

    cout << "key: " << pCurr->key << " " << " val: " << pCurr->val << endl;

    if (pCurr->left) {
      display_node(pCurr->left);
    }
    if (pCurr->right) {
      display_node(pCurr->right);
    }
    return;
  }

};

int main() {
  const char* path = "./testfile";

  ptree* tree = new ptree();

  int val;
  srand(time(NULL));

  tree->insert(10, 10);
  tree->insert(30, 30);
  tree->insert(5, 20);

  std::string str = "./test";
  tree->display();

  delete tree;

}

