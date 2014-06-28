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
class AvlTree {
 public:

  /**
   * Node
   */
  struct AvlNode {
    AvlNode(const int& key, const int& val)
        : parent_node(0),
          left_node(0),
          right_node(0),
          left_max_depth(0),
          right_max_depth(0),
          key(key),
          val(val) {
    }

    AvlNode* parent_node;
    AvlNode* left_node;
    AvlNode* right_node;
    int left_max_depth;
    int right_max_depth;
    int key;
    int val;
  };

  AvlNode* root_node;

  int size;

  bool doBalancing;

  /**
   * creates a new empty tree
   */
  AvlTree()
      : root_node(0),
        size(0),
        doBalancing(true) {
  }

  /**
   * Destroys the tree
   */
  virtual ~AvlTree(void) {
    reset();
  }

  /**
   * reset (clear) the tree
   */
  void reset() {
    removeNode_(root_node);
  }

  /**
   * balance a node, invoke rotations on demand.
   */
  bool balanceNode_(AvlNode* parent_node) {
    bool didBalance = false;

    if (doBalancing) {
      int balancing = getBalance_(parent_node);

      if (balancing < -1) {
        int balanceRightHeavy = getBalance_(parent_node->left_node);
        if (balanceRightHeavy >= 1) {
          slr(parent_node->left_node);
          srr(parent_node);
        } else {
          srr(parent_node);
        }
        didBalance = true;

      } else if (balancing > 1) {
        int balanceLeftHeavy = getBalance_(parent_node->right_node);
        if (balanceLeftHeavy <= -1) {
          srr(parent_node->right_node);
          slr(parent_node);
        } else {
          slr(parent_node);
        }
        didBalance = true;

      }
    }

    return didBalance;
  }

  /**
   * remove an entry from the tree
   */
  bool remove(const int& key) {
    AvlNode* node = getNode(key);
    if (!node) {
      return false;
    }

    this->size--;

    AvlNode* parent_node = node->parent_node;
    bool is_root_node = node->parent_node ? false : true;
    bool parent_pos_right =
        !is_root_node ? parent_node->right_node == node : false;
    bool has_left = node->left_node ? true : false;
    bool has_right = node->right_node ? true : false;

    // deleted node has no leaves
    if (!has_left && !has_right) {
      if (!is_root_node) {
        if (!parent_pos_right) {
          parent_node->left_node = 0;
          parent_node->left_max_depth--;
        } else {
          parent_node->right_node = 0;
          parent_node->right_max_depth--;
        }
      }

      delete node;

      if (is_root_node) {
        root_node = 0;
        return true;
      }
    }

    // deleted node has exactly one leaf
    else if ((has_left && !has_right) || (has_right && !has_left)) {
      bool detachRight = node->right_node ? true : false;

      if ((!parent_pos_right) && (!is_root_node)) {
        parent_node->left_node =
            detachRight ? node->right_node : node->left_node;
        parent_node->left_max_depth = (
            detachRight ?
                getMaxChildrenSize_(node->right_node) :
                getMaxChildrenSize_(node->left_node)) + 1;
      } else if (!is_root_node) {
        parent_node->right_node =
            detachRight ? node->right_node : node->left_node;
        parent_node->right_max_depth = (
            detachRight ?
                getMaxChildrenSize_(node->right_node) :
                getMaxChildrenSize_(node->left_node)) + 1;
      }

      if (detachRight) {
        node->right_node->parent_node = parent_node;
      } else {
        node->left_node->parent_node = parent_node;
      }

      if (is_root_node) {
        root_node = detachRight ? node->right_node : node->left_node;
        parent_node = root_node;
      }

      delete node;
    }

    // deleted node has 2 leaves
    else if (has_left && has_right) {
      AvlNode* replace_node = getLeftMostNode(node->right_node);
      AvlNode* propagation_root_node = replace_node->parent_node;
      node->key = replace_node->key;
      node->val = replace_node->val;

      if (replace_node != node->right_node) {
        replace_node->parent_node->left_node = 0;
        replace_node->parent_node->left_max_depth = 0;
        parent_node = propagation_root_node;
      } else {
        AvlNode* old_right_node = replace_node->right_node;
        node->right_node = old_right_node;
        node->right_max_depth =
            old_right_node ? (old_right_node->right_max_depth + 1) : 0;
        if (old_right_node) {
          old_right_node->parent_node = node;
        }
        parent_node = node;
      }

      delete (replace_node);
    }

    bool didBalance = balanceNode_(parent_node);
    if (didBalance) {
      parent_node = parent_node->parent_node;
    }

    if (parent_node && parent_node->parent_node) {
      propagate_max_children_size(parent_node->parent_node, parent_node,
                                  getMaxChildrenSize_(parent_node), true);
    }

    return true;
  }

  /**
   * retrieves the leftmost child
   */
  AvlNode* getLeftMostNode(AvlNode* node) const {
    AvlNode* current_node = node;
    if (!current_node->left_node) {
      return current_node;
    } else {
      return getLeftMostNode(current_node->left_node);
    }
  }

  /**
   * put
   */
  void put(const int& key, const int& val) {
    AvlNode* current_node = this->root_node;

    if (!current_node) {
      this->root_node = new AvlNode(key, val);
      size++;
      return;
    }

    for (;;) {
      if (current_node->key == key) {
        current_node->val = val;
        return;
      }

      if (current_node->key > key) {
        if (!current_node->left_node) {
          current_node->left_node = newNode_(current_node, key, val);
          size++;
          propagate_max_children_size(current_node, current_node->left_node, 0,
                                      false);
          return;
        }
        current_node = current_node->left_node;

      } else {
        if (!current_node->right_node) {
          current_node->right_node = newNode_(current_node, key, val);
          size++;
          propagate_max_children_size(current_node, current_node->right_node, 0,
                                      false);
          return;
        }
        current_node = current_node->right_node;

      }

    }

  }

  /**
   * Gets an key out of the avl tree
   */
  int get(const int& key) const {
    AvlNode* ret = getNode(key);
    return ret ? ret->val : 0;
  }

  /**
   * Whether a key exists in the tree
   */
  bool contains(const int& key) const {
    return getNode(key) ? true : false;
  }

  /**
   * Returns the size of the tree.
   */
  int getSize() const {
    return this->size;
  }

  /**
   * Gets an key out of the avl tree
   */
  AvlNode* getNode(const int& key) const {
    AvlNode* current_node = this->root_node;

    if (!current_node) {
      return 0;
    }

    for (;;) {
      if (current_node->key == key) {
        return current_node;
      }

      if (current_node->key > key) {
        if (current_node->left_node) {
          current_node = current_node->left_node;
        } else {
          return 0;
        }

      } else {
        if (current_node->right_node) {
          current_node = current_node->right_node;
        } else {
          return 0;
        }
      }
    }
  }

  /**
   * Generates a new node (helper).
   */
  AvlNode* newNode_(AvlNode* parent, const int& key, const int& val) {
    AvlNode* newNode = new AvlNode(key, val);
    newNode->parent_node = parent;
    return newNode;
  }

  /**
   * Propagates new max children size to parents, does balancing on demand.
   */
  void propagate_max_children_size(AvlNode* notified_node, AvlNode* sender_node,
                                   int child_maxsize, bool is_deletion) {
    bool isRight = sender_node == notified_node->right_node;
    int maxsize = child_maxsize + 1;
    int old_maxsize = getMaxChildrenSize_(notified_node);
    if (isRight) {
      notified_node->right_max_depth = maxsize;
    } else {
      notified_node->left_max_depth = maxsize;
    }
    int new_maxsize = getMaxChildrenSize_(notified_node);

    if (balanceNode_(notified_node)) {
      if (is_deletion) {
        notified_node = notified_node->parent_node;  // our notified_node moved, readjust
      } else {
        return;
      }
    }

    if (notified_node->parent_node) {
      if (is_deletion ?
          new_maxsize != old_maxsize : new_maxsize > old_maxsize) {
        propagate_max_children_size(notified_node->parent_node, notified_node,
                                    new_maxsize, is_deletion);
      }
    }
  }

  /**
   * returns the balance for a given avl node
   */
  int getBalance_(AvlNode* node) const {
    return node->right_max_depth - node->left_max_depth;
  }

  /**
   * returns the max children size
   */
  int getMaxChildrenSize_(AvlNode* node) const {
    return std::max(node->left_max_depth, node->right_max_depth);
  }

  /**
   * remove Node and all of its children, deallocate memory,
   * unlink the parents pointers (setting it to 0) and child count.
   */
  void removeNode_(AvlNode* node) {
    if (!node) {
      return;
    }

    if (node->left_node) {
      removeNode_(node->left_node);
    }
    if (node->right_node) {
      removeNode_(node->right_node);
    }

    this->size--;

    if (node->parent_node) {
      if (node->parent_node->left_node == node) {
        node->parent_node->left_node = 0;
        node->parent_node->left_max_depth--;
      } else {
        node->parent_node->right_node = 0;
        node->parent_node->right_max_depth--;
      }
    } else {
      root_node = 0;
    }

    delete node;
  }

  /**
   * set tree balancing mode (useful for debugging purposes)
   */
  void setBalancing(bool mode) {
    doBalancing = mode;
  }

  /**
   * single right rotate
   */
  void srr(AvlNode* node) {
    bool newRoot = (node == root_node);
    node->left_node->parent_node = node->parent_node;
    AvlNode* old_right_node = node->left_node->right_node;
    node->left_node->right_node = node;
    node->parent_node = node->left_node;
    node->parent_node->right_node = node;
    node->left_node = old_right_node;
    node->left_max_depth =
        old_right_node ? getMaxChildrenSize_(old_right_node) : 0;
    if (old_right_node) {
      old_right_node->parent_node = node;
      node->left_max_depth++;
    }
    node->parent_node->right_max_depth++;

    if (newRoot) {
      root_node = node->parent_node;
    } else {
      if (node->parent_node->parent_node->left_node == node) {
        node->parent_node->parent_node->left_node = node->parent_node;
      } else {
        node->parent_node->parent_node->right_node = node->parent_node;
      }
    }
  }

  /**
   * single left rotate
   */
  void slr(AvlNode* node) {
    bool newRoot = (node == root_node);
    node->right_node->parent_node = node->parent_node;
    AvlNode* old_left_node = node->right_node->left_node;
    node->right_node->left_node = node;
    node->parent_node = node->right_node;
    node->parent_node->left_node = node;
    node->right_node = old_left_node;
    node->right_max_depth =
        old_left_node ? getMaxChildrenSize_(old_left_node) : 0;
    if (old_left_node) {
      old_left_node->parent_node = node;
      node->right_max_depth++;
    }
    node->parent_node->left_max_depth++;

    if (newRoot) {
      root_node = node->parent_node;
    } else {
      if (node->parent_node->parent_node->left_node == node) {
        node->parent_node->parent_node->left_node = node->parent_node;
      } else {
        node->parent_node->parent_node->right_node = node->parent_node;
      }
    }
  }

  /**
   * dump the map into dotty format
   */
  void display() {
    display_node(root_node);
  }

  /**
   * draw a single node for dotty output
   */
  void display_node(AvlNode* pCurr) {
    if (!pCurr) {
      return;
    }

    cout << "key: " << pCurr->key << " " << " val: " << pCurr->val << endl;

    if (pCurr->left_node) {
      display_node(pCurr->left_node);
    }
    if (pCurr->right_node) {
      display_node(pCurr->right_node);
    }
    return;
  }

};

int main() {
  const char* path = "./testfile";

  AvlTree* tree = new AvlTree();

  int val;
  srand(time(NULL));

  tree->put(10, 10);
  tree->put(30, 30);
  tree->put(5, 20);

  std::string str = "./test";
  tree->display();

  delete tree;

}

