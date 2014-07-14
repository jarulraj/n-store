#ifndef BPT_H
#define BPT_H

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include "bpt.h"

#include <stdlib.h>

#include <list>
#include <algorithm>
using std::swap;
using std::binary_search;
using std::lower_bound;
using std::upper_bound;

namespace bpt {

/* predefined B+ info */
#ifdef UNIT_TEST
#define BP_ORDER 4
#else
#define BP_ORDER 20
#endif

/* key/value type */
typedef int value_t;
struct key_t {
  char k[16];

  key_t(const char *str = "") {
    bzero(k, sizeof(k));
    strcpy(k, str);
  }
};

inline int keycmp(const key_t &a, const key_t &b) {
#ifdef UNIT_TEST
  return strcmp(a.k, b.k);
#else
  int x = strlen(a.k) - strlen(b.k);
  return x == 0 ? strcmp(a.k, b.k) : x;
#endif
}

#define OPERATOR_KEYCMP(type) \
bool operator< (const key_t &l, const type &r) {\
return keycmp(l, r.key) < 0;\
}\
bool operator< (const type &l, const key_t &r) {\
return keycmp(l.key, r) < 0;\
}\
bool operator== (const key_t &l, const type &r) {\
return keycmp(l, r.key) == 0;\
}\
bool operator== (const type &l, const key_t &r) {\
return keycmp(l.key, r) == 0;\
}

/* offsets */
#define OFFSET_META 0
#define OFFSET_BLOCK OFFSET_META + sizeof(meta_t)
#define SIZE_NO_CHILDREN sizeof(leaf_node_t) - BP_ORDER * sizeof(record_t)

/* meta information of B+ tree */
typedef struct {
  size_t order; /* `order` of B+ tree */
  size_t value_size; /* size of value */
  size_t key_size; /* size of key */
  size_t internal_node_num; /* how many internal nodes */
  size_t leaf_node_num; /* how many leafs */
  size_t height; /* height of tree (exclude leafs) */
  off_t slot; /* where to store new block */
  off_t root_offset; /* where is the root of internal nodes */
  off_t leaf_offset; /* where is the first leaf */
} meta_t;

/* internal nodes' index segment */
struct index_t {
  key_t key;
  off_t child; /* child's offset */
};

/***
 * internal node block
 ***/
struct internal_node_t {
  typedef index_t * child_t;

  off_t parent; /* parent node offset */
  off_t next;
  off_t prev;
  size_t n; /* how many children */
  index_t children[BP_ORDER];
};

/* the final record of value */
struct record_t {
  key_t key;
  value_t value;
};

/* custom compare operator for STL algorithms */
OPERATOR_KEYCMP(index_t)
OPERATOR_KEYCMP(record_t)

/* leaf node block */
struct leaf_node_t {
  typedef record_t *child_t;

  off_t parent; /* parent node offset */
  off_t next;
  off_t prev;
  size_t n;
  record_t children[BP_ORDER];
};

/* the encapulated B+ tree */
class bplus_tree {
 public:
  char path[512];
  meta_t meta;

  bplus_tree(const char *p, bool force_empty = false)
      : fp(NULL),
        fp_level(0) {
    bzero(path, sizeof(path));
    strcpy(path, p);

    if (!force_empty)
      // read tree from file
      if (read_block(&meta, OFFSET_META) != 0)
        force_empty = true;

    if (force_empty) {
      open_file("w+");  // truncate file

      // create empty tree if file doesn't exist
      init_from_empty();
      close_file();
    }
  }

  /* init empty tree */
  void init_from_empty() {
    // init default meta
    bzero(&meta, sizeof(meta_t));
    meta.order = BP_ORDER;
    meta.value_size = sizeof(value_t);
    meta.key_size = sizeof(key_t);
    meta.height = 1;
    meta.slot = OFFSET_BLOCK;

    // init root node
    internal_node_t root;
    root.next = root.prev = root.parent = 0;
    meta.root_offset = allocate(&root);

    // init empty leaf
    leaf_node_t leaf;
    leaf.next = leaf.prev = 0;
    leaf.parent = meta.root_offset;
    meta.leaf_offset = root.children[0].child = allocate(&leaf);

    // save
    write_block(&meta, OFFSET_META);
    write_block(&root, meta.root_offset);
    write_block(&leaf, root.children[0].child);
  }

  /* multi-level file open/close */
  mutable FILE *fp;
  mutable int fp_level;

  void open_file(const char *mode = "rb+") const {
    // `rb+` will make sure we can write everywhere without truncating file
    if (fp_level == 0)
      fp = fopen(path, mode);

    ++fp_level;
  }

  void close_file() const {
    if (fp_level == 1)
      fclose(fp);

    --fp_level;
  }

  /* alloc from disk */
  off_t allocate(size_t size) {
    off_t slot = meta.slot;
    meta.slot += size;
    return slot;
  }

  off_t allocate(leaf_node_t *leaf) {
    leaf->n = 0;
    meta.leaf_node_num++;
    return allocate(sizeof(leaf_node_t));
  }

  off_t allocate(internal_node_t *node) {
    node->n = 1;
    meta.internal_node_num++;
    return allocate(sizeof(internal_node_t));
  }

  void release(leaf_node_t *leaf, off_t offset) {
    --meta.leaf_node_num;
  }

  void release(internal_node_t *node, off_t offset) {
    --meta.internal_node_num;
  }

  /* helper iterating function */
  template<class T>
  inline typename T::child_t begin(T &node) const {
    return node.children;
  }

  template<class T>
  inline typename T::child_t end(T &node) const {
    return node.children + node.n;
  }

  /* helper searching function */
  inline index_t *find(internal_node_t &node, const key_t &key) const {
    return upper_bound(begin(node), end(node) - 1, key);
  }

  inline record_t *find(leaf_node_t &node, const key_t &key) const {
    return lower_bound(begin(node), end(node), key);
  }

  /* read block from disk */
  int read_block(void *block, off_t offset, size_t size) const {
    open_file();
    fseek(fp, offset, SEEK_SET);
    size_t rd = fread(block, size, 1, fp);
    close_file();

    // 0 on success
    return rd - 1;
  }

  template<class T>
  int read_block(T *block, off_t offset) const {
    return read_block(block, offset, sizeof(T));
  }

  template<class T>
  int write_block(T *block, off_t offset) const {
    return write_block(block, offset, sizeof(T));
  }

  /* write block to disk */
  int write_block(void *block, off_t offset, size_t size) const {
    open_file();
    fseek(fp, offset, SEEK_SET);
    size_t wd = fwrite(block, size, 1, fp);
    close_file();

    return wd - 1;
  }

  /* find index */
  off_t search_index(const key_t &key) const {
    off_t cur = meta.root_offset;
    int height = meta.height;

    while (height > 1) {
      internal_node_t node;
      read_block(&node, cur);

      index_t *i = upper_bound(begin(node), end(node) - 1, key);
      cur = i->child;
      --height;
    }

    return cur;
  }

  off_t search_leaf(off_t index, const key_t &key) const {
    internal_node_t node;
    read_block(&node, index);

    index_t *i = upper_bound(begin(node), end(node) - 1, key);
    return i->child;
  }

  /* find leaf */
  off_t search_leaf(const key_t &key) const {
    return search_leaf(search_index(key), key);
  }

  int search(const key_t& key, value_t *value) const {
    leaf_node_t leaf;
    read_block(&leaf, search_leaf(key));

    // finding the record
    record_t *record = find(leaf, key);
    if (record != leaf.children + leaf.n) {
      // always return the lower bound
      *value = record->value;

      return keycmp(record->key, key);
    } else {
      return -1;
    }
  }

  int search_range(key_t *left, const key_t &right, value_t *values, size_t max,
                   bool *next = NULL) const {
    if (left == NULL || keycmp(*left, right) > 0)
      return -1;

    off_t off_left = search_leaf(*left);
    off_t off_right = search_leaf(right);
    off_t off = off_left;
    size_t i = 0;
    record_t *b, *e;

    leaf_node_t leaf;
    while (off != off_right && off != 0 && i < max) {
      read_block(&leaf, off);

      // start point
      if (off_left == off)
        b = find(leaf, *left);
      else
        b = begin(leaf);

      // copy
      e = leaf.children + leaf.n;
      for (; b != e && i < max; ++b, ++i)
        values[i] = b->value;

      off = leaf.next;
    }

    // the last leaf
    if (i < max) {
      read_block(&leaf, off_right);

      b = find(leaf, *left);
      e = upper_bound(begin(leaf), end(leaf), right);
      for (; b != e && i < max; ++b, ++i)
        values[i] = b->value;
    }

    // mark for next iteration
    if (next != NULL) {
      if (i == max && b != e) {
        *next = true;
        *left = b->key;
      } else {
        *next = false;
      }
    }

    return i;
  }

  template<class T>
  void node_create(off_t offset, T *node, T *next) {
    // new sibling node
    next->parent = node->parent;
    next->next = node->next;
    next->prev = offset;
    node->next = allocate(next);

    // update next node's prev
    if (next->next != 0) {
      T old_next;
      read_block(&old_next, next->next, SIZE_NO_CHILDREN);
      old_next.prev = node->next;
      write_block(&old_next, next->next, SIZE_NO_CHILDREN);
    }
    write_block(&meta, OFFSET_META);
  }

  template<class T>
  void node_remove(T *prev, T *node) {
    release(node, prev->next);
    prev->next = node->next;

    if (node->next != 0) {
      T next;
      read_block(&next, node->next, SIZE_NO_CHILDREN);
      next.prev = node->prev;
      write_block(&next, node->next, SIZE_NO_CHILDREN);
    }
    write_block(&meta, OFFSET_META);
  }

  // INSERT

  int insert(const key_t& key, value_t value) {
    off_t parent = search_index(key);
    off_t offset = search_leaf(parent, key);
    leaf_node_t leaf;
    read_block(&leaf, offset);

    // check if we have the same key
    if (binary_search(begin(leaf), end(leaf), key))
      return 1;

    if (leaf.n == meta.order) {
      // split when full

      // new sibling leaf
      leaf_node_t new_leaf;
      node_create(offset, &leaf, &new_leaf);

      // find even split point
      size_t point = leaf.n / 2;
      bool place_right = keycmp(key, leaf.children[point].key) > 0;
      if (place_right)
        ++point;

      // split
      std::copy(leaf.children + point, leaf.children + leaf.n,
                new_leaf.children);
      new_leaf.n = leaf.n - point;
      leaf.n = point;

      // which part do we put the key
      if (place_right)
        insert_record_no_split(&new_leaf, key, value);
      else
        insert_record_no_split(&leaf, key, value);

      // save leafs
      write_block(&leaf, offset);
      write_block(&new_leaf, leaf.next);

      // insert new index key
      insert_key_to_index(parent, new_leaf.children[0].key, offset, leaf.next);
    } else {
      insert_record_no_split(&leaf, key, value);
      write_block(&leaf, offset);
    }

    return 0;
  }

  /* insert into leaf without split */
  void insert_record_no_split(leaf_node_t *leaf, const key_t &key,
                              const value_t &value) {
    record_t *where = upper_bound(begin(*leaf), end(*leaf), key);
    std::copy_backward(where, end(*leaf), end(*leaf) + 1);

    where->key = key;
    where->value = value;
    leaf->n++;
  }

  /* add key to the internal node */
  void insert_key_to_index(off_t offset, const key_t &key, off_t old,
                           off_t after) {
    if (offset == 0) {
      // create new root node
      internal_node_t root;
      root.next = root.prev = root.parent = 0;
      meta.root_offset = allocate(&root);
      meta.height++;

      // insert `old` and `after`
      root.n = 2;
      root.children[0].key = key;
      root.children[0].child = old;
      root.children[1].child = after;

      write_block(&meta, OFFSET_META);
      write_block(&root, meta.root_offset);

      // update children's parent
      reset_index_children_parent(begin(root), end(root), meta.root_offset);
      return;
    }

    internal_node_t node;
    read_block(&node, offset);
    assert(node.n <= meta.order);

    if (node.n == meta.order) {
      // split when full

      internal_node_t new_node;
      node_create(offset, &node, &new_node);

      // find even split point
      size_t point = (node.n - 1) / 2;
      bool place_right = keycmp(key, node.children[point].key) > 0;
      if (place_right)
        ++point;

      // prevent the `key` being the right `middle_key`
      // example: insert 48 into |42|45| 6|  |
      if (place_right && keycmp(key, node.children[point].key) < 0)
        point--;

      key_t middle_key = node.children[point].key;

      // split
      std::copy(begin(node) + point + 1, end(node), begin(new_node));
      new_node.n = node.n - point - 1;
      node.n = point + 1;

      // put the new key
      if (place_right)
        insert_key_to_index_no_split(new_node, key, after);
      else
        insert_key_to_index_no_split(node, key, after);

      write_block(&node, offset);
      write_block(&new_node, node.next);

      // update children's parent
      reset_index_children_parent(begin(new_node), end(new_node), node.next);

      // give the middle key to the parent
      // note: middle key's child is reserved
      insert_key_to_index(node.parent, middle_key, offset, node.next);
    } else {
      insert_key_to_index_no_split(node, key, after);
      write_block(&node, offset);
    }
  }

  void insert_key_to_index_no_split(internal_node_t &node, const key_t &key,
                                    off_t value) {
    index_t *where = upper_bound(begin(node), end(node) - 1, key);

    // move later index forward
    std::copy_backward(where, end(node), end(node) + 1);

    // insert this key
    where->key = key;
    where->child = (where + 1)->child;
    (where + 1)->child = value;

    node.n++;
  }

  // REMOVE

  int remove(const key_t& key) {
    internal_node_t parent;
    leaf_node_t leaf;

    // find parent node
    off_t parent_off = search_index(key);
    read_block(&parent, parent_off);

    // find current node
    index_t *where = find(parent, key);
    off_t offset = where->child;
    read_block(&leaf, offset);

    // verify
    if (!binary_search(begin(leaf), end(leaf), key))
      return -1;

    size_t min_n = meta.leaf_node_num == 1 ? 0 : meta.order / 2;
    assert(leaf.n >= min_n && leaf.n <= meta.order);

    // delete the key
    record_t *to_delete = find(leaf, key);
    std::copy(to_delete + 1, end(leaf), to_delete);
    leaf.n--;

    // merge or borrow
    if (leaf.n < min_n) {
      // first borrow from left
      bool borrowed = false;
      if (leaf.prev != 0)
        borrowed = borrow_key(false, leaf);

      // then borrow from right
      if (!borrowed && leaf.next != 0)
        borrowed = borrow_key(true, leaf);

      // finally we merge
      if (!borrowed) {
        assert(leaf.next != 0 || leaf.prev != 0);

        key_t index_key;

        if (where == end(parent) - 1) {
          // if leaf is last element then merge | prev | leaf |
          assert(leaf.prev != 0);
          leaf_node_t prev;
          read_block(&prev, leaf.prev);
          index_key = begin(prev)->key;

          merge_leafs(&prev, &leaf);
          node_remove(&prev, &leaf);
          write_block(&prev, leaf.prev);
        } else {
          // else merge | leaf | next |
          assert(leaf.next != 0);
          leaf_node_t next;
          read_block(&next, leaf.next);
          index_key = begin(leaf)->key;

          merge_leafs(&leaf, &next);
          node_remove(&leaf, &next);
          write_block(&leaf, offset);
        }

        // remove parent's key
        remove_from_index(parent_off, parent, index_key);
      } else {
        write_block(&leaf, offset);
      }
    } else {
      write_block(&leaf, offset);
    }

    return 0;
  }

  /* merge right leaf to left leaf */
  void merge_leafs(leaf_node_t *left, leaf_node_t *right) {
    std::copy(begin(*right), end(*right), end(*left));
    left->n += right->n;
  }

  void merge_keys(index_t *where, internal_node_t &node,
                  internal_node_t &next) {
    //(end(node) - 1)->key = where->key;
    //where->key = (end(next) - 1)->key;
    std::copy(begin(next), end(next), end(node));
    node.n += next.n;
    node_remove(&node, &next);
  }

  /* remove internal node */
  void remove_from_index(off_t offset, internal_node_t &node,
                         const key_t &key) {
    size_t min_n = meta.root_offset == offset ? 1 : meta.order / 2;
    assert(node.n >= min_n && node.n <= meta.order);

    // remove key
    key_t index_key = begin(node)->key;
    index_t *to_delete = find(node, key);
    if (to_delete != end(node)) {
      (to_delete + 1)->child = to_delete->child;
      std::copy(to_delete + 1, end(node), to_delete);
    }
    node.n--;

    // remove to only one key
    if (node.n == 1 && meta.root_offset == offset
        && meta.internal_node_num != 1) {
      release(&node, meta.root_offset);
      meta.height--;
      meta.root_offset = node.children[0].child;
      write_block(&meta, OFFSET_META);
      return;
    }

    // merge or borrow
    if (node.n < min_n) {
      internal_node_t parent;
      read_block(&parent, node.parent);

      // first borrow from left
      bool borrowed = false;
      if (offset != begin(parent)->child)
        borrowed = borrow_key(false, node, offset);

      // then borrow from right
      if (!borrowed && offset != (end(parent) - 1)->child)
        borrowed = borrow_key(true, node, offset);

      // finally we merge
      if (!borrowed) {
        assert(node.next != 0 || node.prev != 0);

        if (offset == (end(parent) - 1)->child) {
          // if leaf is last element then merge | prev | leaf |
          assert(node.prev != 0);
          internal_node_t prev;
          read_block(&prev, node.prev);

          // merge
          index_t *where = find(parent, begin(prev)->key);
          reset_index_children_parent(begin(node), end(node), node.prev);
          merge_keys(where, prev, node);
          write_block(&prev, node.prev);
        } else {
          // else merge | leaf | next |
          assert(node.next != 0);
          internal_node_t next;
          read_block(&next, node.next);

          // merge
          index_t *where = find(parent, index_key);
          reset_index_children_parent(begin(next), end(next), offset);
          merge_keys(where, node, next);
          write_block(&node, offset);
        }

        // remove parent's key
        remove_from_index(node.parent, parent, index_key);
      } else {
        write_block(&node, offset);
      }
    } else {
      write_block(&node, offset);
    }
  }

  /* borrow one key from other internal node */
  bool borrow_key(bool from_right, internal_node_t &borrower, off_t offset) {
    typedef typename internal_node_t::child_t child_t;

    off_t lender_off = from_right ? borrower.next : borrower.prev;
    internal_node_t lender;
    read_block(&lender, lender_off);

    assert(lender.n >= meta.order / 2);
    if (lender.n != meta.order / 2) {
      child_t where_to_lend, where_to_put;

      internal_node_t parent;

      // swap keys, draw on paper to see why
      if (from_right) {
        where_to_lend = begin(lender);
        where_to_put = end(borrower);

        read_block(&parent, borrower.parent);
        child_t where = lower_bound(begin(parent), end(parent) - 1,
                                    (end(borrower) - 1)->key);
        where->key = where_to_lend->key;
        write_block(&parent, borrower.parent);
      } else {
        where_to_lend = end(lender) - 1;
        where_to_put = begin(borrower);

        read_block(&parent, lender.parent);
        child_t where = find(parent, begin(lender)->key);
        where_to_put->key = where->key;
        where->key = (where_to_lend - 1)->key;
        write_block(&parent, lender.parent);
      }

      // store
      std::copy_backward(where_to_put, end(borrower), end(borrower) + 1);
      *where_to_put = *where_to_lend;
      borrower.n++;

      // erase
      reset_index_children_parent(where_to_lend, where_to_lend + 1, offset);
      std::copy(where_to_lend + 1, end(lender), where_to_lend);
      lender.n--;
      write_block(&lender, lender_off);
      return true;
    }

    return false;
  }

  /* borrow one record from other leaf */
  bool borrow_key(bool from_right, leaf_node_t &borrower) {
    off_t lender_off = from_right ? borrower.next : borrower.prev;
    leaf_node_t lender;
    read_block(&lender, lender_off);

    assert(lender.n >= meta.order / 2);
    if (lender.n != meta.order / 2) {
      typename leaf_node_t::child_t where_to_lend, where_to_put;

      // decide offset and update parent's index key
      if (from_right) {
        where_to_lend = begin(lender);
        where_to_put = end(borrower);
        change_parent_child(borrower.parent, begin(borrower)->key,
                            lender.children[1].key);
      } else {
        where_to_lend = end(lender) - 1;
        where_to_put = begin(borrower);
        change_parent_child(lender.parent, begin(lender)->key,
                            where_to_lend->key);
      }

      // store
      std::copy_backward(where_to_put, end(borrower), end(borrower) + 1);
      *where_to_put = *where_to_lend;
      borrower.n++;

      // erase
      std::copy(where_to_lend + 1, end(lender), where_to_lend);
      lender.n--;
      write_block(&lender, lender_off);
      return true;
    }

    return false;
  }

  /* change one's parent key to another key */
  void change_parent_child(off_t parent, const key_t &o, const key_t &n) {
    internal_node_t node;
    read_block(&node, parent);

    index_t *w = find(node, o);
    assert(w != node.children + node.n);

    w->key = n;
    write_block(&node, parent);
    if (w == node.children + node.n - 1) {
      change_parent_child(node.parent, o, n);
    }
  }

  int update(const key_t& key, value_t value) {
    off_t offset = search_leaf(key);
    leaf_node_t leaf;
    read_block(&leaf, offset);

    record_t *record = find(leaf, key);
    if (record != leaf.children + leaf.n)
      if (keycmp(key, record->key) == 0) {
        record->value = value;
        write_block(&leaf, offset);

        return 0;
      } else {
        return 1;
      }
    else
      return -1;
  }

  /* change children's parent */
  void reset_index_children_parent(index_t *begin, index_t *end, off_t parent) {
    // this function can change both internal_node_t and leaf_node_t's parent
    // field, but we should ensure that:
    // 1. sizeof(internal_node_t) <= sizeof(leaf_node_t)
    // 2. parent field is placed in the beginning and have same size
    internal_node_t node;
    while (begin != end) {
      read_block(&node, begin->child);
      node.parent = parent;
      write_block(&node, begin->child, SIZE_NO_CHILDREN);
      ++begin;
    }
  }

  meta_t get_meta() const {
    return meta;
  }

};

}

#endif /* end of BPT_H */
