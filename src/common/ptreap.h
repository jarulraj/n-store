#ifndef __P_TREE_H__
#define __P_TREE_H__

#include <iostream>
#include <cstdlib>
#include <cassert>
#include <algorithm>
#include <atomic>

#include "libpm.h"
#include "ptreap.h"

using namespace std;

#define g_return_val_if_fail(expr,val) if(!(expr)) return (val);
#define g_return_if_fail(expr) if(!(expr)) return;

#define GPOINTER_TO_UINT(p) ((unsigned int) (unsigned long) (p))
#define GUINT_TO_POINTER(u) ((void*) (unsigned long) (u))

#define MAX_ROOTS 1024

#define MAX_IN_DEGREE 3
#define TABLE_SIZE (MAX_IN_DEGREE+1)

/*
 * Persistent balanced binary tree
 */
template<typename K, typename V>
class ptreap {
 public:
  /* Tree Search Types */
  typedef enum {
    P_TREE_SEARCH_EXACT,
    P_TREE_SEARCH_SUCCESSOR,
    P_TREE_SEARCH_PREDECESSOR
  } ptreap_search_type;

  class ptreap_root_version;
  class ptreap_node_version;
  class ptreap_node_data;
  class ptreap_node;

  class ptreap_root_version {
   public:
    ptreap_node *root;
    unsigned int version;
  };

  class ptreap_node_version {
   public:
    unsigned int version;
    ptreap_node *left; /* left subtree */
    ptreap_node *right; /* right subtree */
    ptreap_node *parent; /* parent node */
  };

  class ptreap_node_data {
   public:
    K key; /* key for this node */
    V value; /* value stored at this node */
    std::atomic_int ref_count;
    short unsigned int stolen; /* true if the node is stolen instead of removed */
  };

  class ptreap_node {
   public:
    ptreap_node_data *data; /* the node's permanent data */
    ptreap_node_version *v; /* versions of pointers for the node.  v[0] is the
     highest (latest) version.  then v[1]..v[nv-1] are
     older versions in ascending order.  Use first_v(),
     next_v() and prev_v() to traverse the list. */
    unsigned int nv; /* number of versions stored in this node */
  };

  ptreap_root_version *r; /* versioned root nodes of the tree.  r[0] is
   the highest (latest) version.  then r[1]..r[nr-1] are
   older versions in ascending order.  Use first_v(),
   next_v() and prev_v() to traverse the list. */
  unsigned int nr;
  unsigned int nnodes;
  std::atomic_int ref_count;
  unsigned int version;

  ptreap_root_version none = { NULL, 0 };
  ptreap* tree = NULL;

  /**
   * new_full:
   * @key_compare_func: qsort()-style comparison function.
   * @key_compare_data: data to pass to comparison function.
   * @key_destroy_func: a function to free the memory allocated for the key
   *   used when removing the entry from the #PTreap or %NULL if you don't
   *   want to supply such a function.
   * @value_destroy_func: a function to free the memory allocated for the
   *   value used when removing the entry from the #PTreap or %NULL if you
   *   don't want to supply such a function.
   *
   * Creates a new #PTreap like new() and allows to specify functions
   * to free the memory allocated for the key and value that get called when
   * removing the entry from the #PTreap.
   *
   * Return value: a new #PTreap.
   **/
  ptreap(void** __tree) {
    tree = (ptreap*) (*__tree);

    if (tree == NULL) {
      r = new ptreap_root_version[MAX_ROOTS];
      pmemalloc_activate(r);
      nr = 1;
      nnodes = 0;
      ref_count = 1;
      version = 0;

      r[0].root = NULL;
      r[0].version = 0;
      (*__tree) = this;

      //cout << "new tree :: " << (*__tree) << "\n";
    } else {
      //cout << "existing tree :: " << tree << "\n";
    }
  }

  /**
   * destroy:
   .
   *
   * Removes all keys and values from the #PTreap and decreases its
   * reference count by one. If keys and/or values are dynamically
   * allocated, you should either free them first or create the #PTreap
   * using new_full().  In the latter case the destroy functions
   * you supplied will be called on all keys and values before destroying
   * the #PTreap.
   **/
  ~ptreap() {
    remove_all();
  }

  ptreap_node*
  node_new(K key, V value) {
    ptreap_node *node = new ptreap_node;
    pmemalloc_activate(node);

    node->data = new ptreap_node_data;
    pmemalloc_activate(node->data);
    node->data->key = key;
    node->data->value = value;
    node->data->ref_count = 1;
    node->data->stolen = false;

    /* for the version 0, only allocate one set of pointers for a node,
     so that we don't use a lot more memory when persistence isn't even
     being used */
    if (version == 0)
      node->v = new ptreap_node_version;
    else
      node->v = new ptreap_node_version[TABLE_SIZE];
    pmemalloc_activate(node->v);

    node->nv = 1;

    node->v[0].version = version;
    node->v[0].left = NULL;
    node->v[0].right = NULL;
    node->v[0].parent = NULL;

    return node;
  }

  bool node_data_unref(ptreap_node *node) {
    /* free the node's data if it's the last node which is using it. just
     because the version of the node was created in the latest version
     of the  doesn't mean there aren't older versions around still */
    if ((--node->data->ref_count) == 0) {
      delete node->data;
      return true;
    }
    return false;
  }

  bool node_free(ptreap_node *node) {
    bool data_free;

    if (node->data)
      data_free = node_data_unref(node);
    else
      data_free = false;

    if (node->v[0].version)
      delete node->v;
    else
      delete[] node->v;

    node->v = NULL;
    node->data = NULL;
    delete node;

    return data_free;
  }

  /* Make a new root pointer if the current one is not for the current version
   of the tree */
  void root_next_version() {
    assert(r[0].version <= version);

    if (r[0].version < version) {
      /* add a new version of the root */
      nr = (nr+1) % MAX_ROOTS;
      /* copy the latest version from r[0] */
      r[nr - 1] = r[0];
      r[0].version = version;
    }
  }

  /* finds the largest version in the array, that is <= the query version */
  inline ptreap_root_version *
  root_find_version(unsigned int version) {
    ptreap_root_version * const v = r;
    const unsigned int nv = nr;
    ptreap_root_version *l, *r;

    if (v[0].version <= version) {
      return v; /* fast when looking for the current version */
    }

    l = v + 1;
    r = v + (nv - 1);
    while (l <= r) /* binary search */
    {
      ptreap_root_version * const m = l + (r - l) / 2;
      if (version == m->version) {
        return m;
      } else if (version < m->version)
        r = m - 1;
      else
        l = m + 1;
    }
    /* If searching earlier than the first root in the  r will be off
     the start of the list (in the first position in the array) */
    assert(r == v || r->version < version);
    /* When searching for the second last root (the last position in the array),
     l will be off the end of the list */
    assert(l == v + nv || l->version > version);
    if (r == v) {
      return &none;
    } else {
      return r;
    }
  }

  inline ptreap_node_version *
  node_find_version(ptreap_node *node, unsigned int version) {
    ptreap_node_version * const v = node->v;
    const unsigned int nv = node->nv;
    ptreap_node_version *n;

    /* note: we never search for a version smaller than the smallest
     version in the array */

    if (v[0].version <= version)
      return v;

    /* there are at most TABLE_SIZE things to look through, which is small,
     so just scan through them from largest version to smallest */
    for (n = v + (nv - 1); n != v; --n)
      if (n->version <= version)
        break;
    /* searched for something smaller than the lowest version in the node */
    assert(n != v);
    return n;
  }

  inline ptreap_node *
  first_node(unsigned int version) {
    ptreap_node *tmp;
    ptreap_node_version *tmpv;
    ptreap_root_version *rv;

    rv = root_find_version(version);
    if (!rv->root)
      return NULL;

    tmpv = node_find_version(tmp = rv->root, version);

    while (tmpv->left)
      tmpv = node_find_version(tmp = tmpv->left, version);

    return tmp;
  }

  inline ptreap_node *
  node_next(ptreap_node *node, unsigned int version) {
    ptreap_node *tmp;
    ptreap_node_version *nodev, *tmpv;

    nodev = node_find_version(node, version);

    if (nodev->right) {
      tmpv = node_find_version(tmp = nodev->right, version);
      while (tmpv->left)
        tmpv = node_find_version(tmp = tmpv->left, version);
    } else {
      tmp = nodev->parent;
      while (tmp) {
        tmpv = node_find_version(tmp, version);
        if (tmpv->left == node)
          break;
        node = tmp;
        tmp = tmpv->parent;
      }
    }
    return tmp;
  }

  inline void node_free_all(ptreap_node *node) {
    unsigned int i;

    if (!node)
      return;

    for (i = 0; i < node->nv; ++i) {
      node_free_all(node->v[i].parent);
      node_free_all(node->v[i].left);
      node_free_all(node->v[i].right);
    }

    node_free(node);
  }

  inline ptreap_node *
  node_decompose(ptreap_node *node) {
    unsigned int i;

    if (!node || !node->data)
      return NULL;

    node_data_unref(node);
    node->data = NULL;

    for (i = 0; i < node->nv; ++i) {
      node->v[i].parent = node_decompose(node->v[i].parent);
      node->v[i].left = node_decompose(node->v[i].left);
      node->v[i].right = node_decompose(node->v[i].right);
    }

    return node;
  }

  void remove_all() {
    unsigned int i;

    /* decompose the graph into individual trees rooted at the various
     root pointers in the graph, so that there are no cycles while we are
     freeing them */
    for (i = 0; i < nr; ++i) {
      r[i].root = node_decompose(r[i].root);
    }

    /* free everything */
    for (i = 0; i < nr; ++i)
      node_free_all(r[i].root);

    r[0].root = NULL;
    r[0].version = version = 0;
    nnodes = 0;
  }

  /**
   * unref:
   .
   *
   * Decrements the reference count of @tree by one.  If the reference count
   * drops to 0, all keys and values will be destroyed (if destroy
   * functions were specified) and all memory allocated by @tree will be
   * released.
   *
   * It is safe to call this function from any thread.
   *
   * Since: 2.22
   **/
  void unref() {
    if ((--ref_count) == 0) {
      remove_all();
      delete r;
    }
  }

  /**
   * current_version:

   *
   * Returns the current version number of the #PTreap.  Inserting or deleting
   * keys will affect the current version, but not change earlier versions.
   *
   * Returns: the current version number of the #PTreap.
   **/
  unsigned int current_version() {
    return version;
  }

  /**
   * next_version:

   *
   * Increments the version number of the tree.  Inserting or deleting keys will
   * affect the new version of the  but not change earlier versions.
   *
   * Returns: the new current version number of the #PTreap.
   **/
  unsigned int next_version() {
    assert(version + 1 != 0);
    return ++version;
  }

  /* Given the length of the array of versions, return the index of the
   first (oldest) version, or @n if there is none. */
#define first_v(n) ((n) > 1 ? 1 : 0)

  /* Given a current index, and the length of the array of versions, return
   the index of the next (newer) version, or @n if there is none. */
#define next_v(i, n) \
  ((i) == 0 ? (n) : /* it was the last version, return something invalid */ \
   ((i) == (n)-1 ? 0 : /* it was the second last, return the last */        \
    (i)+1)) /* it was somewhere else in the array, return the next */

  /* Given a current index, and the length of the array of versions, return
   the index of the previous (older) version, or @n if there is none. */
#define prev_v(i, n) \
  ((i) == 0 ? (n)-1 : /* it was the last version, return the second last */ \
   ((i) == 1 ? (n) : /* it was the first, return something invalid */       \
    (i)-1)); /* it was somewhere else in the array, return the previous */

  unsigned int node_delete_versions(ptreap_node *node, unsigned int pnext,
                                    unsigned int version) {
    unsigned int rm, i, nv, next, nextv, lnext, rnext, ret;

    if (!node)
      return 0;

    nv = node->nv;

    ret = 0;
    rm = 0;
    for (i = first_v(nv); i < nv && node->v[i].version <= version; i = next) {
      next = next_v(i, nv);

      if (next < nv)
        nextv = node->v[next].version - 1;
      else
        nextv = version;

      if (next < nv && node->v[i].left && node->v[next].left == node->v[i].left
          && (!pnext || node->v[next].version <= pnext))
        lnext = node->v[next].version;
      else if (next == nv || node->v[next].version > pnext)
        lnext = pnext;
      else
        lnext = 0;
      lnext = node_delete_versions(node->v[i].left, lnext, nextv);

      if (next < nv && node->v[i].right
          && node->v[next].right == node->v[i].right
          && (!pnext || node->v[next].version <= pnext))
        rnext = node->v[next].version;
      else if (next == nv || node->v[next].version > pnext)
        rnext = pnext;
      else
        rnext = 0;
      rnext = node_delete_versions(node->v[i].right, rnext, nextv);

      /* smallest non-zero of pnext, rnext, and lnext (or zero if all 3 are) */
      nextv = std::min(
          (pnext ? pnext : (lnext ? lnext : rnext)),
          std::min((lnext ? lnext : (rnext ? rnext : pnext)),
                   (rnext ? rnext : (lnext ? lnext : pnext))));

      if (nextv && (next == nv || node->v[next].version > nextv)) { /* leave this one behind as there are more pointers coming here */
        ret = node->v[i].version = nextv;
        break;
      }

      ++rm;
    }

    /* remove the first 'rm' versioned pointers from the node, they are <=
     'version' */
    for (i = 1; rm && i + rm < nv; ++i)
      node->v[i] = node->v[i + rm];
    node->nv -= rm;

    /* if we removed the last version inside the node, then we can free it */
    if (node->nv == 0) {
      node_free(node);
      node = NULL;
    }

    /* if we saved a version here, then it's because a child still needs it
     or the parent still needs it.  if the child needs it, then we will
     still have a pointer back to the parent, so it needs to also know.  so
     either way the parent needs it.
     if we did not save a version, but the parent needs us to stick around for
     something, then we can let it know that we did
     */
    if (!ret && node && pnext) {
      assert(node->v[first_v(node->nv)].version == pnext);
      ret = pnext;
    }

    return ret;
  }

  /**
   * delete_versions:
   .
   * @version: the highest version to delete.
   *
   * Deletes all versions in the #PTreap which are at most @version.  You can not
   * delete the latest version of the #PTreap, so @version must be less than the
   * value returned by current_version().

   * Deleting a version from the #PTreap frees all resources used that are not
   * needed for later versions in the #PTreap.  All elements which have been
   * removed from the tree in a version no later than @version will be freed,
   * and if you supplied a @key_destroy_func or @value_destroy_func when
   * creating the #PTreap, any such nodes will have their keys and values
   * freed using these functions (unless it was removed from the #PTreap by
   * steal(), in which case the key and value is not freed).
   **/
  void delete_versions(unsigned int version) {
    unsigned int rm, i, l, keep, next;

    g_return_if_fail(version < version);

    if (version < r[first_v(nr)].version) {
      return;
    }

    rm = 0;
    keep = i = first_v(nr);
    next = next_v(i, nr);
    while (i < nr && r[i].version < version + 1) {
      unsigned int nextv, v;

      if (next == nr || r[next].version > version + 1)
        nextv = version + 1;
      else if (next < nr && r[next].root == r[i].root)
        nextv = r[next].version;
      else
        nextv = 0;

      if (next < nr && r[next].version <= version)
        v = r[next].version - 1;
      else
        v = version;

      l = node_delete_versions(r[i].root, nextv, v);
      assert(l == 0 || l > v);

      if (l > v)
        r[i].version = l;
      else {
        ++rm;
        keep = next;
      }

      i = next;
      next = next_v(i, nr);
    }

    if (rm) {
      unsigned int j;
      for (j = first_v(nr - rm); j < nr - rm; j = next_v(j, nr - rm), keep =
          next_v(keep, nr)) {
        r[j] = r[keep];
      }
    }

    nr -= rm;
    //r = new PTreapRootVersion[nr];

#ifdef P_TREE_DEBUG
    {
      unsigned int i;
      for (i = 0; i <= version; ++i)
      node_check (root_find_version ( i)->root, i);
    }
#endif
  }

  /* add a new version of pointers to a node.  return true if ok, and false
   if there is no room to do so. */
  inline ptreap_node *
  node_add_version(ptreap_node *node) {
    if (!node)
      return NULL;

    /* node already has the current version */
    if (node->v[0].version == version)
      return node;
    /* if we filled the node's pointer table and need to make a new PTreapNode */
    else if (node->v[0].version == 0 || node->nv >= TABLE_SIZE) {
      ptreap_node *newnode = new ptreap_node;
      pmemalloc_activate(newnode);
      newnode->v = new ptreap_node_version[TABLE_SIZE];
      pmemalloc_activate(newnode->v);
      newnode->data = node->data;
      newnode->data->ref_count++;
      newnode->v[0] = node->v[0]; /* copy the latest version to here */
      newnode->v[0].version = version;
      newnode->nv = 1;
      return newnode;
    }
    /* there is space left in the node's pointer table for a new version */
    else {
      node->nv++;
      /* copy the latest version from v[0] */
      node->v[node->nv - 1] = node->v[0];
      node->v[0].version = version;
      return node;
    }
  }

  inline void node_fix_incoming(ptreap_node *oldnode, ptreap_node *newnode) {
    ptreap_node *oldparent, *oldleft, *oldright;
    ptreap_node *newparent, *newleft, *newright;

    if (oldnode == newnode)
      return;

    assert(oldnode != NULL);
    assert(newnode != NULL);

    /* find incoming edges to the old version of the node */
    oldparent = newnode->v[0].parent;
    if (oldparent && oldparent->v[0].left != oldnode
        && oldparent->v[0].right != oldnode)
      oldparent = NULL;
    oldleft = newnode->v[0].left;
    if (oldleft && oldleft->v[0].parent != oldnode)
      oldleft = NULL;
    oldright = newnode->v[0].right;
    if (oldright && oldright->v[0].parent != oldnode)
      oldright = NULL;

    /* add new pointers for nodes that point to me, possibly creating new
     versions of the nodes */
    newparent = node_add_version(oldparent);
    newleft = node_add_version(oldleft);
    newright = node_add_version(oldright);

    /* 1. update my pointers to point to the new versions (if a prev node
     points to me, then i'm its next node, and its my parent or
     it is the rightmost node in my left subtree. case a, i update my
     pointer to it already.  case b, if it's my left child i update
     my pointer to it already, otherwise i don't have a pointer to it,
     because i do have a left subtree.
     */
    if (newparent != oldparent)
      newnode->v[0].parent = newparent;
    if (newleft != oldleft)
      newnode->v[0].left = newleft;
    if (newright != oldright)
      newnode->v[0].right = newright;

    /* 2. update my incoming nodes to point back to the new version of me */
    if (newparent) {
      if (newparent->v[0].left == oldnode)
        newparent->v[0].left = newnode;
      else
        newparent->v[0].right = newnode;
    }
    if (newleft)
      newleft->v[0].parent = newnode;
    if (newright)
      newright->v[0].parent = newnode;

    /* 3. recurse on each of the incoming nodes if they had to create a new
     version */
    node_fix_incoming(oldparent, newparent);
    node_fix_incoming(oldleft, newleft);
    node_fix_incoming(oldright, newright);

    /* 4. if this was the root node, then the root pointer into the tree needs
     be updated */
    if (r[0].root == oldnode) {
      root_next_version();
      r[0].root = newnode;
    }
  }

  /* You must call this on a node before you start changing its pointers,
   or you might be changing an old (permanent) version of the node */
  ptreap_node*
  node_next_version(ptreap_node *oldnode) {
    ptreap_node *newnode;

    if (!oldnode)
      return NULL;

    assert(oldnode->v[0].version <= version);

    newnode = node_add_version(oldnode);
    node_fix_incoming(oldnode, newnode);
    return newnode;
  }

  /**
   * insert:
   * @key: the key to insert.
   * @value: the value corresponding to the key.
   *
   * Inserts a key/value pair into a #PTreap. If the given key already exists
   * in the #PTreap its corresponding value is set to the new value. If you
   * supplied a value_destroy_func when creating the #PTreap, the old value is
   * freed using that function. If you supplied a @key_destroy_func when
   * creating the #PTreap, the passed key is freed using that function.
   *
   * The tree is automatically 'balanced' as new key/value pairs are added,
   * so that the distance from the root to every leaf is small.
   **/
  void insert(K key, V value) {

    insert_internal(key, value, false);

#ifdef P_TREE_DEBUG
    {
      unsigned int i;
      for (i = 0; i <= version; ++i)
      node_check (root_find_version ( i)->root, i);
    }
#endif
  }

  /**
   * replace:
   * @key: the key to insert.
   * @value: the value corresponding to the key.
   *
   * Inserts a new key and value into a #PTreap similar to insert().
   * The difference is that if the key already exists in the #PTreap, it gets
   * replaced by the new key. If you supplied a @value_destroy_func when
   * creating the #PTreap, the old value is freed using that function. If you
   * supplied a @key_destroy_func when creating the #PTreap, the old key is
   * freed using that function.
   *
   * The tree is automatically 'balanced' as new key/value pairs are added,
   * so that the distance from the root to every leaf is small.
   **/
  void replace(K key, V value) {

    insert_internal(key, value, true);

#ifdef P_TREE_DEBUG
    {
      unsigned int i;
      for (i = 0; i <= version; ++i)
      node_check (root_find_version ( i)->root, i);
    }
#endif
  }

  /*
   * Implementation of a treap
   *
   * (Copied from gsequence.c)
   */
  unsigned int priority(ptreap_node *node) {
    unsigned int key = GPOINTER_TO_UINT(node->data);

    /* This hash function is based on one found on Thomas Wang's
     * web page at
     *
     *    http://www.concentric.net/~Ttwang/tech/inthash.htm
     *
     */
    key = (key << 15) - key - 1;
    key = key ^ (key >> 12);
    key = key + (key << 2);
    key = key ^ (key >> 4);
    key = key + (key << 3) + (key << 11);
    key = key ^ (key >> 16);

    /* We rely on 0 being less than all other priorities */
    return key ? key : 1;
  }

  /* Internal insert routine.  Always inserts into the current version of the
   tree. */
  void insert_internal(K key, V value,
  bool replace) {
    ptreap_node *node, *child;

    if (!r[0].root) {
      root_next_version();
      r[0].root = node_new(key, value);
      nnodes++;
      return;
    }

    node = r[0].root;

    while (1) {
      if (key == node->data->key) {
        node->data->value = value;

        if (replace) {
          node->data->key = key;
        }

        return;
      } else if (key < node->data->key) {
        if (node->v[0].left)
          node = node->v[0].left;
        else {
          child = node_new(key, value);
          node = node_next_version(node);

          child->v[0].parent = node;
          node->v[0].left = child;

          nnodes++;

          break;
        }
      } else {
        if (node->v[0].right)
          node = node->v[0].right;
        else {
          child = node_new(key, value);
          node = node_next_version(node);

          child->v[0].parent = node;
          node->v[0].right = child;

          nnodes++;

          break;
        }
      }
    }

    /* rotate the new node up until the heap property is restored */
    node = child;
    while (node->v[0].parent && priority(node) < priority(node->v[0].parent)) {
      ptreap_node *sp, *p;

      /* we'll be changing both of these nodes */
      p = node_next_version(node->v[0].parent);
      sp = node_next_version(p->v[0].parent);

      if (p->v[0].left == node)
        node = node_rotate_right(p);
      else
        node = node_rotate_left(p);

      if (!sp) {
        root_next_version();
        r[0].root = node;
      } else if (sp->v[0].left == p)
        sp->v[0].left = node;
      else
        sp->v[0].right = node;
    }
  }

  /**
   * remove:
   .
   * @key: the key to remove.
   *
   * Removes a key/value pair from a #PTreap.
   *
   * If the #PTreap was created using new_full(), the key and value
   * are freed using the supplied destroy functions, otherwise you have to
   * make sure that any dynamically allocated values are freed yourself.
   * Note that if the key existed in
   * earlier versions of the tree (next_version() has been called since
   * it was inserted), then it cannot be removed from the tree. *
   * If the key does not exist in the #PTreap, the function does nothing.
   *
   * Returns: %true if the key was found and able to be removed
   *   (prior to 2.8, this function returned nothing)
   **/
  bool remove(const K key) {
    bool removed;

    removed = remove_internal(key, false);

#ifdef P_TREE_DEBUG
    {
      unsigned int i;
      for (i = 0; i <= version; ++i)
      node_check (root_find_version ( i)->root, i);
    }
#endif

    return removed;
  }

  /**
   * steal:
   .
   * @key: the key to remove.
   *
   * Removes a key and its associated value from a #PTreap without calling
   * the key and value destroy functions.  Note that if the key existed in
   * earlier versions of the tree (next_version() has been called since
   * it was inserted), then it cannot be removed from the tree until all
   * versions containing the node are removed from the  by calling
   * delete_versions().  However, if the node is removed from the current
   * version of the tree with steal() then the key and value destroy
   * functions will not be called once the last version of the key/value pair
   * is removed.
   * If the key does not exist in the #PTreap, the function does nothing.
   *
   * Returns: %true if the key was found and able to be removed
   *   (prior to 2.8, this function returned nothing)
   **/
  bool steal(const K key) {
    bool removed;

    removed = remove_internal(key, true);

#ifdef P_TREE_DEBUG
    {
      unsigned int i;
      for (i = 0; i <= version; ++i)
      node_check (root_find_version ( i)->root, i);
    }
#endif

    return removed;
  }

  /* internal remove routine */
  bool remove_internal(const K key,
  bool steal) {
    ptreap_node *node, *parent;
    bool is_leftchild;
    bool data_free;

    if (!r[0].root)
      return false;

    node = r[0].root;
    parent = NULL;
    is_leftchild = false;

    while (1) {
      if (key == node->data->key)
        break;
      else if (key < node->data->key) {
        if (!node->v[0].left)
          return false;

        parent = node;
        is_leftchild = true;
        node = node->v[0].left;
      } else {
        if (!node->v[0].right)
          return false;

        parent = node;
        is_leftchild = false;
        node = node->v[0].right;
      }
    }

    /* rotate the node down the  maintaining the heap property */
    while (node->v[0].left || node->v[0].right) {
      /* we're changing this node, make sure our pointer will stay valid
       when we rotate it */
      node = node_next_version(node);
      /* getting the next version for the node may change where its parent
       lies, so get a new pointer for it, from the node */
      parent = node->v[0].parent;

      if (!node->v[0].left
          || (node->v[0].right
              && priority(node->v[0].left) > priority(node->v[0].right))) {
        /* rotate the right child up */
        if (!parent) {
          root_next_version();
          parent = r[0].root = node_rotate_left(node);
        } else {
          parent = node_next_version(parent);
          if (is_leftchild)
            parent = parent->v[0].left = node_rotate_left(node);
          else
            parent = parent->v[0].right = node_rotate_left(node);
        }
        is_leftchild = true;
      } else {
        /* rotate the left child up */
        if (!parent) {
          root_next_version();
          parent = r[0].root = node_rotate_right(node);
        } else {
          parent = node_next_version(parent);
          if (is_leftchild)
            parent = parent->v[0].left = node_rotate_right(node);
          else
            parent = parent->v[0].right = node_rotate_right(node);
        }
        is_leftchild = false;
      }
    }

    /* remove any pointers to the node in the treap */
    if (!parent) {
      root_next_version();
      r[0].root = NULL;
    } else {
      /* make a new version of the parent to cut the child off.
       making a new version of the parent may make a new version of
       the node being deleted, which is the one we'd want to free then
       instead, so update the node pointer */
      parent = node_next_version(parent);
      if (is_leftchild) {
        node = parent->v[0].left;
        parent->v[0].left = NULL;
      } else {
        node = parent->v[0].right;
        parent->v[0].right = NULL;
      }
    }

    nnodes--;

    if (node->v[0].version == version) {
      /* remove the entry from the node's pointer table */
      node->v[0] = node->v[node->nv - 1];
      --node->nv;
    }

    node->data->stolen = steal;
    data_free = false;

    /* only really delete the node if it was only in the current version,
     otherwise it needs to be remembered */
    if (node->nv == 0)
      data_free = node_free(node);

    return data_free;
  }

  /**
   * lookup:
   .
   * @key: the key to look up.
   *
   * Gets the value corresponding to the given key. Since a #PTreap is
   * automatically balanced as key/value pairs are added, key lookup is very
   * fast.
   *
   * Return value: the value corresponding to the key, or %NULL if the key was
   * not found.
   **/
  V at(const K key) {
    return lookup_related_v(version, key, P_TREE_SEARCH_EXACT);
  }

  /**
   * lookup_v:
   .
   * @version: the version of the tree within which to search.  If
   *   next_version() has not been used, then this is 0.  This value
   *   must be at most the value returned by current_version().
   * @key: the key to look up.
   *
   * Gets the value corresponding to the given key in a specified @version of
   * the #PTreap. Since a #PTreap is
   * automatically balanced as key/value pairs are added, key lookup is very
   * fast.
   *
   * Return value: the value corresponding to the key, or %NULL if the key was
   * not found.
   **/
  V at(const K key, unsigned int version) {
    return lookup_related_v(version, key, P_TREE_SEARCH_EXACT);
  }

  /**
   * lookup_related:
   .
   * @key: the key to look up.
   * @search_type: the search behavior if the @key is not present in the #PTreap,
   *   one of %P_TREE_SEARCH_EXACT, %P_TREE_SEARCH_SUCCESSOR, and
   *   %P_TREE_SEARCH_PREDECESSOR.
   *
   * Gets a value corresponding to the given key.
   *
   * If the given @key is present in the  then its corresponding value is
   * always returned.  If it is not, then the @search_type will define the
   * function's behavior.  If @search_type is %P_TREE_SEARCH_EXACT, the
   * function will return %NULL if the @key is not found.  If @search_type is
   * %P_TREE_SEARCH_SUCCESSOR, the function will find the next larger key that
   * is present in the tree and return its value, or %NULL if there are no keys
   * larger than @key in the tree.  If @search_type is
   * %P_TREE_SEARCH_PREDECESSOR, the function will find the next smaller key that
   * is present in the tree and return its value, or %NULL if there are no keys
   * smaller than @key in the tree.
   * Since a #PTreap is
   * automatically balanced as key/value pairs are added, key lookup is very
   * fast.
   *
   * Return value: the value corresponding to the found key, or %NULL if no
   * matching key was found.
   **/
  V lookup_related(const K key, ptreap_search_type search_type) {
    return lookup_related_v(version, key, search_type);
  }

  /**
   * lookup_related_v:
   * @version: the version of the tree within which to search.  If
   *   next_version() has not been used, then this is 0.  This value
   *   must be at most the value returned by current_version().
   * @key: the key to look up.
   * @search_type: the search behavior if the @key is not present in the #PTreap,
   *   one of %P_TREE_SEARCH_EXACT, %P_TREE_SEARCH_SUCCESSOR, and
   *   %P_TREE_SEARCH_PREDECESSOR.
   *
   * Gets a value corresponding to the given key in a specified @version of the
   * #PTreap.
   *
   * If the given @key is present in the  then its corresponding value is
   * always returned.  If it is not, then the @search_type will define the
   * function's behavior.  If @search_type is %P_TREE_SEARCH_EXACT, the
   * function will return %NULL if the @key is not found.  If @search_type is
   * %P_TREE_SEARCH_SUCCESSOR, the function will find the next larger key that
   * is present in the tree and return its value, or %NULL if there are no keys
   * larger than @key in the tree.  If @search_type is
   * %P_TREE_SEARCH_PREDECESSOR, the function will find the next smaller key that
   * is present in the tree and return its value, or %NULL if there are no keys
   * smaller than @key in the tree.
   * Since a #PTreap is
   * automatically balanced as key/value pairs are added, key lookup is very
   * fast.
   *
   * Return value: the value corresponding to the found key, or %NULL if no
   * matching key was found.
   **/
  V lookup_related_v(unsigned int version, const K key,
                     ptreap_search_type search_type) {
    ptreap_node *node;

    g_return_val_if_fail(version <= version, NULL);

    node = find_node(key, search_type, version);

    return node ? node->data->value : NULL;
  }

  /**
   * lookup_extended:
   * @lookup_key: the key to look up.
   * @orig_key: returns the original key.
   * @value: returns the value associated with the key.
   *
   * Looks up a key in the #PTreap, returning the original key and the
   * associated value and a #bool which is %true if the key was found. This
   * is useful if you need to free the memory allocated for the original key,
   * for example before calling remove().
   *
   * Return value: %true if the key was found in the #PTreap.
   **/
  bool lookup_extended(const K lookup_key, K *orig_key, V *value) {
    return lookup_extended_v(version, lookup_key, orig_key, value);
  }

  /**
   * lookup_extended_v:
   * @version: the version of the tree within which to search.  If
   *   next_version() has not been used, then this is 0.  This value
   *   must be at most the value returned by current_version().
   * @lookup_key: the key to 5Blook up.
   * @orig_key: returns the original key.
   * @value: returns the value associated with the key.
   *
   * Looks up a key in a specified @version of the #PTreap, returning the
   * original key and the
   * associated value and a #bool which is %true if the key was found. This
   * is useful if you need to free the memory allocated for the original key,
   * for example before calling remove().
   *
   * Return value: %true if the key was found in the #PTreap.
   **/
  bool lookup_extended_v(unsigned int version, const K lookup_key, K *orig_key,
                         V *value) {
    ptreap_node *node;

    g_return_val_if_fail(version <= version, false);

    node = find_node(lookup_key, P_TREE_SEARCH_EXACT, version);

    if (node) {
      if (orig_key)
        *orig_key = node->data->key;
      if (value)
        *value = node->data->value;
      return true;
    } else
      return false;
  }

  int node_height(ptreap_node *node, unsigned int version) {
    ptreap_node_version *nv;
    int l = 0, r = 0;
    if (node == NULL)
      return 0;
    nv = node_find_version(node, version);
    if (nv->left)
      l = node_height(nv->left, version);
    if (nv->right)
      r = node_height(nv->right, version);
    return 1 + std::max(l, r);
  }

  /**
   * height:
   *
   * Gets the height of a #PTreap.
   *
   * If the #PTreap contains no nodes, the height is 0.
   * If the #PTreap contains only one root node the height is 1.
   * If the root node has children the height is 2, etc.
   *
   * Return value: the height of the #PTreap.
   **/
  int height(ptreap *tree) {
    return height_v(version);
  }

  /**
   * height_v:
   * @version: the version of the tree which should be queried. If
   *   next_version() has not been used, then this is 0.  This value
   *   must be at most the value returned by current_version().
   *
   * Gets the height of a specified @version of the #PTreap.
   *
   * If the #PTreap contains no nodes, the height is 0.
   * If the #PTreap contains only one root node the height is 1.
   * If the root node has children the height is 2, etc.
   *
   * Return value: the height of the #PTreap.
   **/
  int height_v(unsigned int version) {
    g_return_val_if_fail(version <= version, 0);

    return node_height(root_find_version(version)->root, version);
  }

  /**
   * nnodes:
   *
   * Gets the number of nodes in the current version of a #PTreap.
   *
   * Return value: the number of nodes in the latest version of the #PTreap.
   **/
  int get_nnodes() {
    return nnodes;
  }

  ptreap_node *
  find_node(const K key, ptreap_search_type search_type, unsigned int version) {
    ptreap_node *node, *remember;
    ptreap_root_version *rv;

    rv = root_find_version(version);
    node = rv->root;
    //cout<<"root ::"<<rv->root<<endl;
    if (!node) {
      return NULL;
    }

    remember = NULL;
    while (1) {
      if (key == node->data->key) {
        return node;
      } else if (key < node->data->key) {
        ptreap_node_version *nodev = node_find_version(node, version);
        if (search_type == P_TREE_SEARCH_SUCCESSOR)
          remember = node;
        if (!nodev->left)
          return remember;

        node = nodev->left;
      } else {
        ptreap_node_version *nodev = node_find_version(node, version);
        if (search_type == P_TREE_SEARCH_PREDECESSOR)
          remember = node;
        if (!nodev->right)
          return remember;

        node = nodev->right;
      }
    }

    return NULL;
  }

  ptreap_node*
  node_rotate_left(ptreap_node *node) {
    ptreap_node *right;

    assert(node->v[0].version == version);

    right = node_next_version(node->v[0].right);

    if (right->v[0].left) {
      node->v[0].right = node_next_version(right->v[0].left);
      node->v[0].right->v[0].parent = node;
    } else
      node->v[0].right = NULL;
    right->v[0].left = node;
    right->v[0].parent = node->v[0].parent;
    node->v[0].parent = right;

    return right;
  }

  ptreap_node*
  node_rotate_right(ptreap_node *node) {
    ptreap_node *left;

    assert(node->v[0].version == version);

    left = node_next_version(node->v[0].left);

    if (left->v[0].right) {
      node->v[0].left = node_next_version(left->v[0].right);
      node->v[0].left->v[0].parent = node;
    } else
      node->v[0].left = NULL;
    left->v[0].right = node;
    left->v[0].parent = node->v[0].parent;
    node->v[0].parent = left;

    return left;
  }

};

#endif /* __P_TREE_H__ */
