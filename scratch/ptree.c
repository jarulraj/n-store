/* ADDS - Open Source Advanced Data Structures Library
 * Copyright (C) 2009 Dana Jansens
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2 as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 02111-1307, USA.
 */

/* 
 * MT safe
 */

#include "ptree.h"

#undef P_TREE_DEBUG

#define MAX_IN_DEGREE 3
#define TABLE_SIZE (MAX_IN_DEGREE+1) /* This is the minimum size required so
                                        that you only have a constant number of
                                        nodes fill up their tables and
                                        require a new node to be created at
                                        a time. */

typedef struct _PTreeRootVersion   PTreeRootVersion;
typedef struct _PTreeNodeVersion   PTreeNodeVersion;
typedef struct _PTreeNodeData      PTreeNodeData;
typedef struct _PTreeNode          PTreeNode;

struct _PTreeRootVersion
{
  PTreeNode *root;
  guint      version;
};

struct _PTree
{
  PTreeRootVersion *r; /* versioned root nodes of the tree.  r[0] is
                          the highest (latest) version.  then r[1]..r[nr-1] are
                          older versions in ascending order.  Use first_v(),
                          next_v() and prev_v() to traverse the list. */
  guint             nr;
  GCompareDataFunc  key_compare;
  GDestroyNotify    key_destroy_func;
  GDestroyNotify    value_destroy_func;
  gpointer          key_compare_data;
  guint             nnodes;
  gint              ref_count;
  guint             version;
};

struct _PTreeNodeVersion
{
  guint      version;
  PTreeNode *left;        /* left subtree */
  PTreeNode *right;       /* right subtree */
  PTreeNode *parent;      /* parent node */
};

struct _PTreeNodeData
{
  gpointer   key;         /* key for this node */
  gpointer   value;       /* value stored at this node */
  gint       ref_count;
  guint8     stolen;      /* true if the node is stolen instead of removed */
};

struct _PTreeNode
{
  PTreeNodeData    *data; /* the node's permanent data */
  PTreeNodeVersion *v;    /* versions of pointers for the node.  v[0] is the
                             highest (latest) version.  then v[1]..v[nv-1] are
                             older versions in ascending order.  Use first_v(),
                             next_v() and prev_v() to traverse the list. */
  guint             nv;   /* number of versions stored in this node */
};

static PTreeNode* p_tree_node_new                   (PTree           *tree,
                                                     gpointer         key,
						     gpointer         value);
static guint      p_tree_priority                   (PTreeNode       *node);
static void       p_tree_insert_internal            (PTree           *tree,
						     gpointer         key,
						     gpointer         value,
						     gboolean         replace);
static gboolean   p_tree_remove_internal            (PTree           *tree,
						     gconstpointer    key,
						     gboolean         steal);
static PTreeNode *p_tree_find_node                  (PTree           *tree,
						     gconstpointer    key,
                                                     PTreeSearchType  search_type,
                                                     guint            version);
static gint       p_tree_node_pre_order             (PTreeNode       *node,
						     GTraverseFunc    traverse_func,
						     gpointer         data);
static gint       p_tree_node_in_order              (PTreeNode       *node,
						     GTraverseFunc    traverse_func,
						     gpointer         data);
static gint       p_tree_node_post_order            (PTreeNode       *node,
						     GTraverseFunc    traverse_func,
						     gpointer         data);
static gpointer   p_tree_node_search                (PTreeNode       *node,
						     GCompareFunc     search_func,
						     gconstpointer    data,
                                                     PTreeSearchType  search_type,
                                                     guint            version);
static PTreeNode* p_tree_node_rotate_left           (PTree           *tree,
                                                     PTreeNode       *node);
static PTreeNode* p_tree_node_rotate_right          (PTree           *tree,
                                                     PTreeNode       *node);
#ifdef P_TREE_DEBUG
static void       p_tree_node_check                 (PTreeNode       *node,
                                                     guint            version);
#endif

static PTreeNode*
p_tree_node_new (PTree   *tree,
                 gpointer key,
		 gpointer value)
{
  PTreeNode *node = g_slice_new (PTreeNode);

  node->data = g_slice_new(PTreeNodeData);
  node->data->key = key;
  node->data->value = value;
  node->data->ref_count = 1;
  node->data->stolen = FALSE;

  /* for the version 0, only allocate one set of pointers for a node,
     so that we don't use a lot more memory when persistence isn't even
     being used */
  node->v = g_slice_alloc(sizeof(PTreeNodeVersion) *
                          (tree->version == 0 ? 1 : TABLE_SIZE));
  node->nv = 1;

  node->v[0].version = tree->version;
  node->v[0].left = NULL;
  node->v[0].right = NULL;
  node->v[0].parent = NULL;

  return node;
}

static gboolean
p_tree_node_data_unref (PTree *tree,
                        PTreeNode *node)
{
  /* free the node's data if it's the last node which is using it. just
     because the version of the node was created in the latest version
     of the tree, doesn't mean there aren't older versions around still */
  if (g_atomic_int_dec_and_test (&node->data->ref_count))
    {
      if (!node->data->stolen)
        {
          if (tree->key_destroy_func)
            tree->key_destroy_func (node->data->key);
          if (tree->value_destroy_func)
            tree->value_destroy_func (node->data->value);
        }
      g_slice_free (PTreeNodeData, node->data);
      return TRUE;
    }
  return FALSE;
}

static gboolean
p_tree_node_free (PTree     *tree,
                  PTreeNode *node)
{
  gboolean data_free;

  if (node->data)
    data_free = p_tree_node_data_unref (tree, node);
  else
    data_free = FALSE;

  g_slice_free1 (sizeof(PTreeNodeVersion) *
                 (node->v[0].version == 0 ? 1 : TABLE_SIZE),
                 node->v);
  node->v = NULL;
  node->data = NULL;
  g_slice_free (PTreeNode, node);

  return data_free;
}

/**
 * p_tree_new:
 * @key_compare_func: the function used to order the nodes in the #PTree.
 *   It should return values similar to the standard strcmp() function -
 *   0 if the two arguments are equal, a negative value if the first argument 
 *   comes before the second, or a positive value if the first argument comes 
 *   after the second.
 * 
 * Creates a new #PTree.
 * 
 * Return value: a new #PTree.
 **/
PTree*
p_tree_new (GCompareFunc key_compare_func)
{
  g_return_val_if_fail (key_compare_func != NULL, NULL);

  return p_tree_new_full ((GCompareDataFunc) key_compare_func, NULL,
                          NULL, NULL);
}

/**
 * p_tree_new_with_data:
 * @key_compare_func: qsort()-style comparison function.
 * @key_compare_data: data to pass to comparison function.
 * 
 * Creates a new #PTree with a comparison function that accepts user data.
 * See p_tree_new() for more details.
 * 
 * Return value: a new #PTree.
 **/
PTree*
p_tree_new_with_data (GCompareDataFunc key_compare_func,
 		      gpointer         key_compare_data)
{
  g_return_val_if_fail (key_compare_func != NULL, NULL);
  
  return p_tree_new_full (key_compare_func, key_compare_data, 
 			  NULL, NULL);
}

/**
 * p_tree_new_full:
 * @key_compare_func: qsort()-style comparison function.
 * @key_compare_data: data to pass to comparison function.
 * @key_destroy_func: a function to free the memory allocated for the key 
 *   used when removing the entry from the #PTree or %NULL if you don't
 *   want to supply such a function.
 * @value_destroy_func: a function to free the memory allocated for the 
 *   value used when removing the entry from the #PTree or %NULL if you 
 *   don't want to supply such a function.
 * 
 * Creates a new #PTree like p_tree_new() and allows to specify functions 
 * to free the memory allocated for the key and value that get called when 
 * removing the entry from the #PTree.
 * 
 * Return value: a new #PTree.
 **/
PTree*	 
p_tree_new_full (GCompareDataFunc key_compare_func,
 		 gpointer         key_compare_data, 		 
                 GDestroyNotify   key_destroy_func,
 		 GDestroyNotify   value_destroy_func)
{
  PTree *tree;
  
  g_return_val_if_fail (key_compare_func != NULL, NULL);
  
  tree = g_slice_new (PTree);
  tree->r                  = g_new(PTreeRootVersion, 1);
  tree->nr                 = 1;
  tree->key_compare        = key_compare_func;
  tree->key_destroy_func   = key_destroy_func;
  tree->value_destroy_func = value_destroy_func;
  tree->key_compare_data   = key_compare_data;
  tree->nnodes             = 0;
  tree->ref_count          = 1;
  tree->version            = 0;

  tree->r[0].root          = NULL;
  tree->r[0].version       = 0;
  
  return tree;
}

/* finds the largest version in the array, that is <= the query version */
static inline PTreeRootVersion *
p_tree_root_find_version (PTree *tree,
                          guint  version)
{
  PTreeRootVersion *const v = tree->r;
  const guint nv = tree->nr;
  PTreeRootVersion *l, *r;
  static PTreeRootVersion none = {
    .root = NULL,
    .version = 0
  };

  if (v[0].version <= version)
    return v; /* fast when looking for the current version */

  l = v+1;
  r = v+(nv-1);
  while (l <= r) /* binary search */
    {
      PTreeRootVersion *const m = l + (r - l) / 2;
      if (version == m->version)
        return m;
      else if (version < m->version)
        r = m-1;
      else
        l = m+1;
    }
  /* If searching earlier than the first root in the tree, r will be off
     the start of the list (in the first position in the array) */
  g_assert (r == v || r->version < version);
  /* When searching for the second last root (the last position in the array),
     l will be off the end of the list */
  g_assert (l == v+nv || l->version > version);
  if (r == v)
    return &none;
  else
    return r;
}

static inline PTreeNodeVersion *
p_tree_node_find_version(PTreeNode *node,
                         guint      version)
{
  PTreeNodeVersion *const v = node->v;
  const guint nv = node->nv;
  PTreeNodeVersion *n;

  /* note: we never search for a version smaller than the smallest
     version in the array */

  if (v[0].version <= version)
    return v;

  /* there are at most TABLE_SIZE things to look through, which is small,
     so just scan through them from largest version to smallest */
  for (n = v+(nv-1); n != v; --n)
    if (n->version <= version)
      break;
  /* searched for something smaller than the lowest version in the node */
  g_assert (n != v);
  return n;
}

static inline PTreeNode *
p_tree_first_node (PTree *tree,
                   guint  version)
{
  PTreeNode *tmp;
  PTreeNodeVersion *tmpv;
  PTreeRootVersion *rv;

  rv = p_tree_root_find_version (tree, version);
  if (!rv->root)
    return NULL;

  tmpv = p_tree_node_find_version (tmp = rv->root, version);

  while (tmpv->left)
    tmpv = p_tree_node_find_version (tmp = tmpv->left, version);

  return tmp;
} 

static inline PTreeNode *
p_tree_node_previous (PTreeNode *node,
                      guint      version)
{
  PTreeNode *tmp;
  PTreeNodeVersion *nodev, *tmpv;

  nodev = p_tree_node_find_version (node, version);

  if (nodev->left)
    {
      tmpv = p_tree_node_find_version (tmp = nodev->left, version);
      while (tmpv->right)
        tmpv = p_tree_node_find_version (tmp = tmpv->right, version);
    }
  else
    {
      tmp = nodev->parent;
      while (tmp)
        {
          tmpv = p_tree_node_find_version (tmp, version);
          if (tmpv->right == node)
              break;
          node = tmp;
          tmp = tmpv->parent;
        }
    }
  return tmp;
}
		  
static inline PTreeNode *
p_tree_node_next (PTreeNode *node,
                  guint      version)
{
  PTreeNode *tmp;
  PTreeNodeVersion *nodev, *tmpv;

  nodev = p_tree_node_find_version (node, version);

  if (nodev->right)
    {
      tmpv = p_tree_node_find_version (tmp = nodev->right, version);
      while (tmpv->left)
        tmpv = p_tree_node_find_version (tmp = tmpv->left, version);
    }
  else
    {
      tmp = nodev->parent;
      while (tmp)
        {
          tmpv = p_tree_node_find_version (tmp, version);
          if (tmpv->left == node)
              break;
          node = tmp;
          tmp = tmpv->parent;
        }
    }
  return tmp;
}

static inline void
p_tree_node_free_all (PTree *tree, PTreeNode *node)
{
  guint i;

  if (!node)
    return;

  for (i = 0; i < node->nv; ++i)
    {
      p_tree_node_free_all (tree, node->v[i].parent);
      p_tree_node_free_all (tree, node->v[i].left);
      p_tree_node_free_all (tree, node->v[i].right);
    }

  p_tree_node_free (tree, node);
}

static inline PTreeNode *
p_tree_node_decompose (PTree     *tree,
                       PTreeNode *node)
{
  guint i;

  if (!node || !node->data)
    return NULL;

  p_tree_node_data_unref (tree, node);
  node->data = NULL;

  for (i = 0; i < node->nv; ++i)
    {
      node->v[i].parent = p_tree_node_decompose (tree, node->v[i].parent);
      node->v[i].left = p_tree_node_decompose (tree, node->v[i].left);
      node->v[i].right = p_tree_node_decompose (tree, node->v[i].right);
    }

  return node;
}

static void
p_tree_remove_all (PTree *tree)
{
  guint i;

  g_return_if_fail (tree != NULL);

  /* decompose the graph into individual trees rooted at the various
     root pointers in the graph, so that there are no cycles while we are
     freeing them */
  for (i = 0; i < tree->nr; ++i)
    tree->r[i].root = p_tree_node_decompose (tree, tree->r[i].root);

  /* free everything */
  for (i = 0; i < tree->nr; ++i)
    p_tree_node_free_all (tree, tree->r[i].root);

  tree->r[0].root = NULL;
  tree->r[0].version = tree->version = 0;
  tree->nnodes = 0;
}

/**
 * p_tree_ref:
 * @tree: a #PTree.
 *
 * Increments the reference count of @tree by one.  It is safe to call
 * this function from any thread.
 *
 * Return value: the passed in #PTree.
 *
 * Since: 2.22
 **/
PTree *
p_tree_ref (PTree *tree)
{
  g_return_val_if_fail (tree != NULL, NULL);

  g_atomic_int_inc (&tree->ref_count);

  return tree;
}

/**
 * p_tree_unref:
 * @tree: a #PTree.
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
void
p_tree_unref (PTree *tree)
{
  g_return_if_fail (tree != NULL);

  if (g_atomic_int_dec_and_test (&tree->ref_count))
    {
      p_tree_remove_all (tree);
      g_free (tree->r);
      g_slice_free (PTree, tree);
    }
}

/**
 * p_tree_destroy:
 * @tree: a #PTree.
 * 
 * Removes all keys and values from the #PTree and decreases its
 * reference count by one. If keys and/or values are dynamically
 * allocated, you should either free them first or create the #PTree
 * using p_tree_new_full().  In the latter case the destroy functions
 * you supplied will be called on all keys and values before destroying
 * the #PTree.
 **/
void
p_tree_destroy (PTree *tree)
{
  g_return_if_fail (tree != NULL);

  p_tree_remove_all (tree);
  p_tree_unref (tree);
}

/**
 * p_tree_current_version:
 * @tree: a #PTree
 *
 * Returns the current version number of the #PTree.  Inserting or deleting
 * keys will affect the current version, but not change earlier versions.
 *
 * Returns: the current version number of the #PTree.
 **/
guint
p_tree_current_version (PTree *tree)
{
  return tree->version;
}

/**
 * p_tree_next_version:
 * @tree: a #PTree
 *
 * Increments the version number of the tree.  Inserting or deleting keys will
 * affect the new version of the tree, but not change earlier versions.
 *
 * Returns: the new current version number of the #PTree.
 **/
guint
p_tree_next_version (PTree *tree)
{
  g_assert (tree->version+1 != 0);
  return ++tree->version;
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

static guint
p_tree_node_delete_versions (PTree     *tree,
                             PTreeNode *node,
                             guint      pnext,
                             guint      version)
{
  guint rm, i, nv, next, nextv, lnext, rnext, ret;

  if (!node)
    return 0;

  nv = node->nv;

  ret = 0;
  rm = 0;
  for (i = first_v (nv); i < nv && node->v[i].version <= version; i = next)
    {
      next = next_v (i, nv);

      if (next < nv)
        nextv = node->v[next].version - 1;
      else
        nextv = version;

      if (next < nv && node->v[i].left &&
          node->v[next].left == node->v[i].left &&
          (!pnext || node->v[next].version <= pnext))
        lnext = node->v[next].version;
      else if (next == nv || node->v[next].version > pnext)
        lnext = pnext;
      else
        lnext = 0;
      lnext = p_tree_node_delete_versions (tree, node->v[i].left, lnext,
                                           nextv);

      if (next < nv && node->v[i].right &&
          node->v[next].right == node->v[i].right &&
          (!pnext || node->v[next].version <= pnext))
        rnext = node->v[next].version;
      else if (next == nv || node->v[next].version > pnext)
        rnext = pnext;
      else
        rnext = 0;
      rnext = p_tree_node_delete_versions (tree, node->v[i].right, rnext,
                                           nextv);

      /* smallest non-zero of pnext, rnext, and lnext (or zero if all 3 are) */
      nextv = MIN ((pnext ? pnext : (lnext ? lnext : rnext)),
                   MIN ((lnext ? lnext : (rnext ? rnext : pnext)),
                        (rnext ? rnext : (lnext ? lnext : pnext))));

      if (nextv && (next == nv || node->v[next].version > nextv))
        { /* leave this one behind as there are more pointers coming here */
          ret = node->v[i].version = nextv;
          break;
        }

      ++rm;
    }

  /* remove the first 'rm' versioned pointers from the node, they are <=
     'version' */
  for (i = 1; rm && i+rm < nv; ++i)
    node->v[i] = node->v[i+rm];
  node->nv -= rm;

  /* if we removed the last version inside the node, then we can free it */
  if (node->nv == 0)
    {
      p_tree_node_free (tree, node);
      node = NULL;
    }

  /* if we saved a version here, then it's because a child still needs it
     or the parent still needs it.  if the child needs it, then we will
     still have a pointer back to the parent, so it needs to also know.  so
     either way the parent needs it.
     if we did not save a version, but the parent needs us to stick around for
     something, then we can let it know that we did
  */
  if (!ret && node && pnext)
    {
      g_assert (node->v[first_v (node->nv)].version == pnext);
      ret = pnext;
    }

  return ret;
}

/**
 * p_tree_delete_versions:
 * @tree: a #PTree.
 * @version: the highest version to delete.
 *
 * Deletes all versions in the #PTree which are at most @version.  You can not
 * delete the latest version of the #PTree, so @version must be less than the
 * value returned by p_tree_current_version().

 * Deleting a version from the #PTree frees all resources used that are not
 * needed for later versions in the #PTree.  All elements which have been
 * removed from the tree in a version no later than @version will be freed,
 * and if you supplied a @key_destroy_func or @value_destroy_func when
 * creating the #PTree, any such nodes will have their keys and values
 * freed using these functions (unless it was removed from the #PTree by
 * p_tree_steal(), in which case the key and value is not freed).
 **/
void
p_tree_delete_versions (PTree *tree,
                        guint  version)
{
  guint rm, i, l, keep, next;

  g_return_if_fail (tree != NULL);
  g_return_if_fail (version < tree->version);

  if (version < tree->r[first_v (tree->nr)].version)
    return;

  rm = 0;
  keep = i = first_v (tree->nr);
  next = next_v (i, tree->nr);
  while (i < tree->nr && tree->r[i].version < version + 1)
    {
      guint nextv, v;

      if (next == tree->nr || tree->r[next].version > version + 1)
        nextv = version + 1;
      else if (next < tree->nr && tree->r[next].root == tree->r[i].root)
        nextv = tree->r[next].version;
      else
        nextv = 0;

      if (next < tree->nr && tree->r[next].version <= version)
        v = tree->r[next].version - 1;
      else
        v = version;

      l = p_tree_node_delete_versions (tree, tree->r[i].root, nextv, v);
      g_assert (l == 0 || l > v);

      if (l > v)
        tree->r[i].version = l;
      else
        {
          ++rm;
          keep = next;
        }

      i = next;
      next = next_v (i, tree->nr);
    }

  if (rm)
    {
      guint j;
      for (j = first_v (tree->nr - rm);
           j < tree->nr - rm;
           j = next_v (j, tree->nr - rm), keep = next_v (keep, tree->nr))
        {
          tree->r[j] = tree->r[keep];
        }
    }

  tree->nr -= rm;
  tree->r = g_renew(PTreeRootVersion, tree->r, tree->nr);

#ifdef P_TREE_DEBUG
  {
    guint i;
    for (i = 0; i <= tree->version; ++i)
      p_tree_node_check (p_tree_root_find_version (tree, i)->root, i);
  }
#endif
}

/* Make a new root pointer if the current one is not for the current version
   of the tree */
static void
p_tree_root_next_version (PTree *tree)
{
  g_assert(tree->r[0].version <= tree->version);

  if (tree->r[0].version < tree->version)
    {
      /* add a new version of the root */
      tree->nr++;
      tree->r = g_renew(PTreeRootVersion, tree->r, tree->nr);
      /* copy the latest version from r[0] */
      tree->r[tree->nr-1] = tree->r[0];
      tree->r[0].version = tree->version;
    }
}

/* add a new version of pointers to a node.  return TRUE if ok, and FALSE
   if there is no room to do so. */
static inline PTreeNode *
p_tree_node_add_version (PTree     *tree,
                         PTreeNode *node)
{
  if (!node)
    return NULL;

  /* node already has the current version */
  if (node->v[0].version == tree->version)
    return node;
  /* if we filled the node's pointer table and need to make a new PTreeNode */
  else if (node->v[0].version == 0 || node->nv >= TABLE_SIZE)
    {
      PTreeNode *newnode = g_slice_new(PTreeNode);
      newnode->v = g_slice_alloc(sizeof(PTreeNodeVersion) * TABLE_SIZE);
      newnode->data = node->data;
      g_atomic_int_inc (&newnode->data->ref_count);
      newnode->v[0] = node->v[0]; /* copy the latest version to here */
      newnode->v[0].version = tree->version;
      newnode->nv = 1;
      return newnode;
    }
  /* there is space left in the node's pointer table for a new version */
  else
    {
      node->nv++;
      /* copy the latest version from v[0] */
      node->v[node->nv-1] = node->v[0];
      node->v[0].version = tree->version;
      return node;
    }
}

static inline void
p_tree_node_fix_incoming (PTree     *tree,
                          PTreeNode *oldnode,
                          PTreeNode *newnode)
{
  PTreeNode *oldparent, *oldleft, *oldright;
  PTreeNode *newparent, *newleft, *newright;

  if (oldnode == newnode) return;

  g_assert (oldnode != NULL);
  g_assert (newnode != NULL);

  /* find incoming edges to the old version of the node */
  oldparent = newnode->v[0].parent;
  if (oldparent && oldparent->v[0].left != oldnode &&
      oldparent->v[0].right != oldnode)
    oldparent = NULL;
  oldleft = newnode->v[0].left;
  if (oldleft && oldleft->v[0].parent != oldnode)
    oldleft = NULL;
  oldright = newnode->v[0].right;
  if (oldright && oldright->v[0].parent != oldnode)
    oldright = NULL;

  /* add new pointers for nodes that point to me, possibly creating new
     versions of the nodes */
  newparent = p_tree_node_add_version (tree, oldparent);
  newleft = p_tree_node_add_version (tree, oldleft);
  newright = p_tree_node_add_version (tree, oldright);

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
  if (newparent)
    {
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
  p_tree_node_fix_incoming (tree, oldparent, newparent);
  p_tree_node_fix_incoming (tree, oldleft, newleft);
  p_tree_node_fix_incoming (tree, oldright, newright);

  /* 4. if this was the root node, then the root pointer into the tree needs
     be updated */
  if (tree->r[0].root == oldnode)
    {
      p_tree_root_next_version (tree);
      tree->r[0].root = newnode;
    }
}

/* You must call this on a node before you start changing its pointers,
   or you might be changing an old (permanent) version of the node */
static PTreeNode*
p_tree_node_next_version (PTree     *tree,
                          PTreeNode *oldnode)
{
  PTreeNode *newnode;

  if (!oldnode)
    return NULL;

  g_assert(oldnode->v[0].version <= tree->version);

  newnode = p_tree_node_add_version (tree, oldnode);
  p_tree_node_fix_incoming (tree, oldnode, newnode);
  return newnode;
}

/**
 * p_tree_insert:
 * @tree: a #PTree.
 * @key: the key to insert.
 * @value: the value corresponding to the key.
 * 
 * Inserts a key/value pair into a #PTree. If the given key already exists 
 * in the #PTree its corresponding value is set to the new value. If you 
 * supplied a value_destroy_func when creating the #PTree, the old value is 
 * freed using that function. If you supplied a @key_destroy_func when 
 * creating the #PTree, the passed key is freed using that function.
 *
 * The tree is automatically 'balanced' as new key/value pairs are added,
 * so that the distance from the root to every leaf is small.
 **/
void
p_tree_insert (PTree    *tree,
	       gpointer  key,
	       gpointer  value)
{
  g_return_if_fail (tree != NULL);

  p_tree_insert_internal (tree, key, value, FALSE);

#ifdef P_TREE_DEBUG
  {
    guint i;
    for (i = 0; i <= tree->version; ++i)
      p_tree_node_check (p_tree_root_find_version (tree, i)->root, i);
  }
#endif
}

/**
 * p_tree_replace:
 * @tree: a #PTree.
 * @key: the key to insert.
 * @value: the value corresponding to the key.
 * 
 * Inserts a new key and value into a #PTree similar to p_tree_insert(). 
 * The difference is that if the key already exists in the #PTree, it gets 
 * replaced by the new key. If you supplied a @value_destroy_func when 
 * creating the #PTree, the old value is freed using that function. If you 
 * supplied a @key_destroy_func when creating the #PTree, the old key is 
 * freed using that function. 
 *
 * The tree is automatically 'balanced' as new key/value pairs are added,
 * so that the distance from the root to every leaf is small.
 **/
void
p_tree_replace (PTree    *tree,
		gpointer  key,
		gpointer  value)
{
  g_return_if_fail (tree != NULL);

  p_tree_insert_internal (tree, key, value, TRUE);

#ifdef P_TREE_DEBUG
  {
    guint i;
    for (i = 0; i <= tree->version; ++i)
      p_tree_node_check (p_tree_root_find_version (tree, i)->root, i);
  }
#endif
}

/*
 * Implementation of a treap
 *
 * (Copied from gsequence.c)
 */
static guint
p_tree_priority (PTreeNode *node)
{
  guint key = GPOINTER_TO_UINT (node->data);

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
  return key? key : 1;
}

/* Internal insert routine.  Always inserts into the current version of the
   tree. */
static void
p_tree_insert_internal (PTree    *tree,
                        gpointer  key,
                        gpointer  value,
                        gboolean  replace)
{
  PTreeNode *node, *child;

  g_return_if_fail (tree != NULL);

  if (!tree->r[0].root)
    {
      p_tree_root_next_version (tree);
      tree->r[0].root = p_tree_node_new (tree, key, value);
      tree->nnodes++;
      return;
    }

  node = tree->r[0].root;

  while (1)
    {
      gint cmp = tree->key_compare (key, node->data->key,
                                    tree->key_compare_data);
      
      if (cmp == 0)
        {
          if (tree->value_destroy_func)
            tree->value_destroy_func (node->data->value);

          node->data->value = value;

          if (replace)
            {
              if (tree->key_destroy_func)
                tree->key_destroy_func (node->data->key);

              node->data->key = key;
            }
          else
            {
              /* free the passed key */
              if (tree->key_destroy_func)
                tree->key_destroy_func (key);
            }

          return;
        }
      else if (cmp < 0)
        {
          if (node->v[0].left)
              node = node->v[0].left;
          else
            {
              child = p_tree_node_new (tree, key, value);
              node = p_tree_node_next_version (tree, node);

              child->v[0].parent = node;
              node->v[0].left = child;

	      tree->nnodes++;

              break;
            }
        }
      else
        {
          if (node->v[0].right)
              node = node->v[0].right;
          else
            {
              child = p_tree_node_new (tree, key, value);
              node = p_tree_node_next_version (tree, node);

              child->v[0].parent = node;
              node->v[0].right = child;

	      tree->nnodes++;

              break;
            }
        }
    }

  /* rotate the new node up until the heap property is restored */
  node = child;
  while (node->v[0].parent &&
         p_tree_priority (node) < p_tree_priority (node->v[0].parent))
    {
      PTreeNode *sp, *p;

      /* we'll be changing both of these nodes */
      p = p_tree_node_next_version (tree, node->v[0].parent);
      sp = p_tree_node_next_version (tree, p->v[0].parent);

      if (p->v[0].left == node)
        node = p_tree_node_rotate_right (tree, p);
      else
        node = p_tree_node_rotate_left (tree, p);

      if (!sp)
        {
          p_tree_root_next_version (tree);
          tree->r[0].root = node;
        }
      else if (sp->v[0].left == p)
        sp->v[0].left = node;
      else
        sp->v[0].right = node;
    }
}

/**
 * p_tree_remove:
 * @tree: a #PTree.
 * @key: the key to remove.
 * 
 * Removes a key/value pair from a #PTree.
 *
 * If the #PTree was created using p_tree_new_full(), the key and value 
 * are freed using the supplied destroy functions, otherwise you have to 
 * make sure that any dynamically allocated values are freed yourself.
 * Note that if the key existed in
 * earlier versions of the tree (p_tree_next_version() has been called since
 * it was inserted), then it cannot be removed from the tree. *
 * If the key does not exist in the #PTree, the function does nothing.
 *
 * Returns: %TRUE if the key was found and able to be removed
 *   (prior to 2.8, this function returned nothing)
 **/
gboolean
p_tree_remove (PTree         *tree,
	       gconstpointer  key)
{
  gboolean removed;

  g_return_val_if_fail (tree != NULL, FALSE);

  removed = p_tree_remove_internal (tree, key, FALSE);

#ifdef P_TREE_DEBUG
  {
    guint i;
    for (i = 0; i <= tree->version; ++i)
      p_tree_node_check (p_tree_root_find_version (tree, i)->root, i);
  }
#endif

  return removed;
}

/**
 * p_tree_steal:
 * @tree: a #PTree.
 * @key: the key to remove.
 * 
 * Removes a key and its associated value from a #PTree without calling 
 * the key and value destroy functions.  Note that if the key existed in
 * earlier versions of the tree (p_tree_next_version() has been called since
 * it was inserted), then it cannot be removed from the tree until all
 * versions containing the node are removed from the tree, by calling
 * p_tree_delete_versions().  However, if the node is removed from the current
 * version of the tree with p_tree_steal() then the key and value destroy
 * functions will not be called once the last version of the key/value pair
 * is removed.
 * If the key does not exist in the #PTree, the function does nothing.
 *
 * Returns: %TRUE if the key was found and able to be removed
 *   (prior to 2.8, this function returned nothing)
 **/
gboolean
p_tree_steal (PTree         *tree,
              gconstpointer  key)
{
  gboolean removed;

  g_return_val_if_fail (tree != NULL, FALSE);

  removed = p_tree_remove_internal (tree, key, TRUE);

#ifdef P_TREE_DEBUG
  {
    guint i;
    for (i = 0; i <= tree->version; ++i)
      p_tree_node_check (p_tree_root_find_version (tree, i)->root, i);
  }
#endif

  return removed;
}

/* internal remove routine */
static gboolean
p_tree_remove_internal (PTree         *tree,
                        gconstpointer  key,
                        gboolean       steal)
{
  PTreeNode *node, *parent;
  gboolean is_leftchild;
  gboolean data_free;

  g_return_val_if_fail (tree != NULL, FALSE);

  if (!tree->r[0].root)
    return FALSE;

  node = tree->r[0].root;
  parent = NULL;
  is_leftchild = FALSE;

  while (1)
    {
      gint cmp = tree->key_compare (key, node->data->key,
                                    tree->key_compare_data);
      
      if (cmp == 0)
        break;
      else if (cmp < 0)
        {
          if (!node->v[0].left)
            return FALSE;

	  parent = node;
          is_leftchild = TRUE;
	  node = node->v[0].left;
        }
      else
        {
          if (!node->v[0].right)
            return FALSE;

	  parent = node;
          is_leftchild = FALSE;
	  node = node->v[0].right;
        }
    }

  /* rotate the node down the tree, maintaining the heap property */
  while (node->v[0].left || node->v[0].right)
    {
      /* we're changing this node, make sure our pointer will stay valid
         when we rotate it */
      node = p_tree_node_next_version (tree, node);
      /* getting the next version for the node may change where its parent
         lies, so get a new pointer for it, from the node */
      parent = node->v[0].parent;

      if (!node->v[0].left ||
          (node->v[0].right &&
           p_tree_priority (node->v[0].left) >
           p_tree_priority (node->v[0].right)))
        {
          /* rotate the right child up */
          if (!parent)
            {
              p_tree_root_next_version (tree);
              parent = tree->r[0].root =
                p_tree_node_rotate_left (tree, node);
            }
          else
            {
              parent = p_tree_node_next_version (tree, parent);
              if (is_leftchild)
                parent = parent->v[0].left =
                  p_tree_node_rotate_left (tree, node);
              else
                parent = parent->v[0].right =
                  p_tree_node_rotate_left (tree, node);
            }
          is_leftchild = TRUE;
        }
      else
        {
          /* rotate the left child up */
          if (!parent)
            {
              p_tree_root_next_version (tree);
              parent = tree->r[0].root =
                p_tree_node_rotate_right (tree, node);
            }
          else
            {
              parent = p_tree_node_next_version (tree, parent);
              if (is_leftchild)
                parent = parent->v[0].left =
                  p_tree_node_rotate_right (tree, node);
              else
                parent = parent->v[0].right =
                  p_tree_node_rotate_right (tree, node);
            }
          is_leftchild = FALSE;
        }
    }

  /* remove any pointers to the node in the treap */
  if (!parent)
    {
      p_tree_root_next_version (tree);
      tree->r[0].root = NULL;
    }
  else
    {
      /* make a new version of the parent to cut the child off.
         making a new version of the parent may make a new version of
         the node being deleted, which is the one we'd want to free then
         instead, so update the node pointer */
      parent = p_tree_node_next_version (tree, parent);
      if (is_leftchild)
        {
          node = parent->v[0].left;
          parent->v[0].left = NULL;
        }
      else
        {
          node = parent->v[0].right;
          parent->v[0].right = NULL;
        }
    }

  tree->nnodes--;

  if (node->v[0].version == tree->version)
    {
      /* remove the entry from the node's pointer table */
      node->v[0] = node->v[node->nv-1];
      --node->nv;
    }

  node->data->stolen = steal;
  data_free = FALSE;

  /* only really delete the node if it was only in the current version,
     otherwise it needs to be remembered */
  if (node->nv == 0)
    data_free = p_tree_node_free (tree, node);

  return data_free;
}

/**
 * p_tree_lookup:
 * @tree: a #PTree.
 * @key: the key to look up.
 * 
 * Gets the value corresponding to the given key. Since a #PTree is 
 * automatically balanced as key/value pairs are added, key lookup is very 
 * fast.
 *
 * Return value: the value corresponding to the key, or %NULL if the key was
 * not found.
 **/
gpointer
p_tree_lookup (PTree         *tree,
	       gconstpointer  key)
{
  return p_tree_lookup_related_v (tree, tree->version, key,
                                  P_TREE_SEARCH_EXACT);
}

/**
 * p_tree_lookup_v:
 * @tree: a #PTree.
 * @version: the version of the tree within which to search.  If
 *   p_tree_next_version() has not been used, then this is 0.  This value
 *   must be at most the value returned by p_tree_current_version().
 * @key: the key to look up.
 *
 * Gets the value corresponding to the given key in a specified @version of
 * the #PTree. Since a #PTree is
 * automatically balanced as key/value pairs are added, key lookup is very
 * fast.
 *
 * Return value: the value corresponding to the key, or %NULL if the key was
 * not found.
 **/
gpointer
p_tree_lookup_v (PTree         *tree,
                 guint          version,
                 gconstpointer  key)
{
  return p_tree_lookup_related_v (tree, version, key, P_TREE_SEARCH_EXACT);
}

/**
 * p_tree_lookup_related:
 * @tree: a #PTree.
 * @key: the key to look up.
 * @search_type: the search behavior if the @key is not present in the #PTree,
 *   one of %P_TREE_SEARCH_EXACT, %P_TREE_SEARCH_SUCCESSOR, and
 *   %P_TREE_SEARCH_PREDECESSOR.
 *
 * Gets a value corresponding to the given key.
 *
 * If the given @key is present in the tree, then its corresponding value is
 * always returned.  If it is not, then the @search_type will define the
 * function's behavior.  If @search_type is %P_TREE_SEARCH_EXACT, the
 * function will return %NULL if the @key is not found.  If @search_type is
 * %P_TREE_SEARCH_SUCCESSOR, the function will find the next larger key that
 * is present in the tree and return its value, or %NULL if there are no keys
 * larger than @key in the tree.  If @search_type is
 * %P_TREE_SEARCH_PREDECESSOR, the function will find the next smaller key that
 * is present in the tree and return its value, or %NULL if there are no keys
 * smaller than @key in the tree.
 * Since a #PTree is
 * automatically balanced as key/value pairs are added, key lookup is very
 * fast.
 *
 * Return value: the value corresponding to the found key, or %NULL if no
 * matching key was found.
 **/
gpointer
p_tree_lookup_related  (PTree          *tree,
                        gconstpointer   key,
                        PTreeSearchType search_type)
{
  return p_tree_lookup_related_v (tree, tree->version, key, search_type);
}

/**
 * p_tree_lookup_related_v:
 * @tree: a #PTree.
 * @version: the version of the tree within which to search.  If
 *   p_tree_next_version() has not been used, then this is 0.  This value
 *   must be at most the value returned by p_tree_current_version().
 * @key: the key to look up.
 * @search_type: the search behavior if the @key is not present in the #PTree,
 *   one of %P_TREE_SEARCH_EXACT, %P_TREE_SEARCH_SUCCESSOR, and
 *   %P_TREE_SEARCH_PREDECESSOR.
 *
 * Gets a value corresponding to the given key in a specified @version of the
 * #PTree.
 *
 * If the given @key is present in the tree, then its corresponding value is
 * always returned.  If it is not, then the @search_type will define the
 * function's behavior.  If @search_type is %P_TREE_SEARCH_EXACT, the
 * function will return %NULL if the @key is not found.  If @search_type is
 * %P_TREE_SEARCH_SUCCESSOR, the function will find the next larger key that
 * is present in the tree and return its value, or %NULL if there are no keys
 * larger than @key in the tree.  If @search_type is
 * %P_TREE_SEARCH_PREDECESSOR, the function will find the next smaller key that
 * is present in the tree and return its value, or %NULL if there are no keys
 * smaller than @key in the tree.
 * Since a #PTree is
 * automatically balanced as key/value pairs are added, key lookup is very
 * fast.
 *
 * Return value: the value corresponding to the found key, or %NULL if no
 * matching key was found.
 **/
gpointer
p_tree_lookup_related_v (PTree          *tree,
                         guint           version,
                         gconstpointer   key,
                         PTreeSearchType search_type)
{
  PTreeNode *node;

  g_return_val_if_fail (tree != NULL, NULL);
  g_return_val_if_fail (version <= tree->version, NULL);

  node = p_tree_find_node (tree, key, search_type, version);
  
  return node ? node->data->value : NULL;
}

/**
 * p_tree_lookup_extended:
 * @tree: a #PTree.
 * @lookup_key: the key to look up.
 * @orig_key: returns the original key.
 * @value: returns the value associated with the key.
 * 
 * Looks up a key in the #PTree, returning the original key and the
 * associated value and a #gboolean which is %TRUE if the key was found. This 
 * is useful if you need to free the memory allocated for the original key, 
 * for example before calling p_tree_remove().
 * 
 * Return value: %TRUE if the key was found in the #PTree.
 **/
gboolean
p_tree_lookup_extended (PTree         *tree,
                        gconstpointer  lookup_key,
                        gpointer      *orig_key,
                        gpointer      *value)
{
  return p_tree_lookup_extended_v (tree, tree->version, lookup_key, orig_key,
                                   value);
}

/**
 * p_tree_lookup_extended_v:
 * @tree: a #PTree.
 * @version: the version of the tree within which to search.  If
 *   p_tree_next_version() has not been used, then this is 0.  This value
 *   must be at most the value returned by p_tree_current_version().
 * @lookup_key: the key to 5Blook up.
 * @orig_key: returns the original key.
 * @value: returns the value associated with the key.
 *
 * Looks up a key in a specified @version of the #PTree, returning the
 * original key and the
 * associated value and a #gboolean which is %TRUE if the key was found. This
 * is useful if you need to free the memory allocated for the original key,
 * for example before calling p_tree_remove().
 *
 * Return value: %TRUE if the key was found in the #PTree.
 **/
gboolean
p_tree_lookup_extended_v (PTree         *tree,
                          guint          version,
                          gconstpointer  lookup_key,
                          gpointer      *orig_key,
                          gpointer      *value)
{
  PTreeNode *node;
  
  g_return_val_if_fail (tree != NULL, FALSE);
  g_return_val_if_fail (version <= tree->version, FALSE);
  
  node = p_tree_find_node (tree, lookup_key, P_TREE_SEARCH_EXACT, version);
  
  if (node)
    {
      if (orig_key)
        *orig_key = node->data->key;
      if (value)
        *value = node->data->value;
      return TRUE;
    }
  else
    return FALSE;
}

/**
 * p_tree_foreach:
 * @tree: a #PTree.
 * @func: the function to call for each node visited. If this function
 *   returns %TRUE, the traversal is stopped.
 * @user_data: user data to pass to the function.
 * 
 * Calls the given function for each of the key/value pairs in the #PTree.
 * The function is passed the key and value of each pair, and the given
 * @data parameter. The tree is traversed in sorted order.
 *
 * The tree may not be modified while iterating over it (you can't 
 * add/remove items). To remove all items matching a predicate, you need 
 * to add each item to a list in your #GTraverseFunc as you walk over 
 * the tree, then walk the list and remove each item.
 **/
void
p_tree_foreach (PTree         *tree,
                GTraverseFunc  func,
                gpointer       user_data)
{
  p_tree_foreach_v (tree, tree->version, func, user_data);
}

/**
 * p_tree_foreach_v:
 * @tree: a #PTree.
 * @version: the version of the tree to traverse.  If
 *   p_tree_next_version() has not been used, then this is 0.  This value
 *   must be at most the value returned by p_tree_current_version().
 * @func: the function to call for each node visited. If this function
 *   returns %TRUE, the traversal is stopped.
 * @user_data: user data to pass to the function.
 *
 * Calls the given function for each of the key/value pairs in a
 * specified @version of the #PTree.
 * The function is passed the key and value of each pair, and the given
 * @data parameter. The tree is traversed in sorted order.
 *
 * The tree may not be modified while iterating over it (you can't
 * add/remove items). To remove all items matching a predicate, you need
 * to add each item to a list in your #GTraverseFunc as you walk over
 * the tree, then walk the list and remove each item.
 **/
void
p_tree_foreach_v (PTree         *tree,
                  guint          version,
                  GTraverseFunc  func,
                  gpointer       user_data)
{
  PTreeNode *node;

  g_return_if_fail (tree != NULL);
  g_return_if_fail (version <= tree->version);
  
  if (!tree->r[0].root)
    return;

  node = p_tree_first_node (tree, version);
  
  while (node)
    {
      if ((*func) (node->data->key, node->data->value, user_data))
	break;
      
      node = p_tree_node_next (node, version);
    }
}

/**
 * p_tree_traverse:
 * @tree: a #PTree.
 * @traverse_func: the function to call for each node visited. If this 
 *   function returns %TRUE, the traversal is stopped.
 * @traverse_type: the order in which nodes are visited, one of %G_IN_ORDER,
 *   %G_PRE_ORDER and %G_POST_ORDER.
 * @user_data: user data to pass to the function.
 * 
 * Calls the given function for each node in the #PTree. 
 *
 * Deprecated:2.2: The order of a balanced tree is somewhat arbitrary. If you 
 * just want to visit all nodes in sorted order, use p_tree_foreach() 
 * instead. If you really need to visit nodes in a different order, consider
 * using an <link linkend="glib-N-ary-Trees">N-ary Tree</link>.
 **/
void
p_tree_traverse (PTree         *tree,
		 GTraverseFunc  traverse_func,
		 GTraverseType  traverse_type,
		 gpointer       user_data)
{
  g_return_if_fail (tree != NULL);

  if (!tree->r[0].root)
    return;

  switch (traverse_type)
    {
    case G_PRE_ORDER:
      p_tree_node_pre_order (tree->r[0].root, traverse_func, user_data);
      break;

    case G_IN_ORDER:
      p_tree_node_in_order (tree->r[0].root, traverse_func, user_data);
      break;

    case G_POST_ORDER:
      p_tree_node_post_order (tree->r[0].root, traverse_func, user_data);
      break;
    
    case G_LEVEL_ORDER:
      g_warning ("p_tree_traverse(): traverse type G_LEVEL_ORDER isn't implemented.");
      break;
    }
}

/**
 * p_tree_search:
 * @tree: a #PTree.
 * @search_func: a function used to search the #PTree. 
 * @user_data: the data passed as the second argument to the @search_func 
 * function.
 * 
 * Searches a #PTree using @search_func.
 *
 * The @search_func is called with a pointer to the key of a key/value pair in 
 * the tree, and the passed in @user_data. If @search_func returns 0 for a 
 * key/value pair, then p_tree_search_func() will return the value of that 
 * pair. If @search_func returns -1,  searching will proceed among the 
 * key/value pairs that have a smaller key; if @search_func returns 1, 
 * searching will proceed among the key/value pairs that have a larger key.
 *
 * Return value: the value corresponding to the found key, or %NULL if the key 
 * was not found.
 **/
gpointer
p_tree_search (PTree         *tree,
	       GCompareFunc   search_func,
	       gconstpointer  user_data)
{
  return p_tree_search_v (tree, tree->version, search_func, user_data);
}

/**
 * p_tree_search_v:
 * @tree: a #PTree.
 * @version: the version of the tree within which to search.  If
 *   p_tree_next_version() has not been used, then this is 0.  This value
 *   must be at most the value returned by p_tree_current_version().
 * @search_func: a function used to search the #PTree.
 * @user_data: the data passed as the second argument to the @search_func
 * function.
 *
 * Searches a specified @version of the #PTree using @search_func.
 *
 * The @search_func is called with a pointer to the key of a key/value pair in
 * the tree, and the passed in @user_data. If @search_func returns 0 for a
 * key/value pair, then p_tree_search_func() will return the value of that
 * pair. If @search_func returns -1,  searching will proceed among the
 * key/value pairs that have a smaller key; if @search_func returns 1,
 * searching will proceed among the key/value pairs that have a larger key.
 *
 * Return value: the value corresponding to the found key, or %NULL if the key
 * was not found.
 **/
gpointer
p_tree_search_v (PTree         *tree,
                 guint          version,
                 GCompareFunc   search_func,
                 gconstpointer  user_data)
{
  g_return_val_if_fail (tree != NULL, NULL);
  g_return_val_if_fail (version <= tree->version, NULL);

  return p_tree_node_search (p_tree_root_find_version (tree, version)->root,
                             search_func, user_data, P_TREE_SEARCH_EXACT,
                             version);
}

/**
 * p_tree_search_related:
 * @tree: a #PTree.
 * @search_func: a function used to search the #PTree.
 * @user_data: the data passed as the second argument to the @search_func
 * @search_type: the search behavior if the @key is not present in the #PTree,
 *   one of %P_TREE_SEARCH_EXACT, %P_TREE_SEARCH_SUCCESSOR, and
 *   %P_TREE_SEARCH_PREDECESSOR.
 *
 * Searches a #PTree using @search_func.
 *
 * The @search_func is called with a pointer to the key of a key/value pair in
 * the tree, and the passed in @user_data. If @search_func returns 0 for a
 * key/value pair, then p_tree_search_func() will return the value of that
 * pair. If @search_func returns -1,  searching will proceed among the
 * key/value pairs that have a smaller key; if @search_func returns 1,
 * searching will proceed among the key/value pairs that have a larger key.
 * If @search_func never returns 0 before searching the entire height of the
 * tree, then the @search_type will define what the function returns.
 * If @search_type is %P_TREE_SEARCH_EXACT, the
 * function will return %NULL.  If @search_type is
 * %P_TREE_SEARCH_SUCCESSOR, the function will return the value corresponding
 * to the last key at which @search_func returned -1.  This key would be the
 * successor to the element being searched for. If @search_type is
 * %P_TREE_SEARCH_PREDECESSOR, the function return the value corresponding
 * to the last key at which @search_func returned 1.  This key would be the
 * predecessor to the element being searched for.
 *
 * Return value: the value corresponding to the found key, or %NULL if no
 * matching key was found.
 **/
gpointer
p_tree_search_related (PTree          *tree,
                       GCompareFunc    search_func,
                       gconstpointer   user_data,
                       PTreeSearchType search_type)
{
  return p_tree_search_related_v (tree, tree->version, search_func, user_data,
                                  search_type);
}

/**
 * p_tree_search_related_v:
 * @tree: a #PTree.
 * @version: the version of the tree within which to search.  If
 *   p_tree_next_version() has not been used, then this is 0.  This value
 *   must be at most the value returned by p_tree_current_version().
 * @search_func: a function used to search the #PTree.
 * @user_data: the data passed as the second argument to the @search_func
 * @search_type: the search behavior if the @key is not present in the #PTree,
 *   one of %P_TREE_SEARCH_EXACT, %P_TREE_SEARCH_SUCCESSOR, and
 *   %P_TREE_SEARCH_PREDECESSOR.
 *
 * Searches a specified @version of the #PTree using @search_func.
 *
 * The @search_func is called with a pointer to the key of a key/value pair in
 * the tree, and the passed in @user_data. If @search_func returns 0 for a
 * key/value pair, then p_tree_search_func() will return the value of that
 * pair. If @search_func returns -1,  searching will proceed among the
 * key/value pairs that have a smaller key; if @search_func returns 1,
 * searching will proceed among the key/value pairs that have a larger key.
 * If @search_func never returns 0 before searching the entire height of the
 * tree, then the @search_type will define what the function returns.
 * If @search_type is %P_TREE_SEARCH_EXACT, the
 * function will return %NULL.  If @search_type is
 * %P_TREE_SEARCH_SUCCESSOR, the function will return the value corresponding
 * to the last key at which @search_func returned -1.  This key would be the
 * successor to the element being searched for. If @search_type is
 * %P_TREE_SEARCH_PREDECESSOR, the function return the value corresponding
 * to the last key at which @search_func returned 1.  This key would be the
 * predecessor to the element being searched for.
 *
 * Return value: the value corresponding to the found key, or %NULL if no
 * matching key was found.
 **/
gpointer
p_tree_search_related_v (PTree          *tree,
                         guint           version,
                         GCompareFunc    search_func,
                         gconstpointer   user_data,
                         PTreeSearchType search_type)
{
  g_return_val_if_fail (tree != NULL, NULL);
  g_return_val_if_fail (version <= tree->version, NULL);

  return p_tree_node_search (p_tree_root_find_version (tree, version)->root,
                             search_func, user_data, search_type,
                             version);
}

static gint
p_tree_node_height (PTreeNode *node,
                    guint version)
{
  PTreeNodeVersion *nv;
  gint l = 0, r = 0;
  if (node == NULL) return 0;
  nv = p_tree_node_find_version (node, version);
  if (nv->left) l = p_tree_node_height (nv->left, version);
  if (nv->right) r = p_tree_node_height (nv->right, version);
  return 1 + MAX(l, r);
}

/**
 * p_tree_height:
 * @tree: a #PTree.
 * 
 * Gets the height of a #PTree.
 *
 * If the #PTree contains no nodes, the height is 0.
 * If the #PTree contains only one root node the height is 1.
 * If the root node has children the height is 2, etc.
 * 
 * Return value: the height of the #PTree.
 **/
gint
p_tree_height (PTree *tree)
{
  return p_tree_height_v (tree, tree->version);
}

/**
 * p_tree_height_v:
 * @tree: a #PTree.
 * @version: the version of the tree which should be queried. If
 *   p_tree_next_version() has not been used, then this is 0.  This value
 *   must be at most the value returned by p_tree_current_version().
 *
 * Gets the height of a specified @version of the #PTree.
 *
 * If the #PTree contains no nodes, the height is 0.
 * If the #PTree contains only one root node the height is 1.
 * If the root node has children the height is 2, etc.
 *
 * Return value: the height of the #PTree.
 **/
gint
p_tree_height_v (PTree *tree,
                 guint  version)
{
  g_return_val_if_fail (tree != NULL, 0);
  g_return_val_if_fail (version <= tree->version, 0);

  return p_tree_node_height (p_tree_root_find_version (tree, version)->root,
                             version);
}

/**
 * p_tree_nnodes:
 * @tree: a #PTree.
 * 
 * Gets the number of nodes in the current version of a #PTree.
 * 
 * Return value: the number of nodes in the latest version of the #PTree.
 **/
gint
p_tree_nnodes (PTree *tree)
{
  g_return_val_if_fail (tree != NULL, 0);

  return tree->nnodes;
}

static PTreeNode *
p_tree_find_node (PTree           *tree,
                  gconstpointer    key,
                  PTreeSearchType  search_type,
                  guint            version)
{
  PTreeNode *node, *remember;
  PTreeRootVersion *rv;
  gint cmp;

  rv = p_tree_root_find_version (tree, version);
  node = rv->root;
  if (!node)
    return NULL;

  remember = NULL;
  while (1)
    {
      cmp = tree->key_compare (key, node->data->key, tree->key_compare_data);
      if (cmp == 0)
	return node;
      else if (cmp < 0)
	{
          PTreeNodeVersion *nodev = p_tree_node_find_version (node, version);
          if (search_type == P_TREE_SEARCH_SUCCESSOR)
            remember = node;
	  if (!nodev->left)
	    return remember;

	  node = nodev->left;
	}
      else
	{
          PTreeNodeVersion *nodev = p_tree_node_find_version (node, version);
          if (search_type == P_TREE_SEARCH_PREDECESSOR)
            remember = node;
	  if (!nodev->right)
	    return remember;

	  node = nodev->right;
	}
    }
}

static gint
p_tree_node_pre_order (PTreeNode     *node,
		       GTraverseFunc  traverse_func,
		       gpointer       data)
{
  if ((*traverse_func) (node->data->key, node->data->value, data))
    return TRUE;

  if (node->v[0].left)
    {
      if (p_tree_node_pre_order (node->v[0].left, traverse_func, data))
	return TRUE;
    }

  if (node->v[0].right)
    {
      if (p_tree_node_pre_order (node->v[0].right, traverse_func, data))
	return TRUE;
    }

  return FALSE;
}

static gint
p_tree_node_in_order (PTreeNode     *node,
		      GTraverseFunc  traverse_func,
		      gpointer       data)
{
  if (node->v[0].left)
    {
      if (p_tree_node_in_order (node->v[0].left, traverse_func, data))
	return TRUE;
    }

  if ((*traverse_func) (node->data->key, node->data->value, data))
    return TRUE;

  if (node->v[0].right)
    {
      if (p_tree_node_in_order (node->v[0].right, traverse_func, data))
	return TRUE;
    }
  
  return FALSE;
}

static gint
p_tree_node_post_order (PTreeNode     *node,
			GTraverseFunc  traverse_func,
			gpointer       data)
{
  if (node->v[0].left)
    {
      if (p_tree_node_post_order (node->v[0].left, traverse_func, data))
	return TRUE;
    }

  if (node->v[0].right)
    {
      if (p_tree_node_post_order (node->v[0].right, traverse_func, data))
	return TRUE;
    }

  if ((*traverse_func) (node->data->key, node->data->value, data))
    return TRUE;

  return FALSE;
}

static gpointer
p_tree_node_search (PTreeNode       *node,
		    GCompareFunc     search_func,
		    gconstpointer    data,
                    PTreeSearchType  search_type,
                    guint            version)
{
  gint dir;
  PTreeNode *remember;

  if (!node)
    return NULL;

  remember = NULL;
  while (1) 
    {
      dir = (* search_func) (node->data->key, data);
      if (dir == 0)
	return node->data->value;
      else if (dir < 0) 
	{ 
          PTreeNodeVersion *nodev = p_tree_node_find_version (node, version);
          if (search_type == P_TREE_SEARCH_SUCCESSOR)
            remember = node;
	  if (!nodev->left)
	    return remember;

	  node = nodev->left;
	}
      else
	{
          PTreeNodeVersion *nodev = p_tree_node_find_version (node, version);
          if (search_type == P_TREE_SEARCH_PREDECESSOR)
            remember = node;
	  if (!nodev->right)
	    return remember;
	  
	  node = nodev->right;
	}
    }
}

static PTreeNode*
p_tree_node_rotate_left (PTree     *tree,
                         PTreeNode *node)
{
  PTreeNode *right;

  g_assert (node->v[0].version == tree->version);

  right = p_tree_node_next_version (tree, node->v[0].right);

  if (right->v[0].left)
    {
      node->v[0].right = p_tree_node_next_version (tree, right->v[0].left);
      node->v[0].right->v[0].parent = node;
    }
  else
      node->v[0].right = NULL;
  right->v[0].left = node;
  right->v[0].parent = node->v[0].parent;
  node->v[0].parent = right;

  return right;
}

static PTreeNode*
p_tree_node_rotate_right (PTree     *tree,
                          PTreeNode *node)
{
  PTreeNode *left;

  g_assert (node->v[0].version == tree->version);

  left = p_tree_node_next_version (tree, node->v[0].left);

  if (left->v[0].right)
    {
      node->v[0].left = p_tree_node_next_version (tree, left->v[0].right);
      node->v[0].left->v[0].parent = node;
    }
  else
    node->v[0].left = NULL;
  left->v[0].right = node;
  left->v[0].parent = node->v[0].parent;
  node->v[0].parent = left;

  return left;
}

#ifdef P_TREE_DEBUG
static void
p_tree_node_check (PTreeNode *node,
                   guint      version)
{
  PTreeNodeVersion *nv, *tmpv;

  if (node)
    {
      nv = p_tree_node_find_version (node, version);

      g_assert (nv->left == NULL || nv->left != nv->right);

      if (nv->left)
	{
          tmpv = p_tree_node_find_version (nv->left, version);
          g_assert (tmpv->parent == node);
	}

      if (nv->right)
	{
          tmpv = p_tree_node_find_version (nv->right, version);
          g_assert (tmpv->parent == node);
	}

      if (nv->parent)
        {
          tmpv = p_tree_node_find_version (nv->parent, version);
          g_assert (tmpv->left == node || tmpv->right == node);
        }

      if (nv->left)
	p_tree_node_check (nv->left, version);
      if (nv->right)
	p_tree_node_check (nv->right, version);
    }
}

static void
p_tree_node_dump (PTreeNode *node,
                  guint      version,
		  gint       indent)
{
  PTreeNodeVersion *nv = p_tree_node_find_version (node, version);

  g_print ("%*s%c\n", indent, "", *(char *)node->data->key);

  if (nv->left)
    p_tree_node_dump (nv->left, version, indent + 2);
  else if ((node = p_tree_node_previous (node, version)))
    g_print ("%*s<%c\n", indent + 2, "", *(char *)node->data->key);

  if (nv->right)
    p_tree_node_dump (nv->right, version, indent + 2);
  else if ((node = p_tree_node_next (node, version)))
    g_print ("%*s>%c\n", indent + 2, "", *(char *)node->data->key);
}


void
p_tree_dump (PTree *tree,
             guint  version)
{
  PTreeRootVersion *rv = p_tree_root_find_version (tree, version);
  if (rv->root)
    p_tree_node_dump (rv->root, version, 0);
}
#endif
