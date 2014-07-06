#ifndef __P_TREE_H__
#define __P_TREE_H__

#include <atomic>
/* Tree Search Types */
typedef enum {
  P_TREE_SEARCH_EXACT,
  P_TREE_SEARCH_SUCCESSOR,
  P_TREE_SEARCH_PREDECESSOR
} PTreapSearchType;

typedef struct _GNode GNode;

/* N-way tree implementation
 */
struct _GNode {
  void* data;
  GNode *next;
  GNode *prev;
  GNode *parent;
  GNode *children;
};

/* Tree traverse orders */

typedef enum {
  G_IN_ORDER,
  G_PRE_ORDER,
  G_POST_ORDER,
  G_LEVEL_ORDER
} GTraverseType;

typedef struct _GNode GNode;

typedef bool (*GNodeTraverseFunc)(GNode *node, void* data);
typedef bool (*GTraverseFunc)(unsigned long key, void* value, void* data);

struct PTreapRootVersion;
struct PTreapNodeVersion;
struct PTreapNodeData;
struct PTreapNode;

struct PTreapRootVersion {
  PTreapNode *root;
  unsigned int version;
};

struct PTreap {
  PTreapRootVersion *r; /* versioned root nodes of the tree.  r[0] is
   the highest (latest) version.  then r[1]..r[nr-1] are
   older versions in ascending order.  Use first_v(),
   next_v() and prev_v() to traverse the list. */
  unsigned int nr;
  unsigned int nnodes;
  std::atomic_int ref_count;
  unsigned int version;
};

struct PTreapNodeVersion {
  unsigned int version;
  PTreapNode *left; /* left subtree */
  PTreapNode *right; /* right subtree */
  PTreapNode *parent; /* parent node */
};

struct PTreapNodeData {
  unsigned long key; /* key for this node */
  void* value; /* value stored at this node */
  std::atomic_int ref_count;
  short unsigned int stolen; /* true if the node is stolen instead of removed */
};

struct PTreapNode {
  PTreapNodeData *data; /* the node's permanent data */
  PTreapNodeVersion *v; /* versions of pointers for the node.  v[0] is the
   highest (latest) version.  then v[1]..v[nv-1] are
   older versions in ascending order.  Use first_v(),
   next_v() and prev_v() to traverse the list. */
  unsigned int nv; /* number of versions stored in this node */
};

typedef struct PTreapRootVersion PTreapRootVersion;
typedef struct PTreapNodeVersion PTreapNodeVersion;
typedef struct PTreapNodeData PTreapNodeData;
typedef struct PTreapNode PTreapNode;

#define g_return_val_if_fail(expr,val) if(!(expr)) return (val);
#define g_return_if_fail(expr) if(!(expr)) return;

#define GPOINTER_TO_UINT(p) ((unsigned int) (unsigned long) (p))
#define GUINT_TO_POINTER(u) ((void*) (unsigned long) (u))

/* Persistent balanced binary trees
 */
PTreap* p_treap_new();
PTreap* p_treap_ref(PTreap *tree);
void p_treap_unref(PTreap *tree);
void p_treap_destroy(PTreap *tree);
unsigned int p_treap_current_version(PTreap *tree);
unsigned int p_treap_next_version(PTreap *tree);
void p_treap_delete_versions(PTreap *tree, unsigned int version);
void p_treap_insert(PTreap *tree, unsigned long key, void* value);
void p_treap_replace(PTreap *tree, unsigned long key, void* value);
bool p_treap_remove(PTreap *tree, const unsigned long key);
bool p_treap_steal(PTreap *tree, const unsigned long key);
void* p_treap_lookup(PTreap *tree, const unsigned long key);
bool p_treap_lookup_extended(PTreap *tree, const void* lookup_key,
                            void* *orig_key, void* *value);
void* p_treap_lookup_related(PTreap *tree, const unsigned long key,
                            PTreapSearchType search_type);
void* p_treap_lookup_v(PTreap *tree, unsigned int version,
                      const unsigned long key);
bool p_treap_lookup_extended_v(PTreap *tree, unsigned int version,
                              const unsigned long lookup_key,
                              unsigned long *orig_key, void* *value);
void* p_treap_lookup_related_v(PTreap *tree, unsigned int version,
                              const unsigned long key,
                              PTreapSearchType search_type);
void p_treap_foreach(PTreap *tree, GTraverseFunc func, void* user_data);
void p_treap_foreach_v(PTreap *tree, unsigned int version, GTraverseFunc func,
                      void* user_data);

#ifndef G_DISABLE_DEPRECATED
void p_treap_traverse(PTreap *tree, GTraverseFunc traverse_func,
                     GTraverseType traverse_type, void* user_data);
#endif /* G_DISABLE_DEPRECATED */

void* p_treap_search(PTreap *tree, const void* user_data);
void* p_treap_search_related(PTreap *tree, const void* user_data,
                            PTreapSearchType search_type);
void* p_treap_search_v(PTreap *tree, unsigned int version, const void* user_data);
void* p_treap_search_related_v(PTreap *tree, unsigned int version,
                              const void* user_data,
                              PTreapSearchType search_type);
int p_treap_height(PTreap *tree);
int p_treap_height_v(PTreap *tree, unsigned int version);
int p_treap_nnodes(PTreap *tree);

unsigned int p_treap_rotations();

#endif /* __P_TREE_H__ */
