#ifndef __P_TREE_H__
#define __P_TREE_H__

#include <atomic>

using namespace std;

/* Tree Search Types */
typedef enum {
  P_TREE_SEARCH_EXACT,
  P_TREE_SEARCH_SUCCESSOR,
  P_TREE_SEARCH_PREDECESSOR
} PTreeSearchType;


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

typedef int (*GCompareFunc)(const void* a, const void* b);
typedef int (*GCompareDataFunc)(const void* a, const void* b, void* user_data);
typedef bool (*GNodeTraverseFunc)(GNode *node, void* data);
typedef void (*GDestroyNotify)(void* data);
typedef bool (*GTraverseFunc)(void* key, void* value, void* data);

struct PTreeRootVersion;
struct PTreeNodeVersion;
struct PTreeNodeData;
struct PTreeNode;

struct PTreeRootVersion {
  PTreeNode *root;
  unsigned int version;
};

struct PTree {
  PTreeRootVersion *r; /* versioned root nodes of the tree.  r[0] is
   the highest (latest) version.  then r[1]..r[nr-1] are
   older versions in ascending order.  Use first_v(),
   next_v() and prev_v() to traverse the list. */
  unsigned int nr;
  GCompareDataFunc key_compare;
  GDestroyNotify key_destroy_func;
  GDestroyNotify value_destroy_func;
  void* key_compare_data;
  unsigned int nnodes;
  std::atomic_int ref_count;
  unsigned int version;
};

struct PTreeNodeVersion {
  unsigned int version;
  PTreeNode *left; /* left subtree */
  PTreeNode *right; /* right subtree */
  PTreeNode *parent; /* parent node */
};

struct PTreeNodeData {
  void* key; /* key for this node */
  void* value; /* value stored at this node */
  std::atomic_int ref_count;
  short unsigned int stolen; /* true if the node is stolen instead of removed */
};

struct PTreeNode {
  PTreeNodeData *data; /* the node's permanent data */
  PTreeNodeVersion *v; /* versions of pointers for the node.  v[0] is the
   highest (latest) version.  then v[1]..v[nv-1] are
   older versions in ascending order.  Use first_v(),
   next_v() and prev_v() to traverse the list. */
  unsigned int nv; /* number of versions stored in this node */
};

typedef struct PTreeRootVersion PTreeRootVersion;
typedef struct PTreeNodeVersion PTreeNodeVersion;
typedef struct PTreeNodeData PTreeNodeData;
typedef struct PTreeNode PTreeNode;

#define g_return_val_if_fail(expr,val) if(!(expr)) return (val);
#define g_return_if_fail(expr) if(!expr) return;

#define GPOINTER_TO_UINT(p) ((unsigned int) (unsigned long) (p))
#define GUINT_TO_POINTER(u) ((void*) (unsigned long) (u))

/* Persistent balanced binary trees
 */
PTree* p_tree_new(GCompareFunc key_compare_func);
PTree* p_tree_new_with_data(GCompareDataFunc key_compare_func,
                            void* key_compare_data);
PTree* p_tree_new_full(GCompareDataFunc key_compare_func,
                       void* key_compare_data, GDestroyNotify key_destroy_func,
                       GDestroyNotify value_destroy_func);
PTree* p_tree_ref(PTree *tree);
void p_tree_unref(PTree *tree);
void p_tree_destroy(PTree *tree);
unsigned int p_tree_current_version(PTree *tree);
unsigned int p_tree_next_version(PTree *tree);
void p_tree_delete_versions(PTree *tree, unsigned int version);
void p_tree_insert(PTree *tree, void* key, void* value);
void p_tree_replace(PTree *tree, void* key, void* value);
bool p_tree_remove(PTree *tree, const void* key);
bool p_tree_steal(PTree *tree, const void* key);
void* p_tree_lookup(PTree *tree, const void* key);
bool p_tree_lookup_extended(PTree *tree, const void* lookup_key,
                            void* *orig_key, void* *value);
void* p_tree_lookup_related(PTree *tree, const void* key,
                            PTreeSearchType search_type);
void* p_tree_lookup_v(PTree *tree, unsigned int version, const void* key);
bool p_tree_lookup_extended_v(PTree *tree, unsigned int version,
                              const void* lookup_key, void* *orig_key,
                              void* *value);
void* p_tree_lookup_related_v(PTree *tree, unsigned int version,
                              const void* key, PTreeSearchType search_type);
void p_tree_foreach(PTree *tree, GTraverseFunc func, void* user_data);
void p_tree_foreach_v(PTree *tree, unsigned int version, GTraverseFunc func,
                      void* user_data);

#ifndef G_DISABLE_DEPRECATED
void p_tree_traverse(PTree *tree, GTraverseFunc traverse_func,
                     GTraverseType traverse_type, void* user_data);
#endif /* G_DISABLE_DEPRECATED */

void* p_tree_search(PTree *tree, GCompareFunc search_func,
                    const void* user_data);
void* p_tree_search_related(PTree *tree, GCompareFunc search_func,
                            const void* user_data, PTreeSearchType search_type);
void* p_tree_search_v(PTree *tree, unsigned int version,
                      GCompareFunc search_func, const void* user_data);
void* p_tree_search_related_v(PTree *tree, unsigned int version,
                              GCompareFunc search_func, const void* user_data,
                              PTreeSearchType search_type);
int p_tree_height(PTree *tree);
int p_tree_height_v(PTree *tree, unsigned int version);
int p_tree_nnodes(PTree *tree);

unsigned int  p_tree_rotations ();

#endif /* __P_TREE_H__ */
