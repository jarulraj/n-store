/* ADDS - Open Source Advanced Data Structures Library
 * Copyright (C) 2009 Dana Jansens
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2 as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	 See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 02111-1307, USA.
 */

#ifndef __P_TREE_H__
#define __P_TREE_H__

#include <stdio.h>
#include <stdlib.h>
#include <glib.h>

G_BEGIN_DECLS

/* Tree Search Types */
typedef enum
{
  P_TREE_SEARCH_EXACT,
  P_TREE_SEARCH_SUCCESSOR,
  P_TREE_SEARCH_PREDECESSOR
} PTreeSearchType;

typedef struct _PTree  PTree;

/* Persistent balanced binary trees
 */
PTree*   p_tree_new               (GCompareFunc      key_compare_func);
PTree*   p_tree_new_with_data     (GCompareDataFunc  key_compare_func,
                                   gpointer          key_compare_data);
PTree*   p_tree_new_full          (GCompareDataFunc  key_compare_func,
                                   gpointer          key_compare_data,
                                   GDestroyNotify    key_destroy_func,
                                   GDestroyNotify    value_destroy_func);
PTree*   p_tree_ref               (PTree            *tree);
void     p_tree_unref             (PTree            *tree);
void     p_tree_destroy           (PTree            *tree);
guint    p_tree_current_version   (PTree            *tree);
guint    p_tree_next_version      (PTree            *tree);
void     p_tree_delete_versions   (PTree            *tree,
                                   guint             version);
void     p_tree_insert            (PTree            *tree,
                                   gpointer          key,
                                   gpointer          value);
void     p_tree_replace           (PTree            *tree,
                                   gpointer          key,
                                   gpointer          value);
gboolean p_tree_remove            (PTree            *tree,
                                   gconstpointer     key);
gboolean p_tree_steal             (PTree            *tree,
                                   gconstpointer     key);
gpointer p_tree_lookup            (PTree            *tree,
                                   gconstpointer     key);
gboolean p_tree_lookup_extended   (PTree            *tree,
                                   gconstpointer     lookup_key,
                                   gpointer         *orig_key,
                                   gpointer         *value);
gpointer p_tree_lookup_related    (PTree            *tree,
                                   gconstpointer     key,
                                   PTreeSearchType   search_type);
gpointer p_tree_lookup_v          (PTree            *tree,
                                   guint             version,
                                   gconstpointer     key);
gboolean p_tree_lookup_extended_v (PTree            *tree,
                                   guint             version,
                                   gconstpointer     lookup_key,
                                   gpointer         *orig_key,
                                   gpointer         *value);
gpointer p_tree_lookup_related_v  (PTree            *tree,
                                   guint             version,
                                   gconstpointer     key,
                                   PTreeSearchType   search_type);
void     p_tree_foreach           (PTree            *tree,
                                   GTraverseFunc     func,
                                   gpointer	     user_data);
void     p_tree_foreach_v         (PTree            *tree,
                                   guint             version,
                                   GTraverseFunc     func,
                                   gpointer	     user_data);

#ifndef G_DISABLE_DEPRECATED
void     p_tree_traverse          (PTree            *tree,
                                   GTraverseFunc     traverse_func,
                                   GTraverseType     traverse_type,
                                   gpointer          user_data);
#endif /* G_DISABLE_DEPRECATED */

gpointer p_tree_search          (PTree              *tree,
                                 GCompareFunc        search_func,
                                 gconstpointer       user_data);
gpointer p_tree_search_related  (PTree              *tree,
                                 GCompareFunc        search_func,
                                 gconstpointer       user_data,
                                 PTreeSearchType     search_type);
gpointer p_tree_search_v        (PTree              *tree,
                                 guint               version,
                                 GCompareFunc        search_func,
                                 gconstpointer       user_data);
gpointer p_tree_search_related_v(PTree              *tree,
                                 guint               version,
                                 GCompareFunc        search_func,
                                 gconstpointer       user_data,
                                 PTreeSearchType     search_type);
gint     p_tree_height          (PTree              *tree);
gint     p_tree_height_v        (PTree              *tree,
                                 guint               version);
gint     p_tree_nnodes          (PTree              *tree);

G_END_DECLS

#endif /* __P_TREE_H__ */
