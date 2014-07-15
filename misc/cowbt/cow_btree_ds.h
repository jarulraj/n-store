#ifndef COW_BTREE_DS_H_
#define COW_BTREE_DS_H_

/*
 * Singly-linked List definitions.
 */
#define SLIST_HEAD(name, type)            \
struct name {               \
  struct type *slh_first; /* first element */     \
}

#define SLIST_HEAD_INITIALIZER(head)          \
  { NULL }

#define SLIST_ENTRY(type)           \
struct {                \
  struct type *sle_next;  /* next element */      \
}

/*
 * Singly-linked List access methods.
 */
#define SLIST_FIRST(head) ((head)->slh_first)
#define SLIST_END(head)   NULL
#define SLIST_EMPTY(head) (SLIST_FIRST(head) == SLIST_END(head))
#define SLIST_NEXT(elm, field)  ((elm)->field.sle_next)

#define SLIST_FOREACH(var, head, field)         \
  for((var) = SLIST_FIRST(head);          \
      (var) != SLIST_END(head);         \
      (var) = SLIST_NEXT(var, field))

#define SLIST_FOREACH_PREVPTR(var, varp, head, field)     \
  for ((varp) = &SLIST_FIRST((head));       \
      ((var) = *(varp)) != SLIST_END(head);     \
      (varp) = &SLIST_NEXT((var), field))

/*
 * Singly-linked List functions.
 */
#define SLIST_INIT(head) {            \
  SLIST_FIRST(head) = SLIST_END(head);        \
}

#define SLIST_INSERT_AFTER(slistelm, elm, field) do {     \
  (elm)->field.sle_next = (slistelm)->field.sle_next;   \
  (slistelm)->field.sle_next = (elm);       \
} while (0)

#define SLIST_INSERT_HEAD(head, elm, field) do {      \
  (elm)->field.sle_next = (head)->slh_first;      \
  (head)->slh_first = (elm);          \
} while (0)

#define SLIST_REMOVE_NEXT(head, elm, field) do {      \
  (elm)->field.sle_next = (elm)->field.sle_next->field.sle_next;  \
} while (0)

#define SLIST_REMOVE_HEAD(head, field) do {       \
  (head)->slh_first = (head)->slh_first->field.sle_next;    \
} while (0)

#define SLIST_REMOVE(head, elm, type, field) do {     \
  if ((head)->slh_first == (elm)) {       \
    SLIST_REMOVE_HEAD((head), field);     \
  } else {              \
    struct type *curelm = (head)->slh_first;    \
                  \
    while (curelm->field.sle_next != (elm))     \
      curelm = curelm->field.sle_next;    \
    curelm->field.sle_next =        \
        curelm->field.sle_next->field.sle_next;   \
  }               \
} while (0)

/*
 * Simple queue definitions.
 */
#define SIMPLEQ_HEAD(name, type)          \
struct name {               \
  struct type *sqh_first; /* first element */     \
  struct type **sqh_last; /* addr of last next element */   \
}

#define SIMPLEQ_HEAD_INITIALIZER(head)          \
  { NULL, &(head).sqh_first }

#define SIMPLEQ_ENTRY(type)           \
struct {                \
  struct type *sqe_next;  /* next element */      \
}

/*
 * Simple queue access methods.
 */
#define SIMPLEQ_FIRST(head)     ((head)->sqh_first)
#define SIMPLEQ_END(head)     NULL
#define SIMPLEQ_EMPTY(head)     (SIMPLEQ_FIRST(head) == SIMPLEQ_END(head))
#define SIMPLEQ_NEXT(elm, field)    ((elm)->field.sqe_next)

#define SIMPLEQ_FOREACH(var, head, field)       \
  for((var) = SIMPLEQ_FIRST(head);        \
      (var) != SIMPLEQ_END(head);         \
      (var) = SIMPLEQ_NEXT(var, field))

/*
 * Simple queue functions.
 */
#define SIMPLEQ_INIT(head) do {           \
  (head)->sqh_first = NULL;         \
  (head)->sqh_last = &(head)->sqh_first;        \
} while (0)

#define SIMPLEQ_INSERT_HEAD(head, elm, field) do {      \
  if (((elm)->field.sqe_next = (head)->sqh_first) == NULL)  \
    (head)->sqh_last = &(elm)->field.sqe_next;    \
  (head)->sqh_first = (elm);          \
} while (0)

#define SIMPLEQ_INSERT_TAIL(head, elm, field) do {      \
  (elm)->field.sqe_next = NULL;         \
  *(head)->sqh_last = (elm);          \
  (head)->sqh_last = &(elm)->field.sqe_next;      \
} while (0)

#define SIMPLEQ_INSERT_AFTER(head, listelm, elm, field) do {    \
  if (((elm)->field.sqe_next = (listelm)->field.sqe_next) == NULL)\
    (head)->sqh_last = &(elm)->field.sqe_next;    \
  (listelm)->field.sqe_next = (elm);        \
} while (0)

#define SIMPLEQ_REMOVE_HEAD(head, field) do {     \
  if (((head)->sqh_first = (head)->sqh_first->field.sqe_next) == NULL) \
    (head)->sqh_last = &(head)->sqh_first;      \
} while (0)

/*
 * Tail queue definitions.
 */
#define TAILQ_HEAD(name, type)            \
struct name {               \
  struct type *tqh_first; /* first element */     \
  struct type **tqh_last; /* addr of last next element */   \
}

#define TAILQ_HEAD_INITIALIZER(head)          \
  { NULL, &(head).tqh_first }

#define TAILQ_ENTRY(type)           \
struct {                \
  struct type *tqe_next;  /* next element */      \
  struct type **tqe_prev; /* address of previous next element */  \
}

/*
 * tail queue access methods
 */
#define TAILQ_FIRST(head)   ((head)->tqh_first)
#define TAILQ_END(head)     NULL
#define TAILQ_NEXT(elm, field)    ((elm)->field.tqe_next)
#define TAILQ_LAST(head, headname)          \
  (*(((struct headname *)((head)->tqh_last))->tqh_last))
/* XXX */
#define TAILQ_PREV(elm, headname, field)        \
  (*(((struct headname *)((elm)->field.tqe_prev))->tqh_last))
#define TAILQ_EMPTY(head)           \
  (TAILQ_FIRST(head) == TAILQ_END(head))

#define TAILQ_FOREACH(var, head, field)         \
  for((var) = TAILQ_FIRST(head);          \
      (var) != TAILQ_END(head);         \
      (var) = TAILQ_NEXT(var, field))

#define TAILQ_FOREACH_REVERSE(var, head, headname, field)   \
  for((var) = TAILQ_LAST(head, headname);       \
      (var) != TAILQ_END(head);         \
      (var) = TAILQ_PREV(var, headname, field))

/*
 * Tail queue functions.
 */
#define TAILQ_INIT(head) do {           \
  (head)->tqh_first = NULL;         \
  (head)->tqh_last = &(head)->tqh_first;        \
} while (0)

#define TAILQ_INSERT_HEAD(head, elm, field) do {      \
  if (((elm)->field.tqe_next = (head)->tqh_first) != NULL)  \
    (head)->tqh_first->field.tqe_prev =     \
        &(elm)->field.tqe_next;       \
  else                \
    (head)->tqh_last = &(elm)->field.tqe_next;    \
  (head)->tqh_first = (elm);          \
  (elm)->field.tqe_prev = &(head)->tqh_first;     \
} while (0)

#define TAILQ_INSERT_TAIL(head, elm, field) do {      \
  (elm)->field.tqe_next = NULL;         \
  (elm)->field.tqe_prev = (head)->tqh_last;     \
  *(head)->tqh_last = (elm);          \
  (head)->tqh_last = &(elm)->field.tqe_next;      \
} while (0)

#define TAILQ_INSERT_AFTER(head, listelm, elm, field) do {    \
  if (((elm)->field.tqe_next = (listelm)->field.tqe_next) != NULL)\
    (elm)->field.tqe_next->field.tqe_prev =     \
        &(elm)->field.tqe_next;       \
  else                \
    (head)->tqh_last = &(elm)->field.tqe_next;    \
  (listelm)->field.tqe_next = (elm);        \
  (elm)->field.tqe_prev = &(listelm)->field.tqe_next;   \
} while (0)

#define TAILQ_INSERT_BEFORE(listelm, elm, field) do {     \
  (elm)->field.tqe_prev = (listelm)->field.tqe_prev;    \
  (elm)->field.tqe_next = (listelm);        \
  *(listelm)->field.tqe_prev = (elm);       \
  (listelm)->field.tqe_prev = &(elm)->field.tqe_next;   \
} while (0)

#define TAILQ_REMOVE(head, elm, field) do {       \
  if (((elm)->field.tqe_next) != NULL)        \
    (elm)->field.tqe_next->field.tqe_prev =     \
        (elm)->field.tqe_prev;        \
  else                \
    (head)->tqh_last = (elm)->field.tqe_prev;   \
  *(elm)->field.tqe_prev = (elm)->field.tqe_next;     \
} while (0)

#define TAILQ_REPLACE(head, elm, elm2, field) do {      \
  if (((elm2)->field.tqe_next = (elm)->field.tqe_next) != NULL) \
    (elm2)->field.tqe_next->field.tqe_prev =    \
        &(elm2)->field.tqe_next;        \
  else                \
    (head)->tqh_last = &(elm2)->field.tqe_next;   \
  (elm2)->field.tqe_prev = (elm)->field.tqe_prev;     \
  *(elm2)->field.tqe_prev = (elm2);       \
} while (0)

// TREE

/* Macros that define a red-black tree */
#define RB_HEAD(name, type)           \
 struct name {               \
   struct type *rbh_root; /* root of the tree */     \
 }

#define RB_INITIALIZER(root)            \
   { NULL }

#define RB_INIT(root) do {            \
   (root)->rbh_root = NULL;          \
 } while (0)

#define RB_BLACK  0
#define RB_RED    1
#define RB_ENTRY(type)              \
 struct {                \
   struct type *rbe_left;    /* left element */    \
   struct type *rbe_right;   /* right element */   \
   struct type *rbe_parent;  /* parent element */    \
   int rbe_color;      /* node color */    \
 }

#define RB_LEFT(elm, field)   (elm)->field.rbe_left
#define RB_RIGHT(elm, field)    (elm)->field.rbe_right
#define RB_PARENT(elm, field)   (elm)->field.rbe_parent
#define RB_COLOR(elm, field)    (elm)->field.rbe_color
#define RB_ROOT(head)     (head)->rbh_root
#define RB_EMPTY(head)      (RB_ROOT(head) == NULL)

#define RB_SET(elm, parent, field) do {         \
   RB_PARENT(elm, field) = parent;         \
   RB_LEFT(elm, field) = RB_RIGHT(elm, field) = NULL;    \
   RB_COLOR(elm, field) = RB_RED;          \
 } while (0)

#define RB_SET_BLACKRED(black, red, field) do {       \
   RB_COLOR(black, field) = RB_BLACK;        \
   RB_COLOR(red, field) = RB_RED;          \
 } while (0)

#ifndef RB_AUGMENT
#define RB_AUGMENT(x) do {} while (0)
#endif

#define RB_ROTATE_LEFT(head, elm, tmp, field) do {      \
   (tmp) = RB_RIGHT(elm, field);         \
   if ((RB_RIGHT(elm, field) = RB_LEFT(tmp, field))) {   \
     RB_PARENT(RB_LEFT(tmp, field), field) = (elm);    \
   }               \
   RB_AUGMENT(elm);            \
   if ((RB_PARENT(tmp, field) = RB_PARENT(elm, field))) {    \
     if ((elm) == RB_LEFT(RB_PARENT(elm, field), field)) \
       RB_LEFT(RB_PARENT(elm, field), field) = (tmp);  \
     else              \
       RB_RIGHT(RB_PARENT(elm, field), field) = (tmp); \
   } else                \
     (head)->rbh_root = (tmp);       \
   RB_LEFT(tmp, field) = (elm);          \
   RB_PARENT(elm, field) = (tmp);          \
   RB_AUGMENT(tmp);            \
   if ((RB_PARENT(tmp, field)))          \
     RB_AUGMENT(RB_PARENT(tmp, field));      \
 } while (0)

#define RB_ROTATE_RIGHT(head, elm, tmp, field) do {     \
   (tmp) = RB_LEFT(elm, field);          \
   if ((RB_LEFT(elm, field) = RB_RIGHT(tmp, field))) {   \
     RB_PARENT(RB_RIGHT(tmp, field), field) = (elm);   \
   }               \
   RB_AUGMENT(elm);            \
   if ((RB_PARENT(tmp, field) = RB_PARENT(elm, field))) {    \
     if ((elm) == RB_LEFT(RB_PARENT(elm, field), field)) \
       RB_LEFT(RB_PARENT(elm, field), field) = (tmp);  \
     else              \
       RB_RIGHT(RB_PARENT(elm, field), field) = (tmp); \
   } else                \
     (head)->rbh_root = (tmp);       \
   RB_RIGHT(tmp, field) = (elm);         \
   RB_PARENT(elm, field) = (tmp);          \
   RB_AUGMENT(tmp);            \
   if ((RB_PARENT(tmp, field)))          \
     RB_AUGMENT(RB_PARENT(tmp, field));      \
 } while (0)

/* Generates prototypes and inline functions */
#define RB_PROTOTYPE(name, type, field, cmp)        \
   RB_PROTOTYPE_INTERNAL(name, type, field, cmp,)
#define RB_PROTOTYPE_STATIC(name, type, field, cmp)     \
   RB_PROTOTYPE_INTERNAL(name, type, field, cmp, __attribute__((__unused__)) static)
#define RB_PROTOTYPE_INTERNAL(name, type, field, cmp, attr)   \
 attr void name##_RB_INSERT_COLOR(struct name *, struct type *);   \
 attr void name##_RB_REMOVE_COLOR(struct name *, struct type *, struct type *);\
 attr struct type *name##_RB_REMOVE(struct name *, struct type *); \
 attr struct type *name##_RB_INSERT(struct name *, struct type *); \
 attr struct type *name##_RB_FIND(struct name *, struct type *);   \
 attr struct type *name##_RB_NFIND(struct name *, struct type *);  \
 attr struct type *name##_RB_NEXT(struct type *);      \
 attr struct type *name##_RB_PREV(struct type *);      \
 attr struct type *name##_RB_MINMAX(struct name *, int);     \
                   \

/* Main rb operation.
 * Moves node close to the key of elm to top
 */
#define RB_GENERATE(name, type, field, cmp)       \
   RB_GENERATE_INTERNAL(name, type, field, cmp,)
#define RB_GENERATE_STATIC(name, type, field, cmp)      \
   RB_GENERATE_INTERNAL(name, type, field, cmp, __attribute__((__unused__)) static)
#define RB_GENERATE_INTERNAL(name, type, field, cmp, attr)    \
 attr void               \
 name##_RB_INSERT_COLOR(struct name *head, struct type *elm)   \
 {                 \
   struct type *parent, *gparent, *tmp;        \
   while ((parent = RB_PARENT(elm, field)) &&      \
       RB_COLOR(parent, field) == RB_RED) {      \
     gparent = RB_PARENT(parent, field);     \
     if (parent == RB_LEFT(gparent, field)) {    \
       tmp = RB_RIGHT(gparent, field);     \
       if (tmp && RB_COLOR(tmp, field) == RB_RED) {  \
         RB_COLOR(tmp, field) = RB_BLACK;  \
         RB_SET_BLACKRED(parent, gparent, field);\
         elm = gparent;        \
         continue;       \
       }           \
       if (RB_RIGHT(parent, field) == elm) {   \
         RB_ROTATE_LEFT(head, parent, tmp, field);\
         tmp = parent;       \
         parent = elm;       \
         elm = tmp;        \
       }           \
       RB_SET_BLACKRED(parent, gparent, field);  \
       RB_ROTATE_RIGHT(head, gparent, tmp, field); \
     } else {            \
       tmp = RB_LEFT(gparent, field);      \
       if (tmp && RB_COLOR(tmp, field) == RB_RED) {  \
         RB_COLOR(tmp, field) = RB_BLACK;  \
         RB_SET_BLACKRED(parent, gparent, field);\
         elm = gparent;        \
         continue;       \
       }           \
       if (RB_LEFT(parent, field) == elm) {    \
         RB_ROTATE_RIGHT(head, parent, tmp, field);\
         tmp = parent;       \
         parent = elm;       \
         elm = tmp;        \
       }           \
       RB_SET_BLACKRED(parent, gparent, field);  \
       RB_ROTATE_LEFT(head, gparent, tmp, field);  \
     }             \
   }               \
   RB_COLOR(head->rbh_root, field) = RB_BLACK;     \
 }                 \
                   \
 attr void               \
 name##_RB_REMOVE_COLOR(struct name *head, struct type *parent, struct type *elm) \
 {                 \
   struct type *tmp;           \
   while ((elm == NULL || RB_COLOR(elm, field) == RB_BLACK) && \
       elm != RB_ROOT(head)) {         \
     if (RB_LEFT(parent, field) == elm) {      \
       tmp = RB_RIGHT(parent, field);      \
       if (RB_COLOR(tmp, field) == RB_RED) {   \
         RB_SET_BLACKRED(tmp, parent, field);  \
         RB_ROTATE_LEFT(head, parent, tmp, field);\
         tmp = RB_RIGHT(parent, field);    \
       }           \
       if ((RB_LEFT(tmp, field) == NULL ||   \
           RB_COLOR(RB_LEFT(tmp, field), field) == RB_BLACK) &&\
           (RB_RIGHT(tmp, field) == NULL ||    \
           RB_COLOR(RB_RIGHT(tmp, field), field) == RB_BLACK)) {\
         RB_COLOR(tmp, field) = RB_RED;    \
         elm = parent;       \
         parent = RB_PARENT(elm, field);   \
       } else {          \
         if (RB_RIGHT(tmp, field) == NULL || \
             RB_COLOR(RB_RIGHT(tmp, field), field) == RB_BLACK) {\
           struct type *oleft;   \
           if ((oleft = RB_LEFT(tmp, field)))\
             RB_COLOR(oleft, field) = RB_BLACK;\
           RB_COLOR(tmp, field) = RB_RED;  \
           RB_ROTATE_RIGHT(head, tmp, oleft, field);\
           tmp = RB_RIGHT(parent, field);  \
         }         \
         RB_COLOR(tmp, field) = RB_COLOR(parent, field);\
         RB_COLOR(parent, field) = RB_BLACK; \
         if (RB_RIGHT(tmp, field))   \
           RB_COLOR(RB_RIGHT(tmp, field), field) = RB_BLACK;\
         RB_ROTATE_LEFT(head, parent, tmp, field);\
         elm = RB_ROOT(head);      \
         break;          \
       }           \
     } else {            \
       tmp = RB_LEFT(parent, field);     \
       if (RB_COLOR(tmp, field) == RB_RED) {   \
         RB_SET_BLACKRED(tmp, parent, field);  \
         RB_ROTATE_RIGHT(head, parent, tmp, field);\
         tmp = RB_LEFT(parent, field);   \
       }           \
       if ((RB_LEFT(tmp, field) == NULL ||   \
           RB_COLOR(RB_LEFT(tmp, field), field) == RB_BLACK) &&\
           (RB_RIGHT(tmp, field) == NULL ||    \
           RB_COLOR(RB_RIGHT(tmp, field), field) == RB_BLACK)) {\
         RB_COLOR(tmp, field) = RB_RED;    \
         elm = parent;       \
         parent = RB_PARENT(elm, field);   \
       } else {          \
         if (RB_LEFT(tmp, field) == NULL ||  \
             RB_COLOR(RB_LEFT(tmp, field), field) == RB_BLACK) {\
           struct type *oright;    \
           if ((oright = RB_RIGHT(tmp, field)))\
             RB_COLOR(oright, field) = RB_BLACK;\
           RB_COLOR(tmp, field) = RB_RED;  \
           RB_ROTATE_LEFT(head, tmp, oright, field);\
           tmp = RB_LEFT(parent, field); \
         }         \
         RB_COLOR(tmp, field) = RB_COLOR(parent, field);\
         RB_COLOR(parent, field) = RB_BLACK; \
         if (RB_LEFT(tmp, field))    \
           RB_COLOR(RB_LEFT(tmp, field), field) = RB_BLACK;\
         RB_ROTATE_RIGHT(head, parent, tmp, field);\
         elm = RB_ROOT(head);      \
         break;          \
       }           \
     }             \
   }               \
   if (elm)              \
     RB_COLOR(elm, field) = RB_BLACK;      \
 }                 \
                   \
 attr struct type *              \
 name##_RB_REMOVE(struct name *head, struct type *elm)     \
 {                 \
   struct type *child, *parent, *old = elm;      \
   int color;              \
   if (RB_LEFT(elm, field) == NULL)        \
     child = RB_RIGHT(elm, field);       \
   else if (RB_RIGHT(elm, field) == NULL)        \
     child = RB_LEFT(elm, field);        \
   else {                \
     struct type *left;          \
     elm = RB_RIGHT(elm, field);       \
     while ((left = RB_LEFT(elm, field)))      \
       elm = left;         \
     child = RB_RIGHT(elm, field);       \
     parent = RB_PARENT(elm, field);       \
     color = RB_COLOR(elm, field);       \
     if (child)            \
       RB_PARENT(child, field) = parent;   \
     if (parent) {           \
       if (RB_LEFT(parent, field) == elm)    \
         RB_LEFT(parent, field) = child;   \
       else            \
         RB_RIGHT(parent, field) = child;  \
       RB_AUGMENT(parent);       \
     } else              \
       RB_ROOT(head) = child;        \
     if (RB_PARENT(elm, field) == old)     \
       parent = elm;         \
     (elm)->field = (old)->field;        \
     if (RB_PARENT(old, field)) {        \
       if (RB_LEFT(RB_PARENT(old, field), field) == old)\
         RB_LEFT(RB_PARENT(old, field), field) = elm;\
       else            \
         RB_RIGHT(RB_PARENT(old, field), field) = elm;\
       RB_AUGMENT(RB_PARENT(old, field));    \
     } else              \
       RB_ROOT(head) = elm;        \
     RB_PARENT(RB_LEFT(old, field), field) = elm;    \
     if (RB_RIGHT(old, field))       \
       RB_PARENT(RB_RIGHT(old, field), field) = elm; \
     if (parent) {           \
       left = parent;          \
       do {            \
         RB_AUGMENT(left);     \
       } while ((left = RB_PARENT(left, field)));  \
     }             \
     goto color;           \
   }               \
   parent = RB_PARENT(elm, field);         \
   color = RB_COLOR(elm, field);         \
   if (child)              \
     RB_PARENT(child, field) = parent;     \
   if (parent) {             \
     if (RB_LEFT(parent, field) == elm)      \
       RB_LEFT(parent, field) = child;     \
     else              \
       RB_RIGHT(parent, field) = child;    \
     RB_AUGMENT(parent);         \
   } else                \
     RB_ROOT(head) = child;          \
 color:                  \
   if (color == RB_BLACK)            \
     name##_RB_REMOVE_COLOR(head, parent, child);    \
   return (old);             \
 }                 \
                   \
 /* Inserts a node into the RB tree */         \
 attr struct type *              \
 name##_RB_INSERT(struct name *head, struct type *elm)     \
 {                 \
   struct type *tmp;           \
   struct type *parent = NULL;         \
   int comp = 0;             \
   tmp = RB_ROOT(head);            \
   while (tmp) {             \
     parent = tmp;           \
     comp = (cmp)(elm, parent);        \
     if (comp < 0)           \
       tmp = RB_LEFT(tmp, field);      \
     else if (comp > 0)          \
       tmp = RB_RIGHT(tmp, field);     \
     else              \
       return (tmp);         \
   }               \
   RB_SET(elm, parent, field);         \
   if (parent != NULL) {           \
     if (comp < 0)           \
       RB_LEFT(parent, field) = elm;     \
     else              \
       RB_RIGHT(parent, field) = elm;      \
     RB_AUGMENT(parent);         \
   } else                \
     RB_ROOT(head) = elm;          \
   name##_RB_INSERT_COLOR(head, elm);        \
   return (NULL);              \
 }                 \
                   \
 /* Finds the node with the same key as elm */       \
 attr struct type *              \
 name##_RB_FIND(struct name *head, struct type *elm)     \
 {                 \
   struct type *tmp = RB_ROOT(head);       \
   int comp;             \
   while (tmp) {             \
     comp = cmp(elm, tmp);         \
     if (comp < 0)           \
       tmp = RB_LEFT(tmp, field);      \
     else if (comp > 0)          \
       tmp = RB_RIGHT(tmp, field);     \
     else              \
       return (tmp);         \
   }               \
   return (NULL);              \
 }                 \
                   \
 /* Finds the first node greater than or equal to the search key */  \
 attr struct type *              \
 name##_RB_NFIND(struct name *head, struct type *elm)      \
 {                 \
   struct type *tmp = RB_ROOT(head);       \
   struct type *res = NULL;          \
   int comp;             \
   while (tmp) {             \
     comp = cmp(elm, tmp);         \
     if (comp < 0) {           \
       res = tmp;          \
       tmp = RB_LEFT(tmp, field);      \
     }             \
     else if (comp > 0)          \
       tmp = RB_RIGHT(tmp, field);     \
     else              \
       return (tmp);         \
   }               \
   return (res);             \
 }                 \
                   \
 /* ARGSUSED */                \
 attr struct type *              \
 name##_RB_NEXT(struct type *elm)          \
 {                 \
   if (RB_RIGHT(elm, field)) {         \
     elm = RB_RIGHT(elm, field);       \
     while (RB_LEFT(elm, field))       \
       elm = RB_LEFT(elm, field);      \
   } else {              \
     if (RB_PARENT(elm, field) &&        \
         (elm == RB_LEFT(RB_PARENT(elm, field), field))) \
       elm = RB_PARENT(elm, field);      \
     else {              \
       while (RB_PARENT(elm, field) &&     \
           (elm == RB_RIGHT(RB_PARENT(elm, field), field)))\
         elm = RB_PARENT(elm, field);    \
       elm = RB_PARENT(elm, field);      \
     }             \
   }               \
   return (elm);             \
 }                 \
                   \
 /* ARGSUSED */                \
 attr struct type *              \
 name##_RB_PREV(struct type *elm)          \
 {                 \
   if (RB_LEFT(elm, field)) {          \
     elm = RB_LEFT(elm, field);        \
     while (RB_RIGHT(elm, field))        \
       elm = RB_RIGHT(elm, field);     \
   } else {              \
     if (RB_PARENT(elm, field) &&        \
         (elm == RB_RIGHT(RB_PARENT(elm, field), field)))  \
       elm = RB_PARENT(elm, field);      \
     else {              \
       while (RB_PARENT(elm, field) &&     \
           (elm == RB_LEFT(RB_PARENT(elm, field), field)))\
         elm = RB_PARENT(elm, field);    \
       elm = RB_PARENT(elm, field);      \
     }             \
   }               \
   return (elm);             \
 }                 \
                   \
 attr struct type *              \
 name##_RB_MINMAX(struct name *head, int val)        \
 {                 \
   struct type *tmp = RB_ROOT(head);       \
   struct type *parent = NULL;         \
   while (tmp) {             \
     parent = tmp;           \
     if (val < 0)            \
       tmp = RB_LEFT(tmp, field);      \
     else              \
       tmp = RB_RIGHT(tmp, field);     \
   }               \
   return (parent);            \
 }

#define RB_NEGINF -1
#define RB_INF  1

#define RB_INSERT(name, x, y) name##_RB_INSERT(x, y)
#define RB_REMOVE(name, x, y) name##_RB_REMOVE(x, y)
#define RB_FIND(name, x, y) name##_RB_FIND(x, y)
#define RB_NFIND(name, x, y)  name##_RB_NFIND(x, y)
#define RB_NEXT(name, x, y) name##_RB_NEXT(y)
#define RB_PREV(name, x, y) name##_RB_PREV(y)
#define RB_MIN(name, x)   name##_RB_MINMAX(x, RB_NEGINF)
#define RB_MAX(name, x)   name##_RB_MINMAX(x, RB_INF)

#define RB_FOREACH(x, name, head)         \
   for ((x) = RB_MIN(name, head);          \
        (x) != NULL;           \
        (x) = name##_RB_NEXT(x))

#define RB_FOREACH_REVERSE(x, name, head)       \
   for ((x) = RB_MAX(name, head);          \
        (x) != NULL;           \
        (x) = name##_RB_PREV(x))

#endif /* COW_BTREE_DS_H_ */
