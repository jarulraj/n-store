
// HEADERS

#define BP_OK 0
#define BP_EFILE 1
#define BP_EFILEREAD_OOB 2
#define BP_EFILEREAD 3
#define BP_EFILEWRITE 4
#define BP_EFILEFLUSH 5
#define BP_EALLOC 6
#define BP_ESPLITPAGE 7
#define BP_ENOTFOUND 8

/* http://www.concentric.net/~Ttwang/tech/inthash.htm */
inline uint32_t bp__compute_hash(uint32_t key) {
  uint32_t hash = key;
  hash = ~hash + (hash << 15); /* hash = (hash << 15) - hash - 1; */
  hash = hash ^ (hash >> 12);
  hash = hash + (hash << 2);
  hash = hash ^ (hash >> 4);
  hash = hash * 2057; /* hash = (hash + (hash << 3)) + (hash << 11); */
  hash = hash ^ (hash >> 16);
  return hash;
}

inline uint32_t bp__read_uint32be(uint8_t* data) {
  return (data[0] << 24) + (data[1] << 16) + (data[2] << 8) + data[3];
}

// PAGES

#define BP__KV_HEADER_SIZE 12
#define BP__KV_SIZE(kv) BP__KV_HEADER_SIZE + kv.length;

typedef struct bp__page_s bp__page_t;
typedef struct bp__kv_s bp__kv_t;
typedef struct bp__page_search_res_s bp__page_search_res_t;

#define BP_KEY_FIELDS \
uint32_t length;\
char* value;

struct bp__kv_s {
  BP_KEY_FIELDS

  uint32_t offset;
  uint32_t config;

  uint8_t allocated;
};

struct bp__page_s {
  uint8_t is_leaf;

  uint32_t length;
  uint32_t byte_size;

  uint32_t offset;
  uint32_t config;

  void* buff_;

  bp__kv_t keys[1];
};

struct bp__page_search_res_s {
  bp__page_t* child;

  uint32_t index;
  int cmp;
};

#define BP_WRITER_PRIVATE \
FILE* fd;\
uint32_t filesize;\
uint32_t flushed;

typedef struct bp__writer_s bp__writer_t;
typedef int (*bp__writer_cb)(bp__writer_t* w, void* data);

int bp__writer_create(bp__writer_t* w, const char* filename);
int bp__writer_destroy(bp__writer_t* w);

int bp__writer_read(bp__writer_t* w, const uint32_t offset, const uint32_t size,
                    void* data);
int bp__writer_write(bp__writer_t* w, const uint32_t size, const void* data,
                     uint32_t* offset);

int bp__writer_find(bp__writer_t* w, const uint32_t size, void* data,
                    bp__writer_cb seek, bp__writer_cb miss);

struct bp__writer_s {
  BP_WRITER_PRIVATE
};

typedef struct bp_tree_s bp_tree_t;
typedef struct bp_key_s bp_key_t;
typedef struct bp_key_s bp_value_t;
typedef int (*bp_compare_cb)(const bp_key_t* a, const bp_key_t* b);

#define BP_TREE_PRIVATE \
BP_WRITER_PRIVATE \
bp__tree_head_t head;\
bp__page_t* head_page;\
bp_compare_cb compare_cb;

typedef struct bp__tree_head_s bp__tree_head_t;

int bp__tree_read_head(bp__writer_t* w, void* data);
int bp__tree_write_head(bp__writer_t* w, void* data);

struct bp__tree_head_s {
  uint32_t offset;
  uint32_t config;
  uint32_t page_size;
  uint32_t hash;

  uint8_t padding[32 - 16];
};

#include <assert.h>
#include <stdint.h> /* uintx_t */

int bp_open(bp_tree_t* tree, const char* filename);
int bp_close(bp_tree_t* tree);

int bp_get(bp_tree_t* tree, const bp_key_t* key, bp_value_t* value);
int bp_gets(bp_tree_t* tree, const char* key, char** value);

int bp_set(bp_tree_t* tree, const bp_key_t* key, const bp_value_t* value);
int bp_sets(bp_tree_t* tree, const char* key, const char* value);

int bp_remove(bp_tree_t* tree, const bp_key_t* key);
int bp_removes(bp_tree_t* tree, const char* key);

void bp_set_compare_cb(bp_tree_t* tree, bp_compare_cb cb);

struct bp_tree_s {
  BP_TREE_PRIVATE
};

struct bp_key_s {
  BP_KEY_FIELDS
};

// PAGES

#include <stdlib.h> /* malloc, free */
#include <string.h> /* memcpy */

void bp__page_shiftr(bp_tree_t* t, bp__page_t* p, const uint32_t index) {
  uint32_t i;

  if (p->length != 0) {
    for (i = p->length - 1; i >= index; i--) {
      bp__kv_copy(&p->keys[i], &p->keys[i + 1], 0);

      if (i == 0)
        break;
    }
  }
}

void bp__page_shiftl(bp_tree_t* t, bp__page_t* p, const uint32_t index) {
  uint32_t i;
  for (i = index + 1; i < p->length; i++) {
    bp__kv_copy(&p->keys[i], &p->keys[i - 1], 0);
  }
}

int bp__page_create(bp_tree_t* t, const int is_leaf, const uint32_t offset,
                    const uint32_t config, bp__page_t** page) {
  /* Allocate space for page + keys */
  bp__page_t* p = malloc(
      sizeof(*p) + sizeof(p->keys[0]) * (t->head.page_size - 1));
  if (p == NULL)
    return BP_EALLOC;

  p->is_leaf = is_leaf;
  if (is_leaf) {
    p->length = 0;
    p->byte_size = 0;
  } else {
    /* non-leaf pages always have left element */
    p->length = 1;
    p->keys[0].value = NULL;
    p->keys[0].length = 0;
    p->keys[0].offset = 0;
    p->keys[0].config = 0;
    p->byte_size = BP__KV_SIZE(p->keys[0])
    ;
  }

  /* this two fields will be changed on page_write */
  p->offset = offset;
  p->config = config;

  p->buff_ = NULL;

  *page = p;
  return BP_OK;
}

int bp__page_destroy(bp_tree_t* t, bp__page_t* page) {
  /* Free all keys */
  int i = 0;
  for (i = 0; i < page->length; i++) {
    if (page->keys[i].value != NULL && page->keys[i].allocated) {
      free(page->keys[i].value);
    }
  }

  if (page->buff_ != NULL) {
    free(page->buff_);
  }

  /* Free page itself */
  free(page);
  return BP_OK;
}

int bp__page_load(bp_tree_t* t, bp__page_t* page) {
  bp__writer_t* w = (bp__writer_t*) t;

  int ret;

  /* Read page size and leaf flag */
  uint32_t size = page->config & 0x7fffffff;
  page->is_leaf = page->config >> 31;

  /* Read page data */
  char* buff = malloc(size);
  if (buff == NULL)
    return BP_EALLOC;

  ret = bp__writer_read(w, page->offset, size, buff);
  if (ret)
    return ret;

  /* Parse data */
  uint32_t i = 0, o = 0;
  while (o < size) {
    page->keys[i].length = *(uint32_t*) (buff + o);
    page->keys[i].offset = *(uint32_t*) (buff + o + 4);
    page->keys[i].config = *(uint32_t*) (buff + o + 8);
    page->keys[i].value = buff + o + 12;
    page->keys[i].allocated = 0;

    o += 12 + page->keys[i].length;
    i++;
  }
  page->length = i;
  page->byte_size = size;

  if (page->buff_ != NULL) {
    free(page->buff_);
  }
  page->buff_ = buff;

  return BP_OK;
}

int bp__page_save(bp_tree_t* t, bp__page_t* page) {
  bp__writer_t* w = (bp__writer_t*) t;

  /* Allocate space for serialization (header + keys); */
  char* buff = malloc(page->byte_size);
  if (buff == NULL)
    return BP_EALLOC;

  uint32_t i, o = 0;
  for (i = 0; i < page->length; i++) {
    *(uint32_t*) (buff + o) = page->keys[i].length;
    *(uint32_t*) (buff + o + 4) = page->keys[i].offset;
    *(uint32_t*) (buff + o + 8) = page->keys[i].config;

    memcpy(buff + o + 12, page->keys[i].value, page->keys[i].length);

    o += BP__KV_SIZE(page->keys[i])
    ;
  }
  assert(o == page->byte_size);

  int ret = 0;
  ret = bp__writer_write(w, page->byte_size, buff, &page->offset);
  free(buff);

  if (ret)
    return ret;

  page->config = page->byte_size | (page->is_leaf << 31);

  return BP_OK;
}

inline int bp__page_search(bp_tree_t* t, bp__page_t* page, const bp__kv_t* kv,
                           bp__page_search_res_t* result) {
  uint32_t i = page->is_leaf ? 0 : 1;
  int cmp = -1;

  while (i < page->length) {
    /* left key is always lower in non-leaf nodes */
    cmp = t->compare_cb((bp_key_t*) &page->keys[i], (bp_key_t*) kv);

    if (cmp >= 0)
      break;
    i++;
  }

  result->cmp = cmp;

  if (page->is_leaf) {
    result->index = i;
    result->child = NULL;

    return BP_OK;
  } else {
    assert(i > 0);
    i--;

    bp__page_t* child;
    int ret;
    ret = bp__page_create(t, 0, page->keys[i].offset, page->keys[i].config,
                          &child);
    if (ret)
      return ret;

    ret = bp__page_load(t, child);
    if (ret)
      return ret;

    result->index = i;
    result->child = child;

    return BP_OK;
  }
}

int bp__page_get(bp_tree_t* t, bp__page_t* page, const bp__kv_t* kv,
                 bp_value_t* value) {
  bp__page_search_res_t res;
  int ret;
  ret = bp__page_search(t, page, kv, &res);
  if (ret)
    return ret;

  if (res.child == NULL) {
    if (res.cmp != 0)
      return BP_ENOTFOUND;

    value->length = page->keys[res.index].config;
    value->value = malloc(value->length);
    if (value->value == NULL)
      return BP_EALLOC;

    return bp__writer_read((bp__writer_t*) t, page->keys[res.index].offset,
                           page->keys[res.index].config, value->value);
  } else {
    return bp__page_get(t, res.child, kv, value);
  }
}

int bp__page_insert(bp_tree_t* t, bp__page_t* page, const bp__kv_t* kv) {
  bp__page_search_res_t res;
  int ret;
  ret = bp__page_search(t, page, kv, &res);
  if (ret)
    return ret;

  if (res.child == NULL) {
    /* TODO: Save reference to previous value */
    if (res.cmp == 0)
      bp__page_remove_idx(t, page, res.index);

    /* Shift all keys right */
    bp__page_shiftr(t, page, res.index);

    /* Insert key in the middle */
    bp__kv_copy(kv, &page->keys[res.index], 1);
    page->byte_size += BP__KV_SIZE((*kv))
    ;
    page->length++;
  } else {
    /* Insert kv in child page */
    ret = bp__page_insert(t, res.child, kv);

    if (ret && ret != BP_ESPLITPAGE) {
      return ret;
    }

    /* kv was inserted but page is full now */
    if (ret == BP_ESPLITPAGE) {
      ret = bp__page_split(t, page, res.index, res.child);
    } else {
      /* Update offsets in page */
      page->keys[res.index].offset = res.child->offset;
      page->keys[res.index].config = res.child->config;

      /* we don't need child now */
      ret = bp__page_destroy(t, res.child);
    }
    if (ret)
      return ret;
  }

  if (page->length == t->head.page_size) {
    if (page == t->head_page) {
      /* split root */
      bp__page_t* new_root;
      bp__page_create(t, 0, 0, 0, &new_root);

      ret = bp__page_split(t, new_root, 0, page);
      if (ret)
        return ret;

      t->head_page = new_root;
      page = new_root;
    } else {
      /* Notify caller that it should split page */
      return BP_ESPLITPAGE;
    }
  }

  ret = bp__page_save(t, page);
  if (ret)
    return ret;

  return BP_OK;
}

int bp__page_remove_idx(bp_tree_t* t, bp__page_t* page, const uint32_t index) {
  assert(index < page->length);

  /* Free memory allocated for kv and reduce byte_size of page */
  page->byte_size -= BP__KV_SIZE(page->keys[index])
  ;
  if (page->keys[index].allocated) {
    free(page->keys[index].value);
  }

  /* Shift all keys left */
  bp__page_shiftl(t, page, index);

  page->length--;

  return BP_OK;
}

int bp__page_remove(bp_tree_t* t, bp__page_t* page, bp__kv_t* kv) {
  uint32_t i;
  for (i = 0; i < page->length; i++) {
    int cmp = t->compare_cb((bp_key_t*) &page->keys[i], (bp_key_t*) kv);

    if (cmp >= 0) {
      if (cmp == 0) {
        return bp__page_remove_idx(t, page, i);
      }
      break;
    }
  }

  return BP_OK;
}

int bp__page_split(bp_tree_t* t, bp__page_t* parent, const uint32_t index,
                   bp__page_t* child) {
  bp__page_t* left;
  bp__page_t* right;

  bp__page_create(t, child->is_leaf, 0, 0, &left);
  bp__page_create(t, child->is_leaf, 0, 0, &right);

  uint32_t middle = t->head.page_size >> 1;
  bp__kv_t middle_key;
  bp__kv_copy(&child->keys[middle], &middle_key, 1);

  uint32_t i;
  for (i = 0; i < middle; i++) {
    bp__kv_copy(&child->keys[i], &left->keys[left->length++], 1);
    left->byte_size += BP__KV_SIZE(child->keys[i])
    ;
  }

  for (; i < t->head.page_size; i++) {
    bp__kv_copy(&child->keys[i], &right->keys[right->length++], 1);
    right->byte_size += BP__KV_SIZE(child->keys[i])
    ;
  }

  /* save left and right parts to get offsets */
  int ret;
  ret = bp__page_save(t, left);

  if (ret)
    return ret;
  ret = bp__page_save(t, right);
  if (ret)
    return ret;

  /* store offsets with middle key */
  middle_key.offset = right->offset;
  middle_key.config = right->config;

  /* insert middle key into parent page */
  bp__page_shiftr(t, parent, index + 1);
  bp__kv_copy(&middle_key, &parent->keys[index + 1], 0);
  parent->byte_size += BP__KV_SIZE(middle_key)
  ;
  parent->length++;

  /* change left element */
  parent->keys[index].offset = left->offset;
  parent->keys[index].config = left->config;

  /* cleanup */
  ret = bp__page_destroy(t, left);
  if (ret)
    return ret;
  ret = bp__page_destroy(t, right);
  if (ret)
    return ret;

  /* caller should not care of child */
  ret = bp__page_destroy(t, child);
  if (ret)
    return ret;

  return ret;
}

int bp__kv_copy(const bp__kv_t* source, bp__kv_t* target, int alloc) {
  /* copy key fields */
  if (alloc) {
    target->value = malloc(source->length);

    if (target->value == NULL)
      return BP_EALLOC;
    memcpy(target->value, source->value, source->length);
    target->allocated = 1;
  } else {
    target->value = source->value;
    target->allocated = source->allocated;
  }

  target->length = source->length;

  /* copy rest */
  target->offset = source->offset;
  target->config = source->config;

  return BP_OK;
}

// WRITER

#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <errno.h>

int bp__writer_create(bp__writer_t* w, const char* filename) {
  w->fd = fopen(filename, "a+");
  if (w->fd == NULL)
    return BP_EFILE;

  /* Determine filesize */
  if (fseeko(w->fd, 0, SEEK_END))
    return BP_EFILE;
  w->filesize = ftello(w->fd);
  w->flushed = w->filesize;

  return BP_OK;
}

int bp__writer_destroy(bp__writer_t* w) {
  if (fclose(w->fd))
    return BP_EFILE;
}

int bp__writer_read(bp__writer_t* w, const uint32_t offset, const uint32_t size,
                    void* data) {
  if (w->filesize < offset + size)
    return BP_EFILEREAD_OOB;

  /* flush any pending data before reading */
  if (offset > w->flushed) {
    if (fflush(w->fd))
      return BP_EFILEFLUSH;
    w->flushed = w->filesize;
  }

  /* Ignore empty reads */
  if (size == 0)
    return BP_OK;

  if (fseeko(w->fd, offset, SEEK_SET))
    return BP_EFILE;

  uint32_t read;
  read = fread(data, 1, size, w->fd);
  if (read != size)
    return BP_EFILEREAD;

  return BP_OK;
}

int bp__writer_write(bp__writer_t* w, const uint32_t size, const void* data,
                     uint32_t* offset) {
  uint32_t written;

  /* Write padding */
  bp__tree_head_t head;
  uint32_t padding = sizeof(head) - (w->filesize % sizeof(head));

  if (padding != sizeof(head)) {
    written = fwrite(&head, 1, padding, w->fd);
    if (written != padding)
      return BP_EFILEWRITE;
    w->filesize += padding;
  }

  /* Ignore empty writes */
  if (size != 0) {
    /* Write data */
    written = fwrite(data, 1, size, w->fd);
    if (written != size)
      return BP_EFILEWRITE;

    /* change offset */
    *offset = w->filesize;
    w->filesize += written;
    w->flushed = 0;
  }

  return BP_OK;
}

int bp__writer_find(bp__writer_t* w, const uint32_t size, void* data,
                    bp__writer_cb seek, bp__writer_cb miss) {
  uint32_t offset = w->filesize;
  int ret = 0;

  /* Write padding first */
  ret = bp__writer_write(w, 0, NULL, NULL);
  if (ret)
    return ret;

  /* Start seeking from bottom of file */
  while (offset >= size) {
    ret = bp__writer_read(w, offset - size, size, data);
    if (ret)
      break;

    /* Break if matched */
    if (seek(w, data) == 0)
      break;

    offset -= size;
  }

  /* Not found - invoke miss */
  if (offset < size) {
    ret = miss(w, data);
  }

  return ret;
}

// TREE

#include <stdlib.h> /* malloc */
#include <stdio.h> /* fopen, fclose, fread, fwrite, ... */
#include <string.h> /* strlen */
#include <arpa/inet.h> /* nothl, htonl */

int bp_open(bp_tree_t* tree, const char* filename) {
  int ret;
  ret = bp__writer_create((bp__writer_t*) tree, filename);
  if (ret)
    return ret;

  tree->head_page = NULL;

  /* Load head */
  ret = bp__writer_find((bp__writer_t*) tree, sizeof(tree->head), &tree->head,
                        bp__tree_read_head, bp__tree_write_head);
  if (ret)
    return ret;

  return BP_OK;
}

int bp_close(bp_tree_t* tree) {
  int ret;
  ret = bp__writer_destroy((bp__writer_t*) tree);

  if (ret)
    return ret;
  ret = bp__page_destroy(tree, tree->head_page);

  return BP_OK;
}

int bp_get(bp_tree_t* tree, const bp_key_t* key, bp_value_t* value) {
  int ret;
  return bp__page_get(tree, tree->head_page, (bp__kv_t*) key, value);
}

int bp_set(bp_tree_t* tree, const bp_key_t* key, const bp_value_t* value) {
  bp__kv_t kv;

  int ret;
  ret = bp__writer_write((bp__writer_t*) tree, value->length, value->value,
                         &kv.offset);
  if (ret)
    return ret;

  kv.length = key->length;
  kv.value = key->value;
  kv.config = value->length;

  ret = bp__page_insert(tree, tree->head_page, &kv);
  if (ret)
    return ret;
  bp__tree_write_head((bp__writer_t*) tree, &tree->head);
}

int bp_remove(bp_tree_t* tree, const bp_key_t* key) {
  return BP_OK;
}

/* Wrappers to allow string to string set/get/remove */

int bp_gets(bp_tree_t* tree, const char* key, char** value) {
  bp_key_t bkey;
  bkey.value = (char*) key;
  bkey.length = strlen(key)+1;

  bp_value_t bvalue;

  int ret;
  ret = bp_get(tree, &bkey, &bvalue);
  if (ret)
    return ret;

  *value = bvalue.value;

  return BP_OK;
}

int bp_sets(bp_tree_t* tree, const char* key, const char* value) {
  bp_key_t bkey;
  bkey.value = (char*) key;
  bkey.length = strlen(key)+1;

  bp_value_t bvalue;
  bvalue.value = (char*) value;
  bvalue.length = strlen(value)+1;

  return bp_set(tree, &bkey, &bvalue);
}

int bp_removes(bp_tree_t* tree, const char* key) {
  bp_key_t bkey;
  bkey.value = (char*) key;
  bkey.length = strlen(key)+1;

  return bp_remove(tree, &bkey);
}

void bp_set_compare_cb(bp_tree_t* tree, bp_compare_cb cb) {
  tree->compare_cb = cb;
}

int bp__tree_read_head(bp__writer_t* w, void* data) {
  bp__tree_head_t* head = (bp__tree_head_t*) data;

  /* Check hash first */
  if (bp__compute_hash(head->offset) != head->hash)
    return 1;

  bp_tree_t* tree = (bp_tree_t*) w;

  if (tree->head_page == NULL) {
    bp__page_create(tree, 1, head->offset, head->config, &tree->head_page);
  }
  int ret = bp__page_load(tree, tree->head_page);

  return ret;
}

int bp__tree_write_head(bp__writer_t* w, void* data) {
  bp_tree_t* t = (bp_tree_t*) w;
  bp__tree_head_t* head = data;

  /* TODO: page size should be configurable */
  head->page_size = 64;

  int ret;
  if (t->head_page == NULL) {
    /* Create empty leaf page */
    ret = bp__page_create(t, 1, 0, 0, &t->head_page);
    if (ret)
      return ret;

    /* Write it and store offset to head */
    ret = bp__page_save(t, t->head_page);
    if (ret)
      return ret;
  }

  /* Update head's position */
  head->offset = t->head_page->offset;
  head->config = t->head_page->config;

  head->hash = bp__compute_hash(head->offset);

  uint32_t offset;
  return bp__writer_write(w, sizeof(head), &head, &offset);
}
