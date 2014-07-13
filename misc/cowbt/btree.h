#include <stdlib.h> /* malloc */
#include <string.h> /* strlen */
#include <assert.h>

#include <fcntl.h> /* open */
#include <unistd.h> /* close, write, read */
#include <sys/stat.h> /* S_IWUSR, S_IRUSR */
#include <stdlib.h> /* malloc, free */
#include <stdio.h> /* sprintf */
#include <string.h> /* memset */
#include <errno.h> /* errno */

#include <stdint.h> /* uint64_t */

#define BP_PADDING 64

#define BP_KEY_FIELDS \
  uint64_t length;\
  char* value;

#include <stdint.h> /* uintx_t */

typedef pthread_mutex_t bp__mutex_t;
typedef pthread_rwlock_t bp__rwlock_t;

typedef struct bp__tree_head_s bp__tree_head_t;

// ERRORS.H

#define BP_OK 0

#define BP_EFILE           0x101
#define BP_EFILEREAD_OOB   0x102
#define BP_EFILEREAD       0x103
#define BP_EFILEWRITE      0x104
#define BP_EFILEFLUSH      0x105
#define BP_EFILERENAME     0x106
#define BP_ECOMPACT_EXISTS 0x107

#define BP_ECOMP 0x201
#define BP_EDECOMP 0x202

#define BP_EALLOC  0x301
#define BP_EMUTEX  0x302
#define BP_ERWLOCK 0x303

#define BP_ENOTFOUND       0x401
#define BP_ESPLITPAGE      0x402
#define BP_EEMPTYPAGE      0x403
#define BP_EUPDATECONFLICT 0x404
#define BP_EREMOVECONFLICT 0x405

typedef struct bp_db_s bp_db_t;

typedef struct bp_key_s bp_key_t;
typedef struct bp_key_s bp_value_t;

typedef int (*bp_compare_cb)(const bp_key_t* a, const bp_key_t* b);
typedef int (*bp_update_cb)(void* arg, const bp_value_t* previous,
                            const bp_value_t* value);
typedef int (*bp_remove_cb)(void* arg, const bp_value_t* value);
typedef void (*bp_range_cb)(void* arg, const bp_key_t* key,
                            const bp_value_t* value);
typedef int (*bp_filter_cb)(void* arg, const bp_key_t* key);

#define BP_WRITER_PRIVATE \
    int fd;\
    char* filename;\
    uint64_t filesize;\
    char padding[BP_PADDING];

#define BP_TREE_PRIVATE\
    BP_WRITER_PRIVATE\
    bp__rwlock_t rwlock;\
    bp__tree_head_t head;\
    bp_compare_cb compare_cb;

#define BP_KEY_PRIVATE\
    uint64_t _prev_offset;\
    uint64_t _prev_length;

// VALUES

#define BP__KV_HEADER_SIZE 24
#define BP__KV_SIZE(kv) BP__KV_HEADER_SIZE + kv.length
#define BP__STOVAL(str, key)\
    key.value = (char*) str;\
    key.length = strlen(str) + 1;

struct bp__kv_s {
  BP_KEY_FIELDS

  uint64_t offset;
  uint64_t config;

  uint8_t allocated;
};

typedef struct bp__kv_s bp__kv_t;

// PAGES

typedef struct bp__page_s bp__page_t;
typedef struct bp__page_search_res_s bp__page_search_res_t;

enum page_type {
  kPage = 0,
  kLeaf = 1
};

enum search_type {
  kNotLoad = 0,
  kLoad = 1
};

struct bp__page_s {
  enum page_type type;

  uint64_t length;
  uint64_t byte_size;

  uint64_t offset;
  uint64_t config;

  void* buff_;
  int is_head;

  bp__kv_t keys[1];
};

struct bp__page_search_res_s {
  bp__page_t* child;

  uint64_t index;
  int cmp;
};

// TREE

#define BP__HEAD_SIZE sizeof(uint64_t) * 4

struct bp__tree_head_s {
  uint64_t offset;
  uint64_t config;
  uint64_t page_size;
  uint64_t hash;

  bp__page_t* page;
};

struct bp_db_s {
  BP_TREE_PRIVATE
};

struct bp_key_s {
  /*
   * uint64_t length;
   * char* value;
   */
  BP_KEY_FIELDS
  BP_KEY_PRIVATE
};

// THREADS

#include <stdlib.h>

#ifndef NDEBUG
#include <stdio.h>
#include <errno.h>
#define ENSURE(expr)\
    int ret = expr;\
    if (ret != 0) {\
      errno = ret;\
      perror(#expr);\
      abort();\
    }
#else
#define ENSURE(expr)\
    if (expr) abort();
#endif

int bp__mutex_init(bp__mutex_t* mutex) {
  return pthread_mutex_init(mutex, NULL) == 0 ? BP_OK : BP_EMUTEX;
}


void bp__mutex_destroy(bp__mutex_t* mutex) {
  ENSURE(pthread_mutex_destroy(mutex));
}


void bp__mutex_lock(bp__mutex_t* mutex) {
  ENSURE(pthread_mutex_lock(mutex));
}


void bp__mutex_unlock(bp__mutex_t* mutex) {
  ENSURE(pthread_mutex_unlock(mutex));
}


int bp__rwlock_init(bp__rwlock_t* rwlock) {
  return pthread_rwlock_init(rwlock, NULL) == 0 ? BP_OK : BP_ERWLOCK;
}


void bp__rwlock_destroy(bp__rwlock_t* rwlock) {
  ENSURE(pthread_rwlock_destroy(rwlock));
}


void bp__rwlock_rdlock(bp__rwlock_t* rwlock) {
  ENSURE(pthread_rwlock_rdlock(rwlock));
}


void bp__rwlock_wrlock(bp__rwlock_t* rwlock) {
  ENSURE(pthread_rwlock_wrlock(rwlock));
}


void bp__rwlock_unlock(bp__rwlock_t* rwlock) {
  ENSURE(pthread_rwlock_unlock(rwlock));
}

// UTILS

/* http://www.concentric.net/~Ttwang/tech/inthash.htm */
uint32_t bp__compute_hash(uint32_t key) {
  uint32_t hash = key;
  hash = ~hash + (hash << 15); /* hash = (hash << 15) - hash - 1; */
  hash = hash ^ (hash >> 12);
  hash = hash + (hash << 2);
  hash = hash ^ (hash >> 4);
  hash = hash * 2057; /* hash = (hash + (hash << 3)) + (hash << 11); */
  hash = hash ^ (hash >> 16);
  return hash;
}

uint64_t bp__compute_hashl(uint64_t key) {
  uint32_t keyh = key >> 32;
  uint32_t keyl = key & 0xffffffffLL;

  return ((uint64_t) bp__compute_hash(keyh) << 32) | bp__compute_hash(keyl);
}

// WRITER

typedef struct bp__writer_s bp__writer_t;
typedef int (*bp__writer_cb)(bp__writer_t* w, void* data);

enum comp_type {
  kNotCompressed = 0,
  kCompressed = 1
};

struct bp__writer_s {
  BP_WRITER_PRIVATE
};


int bp__value_load(bp_db_t* t, const uint64_t offset, const uint64_t length,
                   bp_value_t* value) {
  int ret;
  char* buff;
  uint64_t buff_len = length;

  /* read data from disk first */
  ret = bp__writer_read((bp__writer_t*) t, kCompressed, offset, &buff_len,
                        (void**) &buff);
  if (ret != BP_OK)
    return ret;

  value->value = malloc(buff_len - 16);
  if (value->value == NULL) {
    free(buff);
    return BP_EALLOC;
  }

  /* first 16 bytes are representing previous value */
  value->_prev_offset = *(uint64_t*) (buff);
  value->_prev_length = *(uint64_t*) (buff + 8);

  /* copy the rest into result buffer */
  memcpy(value->value, buff + 16, buff_len - 16);
  value->length = buff_len - 16;

  free(buff);

  return BP_OK;
}

int bp__value_save(bp_db_t* t, const bp_value_t* value,
                   const bp__kv_t* previous, uint64_t* offset,
                   uint64_t* length) {
  int ret;
  char* buff;

  buff = malloc(value->length + 16);
  if (buff == NULL)
    return BP_EALLOC;

  /* insert offset, length of previous value */
  if (previous != NULL) {
    *(uint64_t*) (buff) = previous->offset;
    *(uint64_t*) (buff + 8) = previous->length;
  } else {
    *(uint64_t*) (buff) = 0;
    *(uint64_t*) (buff + 8) = 0;
  }

  /* insert current value itself */
  memcpy(buff + 16, value->value, value->length);

  *length = value->length + 16;
  ret = bp__writer_write((bp__writer_t*) t, kCompressed, buff, offset, length);
  free(buff);

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


/* various functions */

void bp_set_compare_cb(bp_db_t* tree, bp_compare_cb cb) {
  tree->compare_cb = cb;
}

int bp_fsync(bp_db_t* tree) {
  int ret;

  bp__rwlock_wrlock(&tree->rwlock);
  ret = bp__writer_fsync((bp__writer_t*) tree);
  bp__rwlock_unlock(&tree->rwlock);

  return ret;
}

/* internal utils */

int bp__tree_read_head(bp__writer_t* w, void* data) {
  int ret;
  bp_db_t* t = (bp_db_t*) w;
  bp__tree_head_t* head = (bp__tree_head_t*) data;

  t->head.offset = head->offset;
  t->head.config = head->config;
  t->head.page_size = head->page_size;
  t->head.hash = head->hash;

  /* we've copied all data - free it */
  free(data);

  /* Check hash first */
  if (bp__compute_hashl(t->head.offset) != t->head.hash)
    return 1;

  ret = bp__page_load(t, t->head.offset, t->head.config, &t->head.page);
  if (ret != BP_OK)
    return ret;

  t->head.page->is_head = 1;

  return ret;
}

int bp__tree_write_head(bp__writer_t* w, void* data) {
  int ret;
  bp_db_t* t = (bp_db_t*) w;
  bp__tree_head_t nhead;
  uint64_t offset;
  uint64_t size;

  if (t->head.page == NULL) {
    /* TODO: page size should be configurable */
    t->head.page_size = 64;

    /* Create empty leaf page */
    ret = bp__page_create(t, kLeaf, 0, 1, &t->head.page);
    if (ret != BP_OK)
      return ret;

    t->head.page->is_head = 1;
  }

  /* Update head's position */
  t->head.offset = t->head.page->offset;
  t->head.config = t->head.page->config;

  t->head.hash = bp__compute_hashl(t->head.offset);

  /* Create temporary head with fields in network byte order */
  nhead.offset = t->head.offset;
  nhead.config = t->head.config;
  nhead.page_size = t->head.page_size;
  nhead.hash = t->head.hash;

  size = BP__HEAD_SIZE;
  ret = bp__writer_write(w, kNotCompressed, &nhead, &offset, &size);

  return ret;
}

int bp__default_compare_cb(const bp_key_t* a, const bp_key_t* b) {
  uint32_t i, len = a->length < b->length ? a->length : b->length;

  for (i = 0; i < len; i++) {
    if (a->value[i] != b->value[i]) {
      return (uint8_t) a->value[i] > (uint8_t) b->value[i] ? 1 : -1;
    }
  }

  return a->length - b->length;
}

int bp__default_filter_cb(void* arg, const bp_key_t* key) {
  /* default filter accepts all keys */
  return 1;
}

// COMPRESSOR

#include <string.h> /* memcpy */

size_t bp__max_compressed_size(size_t size) {
  return size;
}

int bp__compress(const char* input,
                 size_t input_length,
                 char* compressed,
                 size_t* compressed_length) {
  memcpy(compressed, input, input_length);
  *compressed_length = input_length;
  return BP_OK;
}

int bp__uncompressed_length(const char* compressed,
                            size_t compressed_length,
                            size_t* result) {
  *result = compressed_length;
  return BP_OK;
}


int bp__uncompress(const char* compressed,
                   size_t compressed_length,
                   char* uncompressed,
                   size_t* uncompressed_length) {
  memcpy(uncompressed, compressed, compressed_length);
  *uncompressed_length = compressed_length;
  return BP_OK;
}

// PAGES

void bp__page_shiftr(bp_db_t* t, bp__page_t* p, const uint64_t index) {
  uint64_t i;

  if (p->length != 0) {
    for (i = p->length - 1; i >= index; i--) {
      bp__kv_copy(&p->keys[i], &p->keys[i + 1], 0);

      if (i == 0) break;
    }
  }
}


void bp__page_shiftl(bp_db_t* t, bp__page_t* p, const uint64_t index) {
  uint64_t i;
  for (i = index + 1; i < p->length; i++) {
    bp__kv_copy(&p->keys[i], &p->keys[i - 1], 0);
  }
}


int bp__page_create(bp_db_t* t,
                    const enum page_type type,
                    const uint64_t offset,
                    const uint64_t config,
                    bp__page_t** page) {
  /* Allocate space for page + keys */
  bp__page_t* p;

  p = malloc(sizeof(*p) + sizeof(p->keys[0]) * (t->head.page_size - 1));
  if (p == NULL) return BP_EALLOC;

  p->type = type;
  if (type == kLeaf) {
    p->length = 0;
    p->byte_size = 0;
  } else {
    /* non-leaf pages always have left element */
    p->length = 1;
    p->keys[0].value = NULL;
    p->keys[0].length = 0;
    p->keys[0].offset = 0;
    p->keys[0].config = 0;
    p->keys[0].allocated = 0;
    p->byte_size = BP__KV_SIZE(p->keys[0]);
  }

  /* this two fields will be changed on page_write */
  p->offset = offset;
  p->config = config;

  p->buff_ = NULL;
  p->is_head = 0;

  *page = p;
  return BP_OK;
}


void bp__page_destroy(bp_db_t* t, bp__page_t* page) {
  /* Free all keys */
  uint64_t i = 0;
  for (i = 0; i < page->length; i++) {
    if (page->keys[i].allocated) {
      free(page->keys[i].value);
      page->keys[i].value = NULL;
    }
  }

  if (page->buff_ != NULL) {
    free(page->buff_);
    page->buff_ = NULL;
  }

  /* Free page itself */
  free(page);
}


int bp__page_clone(bp_db_t* t, bp__page_t* page, bp__page_t** clone) {
  int ret = BP_OK;
  uint64_t i = 0;
  ret = bp__page_create(t, page->type, page->offset, page->config, clone);
  if (ret != BP_OK) return ret;

  (*clone)->is_head = page->is_head;

  (*clone)->length = 0;
  for (i = 0; i < page->length; i++) {
    ret = bp__kv_copy(&page->keys[i], &(*clone)->keys[i], 1);
    (*clone)->length++;
    if (ret != BP_OK) break;
  }
  (*clone)->byte_size = page->byte_size;

  /* if failed - free memory for all allocated keys */
  if (ret != BP_OK) bp__page_destroy(t, *clone);

  return ret;
}


int bp__page_read(bp_db_t* t, bp__page_t* page) {
  int ret;
  uint64_t size, o;
  uint64_t i;
  bp__writer_t* w = (bp__writer_t*) t;

  char* buff = NULL;

  /* Read page size and leaf flag */
  size = page->config >> 1;
  page->type = page->config & 1 ? kLeaf : kPage;

  /* Read page data */
  ret = bp__writer_read(w, kCompressed, page->offset, &size, (void**) &buff);
  if (ret != BP_OK) return ret;

  /* Parse data */
  i = 0;
  o = 0;
  while (o < size) {
    page->keys[i].length = *(uint64_t*) (buff + o);
    page->keys[i].offset = *(uint64_t*) (buff + o + 8);
    page->keys[i].config = *(uint64_t*) (buff + o + 16);
    page->keys[i].value = buff + o + 24;
    page->keys[i].allocated = 0;

    o += BP__KV_SIZE(page->keys[i]);
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


int bp__page_load(bp_db_t* t,
                  const uint64_t offset,
                  const uint64_t config,
                  bp__page_t** page) {
  int ret;

  bp__page_t* new_page;
  ret = bp__page_create(t, 0, offset, config, &new_page);
  if (ret != BP_OK) return ret;

  ret = bp__page_read(t, new_page);
  if (ret != BP_OK) {
    bp__page_destroy(t, new_page);
    return ret;
  }

  /* bp__page_load should be atomic */
  *page = new_page;

  return BP_OK;
}


int bp__page_save(bp_db_t* t, bp__page_t* page) {
  int ret;
  bp__writer_t* w = (bp__writer_t*) t;
  uint64_t i;
  uint64_t o;
  char* buff;

  assert(page->type == kLeaf || page->length != 0);

  /* Allocate space for serialization (header + keys); */
  buff = malloc(page->byte_size);
  if (buff == NULL) return BP_EALLOC;

  o = 0;
  for (i = 0; i < page->length; i++) {
    assert(o + BP__KV_SIZE(page->keys[i]) <= page->byte_size);

    *(uint64_t*) (buff + o) = page->keys[i].length;
    *(uint64_t*) (buff + o + 8) = page->keys[i].offset;
    *(uint64_t*) (buff + o + 16) = page->keys[i].config;

    memcpy(buff + o + 24, page->keys[i].value, page->keys[i].length);

    o += BP__KV_SIZE(page->keys[i]);
  }
  assert(o == page->byte_size);

  page->config = page->byte_size;
  ret = bp__writer_write(w,
                         kCompressed,
                         buff,
                         &page->offset,
                         &page->config);
  page->config = (page->config << 1) | (page->type == kLeaf);

  free(buff);
  return ret;
}


int bp__page_load_value(bp_db_t* t,
                        bp__page_t* page,
                        const uint64_t index,
                        bp_value_t* value) {
  return bp__value_load(t,
                        page->keys[index].offset,
                        page->keys[index].config,
                        value);
}


int bp__page_save_value(bp_db_t* t,
                        bp__page_t* page,
                        const uint64_t index,
                        const int cmp,
                        const bp_key_t* key,
                        const bp_value_t* value,
                        bp_update_cb update_cb,
                        void* arg) {
  int ret;
  bp__kv_t previous, tmp;

  /* replace item with same key from page */
  if (cmp == 0) {
    /* solve conflicts if callback was provided */
    if (update_cb != NULL) {
      bp_value_t prev_value;

      ret = bp__page_load_value(t, page, index, &prev_value);
      if (ret != BP_OK) return ret;

      ret = update_cb(arg, &prev_value, value);
      free(prev_value.value);

      if (!ret) return BP_EUPDATECONFLICT;
    }
    previous.offset = page->keys[index].offset;
    previous.length = page->keys[index].config;
    bp__page_remove_idx(t, page, index);
  }

  /* store key */
  tmp.value = key->value;
  tmp.length = key->length;

  /* store value */
  ret = bp__value_save(t,
                       value,
                       cmp == 0 ? &previous : NULL,
                       &tmp.offset,
                       &tmp.config);
  if (ret != BP_OK) return ret;

  /* Shift all keys right */
  bp__page_shiftr(t, page, index);

  /* Insert key in the middle */
  ret = bp__kv_copy(&tmp, &page->keys[index], 1);
  if (ret != BP_OK) {
    /* shift keys back */
    bp__page_shiftl(t, page, index);
    return ret;
  }

  page->byte_size += BP__KV_SIZE(tmp);
  page->length++;

  return BP_OK;
}


int bp__page_search(bp_db_t* t,
                    bp__page_t* page,
                    const bp_key_t* key,
                    const enum search_type type,
                    bp__page_search_res_t* result) {
  int ret;
  uint64_t i = page->type == kPage;
  int cmp = -1;
  bp__page_t* child;

  /* assert infinite recursion */
  assert(page->type == kLeaf || page->length > 0);

  while (i < page->length) {
    /* left key is always lower in non-leaf nodes */
    cmp = t->compare_cb((bp_key_t*) &page->keys[i], key);

    if (cmp >= 0) break;
    i++;
  }

  result->cmp = cmp;

  if (page->type == kLeaf) {
    result->index = i;
    result->child = NULL;

    return BP_OK;
  } else {
    assert(i > 0);
    if (cmp != 0) i--;

    if (type == kLoad) {
      ret = bp__page_load(t,
                          page->keys[i].offset,
                          page->keys[i].config,
                          &child);
      if (ret != BP_OK) return ret;

      result->child = child;
    } else {
      result->child = NULL;
    }

    result->index = i;

    return BP_OK;
  }
}


int bp__page_get(bp_db_t* t,
                 bp__page_t* page,
                 const bp_key_t* key,
                 bp_value_t* value) {
  int ret;
  bp__page_search_res_t res;
  ret = bp__page_search(t, page, key, kLoad, &res);
  if (ret != BP_OK) return ret;

  if (res.child == NULL) {
    if (res.cmp != 0) return BP_ENOTFOUND;

    return bp__page_load_value(t, page, res.index, value);
  } else {
    ret = bp__page_get(t, res.child, key, value);
    bp__page_destroy(t, res.child);
    res.child = NULL;
    return ret;
  }
}


int bp__page_get_range(bp_db_t* t,
                       bp__page_t* page,
                       const bp_key_t* start,
                       const bp_key_t* end,
                       bp_filter_cb filter,
                       bp_range_cb cb,
                       void* arg) {
  int ret;
  uint64_t i;
  bp__page_search_res_t start_res, end_res;

  /* find start and end indexes */
  ret = bp__page_search(t, page, start, kNotLoad, &start_res);
  if (ret != BP_OK) return ret;
  ret = bp__page_search(t, page, end, kNotLoad, &end_res);
  if (ret != BP_OK) return ret;

  if (page->type == kLeaf) {
    /* on leaf pages end-key should always be greater or equal than first key */
    if (end_res.cmp > 0 && end_res.index == 0) return BP_OK;

    if (end_res.cmp < 0) end_res.index--;
  }

  /* go through each page item */
  for (i = start_res.index; i <= end_res.index; i++) {
    /* run filter */
    if (!filter(arg, (bp_key_t*) &page->keys[i])) continue;

    if (page->type == kPage) {
      /* load child page and apply range get to it */
      bp__page_t* child;

      ret = bp__page_load(t,
                          page->keys[i].offset,
                          page->keys[i].config,
                          &child);
      if (ret != BP_OK) return ret;

      ret = bp__page_get_range(t, child, start, end, filter, cb, arg);

      /* destroy child regardless of error */
      bp__page_destroy(t, child);

      if (ret != BP_OK) return ret;
    } else {
      /* load value and pass it to callback */
      bp_value_t value;
      ret = bp__page_load_value(t, page, i, &value);
      if (ret != BP_OK) return ret;

      cb(arg, (bp_key_t*) &page->keys[i], &value);

      free(value.value);
    }
  }

  return BP_OK;
}


int bp__page_insert(bp_db_t* t,
                    bp__page_t* page,
                    const bp_key_t* key,
                    const bp_value_t* value,
                    bp_update_cb update_cb,
                    void* arg) {
  int ret;
  bp__page_search_res_t res;
  ret = bp__page_search(t, page, key, kLoad, &res);
  if (ret != BP_OK) return ret;

  if (res.child == NULL) {
    /* store value in db file to get offset and config */
    ret = bp__page_save_value(t,
                              page,
                              res.index,
                              res.cmp,
                              key,
                              value,
                              update_cb,
                              arg);
    if (ret != BP_OK) return ret;
  } else {
    /* Insert kv in child page */
    ret = bp__page_insert(t, res.child, key, value, update_cb, arg);

    /* kv was inserted but page is full now */
    if (ret == BP_ESPLITPAGE) {
      ret = bp__page_split(t, page, res.index, res.child);
    } else if (ret == BP_OK) {
      /* Update offsets in page */
      page->keys[res.index].offset = res.child->offset;
      page->keys[res.index].config = res.child->config;
    }

    bp__page_destroy(t, res.child);
    res.child = NULL;

    if (ret != BP_OK) {
      return ret;
    }
  }

  if (page->length == t->head.page_size) {
    if (page->is_head) {
      ret = bp__page_split_head(t, &page);
      if (ret != BP_OK) return ret;
    } else {
      /* Notify caller that it should split page */
      return BP_ESPLITPAGE;
    }
  }

  assert(page->length < t->head.page_size);

  ret = bp__page_save(t, page);
  if (ret != BP_OK) return ret;

  return BP_OK;
}


int bp__page_bulk_insert(bp_db_t* t,
                         bp__page_t* page,
                         const bp_key_t* limit,
                         uint64_t* count,
                         bp_key_t** keys,
                         bp_value_t** values,
                         bp_update_cb update_cb,
                         void* arg) {
  int ret;
  bp__page_search_res_t res;

  while (*count > 0 &&
         (limit == NULL || t->compare_cb(limit, *keys) > 0)) {

    ret = bp__page_search(t, page, *keys, kLoad, &res);
    if (ret != BP_OK) return ret;

    if (res.child == NULL) {
      /* store value in db file to get offset and config */
      ret = bp__page_save_value(t,
                                page,
                                res.index,
                                res.cmp,
                                *keys,
                                *values,
                                update_cb,
                                arg);
      /*
       * ignore update conflicts, to handle situations where
       * only one kv failed in a bulk
       */
      if (ret != BP_OK && ret != BP_EUPDATECONFLICT) return ret;

      *keys = *keys + 1;
      *values = *values + 1;
      *count = *count - 1;
    } else {
      /* we're in regular page */
      bp_key_t* new_limit = NULL;

      if (res.index + 1 < page->length) {
        new_limit = (bp_key_t*) &page->keys[res.index + 1];
      }

      ret = bp__page_bulk_insert(t,
                                 res.child,
                                 new_limit,
                                 count,
                                 keys,
                                 values,
                                 update_cb,
                                 arg);
      if (ret == BP_ESPLITPAGE) {
        ret = bp__page_split(t, page, res.index, res.child);
      } else if (ret == BP_OK) {
        /* Update offsets in page */
        page->keys[res.index].offset = res.child->offset;
        page->keys[res.index].config = res.child->config;
      }

      bp__page_destroy(t, res.child);
      res.child = NULL;

      if (ret != BP_OK) return ret;
    }

    if (page->length == t->head.page_size) {
      if (page->is_head) {
        ret = bp__page_split_head(t, &page);
        if (ret != BP_OK) return ret;
      } else {
        /* Notify caller that it should split page */
        return BP_ESPLITPAGE;
      }
    }

    assert(page->length < t->head.page_size);
  }

  return bp__page_save(t, page);
}


int bp__page_remove(bp_db_t* t,
                    bp__page_t* page,
                    const bp_key_t* key,
                    bp_remove_cb remove_cb,
                    void* arg) {
  int ret;
  bp__page_search_res_t res;
  ret = bp__page_search(t, page, key, kLoad, &res);
  if (ret != BP_OK) return ret;

  if (res.child == NULL) {
    if (res.cmp != 0) return BP_ENOTFOUND;

    /* remove only if remove_cb returns BP_OK */
    if (remove_cb != NULL) {
      bp_value_t prev_val;

      ret = bp__page_load_value(t, page, res.index, &prev_val);
      if (ret != BP_OK) return ret;

      ret = remove_cb(arg, &prev_val);
      free(prev_val.value);

      if (!ret) return BP_EREMOVECONFLICT;
    }
    bp__page_remove_idx(t, page, res.index);

    if (page->length == 0 && !page->is_head) return BP_EEMPTYPAGE;
  } else {
    /* Insert kv in child page */
    ret = bp__page_remove(t, res.child, key, remove_cb, arg);

    if (ret != BP_OK && ret != BP_EEMPTYPAGE) {
      return ret;
    }

    /* kv was inserted but page is full now */
    if (ret == BP_EEMPTYPAGE) {
      bp__page_remove_idx(t, page, res.index);

      /* we don't need child now */
      bp__page_destroy(t, res.child);
      res.child = NULL;

      /* only one item left - lift kv from last child to current page */
      if (page->length == 1) {
        page->offset = page->keys[0].offset;
        page->config = page->keys[0].config;

        /* remove child to free memory */
        bp__page_remove_idx(t, page, 0);

        /* and load child as current page */
        ret = bp__page_read(t, page);
        if (ret != BP_OK) return ret;
      }
    } else {
      /* Update offsets in page */
      page->keys[res.index].offset = res.child->offset;
      page->keys[res.index].config = res.child->config;

      /* we don't need child now */
      bp__page_destroy(t, res.child);
      res.child = NULL;
    }
  }

  return bp__page_save(t, page);
}


int bp__page_copy(bp_db_t* source, bp_db_t* target, bp__page_t* page) {
  int ret;
  uint64_t i;
  for (i = 0; i < page->length; i++) {
    if (page->type == kPage) {
      /* copy child page */
      bp__page_t* child;
      ret = bp__page_load(source,
                          page->keys[i].offset,
                          page->keys[i].config,
                          &child);
      if (ret != BP_OK) return ret;

      ret = bp__page_copy(source, target, child);
      if (ret != BP_OK) return ret;

      /* update child position */
      page->keys[i].offset = child->offset;
      page->keys[i].config = child->config;

      bp__page_destroy(source, child);
    } else {
      /* copy value */
      bp_value_t value;

      ret = bp__page_load_value(source, page, i, &value);
      if (ret != BP_OK) return ret;

      page->keys[i].config = value.length;
      ret = bp__value_save(target,
                           &value,
                           NULL,
                           &page->keys[i].offset,
                           &page->keys[i].config);

      /* value is not needed anymore */
      free(value.value);
      if (ret != BP_OK) return ret;
    }
  }

  return bp__page_save(target, page);
}


int bp__page_remove_idx(bp_db_t* t, bp__page_t* page, const uint64_t index) {
  assert(index < page->length);

  /* Free memory allocated for kv and reduce byte_size of page */
  page->byte_size -= BP__KV_SIZE(page->keys[index]);
  if (page->keys[index].allocated) {
    free(page->keys[index].value);
    page->keys[index].value = NULL;
  }

  /* Shift all keys left */
  bp__page_shiftl(t, page, index);

  page->length--;

  return BP_OK;
}


int bp__page_split(bp_db_t* t,
                   bp__page_t* parent,
                   const uint64_t index,
                   bp__page_t* child) {
  int ret;
  uint64_t i, middle;
  bp__page_t* left = NULL;
  bp__page_t* right = NULL;
  bp__kv_t middle_key;

  bp__page_create(t, child->type, 0, 0, &left);
  bp__page_create(t, child->type, 0, 0, &right);

  middle = t->head.page_size >> 1;
  ret = bp__kv_copy(&child->keys[middle], &middle_key, 1);
  if (ret != BP_OK) goto fatal;

  /* non-leaf nodes has byte_size > 0 nullify it first */
  left->byte_size = 0;
  left->length = 0;
  for (i = 0; i < middle; i++) {
    ret = bp__kv_copy(&child->keys[i], &left->keys[left->length++], 1);
    if (ret != BP_OK) goto fatal;
    left->byte_size += BP__KV_SIZE(child->keys[i]);
  }

  right->byte_size = 0;
  right->length = 0;
  for (; i < t->head.page_size; i++) {
    ret = bp__kv_copy(&child->keys[i], &right->keys[right->length++], 1);
    if (ret != BP_OK) goto fatal;
    right->byte_size += BP__KV_SIZE(child->keys[i]);
  }

  /* save left and right parts to get offsets */
  ret = bp__page_save(t, left);
  if (ret != BP_OK) goto fatal;

  ret = bp__page_save(t, right);
  if (ret != BP_OK) goto fatal;

  /* store offsets with middle key */
  middle_key.offset = right->offset;
  middle_key.config = right->config;

  /* insert middle key into parent page */
  bp__page_shiftr(t, parent, index + 1);
  bp__kv_copy(&middle_key, &parent->keys[index + 1], 0);

  parent->byte_size += BP__KV_SIZE(middle_key);
  parent->length++;

  /* change left element */
  parent->keys[index].offset = left->offset;
  parent->keys[index].config = left->config;

  ret = BP_OK;
fatal:
  /* cleanup */
  bp__page_destroy(t, left);
  bp__page_destroy(t, right);
  return ret;
}


int bp__page_split_head(bp_db_t* t, bp__page_t** page) {
  int ret;
  bp__page_t* new_head = NULL;
  bp__page_create(t, 0, 0, 0, &new_head);
  new_head->is_head = 1;

  ret = bp__page_split(t, new_head, 0, *page);
  if (ret != BP_OK) {
    bp__page_destroy(t, new_head);
    return ret;
  }

  t->head.page = new_head;
  bp__page_destroy(t, *page);
  *page = new_head;

  return BP_OK;
}

// WRITER

int bp__writer_create(bp__writer_t* w, const char* filename) {
  off_t filesize;
  size_t filename_length;

  /* copy filename + '\0' char */
  filename_length = strlen(filename) + 1;
  w->filename = malloc(filename_length);
  if (w->filename == NULL)
    return BP_EALLOC;
  memcpy(w->filename, filename, filename_length);

  w->fd = open(filename, O_RDWR | O_APPEND | O_CREAT,
               S_IRUSR | S_IRGRP | S_IWGRP | S_IWUSR);
  if (w->fd == -1)
    goto error;

  /* Determine filesize */
  filesize = lseek(w->fd, 0, SEEK_END);
  if (filesize == -1)
    goto error;

  w->filesize = (uint64_t) filesize;

  /* Nullify padding to shut up valgrind */
  memset(&w->padding, 0, sizeof(w->padding));

  return BP_OK;

  error: free(w->filename);
  return BP_EFILE;
}

int bp__writer_destroy(bp__writer_t* w) {
  free(w->filename);
  w->filename = NULL;
  if (close(w->fd))
    return BP_EFILE;
  return BP_OK;
}

int bp__writer_fsync(bp__writer_t* w) {
  return fdatasync(w->fd) == 0 ? BP_OK : BP_EFILEFLUSH;
}

int bp__writer_compact_name(bp__writer_t* w, char** compact_name) {
  char* filename = malloc(strlen(w->filename) + sizeof(".compact") + 1);
  if (filename == NULL)
    return BP_EALLOC;

  sprintf(filename, "%s.compact", w->filename);
  if (access(filename, F_OK) != -1 || errno != ENOENT) {
    free(filename);
    return BP_ECOMPACT_EXISTS;
  }

  *compact_name = filename;
  return BP_OK;
}

void bp__destroy(bp_db_t* tree);

int bp__writer_compact_finalize(bp__writer_t* s, bp__writer_t* t) {
  int ret;
  char* name;
  char* compacted_name;

  /* save filename and prevent freeing it */
  name = s->filename;
  compacted_name = t->filename;
  s->filename = NULL;
  t->filename = NULL;

  /* close both trees */
  bp__destroy((bp_db_t*) s);
  ret = bp_close((bp_db_t*) t);
  if (ret != BP_OK)
    goto fatal;

  if (rename(compacted_name, name) != 0)
    return BP_EFILERENAME;

  /* reopen source tree */
  ret = bp__writer_create(s, name);
  if (ret != BP_OK)
    goto fatal;
  ret = bp__init((bp_db_t*) s);

  fatal: free(compacted_name);
  free(name);

  return ret;
}

int bp__writer_read(bp__writer_t* w, const enum comp_type comp,
                    const uint64_t offset, uint64_t* size, void** data) {
  ssize_t bytes_read;
  char* cdata;

  if (w->filesize < offset + *size)
    return BP_EFILEREAD_OOB;

  /* Ignore empty reads */
  if (*size == 0) {
    *data = NULL;
    return BP_OK;
  }

  cdata = malloc(*size);
  if (cdata == NULL)
    return BP_EALLOC;

  bytes_read = pread(w->fd, cdata, (size_t) *size, (off_t) offset);
  if ((uint64_t) bytes_read != *size) {
    free(cdata);
    return BP_EFILEREAD;
  }

  /* no compression for head */
  if (comp == kNotCompressed) {
    *data = cdata;
  } else {
    int ret = 0;

    char* uncompressed = NULL;
    size_t usize;

    if (bp__uncompressed_length(cdata, *size, &usize) != BP_OK) {
      ret = BP_EDECOMP;
    } else {
      uncompressed = malloc(usize);
      if (uncompressed == NULL) {
        ret = BP_EALLOC;
      } else if (bp__uncompress(cdata, *size, uncompressed, &usize) != BP_OK) {
        ret = BP_EDECOMP;
      } else {
        *data = uncompressed;
        *size = usize;
      }
    }

    free(cdata);

    if (ret != BP_OK) {
      free(uncompressed);
      return ret;
    }
  }

  return BP_OK;
}

int bp__writer_write(bp__writer_t* w, const enum comp_type comp,
                     const void* data, uint64_t* offset, uint64_t* size) {
  ssize_t written;
  uint32_t padding = sizeof(w->padding) - (w->filesize % sizeof(w->padding));

  /* Write padding */
  if (padding != sizeof(w->padding)) {
    written = write(w->fd, &w->padding, (size_t) padding);
    if ((uint32_t) written != padding)
      return BP_EFILEWRITE;
    w->filesize += padding;
  }

  /* Ignore empty writes */
  if (size == NULL || *size == 0) {
    if (offset != NULL)
      *offset = w->filesize;
    return BP_OK;
  }

  /* head shouldn't be compressed */
  if (comp == kNotCompressed) {
    written = write(w->fd, data, *size);
  } else {
    int ret;
    size_t max_csize = bp__max_compressed_size(*size);
    size_t result_size;
    char* compressed = malloc(max_csize);
    if (compressed == NULL)
      return BP_EALLOC;

    result_size = max_csize;
    ret = bp__compress(data, *size, compressed, &result_size);
    if (ret != BP_OK) {
      free(compressed);
      return BP_ECOMP;
    }

    *size = result_size;
    written = write(w->fd, compressed, result_size);
    free(compressed);
  }

  if ((uint64_t) written != *size)
    return BP_EFILEWRITE;

  /* change offset */
  *offset = w->filesize;
  w->filesize += written;

  return BP_OK;
}

int bp__writer_find(bp__writer_t* w, const enum comp_type comp,
                    const uint64_t size, void* data, bp__writer_cb seek,
                    bp__writer_cb miss) {
  int ret = 0;
  int match = 0;
  uint64_t offset, size_tmp;

  /* Write padding first */
  ret = bp__writer_write(w, kNotCompressed, NULL, NULL, NULL);
  if (ret != BP_OK)
    return ret;

  offset = w->filesize;
  size_tmp = size;

  /* Start seeking from bottom of file */
  while (offset >= size) {
    ret = bp__writer_read(w, comp, offset - size, &size_tmp, &data);
    if (ret != BP_OK)
      break;

    /* Break if matched */
    if (seek(w, data) == 0) {
      match = 1;
      break;
    }

    offset -= size;
  }

  /* Not found - invoke miss */
  if (!match) {
    ret = miss(w, data);
  }

  return ret;
}

// TREE

int bp_open(bp_db_t* tree, const char* filename) {
  int ret;

  ret = bp__rwlock_init(&tree->rwlock);
  if (ret != BP_OK)
    return ret;

  ret = bp__writer_create((bp__writer_t*) tree, filename);
  if (ret != BP_OK)
    goto fatal;

  tree->head.page = NULL;

  ret = bp__init(tree);
  if (ret != BP_OK)
    goto fatal;

  return BP_OK;

  fatal: bp__rwlock_destroy(&tree->rwlock);
  return ret;
}

int bp_close(bp_db_t* tree) {
  bp__rwlock_wrlock(&tree->rwlock);
  bp__destroy(tree);
  bp__rwlock_unlock(&tree->rwlock);

  bp__rwlock_destroy(&tree->rwlock);
  return BP_OK;
}

int bp__init(bp_db_t* tree) {
  int ret;
  /*
   * Load head.
   * Writer will not compress data chunk smaller than head,
   * that's why we're passing head size as compressed size here
   */
  ret = bp__writer_find((bp__writer_t*) tree, kNotCompressed,
  BP__HEAD_SIZE,
                        &tree->head, bp__tree_read_head, bp__tree_write_head);
  if (ret == BP_OK) {
    /* set default compare function */
    bp_set_compare_cb(tree, bp__default_compare_cb);
  }

  return ret;
}

void bp__destroy(bp_db_t* tree) {
  bp__writer_destroy((bp__writer_t*) tree);
  if (tree->head.page != NULL) {
    bp__page_destroy(tree, tree->head.page);
    tree->head.page = NULL;
  }
}

int bp_get(bp_db_t* tree, const bp_key_t* key, bp_value_t* value) {
  int ret;

  bp__rwlock_rdlock(&tree->rwlock);

  ret = bp__page_get(tree, tree->head.page, key, value);

  bp__rwlock_unlock(&tree->rwlock);

  return ret;
}

int bp_get_previous(bp_db_t* tree, const bp_value_t* value,
                    bp_value_t* previous) {
  if (value->_prev_offset == 0 && value->_prev_length == 0) {
    return BP_ENOTFOUND;
  }
  return bp__value_load(tree, value->_prev_offset, value->_prev_length,
                        previous);
}

int bp_update(bp_db_t* tree, const bp_key_t* key, const bp_value_t* value,
              bp_update_cb update_cb, void* arg) {
  int ret;

  bp__rwlock_wrlock(&tree->rwlock);

  ret = bp__page_insert(tree, tree->head.page, key, value, update_cb, arg);
  if (ret == BP_OK) {
    ret = bp__tree_write_head((bp__writer_t*) tree, NULL);
  }

  bp__rwlock_unlock(&tree->rwlock);

  return ret;
}

int bp_bulk_update(bp_db_t* tree, const uint64_t count, const bp_key_t** keys,
                   const bp_value_t** values, bp_update_cb update_cb,
                   void* arg) {
  int ret;
  bp_key_t* keys_iter = (bp_key_t*) *keys;
  bp_value_t* values_iter = (bp_value_t*) *values;
  uint64_t left = count;

  bp__rwlock_wrlock(&tree->rwlock);

  ret = bp__page_bulk_insert(tree, tree->head.page,
  NULL,
                             &left, &keys_iter, &values_iter, update_cb, arg);
  if (ret == BP_OK) {
    ret = bp__tree_write_head((bp__writer_t*) tree, NULL);
  }

  bp__rwlock_unlock(&tree->rwlock);

  return ret;
}

int bp_set(bp_db_t* tree, const bp_key_t* key, const bp_value_t* value) {
  return bp_update(tree, key, value, NULL, NULL);
}

int bp_bulk_set(bp_db_t* tree, const uint64_t count, const bp_key_t** keys,
                const bp_value_t** values) {
  return bp_bulk_update(tree, count, keys, values, NULL, NULL);
}

int bp_removev(bp_db_t* tree, const bp_key_t* key, bp_remove_cb remove_cb,
               void *arg) {
  int ret;

  bp__rwlock_wrlock(&tree->rwlock);

  ret = bp__page_remove(tree, tree->head.page, key, remove_cb, arg);
  if (ret == BP_OK) {
    ret = bp__tree_write_head((bp__writer_t*) tree, NULL);
  }

  bp__rwlock_unlock(&tree->rwlock);

  return ret;
}

int bp_remove(bp_db_t* tree, const bp_key_t* key) {
  return bp_removev(tree, key, NULL, NULL);
}

int bp_compact(bp_db_t* tree) {
  int ret;
  char* compacted_name;
  bp_db_t compacted;

  /* get name of compacted database (prefixed with .compact) */
  ret = bp__writer_compact_name((bp__writer_t*) tree, &compacted_name);
  if (ret != BP_OK)
    return ret;

  /* open it */
  ret = bp_open(&compacted, compacted_name);
  free(compacted_name);
  if (ret != BP_OK)
    return ret;

  /* destroy stub head page */
  bp__page_destroy(&compacted, compacted.head.page);

  bp__rwlock_rdlock(&tree->rwlock);

  /* clone source tree's head page */
  ret = bp__page_clone(&compacted, tree->head.page, &compacted.head.page);

  bp__rwlock_unlock(&tree->rwlock);

  /* copy all pages starting from head */
  ret = bp__page_copy(tree, &compacted, compacted.head.page);
  if (ret != BP_OK)
    return ret;

  ret = bp__tree_write_head((bp__writer_t*) &compacted, NULL);
  if (ret != BP_OK)
    return ret;

  bp__rwlock_wrlock(&tree->rwlock);

  ret = bp__writer_compact_finalize((bp__writer_t*) tree,
                                    (bp__writer_t*) &compacted);
  bp__rwlock_unlock(&tree->rwlock);

  return ret;
}

int bp_get_filtered_range(bp_db_t* tree, const bp_key_t* start,
                          const bp_key_t* end, bp_filter_cb filter,
                          bp_range_cb cb, void* arg) {
  int ret;

  bp__rwlock_rdlock(&tree->rwlock);

  ret = bp__page_get_range(tree, tree->head.page, start, end, filter, cb, arg);

  bp__rwlock_unlock(&tree->rwlock);

  return ret;
}

int bp_get_range(bp_db_t* tree, const bp_key_t* start, const bp_key_t* end,
                 bp_range_cb cb, void* arg) {
  return bp_get_filtered_range(tree, start, end, bp__default_filter_cb, cb, arg);
}

/* Wrappers to allow string to string set/get/remove */

int bp_gets(bp_db_t* tree, const char* key, char** value) {
  int ret;
  bp_key_t bkey;
  bp_value_t bvalue;

  BP__STOVAL(key, bkey);

  ret = bp_get(tree, &bkey, &bvalue);
  if (ret != BP_OK)
    return ret;

  *value = bvalue.value;

  return BP_OK;
}

int bp_updates(bp_db_t* tree, const char* key, const char* value,
               bp_update_cb update_cb, void* arg) {
  bp_key_t bkey;
  bp_value_t bvalue;

  BP__STOVAL(key, bkey);
  BP__STOVAL(value, bvalue);

  return bp_update(tree, &bkey, &bvalue, update_cb, arg);
}

int bp_sets(bp_db_t* tree, const char* key, const char* value) {
  return bp_updates(tree, key, value, NULL, NULL);
}

int bp_bulk_updates(bp_db_t* tree, const uint64_t count, const char** keys,
                    const char** values, bp_update_cb update_cb, void* arg) {
  int ret;
  bp_key_t* bkeys;
  bp_value_t* bvalues;
  uint64_t i;

  /* allocated memory for keys/values */
  bkeys = malloc(sizeof(*bkeys) * count);
  if (bkeys == NULL)
    return BP_EALLOC;

  bvalues = malloc(sizeof(*bvalues) * count);
  if (bvalues == NULL) {
    free(bkeys);
    return BP_EALLOC;
  }

  /* copy keys/values to allocated memory */
  for (i = 0; i < count; i++) {
    BP__STOVAL(keys[i], bkeys[i]);
    BP__STOVAL(values[i], bvalues[i]);
  }

  ret = bp_bulk_update(tree, count, (const bp_key_t**) &bkeys,
                       (const bp_value_t**) &bvalues, update_cb, arg);

  free(bkeys);
  free(bvalues);

  return ret;
}

int bp_bulk_sets(bp_db_t* tree, const uint64_t count, const char** keys,
                 const char** values) {
  return bp_bulk_updates(tree, count, keys, values, NULL, NULL);
}

int bp_removevs(bp_db_t* tree, const char* key, bp_remove_cb remove_cb,
                void *arg) {
  bp_key_t bkey;

  BP__STOVAL(key, bkey);

  return bp_removev(tree, &bkey, remove_cb, arg);
}

int bp_removes(bp_db_t* tree, const char* key) {
  return bp_removevs(tree, key, NULL, NULL);
}

int bp_get_filtered_ranges(bp_db_t* tree, const char* start, const char* end,
                           bp_filter_cb filter, bp_range_cb cb, void* arg) {
  bp_key_t bstart;
  bp_key_t bend;

  BP__STOVAL(start, bstart);
  BP__STOVAL(end, bend);

  return bp_get_filtered_range(tree, &bstart, &bend, filter, cb, arg);
}

int bp_get_ranges(bp_db_t* tree, const char* start, const char* end,
                  bp_range_cb cb, void* arg) {
  return bp_get_filtered_ranges(tree, start, end, bp__default_filter_cb, cb,
                                arg);
}

