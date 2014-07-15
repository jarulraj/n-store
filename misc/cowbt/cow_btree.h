#ifndef COW_BTREE_H_
#define COW_BTREE_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <sys/uio.h>
#include <sys/file.h>

#include <assert.h>
#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <time.h>
#include <unistd.h>
#include <cstring>

#include "cow_btree_ds.h"

// BTREE

struct cow_btval {
  void *data;
  size_t size;
  int release_data; /* true if data allocated */
  struct mpage *mp; /* ref'd memory page */
};

typedef int (*bt_cmp_func)(const struct cow_btval *a,
                           const struct cow_btval *b);
typedef void (*bt_prefix_func)(const struct cow_btval *a,
                               const struct cow_btval *b,
                               struct cow_btval *sep);

#define BT_NOOVERWRITE   1

enum cursor_op { /* cursor operations */
  BT_CURSOR, /* position at given key */
  BT_CURSOR_EXACT, /* position at key, or fail */
  BT_FIRST,
  BT_NEXT,
  BT_LAST, /* not implemented */
  BT_PREV /* not implemented */
};

/* return codes */
#define BT_FAIL   -1
#define BT_SUCCESS   0

/* btree flags */
#define BT_NOSYNC    0x02   /* don't fsync after commit */
#define BT_RDONLY    0x04   /* read only */
#define BT_REVERSEKEY    0x08   /* use reverse string keys */

struct cow_btree_stat {
  unsigned long long int hits; /* cache hits */
  unsigned long long int reads; /* page reads */
  unsigned int max_cache; /* max cached pages */
  unsigned int cache_size; /* current cache size */
  unsigned int branch_pages;
  unsigned int leaf_pages;
  unsigned int overflow_pages;
  unsigned int revisions;
  unsigned int depth;
  unsigned long long int entries;
  unsigned int psize;
  time_t created_at;
};

# define DPRINTF(...) do { fprintf(stderr, "%s:%d: ", __func__, __LINE__); \
fprintf(stderr, __VA_ARGS__); \
fprintf(stderr, "\n"); } while(0)

#define PAGESIZE   4096
#define BT_MINKEYS   4
#define BT_MAGIC   0xB3DBB3DB
#define BT_VERSION   4
#define MAXKEYSIZE   255

#define P_INVALID  0xFFFFFFFF

#define F_ISSET(w, f)  (((w) & (f)) == (f))

typedef uint32_t pgno_t;
typedef uint16_t indx_t;

/* There are four page types: meta, index, leaf and overflow.
 * They all share the same page header.
 */
struct page { /* represents an on-disk page */
  pgno_t pgno; /* page number */
#define P_BRANCH   0x01   /* branch page */
#define P_LEAF     0x02   /* leaf page */
#define P_OVERFLOW   0x04   /* overflow page */
#define P_META     0x08   /* meta page */
#define P_HEAD     0x10   /* header page */
  uint32_t flags;
#define lower    b.fb.fb_lower
#define upper    b.fb.fb_upper
#define p_next_pgno  b.pb_next_pgno
  union page_bounds {
    struct {
      indx_t fb_lower; /* lower bound of free space */
      indx_t fb_upper; /* upper bound of free space */
    } fb;
    pgno_t pb_next_pgno; /* overflow page linked list */
  } b;
  indx_t ptrs[1]; /* dynamic size */
} __packed;

#define PAGEHDRSZ  offsetof(struct page, ptrs)

#define NUMKEYSP(p)  (((p)->lower - PAGEHDRSZ) >> 1)
#define NUMKEYS(mp)  (((mp)->page->lower - PAGEHDRSZ) >> 1)
#define SIZELEFT(mp)   (indx_t)((mp)->page->upper - (mp)->page->lower)
#define PAGEFILL(mp) (1000 * (head.psize - PAGEHDRSZ - SIZELEFT(mp)) / \
        (head.psize - PAGEHDRSZ))
#define IS_LEAF(mp)  F_ISSET((mp)->page->flags, P_LEAF)
#define IS_BRANCH(mp)  F_ISSET((mp)->page->flags, P_BRANCH)
#define IS_OVERFLOW(mp)  F_ISSET((mp)->page->flags, P_OVERFLOW)

struct bt_head { /* header page content */
  uint32_t magic;
  uint32_t version;
  uint32_t flags;
  uint32_t psize; /* page size */
};

struct bt_meta { /* meta (footer) page content */
#define BT_TOMBSTONE   0x01     /* file is replaced */
  uint32_t flags;
  pgno_t root; /* page number of root page */
  pgno_t prev_meta; /* previous meta page number */
  time_t created_at;
  uint32_t branch_pages;
  uint32_t leaf_pages;
  uint32_t overflow_pages;
  uint32_t revisions;
  uint32_t depth;
  uint64_t entries;
};

struct btkey {
  size_t len;
  char str[MAXKEYSIZE];
};

struct mpage { /* an in-memory cached page */
  RB_ENTRY(mpage)
  entry; /* page cache entry */
  SIMPLEQ_ENTRY(mpage)
  next; /* queue of dirty pages */
  TAILQ_ENTRY(mpage)
  lru_next; /* LRU queue */
  struct mpage *parent; /* NULL if root */
  unsigned int parent_index; /* keep track of cow_node index */
  struct btkey prefix;
  struct page *page;
  pgno_t pgno; /* copy of page->pgno */
  short ref; /* increased by cursors */
  short dirty; /* 1 if on dirty queue */
};
RB_HEAD(page_cache, mpage);
SIMPLEQ_HEAD(dirty_queue, mpage);
TAILQ_HEAD(lru_queue, mpage);

int mpage_cmp(struct mpage *a, struct mpage *b) {
  if (a->pgno > b->pgno)
    return 1;
  if (a->pgno < b->pgno)
    return -1;
  return 0;
}

RB_PROTOTYPE(page_cache, mpage, entry, mpage_cmp);
RB_GENERATE(page_cache, mpage, entry, mpage_cmp);

struct ppage { /* ordered list of pages */
  SLIST_ENTRY(ppage)
  entry;
  struct mpage *mpage;
  unsigned int ki; /* cursor index on page */
};
SLIST_HEAD(page_stack, ppage);

#define CURSOR_EMPTY(c)    SLIST_EMPTY(&(c)->stack)
#define CURSOR_TOP(c)    SLIST_FIRST(&(c)->stack)
#define CURSOR_POP(c)    SLIST_REMOVE_HEAD(&(c)->stack, entry)
#define CURSOR_PUSH(c,p)   SLIST_INSERT_HEAD(&(c)->stack, p, entry)

struct cursor {
  struct cow_btree_txn *txn;
  struct page_stack stack; /* stack of parent pages */
  short initialized; /* 1 if initialized */
  short eof; /* 1 if end is reached */
};

#define METADATA(p)  ((void *)((char *)p + PAGEHDRSZ))

struct cow_node {
#define n_pgno     p.np_pgno
#define n_dsize    p.np_dsize
  union {
    pgno_t np_pgno; /* child page number */
    uint32_t np_dsize; /* leaf data size */
  } p;
  uint16_t ksize; /* key size */
#define F_BIGDATA  0x01     /* data put on overflow page */
  uint8_t flags;
  char data[1];
};

struct cow_btree_txn {
  pgno_t root; /* current / new root page */
  pgno_t next_pgno; /* next unallocated page */
  struct dirty_queue *dirty_queue; /* modified pages */
#define BT_TXN_RDONLY    0x01   /* read-only transaction */
#define BT_TXN_ERROR     0x02   /* an error has occurred */
  unsigned int flags;
};

#define NODESIZE   offsetof(struct cow_node, data)

#define INDXSIZE(k)  (NODESIZE + ((k) == NULL ? 0 : (k)->size))
#define LEAFSIZE(k, d)   (NODESIZE + (k)->size + (d)->size)
#define NODEPTRP(p, i)   ((struct cow_node *)((char *)(p) + (p)->ptrs[i]))
#define NODEPTR(mp, i)   NODEPTRP((mp)->page, i)
#define NODEKEY(cow_node)  (void *)((cow_node)->data)
#define NODEDATA(cow_node)   (void *)((char *)(cow_node)->data + (cow_node)->ksize)
#define NODEPGNO(cow_node)   ((cow_node)->p.np_pgno)
#define NODEDSZ(cow_node)  ((cow_node)->p.np_dsize)

#define BT_COMMIT_PAGES  64 /* max number of pages to write in one commit */
#define BT_MAXCACHE_DEF  1024 /* max number of pages to keep in cache  */

int memncmp(const void *s1, size_t n1, const void *s2, size_t n2) {
  if (n1 < n2) {
    if (memcmp(s1, s2, n1) == 0)
      return -1;
  } else if (n1 > n2) {
    if (memcmp(s1, s2, n2) == 0)
      return 1;
  }
  return memcmp(s1, s2, n1);
}

int memnrcmp(const void *s1, size_t n1, const void *s2, size_t n2) {
  const unsigned char *p1;
  const unsigned char *p2;

  if (n1 == 0)
    return n2 == 0 ? 0 : -1;

  if (n2 == 0)
    return n1 == 0 ? 0 : 1;

  p1 = (const unsigned char *) s1 + n1 - 1;
  p2 = (const unsigned char *) s2 + n2 - 1;

  while (*p1 == *p2) {
    if (p1 == s1)
      return (p2 == s2) ? 0 : -1;
    if (p2 == s2)
      return (p1 == p2) ? 0 : 1;
    p1--;
    p2--;
  }
  return *p1 - *p2;
}

class cow_btree {
 public:
  int fd;
  char *path;
#define BT_FIXPADDING    0x01   /* internal */
  unsigned int flags;
  bt_cmp_func cmp; /* user compare function */
  struct bt_head head;
  struct bt_meta meta;
  struct page_cache *page_cache;
  struct lru_queue *lru_queue;
  struct cow_btree_txn *txn; /* current write transaction */
  int ref; /* increased by cursors & txn */
  struct cow_btree_stat stat;
  off_t size; /* current file size */

  int cow_btree_open_fd(int _fd, unsigned int _flags) {
    int fl;

    fl = fcntl(_fd, F_GETFL, 0);
    if (fcntl(_fd, F_SETFL, fl | O_APPEND) == -1) {
      perror("file open");
      return BT_FAIL;
    }

    fd = _fd;
    flags = _flags;
    flags &= ~BT_FIXPADDING;
    ref = 1;
    meta.root = P_INVALID;

    if ((page_cache = new struct page_cache()) == NULL)
      goto fail;
    stat.max_cache = BT_MAXCACHE_DEF;
    RB_INIT(page_cache);

    if ((lru_queue = new struct lru_queue()) == NULL)
      goto fail;
    TAILQ_INIT(lru_queue);

    if (cow_btree_read_header() != 0) {
      if (errno != ENOENT)
        goto fail;
      DPRINTF("new database");
      cow_btree_write_header(fd);
    }

    if (cow_btree_read_meta(NULL) != 0)
      goto fail;

    return BT_SUCCESS;

    fail: delete lru_queue;
    delete page_cache;
    return BT_FAIL;
  }

  cow_btree(int _fd, unsigned int _flags) {
    if (cow_btree_open_fd(_fd, _flags) == BT_FAIL) {
      perror("btree construction failed");
    }
  }

  ~cow_btree() {
    cow_btree_close();
  }

  cow_btree(const char *_path, unsigned int _flags, mode_t _mode) {
    int fd, oflags;

    if (F_ISSET(_flags, BT_RDONLY))
      oflags = O_RDONLY;
    else
      oflags = O_RDWR | O_CREAT | O_APPEND;

    if ((fd = open(_path, oflags, _mode)) == -1)
      return;

    if (cow_btree_open_fd(fd, _flags) == BT_FAIL)
      close(fd);
    else {
      path = strdup(_path);
      DPRINTF("opened btree");
    }
  }

  cow_btree(const char *_path) {
    int fd, oflags;
    unsigned int _flags = 0 | BT_NOSYNC;
    mode_t _mode = 0644;

    if (F_ISSET(_flags, BT_RDONLY))
      oflags = O_RDONLY;
    else
      oflags = O_RDWR | O_CREAT | O_APPEND;

    if ((fd = open(_path, oflags, _mode)) == -1)
      return;

    if (cow_btree_open_fd(fd, _flags) == BT_FAIL)
      close(fd);
    else {
      path = strdup(_path);
      DPRINTF("opened btree");
    }
  }

  int cow_btree_cmp(const struct cow_btval *a, const struct cow_btval *b) {
    return cmp(a, b);
  }

  void common_prefix(struct btkey *min, struct btkey *max, struct btkey *pfx) {
    size_t n = 0;
    char *p1;
    char *p2;

    if (min->len == 0 || max->len == 0) {
      pfx->len = 0;
      return;
    }

    if (F_ISSET(flags, BT_REVERSEKEY)) {
      p1 = min->str + min->len - 1;
      p2 = max->str + max->len - 1;

      while (*p1 == *p2) {
        if (p1 < min->str || p2 < max->str)
          break;
        p1--;
        p2--;
        n++;
      }

      assert(n <= (int )sizeof(pfx->str));
      pfx->len = n;
      bcopy(p2 + 1, pfx->str, n);
    } else {
      p1 = min->str;
      p2 = max->str;

      while (*p1 == *p2) {
        if (n == min->len || n == max->len)
          break;
        p1++;
        p2++;
        n++;
      }

      assert(n <= (int )sizeof(pfx->str));
      pfx->len = n;
      bcopy(max->str, pfx->str, n);
    }
  }

  void remove_prefix(struct cow_btval *key, size_t pfxlen) {
    if (pfxlen == 0 || cmp != NULL)
      return;

    DPRINTF("removing %zu bytes of prefix from key [%.*s]", pfxlen,
            (int )key->size, (char * )key->data);
    assert(pfxlen <= key->size);
    key->size -= pfxlen;
    if (!F_ISSET(flags, BT_REVERSEKEY))
      key->data = (char *) key->data + pfxlen;
  }

  void expand_prefix(struct mpage *mp, indx_t indx, struct btkey *expkey) {
    struct cow_node *cow_node;

    cow_node = NODEPTR(mp, indx);
    expkey->len = sizeof(expkey->str);
    concat_prefix(mp->prefix.str, mp->prefix.len, (char*) NODEKEY(cow_node),
                  cow_node->ksize, expkey->str, &expkey->len);
  }

  int bt_cmp(const struct cow_btval *key1, const struct cow_btval *key2,
             struct btkey *pfx) {
    if (F_ISSET(flags, BT_REVERSEKEY))
      return memnrcmp(key1->data, key1->size - pfx->len, key2->data, key2->size);
    else
      return memncmp((char *) key1->data + pfx->len, key1->size - pfx->len,
                     key2->data, key2->size);
  }

  void btval_reset(struct cow_btval *btv) {
    if (btv) {
      if (btv->mp)
        btv->mp->ref--;
      if (btv->release_data)
        delete (char*) btv->data;
      bzero(btv, sizeof(*btv));
    }
  }

  struct mpage* mpage_lookup(pgno_t pgno) {
    struct mpage find, *mp;

    find.pgno = pgno;
    mp = RB_FIND(page_cache, page_cache, &find);
    if (mp) {
      stat.hits++;
      /* Update LRU queue. Move page to the end. */
      TAILQ_REMOVE(lru_queue, mp, lru_next);
      TAILQ_INSERT_TAIL(lru_queue, mp, lru_next);
    }
    return mp;
  }

  void mpage_add(struct mpage *mp) {
    assert(RB_INSERT(page_cache, page_cache, mp) == NULL);
    stat.cache_size++;
    TAILQ_INSERT_TAIL(lru_queue, mp, lru_next);
  }

  void mpage_release(struct mpage *mp) {
    if (mp != NULL) {
      delete mp->page;
      delete mp;
    }
  }

  void mpage_del(struct mpage *mp) {
    assert(RB_REMOVE(page_cache, page_cache, mp) == mp);
    assert(stat.cache_size > 0);
    stat.cache_size--;
    TAILQ_REMOVE(lru_queue, mp, lru_next);
  }

  void mpage_flush() {
    struct mpage *mp;

    while ((mp = RB_MIN(page_cache, page_cache)) != NULL) {
      mpage_del(mp);
      mpage_release(mp);
    }
  }

  struct mpage* mpage_copy(struct mpage *mp) {
    struct mpage *copy;

    if ((copy = new mpage()) == NULL)
      return NULL;
    if ((copy->page = (page*) new char[head.psize]) == NULL) {
      delete copy;
      return NULL;
    }
    bcopy(mp->page, copy->page, head.psize);
    bcopy(&mp->prefix, &copy->prefix, sizeof(mp->prefix));
    copy->parent = mp->parent;
    copy->parent_index = mp->parent_index;
    copy->pgno = mp->pgno;

    return copy;
  }

  /* Remove the least recently used memory pages until the cache size is
   * within the configured bounds. Pages referenced by cursors or returned
   * key/data are not pruned.
   */
  void mpage_prune() {
    struct mpage *mp, *next;

    for (mp = TAILQ_FIRST(lru_queue); mp; mp = next) {
      if (stat.cache_size <= stat.max_cache)
        break;
      next = TAILQ_NEXT(mp, lru_next);
      if (!mp->dirty && mp->ref <= 0) {
        mpage_del(mp);
        mpage_release(mp);
      }
    }
  }

  /* Mark a page as dirty and push it on the dirty queue.
   */
  void mpage_dirty(struct mpage *mp) {
    assert(txn != NULL);

    if (!mp->dirty) {
      mp->dirty = 1;
      SIMPLEQ_INSERT_TAIL(txn->dirty_queue, mp, next);
    }
  }

  /* Touch a page: make it dirty and re-insert into tree with updated pgno.
   */
  struct mpage* mpage_touch(struct mpage *mp) {
    assert(txn != NULL);
    assert(mp != NULL);

    if (!mp->dirty) {
      DPRINTF("touching page %u -> %u", mp->pgno, txn->next_pgno);
      if (mp->ref == 0)
        mpage_del(mp);
      else {
        if ((mp = mpage_copy(mp)) == NULL)
          return NULL;
      }
      mp->pgno = mp->page->pgno = txn->next_pgno++;
      mpage_dirty(mp);
      mpage_add(mp);

      /* Update the page number to new touched page. */
      if (mp->parent != NULL)
        NODEPGNO(NODEPTR(mp->parent,
                mp->parent_index)) = mp->pgno;
    }

    return mp;
  }

  int cow_btree_read_page(pgno_t pgno, struct page *page) {
    ssize_t rc;

    DPRINTF("reading page %u", pgno);
    stat.reads++;
    if ((rc = pread(fd, page, head.psize, (off_t) pgno * head.psize)) == 0) {
      DPRINTF("page %u doesn't exist", pgno);
      errno = ENOENT;
      return BT_FAIL;
    } else if (rc != (ssize_t) head.psize) {
      if (rc > 0)
        errno = EINVAL;
      DPRINTF("read: %s", strerror(errno));
      return BT_FAIL;
    }

    if (page->pgno != pgno) {
      DPRINTF("page numbers don't match: %u != %u", pgno, page->pgno);
      errno = EINVAL;
      return BT_FAIL;
    }

    DPRINTF("page %u has flags 0x%X", pgno, page->flags);

    return BT_SUCCESS;
  }

  int cow_btree_sync() {
    if (!F_ISSET(flags, BT_NOSYNC))
      return fsync(fd);
    return 0;
  }

  struct cow_btree_txn* txn_begin(int rdonly) {
    struct cow_btree_txn *_txn;

    if (!rdonly && txn != NULL) {
      DPRINTF("write transaction already begun");
      errno = EBUSY;
      return NULL;
    }

    if ((_txn = new cow_btree_txn()) == NULL) {
      DPRINTF("new: %s", strerror(errno));
      return NULL;
    }

    if (rdonly) {
      _txn->flags |= BT_TXN_RDONLY;
    } else {
      _txn->dirty_queue = new dirty_queue();
      if (_txn->dirty_queue == NULL) {
        delete _txn;
        return NULL;
      }
      SIMPLEQ_INIT(_txn->dirty_queue);

      DPRINTF("taking write lock on txn %p", _txn);
      if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
        DPRINTF("flock: %s", strerror(errno));
        errno = EBUSY;
        delete _txn->dirty_queue;
        delete _txn;
        return NULL;
      }
      txn = _txn;
    }

    cow_btree_ref();

    if (cow_btree_read_meta(&_txn->next_pgno) != BT_SUCCESS) {
      cow_btree_txn_abort(_txn);
      return NULL;
    }

    _txn->root = meta.root;
    DPRINTF("begin transaction on btree root page %u", _txn->root);

    return _txn;
  }

  void cow_btree_txn_abort(struct cow_btree_txn *_txn) {
    struct mpage *mp;

    if (_txn == NULL)
      return;

    DPRINTF("abort transaction on root page %u", _txn->root);

    if (!F_ISSET(_txn->flags, BT_TXN_RDONLY)) {
      /* Discard all dirty pages.
       */
      while (!SIMPLEQ_EMPTY(_txn->dirty_queue)) {
        mp = SIMPLEQ_FIRST(_txn->dirty_queue);
        assert(mp->ref == 0); /* cursors should be closed */
        mpage_del(mp);
        SIMPLEQ_REMOVE_HEAD(_txn->dirty_queue, next);
        mpage_release(mp);
      }

      DPRINTF("releasing write lock on txn %p", _txn);
      txn = NULL;
      if (flock(fd, LOCK_UN) != 0) {
        DPRINTF("failed to unlock fd %d: %s", fd, strerror(errno));
      }
      delete _txn->dirty_queue;
    }

    cow_btree_close();
    delete _txn;
  }

  int txn_commit(struct cow_btree_txn *_txn) {
    int n, done;
    ssize_t rc;
    off_t size;
    struct mpage *mp;
    struct iovec iov[BT_COMMIT_PAGES];

    assert(_txn != NULL);

    if (F_ISSET(_txn->flags, BT_TXN_RDONLY)) {
      DPRINTF("attempt to commit read-only transaction");
      cow_btree_txn_abort(_txn);
      errno = EPERM;
      return BT_FAIL;
    }

    if (F_ISSET(_txn->flags, BT_TXN_ERROR)) {
      DPRINTF("error flag is set, can't commit");
      cow_btree_txn_abort(_txn);
      errno = EINVAL;
      return BT_FAIL;
    }

    if (SIMPLEQ_EMPTY(_txn->dirty_queue))
      goto done;

    if (F_ISSET(flags, BT_FIXPADDING)) {
      size = lseek(fd, 0, SEEK_END);
      size += head.psize - (size % head.psize);
      DPRINTF("extending to multiple of page size: %ld", size);
      if (ftruncate(fd, size) != 0) {
        DPRINTF("ftruncate: %s", strerror(errno));
        cow_btree_txn_abort(_txn);
        return BT_FAIL;
      }
      flags &= ~BT_FIXPADDING;
    }

    DPRINTF("committing transaction on btree, root page %u", _txn->root);

    /* Commit up to BT_COMMIT_PAGES dirty pages to disk until done.
     */
    do {
      n = 0;
      done = 1;
      SIMPLEQ_FOREACH(mp, _txn->dirty_queue, next)
      {
        DPRINTF("commiting page %u", mp->pgno);
        iov[n].iov_len = head.psize;
        iov[n].iov_base = mp->page;
        if (++n >= BT_COMMIT_PAGES) {
          done = 0;
          break;
        }
      }

      if (n == 0)
        break;

      DPRINTF("commiting %u dirty pages", n);
      rc = writev(fd, iov, n);
      if (rc != (ssize_t) head.psize * n) {
        if (rc > 0)
          DPRINTF("short write, filesystem full?");
        else
          DPRINTF("writev: %s", strerror(errno));
        cow_btree_txn_abort(_txn);
        return BT_FAIL;
      }

      /* Remove the dirty flag from the written pages.
       */
      while (!SIMPLEQ_EMPTY(_txn->dirty_queue)) {
        mp = SIMPLEQ_FIRST(_txn->dirty_queue);
        mp->dirty = 0;
        SIMPLEQ_REMOVE_HEAD(_txn->dirty_queue, next);
        if (--n == 0)
          break;
      }
    } while (!done);

    if (cow_btree_sync() != 0
        || cow_btree_write_meta(_txn->root, 0) != BT_SUCCESS
        || cow_btree_sync() != 0) {
      cow_btree_txn_abort(_txn);
      return BT_FAIL;
    }

    done: mpage_prune();
    cow_btree_txn_abort(_txn);

    return BT_SUCCESS;
  }

  int cow_btree_write_header(int fd) {
    struct stat sb;
    struct bt_head *h;
    struct page *p;
    ssize_t rc;
    unsigned int psize;

    DPRINTF("writing header page");

    /* Ask stat for 'optimal blocksize for I/O'.
     */
    if (fstat(fd, &sb) == 0)
      psize = sb.st_blksize;
    else
      psize = PAGESIZE;

    if ((p = (page*) new char[psize]()) == NULL)
      return -1;
    p->flags = P_HEAD;

    h = (bt_head*) METADATA(p);
    h->magic = BT_MAGIC;
    h->version = BT_VERSION;
    h->psize = psize;
    bcopy(h, &head, sizeof(*h));

    rc = write(fd, p, head.psize);
    delete p;
    if (rc != (ssize_t) head.psize) {
      if (rc > 0)
        DPRINTF("short write, filesystem full?");
      return BT_FAIL;
    }

    return BT_SUCCESS;
  }

  int cow_btree_read_header() {
    char page[PAGESIZE];
    struct page *p;
    struct bt_head *h;
    int rc;

    /* We don't know the page size yet, so use a minimum value.
     */

    if ((rc = pread(fd, page, PAGESIZE, 0)) == 0) {
      errno = ENOENT;
      return -1;
    } else if (rc != PAGESIZE) {
      if (rc > 0)
        errno = EINVAL;
      DPRINTF("read: %s", strerror(errno));
      return -1;
    }

    p = (struct page *) page;

    if (!F_ISSET(p->flags, P_HEAD)) {
      DPRINTF("page %d not a header page", p->pgno);
      errno = EINVAL;
      return -1;
    }

    h = (bt_head*) METADATA(p);
    if (h->magic != BT_MAGIC) {
      DPRINTF("header has invalid magic");
      errno = EINVAL;
      return -1;
    }

    if (h->version != BT_VERSION) {
      DPRINTF("database is version %u, expected version %u", head.version,
              BT_VERSION);
      errno = EINVAL;
      return -1;
    }

    bcopy(h, &head, sizeof(*h));
    return 0;
  }

  int cow_btree_write_meta(pgno_t root, unsigned int flags) {
    struct mpage *mp;
    struct bt_meta *_meta;
    ssize_t rc;

    DPRINTF("writing meta page for root page %u", root);

    assert(txn != NULL);

    if ((mp = cow_btree_new_page(P_META)) == NULL)
      return -1;

    meta.prev_meta = meta.root;
    meta.root = root;
    meta.flags = flags;
    meta.created_at = time(0);
    meta.revisions++;

    /* Copy the meta data changes to the new meta page. */
    _meta = (bt_meta*) METADATA(mp->page);
    bcopy(&meta, _meta, sizeof(*_meta));

    rc = write(fd, mp->page, head.psize);
    mp->dirty = 0;
    SIMPLEQ_REMOVE_HEAD(txn->dirty_queue, next);
    if (rc != (ssize_t) head.psize) {
      if (rc > 0)
        DPRINTF("short write, filesystem full?");
      return BT_FAIL;
    }

    if ((size = lseek(fd, 0, SEEK_END)) == -1) {
      DPRINTF("failed to update file size: %s", strerror(errno));
      size = 0;
    }

    return BT_SUCCESS;
  }

  /* Returns true if page p is a valid meta page, false otherwise.
   */
  int cow_btree_is_meta_page(struct page *p) {
    struct bt_meta *m;

    m = (bt_meta*) METADATA(p);
    if (!F_ISSET(p->flags, P_META)) {
      DPRINTF("page %d not a meta page", p->pgno);
      errno = EINVAL;
      return 0;
    }

    if (m->root >= p->pgno && m->root != P_INVALID) {
      DPRINTF("page %d points to an invalid root page", p->pgno);
      errno = EINVAL;
      return 0;
    }

    return 1;
  }

  int cow_btree_read_meta(pgno_t *p_next) {
    struct mpage *mp;
    struct bt_meta *_meta;
    pgno_t meta_pgno, next_pgno;
    off_t _size;

    if ((_size = lseek(fd, 0, SEEK_END)) == -1)
      goto fail;

    DPRINTF("cow_btree_read_meta: size = %ld", _size);

    if (_size < size) {
      DPRINTF("file has shrunk!");
      errno = EIO;
      goto fail;
    }

    if (_size == head.psize) { /* there is only the header */
      if (p_next != NULL)
        *p_next = 1;
      return BT_SUCCESS; /* new file */
    }

    next_pgno = _size / head.psize;
    if (next_pgno == 0) {
      DPRINTF("corrupt file");
      errno = EIO;
      goto fail;
    }

    meta_pgno = next_pgno - 1;

    if (_size % head.psize != 0) {
      DPRINTF("filesize not a multiple of the page size!");
      flags |= BT_FIXPADDING;
      next_pgno++;
    }

    if (p_next != NULL)
      *p_next = next_pgno;

    if (_size == size) {
      DPRINTF("size unchanged, keeping current meta page");
      if (F_ISSET(meta.flags, BT_TOMBSTONE)) {
        DPRINTF("file is dead");
        errno = ESTALE;
        return BT_FAIL;
      } else
        return BT_SUCCESS;
    }
    size = _size;

    while (meta_pgno > 0) {
      if ((mp = cow_btree_get_mpage(meta_pgno)) == NULL)
        break;
      if (cow_btree_is_meta_page(mp->page)) {
        _meta = (bt_meta*) METADATA(mp->page);
        DPRINTF("flags = 0x%x", _meta->flags);
        if (F_ISSET(_meta->flags, BT_TOMBSTONE)) {
          DPRINTF("file is dead");
          errno = ESTALE;
          return BT_FAIL;
        } else {
          /* Make copy of last meta page. */
          bcopy(_meta, &meta, sizeof(meta));
          return BT_SUCCESS;
        }
      }
      --meta_pgno; /* scan backwards to first valid meta page */
    }

    errno = EIO;
    fail: if (p_next != NULL)
      *p_next = P_INVALID;
    return BT_FAIL;
  }

  void cow_btree_ref() {
    ref++;
    DPRINTF("ref is now %d", ref);
  }

  void cow_btree_close() {
    if (--ref == 0) {
      DPRINTF("ref is zero, closing btree");
      close(fd);
      mpage_flush();
      delete page_cache;
    } else
      DPRINTF("ref is now %d ", ref);
  }

  /* Search for key within a leaf page, using binary search.
   * Returns the smallest entry larger or equal to the key.
   * If exactp is non-null, stores whether the found entry was an exact match
   * in *exactp (1 or 0).
   * If kip is non-null, stores the index of the found entry in *kip.
   * If no entry larger of equal to the key is found, returns NULL.
   */
  struct cow_node* cow_btree_search_node(struct mpage *mp,
                                         struct cow_btval *key, int *exactp,
                                         unsigned int *kip) {
    unsigned int i = 0;
    int low, high;
    int rc = 0;
    struct cow_node *cow_node;
    struct cow_btval nodekey;

    DPRINTF("searching %lu keys in %s page %u with prefix [%.*s]", NUMKEYS(mp),
            IS_LEAF(mp) ? "leaf" : "branch", mp->pgno, (int )mp->prefix.len,
            (char * )mp->prefix.str);

    assert(NUMKEYS(mp) > 0);

    bzero(&nodekey, sizeof(nodekey));

    low = IS_LEAF(mp) ? 0 : 1;
    high = NUMKEYS(mp) - 1;
    while (low <= high) {
      i = (low + high) >> 1;
      cow_node = NODEPTR(mp, i);

      nodekey.size = cow_node->ksize;
      nodekey.data = NODEKEY(cow_node);

      if (cmp)
        rc = cmp(key, &nodekey);
      else
        rc = bt_cmp(key, &nodekey, &mp->prefix);

      if (IS_LEAF(mp))
        DPRINTF("found leaf index %u [%.*s], rc = %i", i, (int )nodekey.size,
                (char * )nodekey.data, rc);
      else
        DPRINTF("found branch index %u [%.*s -> %u], rc = %i", i,
                (int )cow_node->ksize, (char *)NODEKEY(cow_node),
                cow_node->n_pgno, rc);

      if (rc == 0)
        break;
      if (rc > 0)
        low = i + 1;
      else
        high = i - 1;
    }

    if (rc > 0) { /* Found entry is less than the key. */
      i++; /* Skip to get the smallest entry larger than key. */
      if (i >= NUMKEYS(mp))
        /* There is no entry larger or equal to the key. */
        return NULL;
    }
    if (exactp)
      *exactp = (rc == 0);
    if (kip) /* Store the key index if requested. */
      *kip = i;

    return NODEPTR(mp, i);
  }

  void cursor_pop_page(struct cursor *cursor) {
    struct ppage *top;

    top = CURSOR_TOP(cursor);
    CURSOR_POP(cursor);
    top->mpage->ref--;

    DPRINTF("popped page %u off cursor %p", top->mpage->pgno, cursor);

    delete top;
  }

  struct ppage *
  cursor_push_page(struct cursor *cursor, struct mpage *mp) {
    struct ppage *ppage;

    DPRINTF("pushing page %u on cursor %p", mp->pgno, cursor);

    if ((ppage = new struct ppage()) == NULL)
      return NULL;
    ppage->mpage = mp;
    mp->ref++;
    CURSOR_PUSH(cursor, ppage);
    return ppage;
  }

  struct mpage * cow_btree_get_mpage(pgno_t pgno) {
    struct mpage *mp;

    mp = mpage_lookup(pgno);
    if (mp == NULL) {
      if ((mp = new mpage()) == NULL)
        return NULL;
      if ((mp->page = (page*) new char[head.psize]) == NULL) {
        delete mp;
        return NULL;
      }
      if (cow_btree_read_page(pgno, mp->page) != BT_SUCCESS) {
        mpage_release(mp);
        return NULL;
      }
      mp->pgno = pgno;
      mpage_add(mp);
    } else
      DPRINTF("returning page %u from cache", pgno);

    return mp;
  }

  void concat_prefix(char *s1, size_t n1, char *s2, size_t n2, char *cs,
                     size_t *cn) {
    assert(*cn >= n1 + n2);
    if (F_ISSET(flags, BT_REVERSEKEY)) {
      bcopy(s2, cs, n2);
      bcopy(s1, cs + n2, n1);
    } else {
      bcopy(s1, cs, n1);
      bcopy(s2, cs + n1, n2);
    }
    *cn = n1 + n2;
  }

  void find_common_prefix(struct mpage *mp) {
    indx_t lbound = 0, ubound = 0;
    struct mpage *lp, *up;
    struct btkey lprefix, uprefix;

    mp->prefix.len = 0;
    if (cmp != NULL)
      return;

    lp = mp;
    while (lp->parent != NULL) {
      if (lp->parent_index > 0) {
        lbound = lp->parent_index;
        break;
      }
      lp = lp->parent;
    }

    up = mp;
    while (up->parent != NULL) {
      if (up->parent_index + 1 < (indx_t) NUMKEYS(up->parent)) {
        ubound = up->parent_index + 1;
        break;
      }
      up = up->parent;
    }

    if (lp->parent != NULL && up->parent != NULL) {
      expand_prefix(lp->parent, lbound, &lprefix);
      expand_prefix(up->parent, ubound, &uprefix);
      common_prefix(&lprefix, &uprefix, &mp->prefix);
    } else if (mp->parent)
      bcopy(&mp->parent->prefix, &mp->prefix, sizeof(mp->prefix));

    DPRINTF("found common prefix [%.*s] (len %zu) for page %u",
            (int )mp->prefix.len, mp->prefix.str, mp->prefix.len, mp->pgno);
  }

  int cow_btree_search_page_root(struct mpage *root, struct cow_btval *key,
                                 struct cursor *cursor, int modify,
                                 struct mpage **mpp) {
    struct mpage *mp, *parent;

    if (cursor && cursor_push_page(cursor, root) == NULL)
      return BT_FAIL;

    mp = root;
    while (IS_BRANCH(mp)) {
      unsigned int i = 0;
      struct cow_node *cow_node;

      DPRINTF("branch page %u has %lu keys", mp->pgno, NUMKEYS(mp));
      assert(NUMKEYS(mp) > 1);
      DPRINTF("found index 0 to page %u", NODEPGNO(NODEPTR(mp, 0)));

      if (key == NULL) /* Initialize cursor to first page. */
        i = 0;
      else {
        int exact;
        cow_node = cow_btree_search_node(mp, key, &exact, &i);
        if (cow_node == NULL)
          i = NUMKEYS(mp) - 1;
        else if (!exact) {
          assert(i > 0);
          i--;
        }
      }

      if (key)
        DPRINTF("following index %u for key %.*s", i, (int )key->size,
                (char * )key->data);
      assert(i >= 0 && i < NUMKEYS(mp));
      cow_node = NODEPTR(mp, i);

      if (cursor)
        CURSOR_TOP(cursor)->ki = i;

      parent = mp;
      if ((mp = cow_btree_get_mpage(NODEPGNO(cow_node))) == NULL)
        return BT_FAIL;
      mp->parent = parent;
      mp->parent_index = i;
      find_common_prefix(mp);

      if (cursor && cursor_push_page(cursor, mp) == NULL)
        return BT_FAIL;

      if (modify && (mp = mpage_touch(mp)) == NULL)
        return BT_FAIL;
    }

    if (!IS_LEAF(mp)) {
      DPRINTF("internal error, index points to a %02X page!?", mp->page->flags);
      return BT_FAIL;
    }

    DPRINTF("found leaf page %u for key %.*s", mp->pgno,
            key ? (int )key->size : 0, key ? (char *)key->data : NULL);

    *mpp = mp;
    return BT_SUCCESS;
  }

  /* Search for the page a given key should be in.
   * Stores a pointer to the found page in *mpp.
   * If key is NULL, search for the lowest page (used by cow_btree_cursor_first).
   * If cursor is non-null, pushes parent pages on the cursor stack.
   * If modify is true, visited pages are updated with new page numbers.
   */
  int cow_btree_search_page(struct cow_btree_txn *txn, struct cow_btval *key,
                            struct cursor *cursor, int modify,
                            struct mpage **mpp) {
    int rc;
    pgno_t root;
    struct mpage *mp;

    /* Can't modify pages outside a transaction. */
    if (txn == NULL && modify) {
      errno = EINVAL;
      return BT_FAIL;
    }

    /* Choose which root page to start with. If a transaction is given
     * use the root page from the transaction, otherwise read the last
     * committed root page.
     */
    if (txn == NULL) {
      if ((rc = cow_btree_read_meta(NULL)) != BT_SUCCESS)
        return rc;
      root = meta.root;
    } else if (F_ISSET(txn->flags, BT_TXN_ERROR)) {
      DPRINTF("transaction has failed, must abort");
      errno = EINVAL;
      return BT_FAIL;
    } else
      root = txn->root;

    if (root == P_INVALID) { /* Tree is empty. */
      DPRINTF("tree is empty");
      errno = ENOENT;
      return BT_FAIL;
    }

    if ((mp = cow_btree_get_mpage(root)) == NULL)
      return BT_FAIL;

    DPRINTF("root page has flags 0x%X", mp->page->flags);

    assert(mp->parent == NULL);
    assert(mp->prefix.len == 0);

    if (modify && !mp->dirty) {
      if ((mp = mpage_touch(mp)) == NULL)
        return BT_FAIL;
      txn->root = mp->pgno;
    }

    return cow_btree_search_page_root(mp, key, cursor, modify, mpp);
  }

  int cow_btree_read_data(struct mpage *mp, struct cow_node *leaf,
                          struct cow_btval *data) {
    struct mpage *omp; /* overflow mpage */
    size_t psz;
    size_t max;
    size_t sz = 0;
    pgno_t pgno;

    bzero(data, sizeof(*data));
    max = head.psize - PAGEHDRSZ;

    if (!F_ISSET(leaf->flags, F_BIGDATA)) {
      data->size = leaf->n_dsize;
      if (data->size > 0) {
        if (mp == NULL) {
          if ((data->data = new char[data->size]) == NULL)
            return BT_FAIL;
          bcopy(NODEDATA(leaf), data->data, data->size);
          data->release_data = 1;
          data->mp = NULL;
        } else {
          data->data = NODEDATA(leaf);
          data->release_data = 0;
          data->mp = mp;
          mp->ref++;
        }
      }
      return BT_SUCCESS;
    }

    /* Read overflow data.
     */
    DPRINTF("allocating %u byte for overflow data", leaf->n_dsize);
    if ((data->data = new char[leaf->n_dsize]) == NULL)
      return BT_FAIL;
    data->size = leaf->n_dsize;
    data->release_data = 1;
    data->mp = NULL;
    bcopy(NODEDATA(leaf), &pgno, sizeof(pgno));
    for (sz = 0; sz < data->size;) {
      if ((omp = cow_btree_get_mpage(pgno)) == NULL
          || !F_ISSET(omp->page->flags, P_OVERFLOW)) {
        DPRINTF("read overflow page %u failed", pgno);
        delete (char*) data->data;
        mpage_release(omp);
        return BT_FAIL;
      }
      psz = data->size - sz;
      if (psz > max)
        psz = max;
      bcopy(omp->page->ptrs, (char *) data->data + sz, psz);
      sz += psz;
      pgno = omp->page->p_next_pgno;
    }

    return BT_SUCCESS;
  }

  int at(struct cow_btree_txn *_txn, struct cow_btval *key,
         struct cow_btval *data) {
    int rc, exact;
    struct cow_node *leaf;
    struct mpage *mp;

    assert(key);
    assert(data);
    DPRINTF("===> get key [%.*s]", (int )key->size, (char * )key->data);

    if (key->size == 0 || key->size > MAXKEYSIZE) {
      errno = EINVAL;
      return BT_FAIL;
    }

    if ((rc = cow_btree_search_page(_txn, key, NULL, 0, &mp)) != BT_SUCCESS)
      return rc;

    leaf = cow_btree_search_node(mp, key, &exact, NULL);
    if (leaf && exact)
      rc = cow_btree_read_data(mp, leaf, data);
    else {
      errno = ENOENT;
      rc = BT_FAIL;
    }

    mpage_prune();
    return rc;
  }

  int cow_btree_sibling(struct cursor *cursor, int move_right) {
    int rc;
    struct cow_node *indx;
    struct ppage *parent, *top;
    struct mpage *mp;

    top = CURSOR_TOP(cursor);
    if ((parent = SLIST_NEXT(top, entry)) == NULL) {
      errno = ENOENT;
      return BT_FAIL; /* root has no siblings */
    }

    DPRINTF("parent page is page %u, index %u", parent->mpage->pgno,
            parent->ki);

    cursor_pop_page(cursor);
    if (move_right ?
        (parent->ki + 1 >= NUMKEYS(parent->mpage)) : (parent->ki == 0)) {
      DPRINTF("no more keys left, moving to %s sibling",
              move_right ? "right" : "left");
      if ((rc = cow_btree_sibling(cursor, move_right)) != BT_SUCCESS)
        return rc;
      parent = CURSOR_TOP(cursor);
    } else {
      if (move_right)
        parent->ki++;
      else
        parent->ki--;
      DPRINTF("just moving to %s index key %u", move_right ? "right" : "left",
              parent->ki);
    }
    assert(IS_BRANCH(parent->mpage));

    indx = NODEPTR(parent->mpage, parent->ki);
    if ((mp = cow_btree_get_mpage(indx->n_pgno)) == NULL)
      return BT_FAIL;
    mp->parent = parent->mpage;
    mp->parent_index = parent->ki;

    cursor_push_page(cursor, mp);
    find_common_prefix(mp);

    return BT_SUCCESS;
  }

  int bt_set_key(struct mpage *mp, struct cow_node *cow_node,
                 struct cow_btval *key) {
    if (key == NULL)
      return 0;

    if (mp->prefix.len > 0) {
      key->size = cow_node->ksize + mp->prefix.len;
      key->data = new char[key->size];
      if (key->data == NULL)
        return -1;
      concat_prefix(mp->prefix.str, mp->prefix.len, (char*) NODEKEY(cow_node),
                    cow_node->ksize, (char*) key->data, &key->size);
      key->release_data = 1;
    } else {
      key->size = cow_node->ksize;
      key->data = NODEKEY(cow_node);
      key->release_data = 0;
      key->mp = mp;
      mp->ref++;
    }

    return 0;
  }

  int cow_btree_cursor_next(struct cursor *cursor, struct cow_btval *key,
                            struct cow_btval *data) {
    struct ppage *top;
    struct mpage *mp;
    struct cow_node *leaf;

    if (cursor->eof) {
      errno = ENOENT;
      return BT_FAIL;
    }

    assert(cursor->initialized);

    top = CURSOR_TOP(cursor);
    mp = top->mpage;

    DPRINTF("cursor_next: top page is %u in cursor %p", mp->pgno, cursor);

    if (top->ki + 1 >= NUMKEYS(mp)) {
      DPRINTF("=====> move to next sibling page");
      if (cow_btree_sibling(cursor, 1) != BT_SUCCESS) {
        cursor->eof = 1;
        return BT_FAIL;
      }
      top = CURSOR_TOP(cursor);
      mp = top->mpage;
      DPRINTF("next page is %u, key index %u", mp->pgno, top->ki);
    } else
      top->ki++;

    DPRINTF("==> cursor points to page %u with %lu keys, key index %u",
            mp->pgno, NUMKEYS(mp), top->ki);

    assert(IS_LEAF(mp));
    leaf = NODEPTR(mp, top->ki);

    if (data && cow_btree_read_data(mp, leaf, data) != BT_SUCCESS)
      return BT_FAIL;

    if (bt_set_key(mp, leaf, key) != 0)
      return BT_FAIL;

    return BT_SUCCESS;
  }

  int cow_btree_cursor_set(struct cursor *cursor, struct cow_btval *key,
                           struct cow_btval *data, int *exactp) {
    int rc;
    struct cow_node *leaf;
    struct mpage *mp;
    struct ppage *top;

    assert(cursor);
    assert(key);
    assert(key->size > 0);

    rc = cow_btree_search_page(cursor->txn, key, cursor, 0, &mp);
    if (rc != BT_SUCCESS)
      return rc;
    assert(IS_LEAF(mp));

    top = CURSOR_TOP(cursor);
    leaf = cow_btree_search_node(mp, key, exactp, &top->ki);
    if (exactp != NULL && !*exactp) {
      /* BT_CURSOR_EXACT specified and not an exact match. */
      errno = ENOENT;
      return BT_FAIL;
    }

    if (leaf == NULL) {
      DPRINTF("===> inexact leaf not found, goto sibling");
      if (cow_btree_sibling(cursor, 1) != BT_SUCCESS)
        return BT_FAIL; /* no entries matched */
      top = CURSOR_TOP(cursor);
      top->ki = 0;
      mp = top->mpage;
      assert(IS_LEAF(mp));
      leaf = NODEPTR(mp, 0);
    }

    cursor->initialized = 1;
    cursor->eof = 0;

    if (data && cow_btree_read_data(mp, leaf, data) != BT_SUCCESS)
      return BT_FAIL;

    if (bt_set_key(mp, leaf, key) != 0)
      return BT_FAIL;
    DPRINTF("==> cursor placed on key %.*s", (int )key->size,
            (char * )key->data);

    return BT_SUCCESS;
  }

  int cow_btree_cursor_first(struct cursor *cursor, struct cow_btval *key,
                             struct cow_btval *data) {
    int rc;
    struct mpage *mp;
    struct cow_node *leaf;

    rc = cow_btree_search_page(cursor->txn, NULL, cursor, 0, &mp);
    if (rc != BT_SUCCESS)
      return rc;
    assert(IS_LEAF(mp));

    leaf = NODEPTR(mp, 0);
    cursor->initialized = 1;
    cursor->eof = 0;

    if (data && cow_btree_read_data(mp, leaf, data) != BT_SUCCESS)
      return BT_FAIL;

    if (bt_set_key(mp, leaf, key) != 0)
      return BT_FAIL;

    return BT_SUCCESS;
  }

  int cow_btree_cursor_get(struct cursor *cursor, struct cow_btval *key,
                           struct cow_btval *data, enum cursor_op op) {
    int rc;
    int exact = 0;

    assert(cursor);

    switch (op) {
      case BT_CURSOR:
      case BT_CURSOR_EXACT:
        while (CURSOR_TOP(cursor) != NULL)
          cursor_pop_page(cursor);
        if (key == NULL || key->size == 0 || key->size > MAXKEYSIZE) {
          errno = EINVAL;
          rc = BT_FAIL;
        } else if (op == BT_CURSOR_EXACT)
          rc = cow_btree_cursor_set(cursor, key, data, &exact);
        else
          rc = cow_btree_cursor_set(cursor, key, data, NULL);
        break;
      case BT_NEXT:
        if (!cursor->initialized)
          rc = cow_btree_cursor_first(cursor, key, data);
        else
          rc = cow_btree_cursor_next(cursor, key, data);
        break;
      case BT_FIRST:
        while (CURSOR_TOP(cursor) != NULL)
          cursor_pop_page(cursor);
        rc = cow_btree_cursor_first(cursor, key, data);
        break;
      default:
        DPRINTF("unhandled/unimplemented cursor operation %u", op);
        rc = BT_FAIL;
        break;
    }

    mpage_prune();

    return rc;
  }

  struct mpage* cow_btree_new_page(uint32_t flags) {
    struct mpage *mp;

    assert(txn != NULL);

    DPRINTF("allocating new mpage %u, page size %u", txn->next_pgno,
            head.psize);
    if ((mp = new mpage()) == NULL)
      return NULL;
    if ((mp->page = (page*) new char[head.psize]) == NULL) {
      delete mp;
      return NULL;
    }
    mp->pgno = mp->page->pgno = txn->next_pgno++;
    mp->page->flags = flags;
    mp->page->lower = PAGEHDRSZ;
    mp->page->upper = head.psize;

    if (IS_BRANCH(mp))
      meta.branch_pages++;
    else if (IS_LEAF(mp))
      meta.leaf_pages++;
    else if (IS_OVERFLOW(mp))
      meta.overflow_pages++;

    mpage_add(mp);
    mpage_dirty(mp);

    return mp;
  }

  size_t bt_leaf_size(struct cow_btval *key, struct cow_btval *data) {
    size_t sz;

    sz = LEAFSIZE(key, data);
    if (data->size >= head.psize / BT_MINKEYS) {
      /* put on overflow page */
      sz -= data->size - sizeof(pgno_t);
    }

    return sz + sizeof(indx_t);
  }

  size_t bt_branch_size(struct cow_btval *key) {
    size_t sz;

    sz = INDXSIZE(key);
    if (sz >= head.psize / BT_MINKEYS) {
      /* put on overflow page */
      /* not implemented */
      /* sz -= key->size - sizeof(pgno_t); */
    }

    return sz + sizeof(indx_t);
  }

  int cow_btree_write_overflow_data(struct page *p, struct cow_btval *data) {
    size_t done = 0;
    size_t sz;
    size_t max;
    pgno_t *linkp; /* linked page stored here */
    struct mpage *next = NULL;

    max = head.psize - PAGEHDRSZ;

    while (done < data->size) {
      if (next != NULL)
        p = next->page;
      linkp = &p->p_next_pgno;
      if (data->size - done > max) {
        /* need another overflow page */
        if ((next = cow_btree_new_page(P_OVERFLOW)) == NULL)
          return BT_FAIL;
        *linkp = next->pgno;
        DPRINTF("linking overflow page %u", next->pgno);
      } else
        *linkp = 0; /* indicates end of list */
      sz = data->size - done;
      if (sz > max)
        sz = max;
      DPRINTF("copying %zu bytes to overflow page %u", sz, p->pgno);
      bcopy((char *) data->data + done, p->ptrs, sz);
      done += sz;
    }

    return BT_SUCCESS;
  }

  /* Key prefix should already be stripped.
   */
  int cow_btree_add_node(struct mpage *mp, indx_t indx, struct cow_btval *key,
                         struct cow_btval *data, pgno_t pgno, uint8_t flags) {
    unsigned int i;
    size_t node_size = NODESIZE;
    indx_t ofs;
    struct cow_node *cow_node;
    struct page *p;
    struct mpage *ofp = NULL; /* overflow page */

    p = mp->page;
    assert(p->upper >= p->lower);

    DPRINTF("add cow_node [%.*s] to %s page %u at index %i, key size %zu",
            key ? (int )key->size : 0, key ? (char *)key->data : NULL,
            IS_LEAF(mp) ? "leaf" : "branch", mp->pgno, indx,
            key ? key->size : 0);

    if (key != NULL)
      node_size += key->size;

    if (IS_LEAF(mp)) {
      assert(data);
      node_size += data->size;
      if (F_ISSET(flags, F_BIGDATA)) {
        /* Data already on overflow page. */
        node_size -= data->size - sizeof(pgno_t);
      } else if (data->size >= head.psize / BT_MINKEYS) {
        /* Put data on overflow page. */
        DPRINTF("data size is %zu, put on overflow page", data->size);
        node_size -= data->size - sizeof(pgno_t);
        if ((ofp = cow_btree_new_page(P_OVERFLOW)) == NULL)
          return BT_FAIL;
        DPRINTF("allocated overflow page %u", ofp->pgno);
        flags |= F_BIGDATA;
      }
    }

    if (node_size + sizeof(indx_t) > SIZELEFT(mp)) {
      DPRINTF("not enough room in page %u, got %lu ptrs", mp->pgno,
              NUMKEYS(mp));
      DPRINTF("upper - lower = %u - %u = %u", p->upper, p->lower,
              p->upper - p->lower);
      DPRINTF("cow_node size = %zu", node_size);
      return BT_FAIL;
    }

    /* Move higher pointers up one slot. */
    for (i = NUMKEYS(mp); i > indx; i--)
      p->ptrs[i] = p->ptrs[i - 1];

    /* Adjust free space offsets. */
    ofs = p->upper - node_size;
    assert(ofs >= p->lower + sizeof(indx_t));
    p->ptrs[indx] = ofs;
    p->upper = ofs;
    p->lower += sizeof(indx_t);

    /* Write the cow_node data. */
    cow_node = NODEPTR(mp, indx);
    cow_node->ksize = (key == NULL) ? 0 : key->size;
    cow_node->flags = flags;
    if (IS_LEAF(mp))
      cow_node->n_dsize = data->size;
    else
      cow_node->n_pgno = pgno;

    if (key)
      bcopy(key->data, NODEKEY(cow_node), key->size);

    if (IS_LEAF(mp)) {
      assert(key);
      if (ofp == NULL) {
        if (F_ISSET(flags, F_BIGDATA))
          bcopy(data->data, cow_node->data + key->size, sizeof(pgno_t));
        else
          bcopy(data->data, cow_node->data + key->size, data->size);
      } else {
        bcopy(&ofp->pgno, cow_node->data + key->size, sizeof(pgno_t));
        if (cow_btree_write_overflow_data(ofp->page, data) == BT_FAIL)
          return BT_FAIL;
      }
    }

    return BT_SUCCESS;
  }

  void cow_btree_del_node(struct mpage *mp, indx_t indx) {
    unsigned int sz;
    indx_t i, j, numkeys, ptr;
    struct cow_node *cow_node;
    char *base;

    DPRINTF("delete cow_node %u on %s page %u", indx,
            IS_LEAF(mp) ? "leaf" : "branch", mp->pgno);
    assert(indx < NUMKEYS(mp));

    cow_node = NODEPTR(mp, indx);
    sz = NODESIZE + cow_node->ksize;
    if (IS_LEAF(mp)) {
      if (F_ISSET(cow_node->flags, F_BIGDATA))
        sz += sizeof(pgno_t);
      else
        sz += NODEDSZ(cow_node);
    }

    ptr = mp->page->ptrs[indx];
    numkeys = NUMKEYS(mp);
    for (i = j = 0; i < numkeys; i++) {
      if (i != indx) {
        mp->page->ptrs[j] = mp->page->ptrs[i];
        if (mp->page->ptrs[i] < ptr)
          mp->page->ptrs[j] += sz;
        j++;
      }
    }

    base = (char *) mp->page + mp->page->upper;
    bcopy(base, base + sz, ptr - mp->page->upper);

    mp->page->lower -= sizeof(indx_t);
    mp->page->upper += sz;
  }

  struct cursor* cow_btree_txn_cursor_open(struct cow_btree_txn *txn) {
    struct cursor *cursor;

    if ((cursor = new struct cursor()) != NULL) {
      SLIST_INIT(&cursor->stack);
      cursor->txn = txn;
      cow_btree_ref();
    }

    return cursor;
  }

  void cow_btree_cursor_close(struct cursor *cursor) {
    if (cursor != NULL) {
      while (!CURSOR_EMPTY(cursor))
        cursor_pop_page(cursor);

      cow_btree_close();
      delete cursor;
    }
  }

  int cow_btree_update_key(struct mpage *mp, indx_t indx,
                           struct cow_btval *key) {
    indx_t ptr, i, numkeys;
    int delta;
    size_t len;
    struct cow_node *cow_node;
    char *base;

    cow_node = NODEPTR(mp, indx);
    ptr = mp->page->ptrs[indx];
    DPRINTF("update key %u (ofs %u) [%.*s] to [%.*s] on page %u", indx, ptr,
            (int )cow_node->ksize, (char *)NODEKEY(cow_node), (int )key->size,
            (char * )key->data, mp->pgno);

    if (key->size != cow_node->ksize) {
      delta = key->size - cow_node->ksize;
      if (delta > 0 && SIZELEFT(mp) < delta) {
        DPRINTF("OUCH! Not enough room, delta = %d", delta);
        return BT_FAIL;
      }

      numkeys = NUMKEYS(mp);
      for (i = 0; i < numkeys; i++) {
        if (mp->page->ptrs[i] <= ptr)
          mp->page->ptrs[i] -= delta;
      }

      base = (char *) mp->page + mp->page->upper;
      len = ptr - mp->page->upper + NODESIZE;
      bcopy(base, base - delta, len);
      mp->page->upper -= delta;

      cow_node = NODEPTR(mp, indx);
      cow_node->ksize = key->size;
    }

    bcopy(key->data, NODEKEY(cow_node), key->size);

    return BT_SUCCESS;
  }

  int cow_btree_adjust_prefix(struct mpage *src, int delta) {
    indx_t i;
    struct cow_node *cow_node;
    struct btkey tmpkey;
    struct cow_btval key;

    DPRINTF("adjusting prefix lengths on page %u with delta %d", src->pgno,
            delta);
    assert(delta != 0);

    for (i = 0; i < NUMKEYS(src); i++) {
      cow_node = NODEPTR(src, i);
      tmpkey.len = cow_node->ksize - delta;
      if (delta > 0) {
        if (F_ISSET(flags, BT_REVERSEKEY))
          bcopy(NODEKEY(cow_node), tmpkey.str, tmpkey.len);
        else
          bcopy((char *) NODEKEY(cow_node) + delta, tmpkey.str, tmpkey.len);
      } else {
        if (F_ISSET(flags, BT_REVERSEKEY)) {
          bcopy(NODEKEY(cow_node), tmpkey.str, cow_node->ksize);
          bcopy(src->prefix.str, tmpkey.str + cow_node->ksize, -delta);
        } else {
          bcopy(src->prefix.str + src->prefix.len + delta, tmpkey.str, -delta);
          bcopy(NODEKEY(cow_node), tmpkey.str - delta, cow_node->ksize);
        }
      }
      key.size = tmpkey.len;
      key.data = tmpkey.str;
      if (cow_btree_update_key(src, i, &key) != BT_SUCCESS)
        return BT_FAIL;
    }

    return BT_SUCCESS;
  }

  /* Move a cow_node from src to dst.
   */
  int cow_btree_move_node(struct mpage *src, indx_t srcindx, struct mpage *dst,
                          indx_t dstindx) {
    int rc;
    unsigned int pfxlen, mp_pfxlen = 0;
    struct cow_node *srcnode;
    struct mpage *mp = NULL;
    struct btkey tmpkey, srckey;
    struct cow_btval key, data;

    assert(src->parent);
    assert(dst->parent);

    srcnode = NODEPTR(src, srcindx);
    DPRINTF("moving %s cow_node %u [%.*s] on page %u to cow_node %u on page %u",
            IS_LEAF(src) ? "leaf" : "branch", srcindx, (int )srcnode->ksize,
            (char *)NODEKEY(srcnode), src->pgno, dstindx, dst->pgno);

    find_common_prefix(src);

    if (IS_BRANCH(src)) {
      /* Need to check if the page the moved cow_node points to
       * changes prefix.
       */
      if ((mp = cow_btree_get_mpage(NODEPGNO(srcnode))) == NULL)
        return BT_FAIL;
      mp->parent = src;
      mp->parent_index = srcindx;
      find_common_prefix(mp);
      mp_pfxlen = mp->prefix.len;
    }

    /* Mark src and dst as dirty. */
    if ((src = mpage_touch(src)) == NULL || (dst = mpage_touch(dst)) == NULL)
      return BT_FAIL;

    find_common_prefix(dst);

    /* Check if src cow_node has destination page prefix. Otherwise the
     * destination page must expand its prefix on all its nodes.
     */
    srckey.len = srcnode->ksize;
    bcopy(NODEKEY(srcnode), srckey.str, srckey.len);
    common_prefix(&srckey, &dst->prefix, &tmpkey);
    if (tmpkey.len != dst->prefix.len) {
      if (cow_btree_adjust_prefix(dst,
                                  tmpkey.len - dst->prefix.len) != BT_SUCCESS)
        return BT_FAIL;
      bcopy(&tmpkey, &dst->prefix, sizeof(tmpkey));
    }

    if (srcindx == 0 && IS_BRANCH(src)) {
      struct mpage *low;

      /* must find the lowest key below src
       */
      assert(
          cow_btree_search_page_root(src, NULL, NULL, 0, &low) == BT_SUCCESS);
      expand_prefix(low, 0, &srckey);
      DPRINTF("found lowest key [%.*s] on leaf page %u", (int )srckey.len,
              srckey.str, low->pgno);
    } else {
      srckey.len = srcnode->ksize;
      bcopy(NODEKEY(srcnode), srckey.str, srcnode->ksize);
    }
    find_common_prefix(src);

    /* expand the prefix */
    tmpkey.len = sizeof(tmpkey.str);
    concat_prefix(src->prefix.str, src->prefix.len, srckey.str, srckey.len,
                  tmpkey.str, &tmpkey.len);

    /* Add the cow_node to the destination page. Adjust prefix for
     * destination page.
     */
    key.size = tmpkey.len;
    key.data = tmpkey.str;
    remove_prefix(&key, dst->prefix.len);
    data.size = NODEDSZ(srcnode);
    data.data = NODEDATA(srcnode);
    rc = cow_btree_add_node(dst, dstindx, &key, &data, NODEPGNO(srcnode),
                            srcnode->flags);
    if (rc != BT_SUCCESS)
      return rc;

    /* Delete the cow_node from the source page.
     */
    cow_btree_del_node(src, srcindx);

    /* Update the parent separators.
     */
    if (srcindx == 0 && src->parent_index != 0) {
      expand_prefix(src, 0, &tmpkey);
      key.size = tmpkey.len;
      key.data = tmpkey.str;
      remove_prefix(&key, src->parent->prefix.len);

      DPRINTF("update separator for source page %u to [%.*s]", src->pgno,
              (int )key.size, (char * )key.data);
      if (cow_btree_update_key(src->parent, src->parent_index,
                               &key) != BT_SUCCESS)
        return BT_FAIL;
    }

    if (srcindx == 0 && IS_BRANCH(src)) {
      struct cow_btval nullkey;
      nullkey.size = 0;
      assert(cow_btree_update_key(src, 0, &nullkey) == BT_SUCCESS);
    }

    if (dstindx == 0 && dst->parent_index != 0) {
      expand_prefix(dst, 0, &tmpkey);
      key.size = tmpkey.len;
      key.data = tmpkey.str;
      remove_prefix(&key, dst->parent->prefix.len);

      DPRINTF("update separator for destination page %u to [%.*s]", dst->pgno,
              (int )key.size, (char * )key.data);
      if (cow_btree_update_key(dst->parent, dst->parent_index,
                               &key) != BT_SUCCESS)
        return BT_FAIL;
    }

    if (dstindx == 0 && IS_BRANCH(dst)) {
      struct cow_btval nullkey;
      nullkey.size = 0;
      assert(cow_btree_update_key(dst, 0, &nullkey) == BT_SUCCESS);
    }

    /* We can get a new page prefix here!
     * Must update keys in all nodes of this page!
     */
    pfxlen = src->prefix.len;
    find_common_prefix(src);
    if (src->prefix.len != pfxlen) {
      if (cow_btree_adjust_prefix(src, src->prefix.len - pfxlen) != BT_SUCCESS)
        return BT_FAIL;
    }

    pfxlen = dst->prefix.len;
    find_common_prefix(dst);
    if (dst->prefix.len != pfxlen) {
      if (cow_btree_adjust_prefix(dst, dst->prefix.len - pfxlen) != BT_SUCCESS)
        return BT_FAIL;
    }

    if (IS_BRANCH(dst)) {
      assert(mp);
      mp->parent = dst;
      mp->parent_index = dstindx;
      find_common_prefix(mp);
      if (mp->prefix.len != mp_pfxlen) {
        DPRINTF("moved branch cow_node has changed prefix");
        if ((mp = mpage_touch(mp)) == NULL)
          return BT_FAIL;
        if (cow_btree_adjust_prefix(mp,
                                    mp->prefix.len - mp_pfxlen) != BT_SUCCESS)
          return BT_FAIL;
      }
    }

    return BT_SUCCESS;
  }

  int cow_btree_merge(struct mpage *src, struct mpage *dst) {
    int rc;
    indx_t i;
    unsigned int pfxlen;
    struct cow_node *srcnode;
    struct btkey tmpkey, dstpfx;
    struct cow_btval key, data;

    DPRINTF("merging page %u and %u", src->pgno, dst->pgno);

    assert(src->parent); /* can't merge root page */
    assert(dst->parent);
    assert(txn != NULL);

    /* Mark src and dst as dirty. */
    if ((src = mpage_touch(src)) == NULL || (dst = mpage_touch(dst)) == NULL)
      return BT_FAIL;

    find_common_prefix(src);
    find_common_prefix(dst);

    /* Check if source nodes has destination page prefix. Otherwise
     * the destination page must expand its prefix on all its nodes.
     */
    common_prefix(&src->prefix, &dst->prefix, &dstpfx);
    if (dstpfx.len != dst->prefix.len) {
      if (cow_btree_adjust_prefix(dst,
                                  dstpfx.len - dst->prefix.len) != BT_SUCCESS)
        return BT_FAIL;
      bcopy(&dstpfx, &dst->prefix, sizeof(dstpfx));
    }

    /* Move all nodes from src to dst.
     */
    for (i = 0; i < NUMKEYS(src); i++) {
      srcnode = NODEPTR(src, i);

      /* If branch cow_node 0 (implicit key), find the real key.
       */
      if (i == 0 && IS_BRANCH(src)) {
        struct mpage *low;

        /* must find the lowest key below src
         */
        assert(
            cow_btree_search_page_root(src, NULL, NULL, 0, &low) == BT_SUCCESS);
        expand_prefix(low, 0, &tmpkey);
        DPRINTF("found lowest key [%.*s] on leaf page %u", (int )tmpkey.len,
                tmpkey.str, low->pgno);
      } else {
        expand_prefix(src, i, &tmpkey);
      }

      key.size = tmpkey.len;
      key.data = tmpkey.str;

      remove_prefix(&key, dst->prefix.len);
      data.size = NODEDSZ(srcnode);
      data.data = NODEDATA(srcnode);
      rc = cow_btree_add_node(dst, NUMKEYS(dst), &key, &data, NODEPGNO(srcnode),
                              srcnode->flags);
      if (rc != BT_SUCCESS)
        return rc;
    }

    DPRINTF("dst page %u now has %lu keys (%.1f%% filled)", dst->pgno,
            NUMKEYS(dst), (float)PAGEFILL(dst) / 10);

    /* Unlink the src page from parent.
     */
    cow_btree_del_node(src->parent, src->parent_index);
    if (src->parent_index == 0) {
      key.size = 0;
      if (cow_btree_update_key(src->parent, 0, &key) != BT_SUCCESS)
        return BT_FAIL;

      pfxlen = src->prefix.len;
      find_common_prefix(src);
      assert(src->prefix.len == pfxlen);
    }

    if (IS_LEAF(src))
      meta.leaf_pages--;
    else
      meta.branch_pages--;

    return cow_btree_rebalance(src->parent);
  }

#define FILL_THRESHOLD   250

  int cow_btree_rebalance(struct mpage *mp) {
    struct cow_node *cow_node;
    struct mpage *parent;
    struct mpage *root;
    struct mpage *neighbor = NULL;
    indx_t si = 0, di = 0;

    assert(txn != NULL);
    assert(mp != NULL);

    DPRINTF("rebalancing %s page %u (has %lu keys, %.1f%% full)",
            IS_LEAF(mp) ? "leaf" : "branch", mp->pgno, NUMKEYS(mp),
            (float)PAGEFILL(mp) / 10);

    if (PAGEFILL(mp) >= FILL_THRESHOLD) {
      DPRINTF("no need to rebalance page %u, above fill threshold", mp->pgno);
      return BT_SUCCESS;
    }

    parent = mp->parent;

    if (parent == NULL) {
      if (NUMKEYS(mp) == 0) {
        DPRINTF("tree is completely empty");
        txn->root = P_INVALID;
        meta.depth--;
        meta.leaf_pages--;
      } else if (IS_BRANCH(mp) && NUMKEYS(mp) == 1) {
        DPRINTF("collapsing root page!");
        txn->root = NODEPGNO(NODEPTR(mp, 0));
        if ((root = cow_btree_get_mpage(txn->root)) == NULL)
          return BT_FAIL;
        root->parent = NULL;
        meta.depth--;
        meta.branch_pages--;
      } else
        DPRINTF("root page doesn't need rebalancing");
      return BT_SUCCESS;
    }

    /* The parent (branch page) must have at least 2 pointers,
     * otherwise the tree is invalid.
     */
    assert(NUMKEYS(parent) > 1);

    /* Leaf page fill factor is below the threshold.
     * Try to move keys from left or right neighbor, or
     * merge with a neighbor page.
     */

    /* Find neighbors.
     */
    if (mp->parent_index == 0) {
      /* We're the leftmost leaf in our parent.
       */
      DPRINTF("reading right neighbor");
      cow_node = NODEPTR(parent, mp->parent_index + 1);
      if ((neighbor = cow_btree_get_mpage(NODEPGNO(cow_node))) == NULL)
        return BT_FAIL;
      neighbor->parent_index = mp->parent_index + 1;
      si = 0;
      di = NUMKEYS(mp);
    } else {
      /* There is at least one neighbor to the left.
       */
      DPRINTF("reading left neighbor");
      cow_node = NODEPTR(parent, mp->parent_index - 1);
      if ((neighbor = cow_btree_get_mpage(NODEPGNO(cow_node))) == NULL)
        return BT_FAIL;
      neighbor->parent_index = mp->parent_index - 1;
      si = NUMKEYS(neighbor) - 1;
      di = 0;
    }
    neighbor->parent = parent;

    DPRINTF("found neighbor page %u (%lu keys, %.1f%% full)", neighbor->pgno,
            NUMKEYS(neighbor), (float)PAGEFILL(neighbor) / 10);

    /* If the neighbor page is above threshold and has at least two
     * keys, move one key from it.
     *
     * Otherwise we should try to merge them, but that might not be
     * possible, even if both are below threshold, as prefix expansion
     * might make keys larger. FIXME: detect this
     */
    if (PAGEFILL(neighbor) >= FILL_THRESHOLD && NUMKEYS(neighbor) >= 2)
      return cow_btree_move_node(neighbor, si, mp, di);
    else { /* FIXME: if (has_enough_room()) */
      if (mp->parent_index == 0)
        return cow_btree_merge(neighbor, mp);
      else
        return cow_btree_merge(mp, neighbor);
    }
  }

  int remove(struct cow_btree_txn *_txn, struct cow_btval *key,
             struct cow_btval *data) {
    int rc, exact, close_txn = 0;
    unsigned int ki;
    struct cow_node *leaf;
    struct mpage *mp;

    DPRINTF("========> delete key %.*s", (int )key->size, (char * )key->data);

    assert(key != NULL);

    if (_txn != NULL && F_ISSET(_txn->flags, BT_TXN_RDONLY)) {
      errno = EINVAL;
      return BT_FAIL;
    }

    if (key->size == 0 || key->size > MAXKEYSIZE) {
      errno = EINVAL;
      return BT_FAIL;
    }

    if (_txn == NULL) {
      close_txn = 1;
      if ((_txn = txn_begin(0)) == NULL)
        return BT_FAIL;
    }

    if ((rc = cow_btree_search_page(_txn, key, NULL, 1, &mp)) != BT_SUCCESS)
      goto done;

    leaf = cow_btree_search_node(mp, key, &exact, &ki);
    if (leaf == NULL || !exact) {
      errno = ENOENT;
      rc = BT_FAIL;
      goto done;
    }

    if (data && (rc = cow_btree_read_data(NULL, leaf, data)) != BT_SUCCESS)
      goto done;

    cow_btree_del_node(mp, ki);
    meta.entries--;
    rc = cow_btree_rebalance(mp);
    if (rc != BT_SUCCESS)
      _txn->flags |= BT_TXN_ERROR;

    done: if (close_txn) {
      if (rc == BT_SUCCESS)
        rc = txn_commit(_txn);
      else
        cow_btree_txn_abort(_txn);
    }
    mpage_prune();
    return rc;
  }

  /* Reduce the length of the prefix separator <sep> to the minimum length that
   * still makes it uniquely distinguishable from <min>.
   *
   * <min> is guaranteed to be sorted less than <sep>
   *
   * On return, <sep> is modified to the minimum length.
   */
  void bt_reduce_separator(struct cow_node *min, struct cow_btval *sep) {
    size_t n = 0;
    char *p1;
    char *p2;

    if (F_ISSET(flags, BT_REVERSEKEY)) {

      assert(sep->size > 0);

      p1 = (char *) NODEKEY(min) + min->ksize - 1;
      p2 = (char *) sep->data + sep->size - 1;

      while (p1 >= (char *) NODEKEY(min) && *p1 == *p2) {
        assert(p2 > (char * )sep->data);
        p1--;
        p2--;
        n++;
      }

      sep->data = p2;
      sep->size = n + 1;
    } else {

      assert(min->ksize > 0);
      assert(sep->size > 0);

      p1 = (char *) NODEKEY(min);
      p2 = (char *) sep->data;

      while (*p1 == *p2) {
        p1++;
        p2++;
        n++;
        if (n == min->ksize || n == sep->size)
          break;
      }

      sep->size = n + 1;
    }

    DPRINTF("reduced separator to [%.*s] > [%.*s]", (int )sep->size,
            (char * )sep->data, (int )min->ksize, (char *)NODEKEY(min));
  }

  /* Split page <*mpp>, and insert <key,(data|newpgno)> in either left or
   * right sibling, at index <*newindxp> (as if unsplit). Updates *mpp and
   * *newindxp with the actual values after split, ie if *mpp and *newindxp
   * refer to a cow_node in the new right sibling page.
   */
  int cow_btree_split(struct mpage **mpp, unsigned int *newindxp,
                      struct cow_btval *newkey, struct cow_btval *newdata,
                      pgno_t newpgno) {
    uint8_t flags;
    int rc = BT_SUCCESS, ins_new = 0;
    indx_t newindx;
    pgno_t pgno = 0;
    size_t orig_pfx_len, left_pfx_diff, right_pfx_diff, pfx_diff;
    unsigned int i, j, split_indx;
    struct cow_node *cow_node;
    struct mpage *pright, *p, *mp;
    struct cow_btval sepkey, rkey, rdata;
    struct btkey tmpkey;
    struct page *copy;

    assert(txn != NULL);

    mp = *mpp;
    newindx = *newindxp;

    DPRINTF("-----> splitting %s page %u and adding [%.*s] at index %i",
            IS_LEAF(mp) ? "leaf" : "branch", mp->pgno, (int )newkey->size,
            (char * )newkey->data, *newindxp);
    DPRINTF("page %u has prefix [%.*s]", mp->pgno, (int )mp->prefix.len,
            (char * )mp->prefix.str);
    orig_pfx_len = mp->prefix.len;

    if (mp->parent == NULL) {
      if ((mp->parent = cow_btree_new_page(P_BRANCH)) == NULL)
        return BT_FAIL;
      mp->parent_index = 0;
      txn->root = mp->parent->pgno;
      DPRINTF("root split! new root = %u", mp->parent->pgno);
      meta.depth++;

      /* Add left (implicit) pointer. */
      if (cow_btree_add_node(mp->parent, 0, NULL, NULL, mp->pgno,
                             0) != BT_SUCCESS)
        return BT_FAIL;
    } else {
      DPRINTF("parent branch page is %u", mp->parent->pgno);
    }

    /* Create a right sibling. */
    if ((pright = cow_btree_new_page(mp->page->flags)) == NULL)
      return BT_FAIL;
    pright->parent = mp->parent;
    pright->parent_index = mp->parent_index + 1;
    DPRINTF("new right sibling: page %u", pright->pgno);

    /* Move half of the keys to the right sibling. */
    if ((copy = (page*) new char[head.psize]) == NULL)
      return BT_FAIL;
    bcopy(mp->page, copy, head.psize);
    assert(mp->ref == 0); /* XXX */
    bzero(&mp->page->ptrs, head.psize - PAGEHDRSZ);
    mp->page->lower = PAGEHDRSZ;
    mp->page->upper = head.psize;

    split_indx = NUMKEYSP(copy) / 2 + 1;

    /* First find the separating key between the split pages.
     */
    bzero(&sepkey, sizeof(sepkey));
    if (newindx == split_indx) {
      sepkey.size = newkey->size;
      sepkey.data = newkey->data;
      remove_prefix(&sepkey, mp->prefix.len);
    } else {
      cow_node = NODEPTRP(copy, split_indx);
      sepkey.size = cow_node->ksize;
      sepkey.data = NODEKEY(cow_node);
    }

    if (IS_LEAF(mp) && cmp == NULL) {
      /* Find the smallest separator. */
      /* Ref: Prefix B-trees, R. Bayer, K. Unterauer, 1977 */
      cow_node = NODEPTRP(copy, split_indx - 1);
      bt_reduce_separator(cow_node, &sepkey);
    }

    /* Fix separator wrt parent prefix. */
    if (cmp == NULL) {
      tmpkey.len = sizeof(tmpkey.str);
      concat_prefix(mp->prefix.str, mp->prefix.len, (char*) sepkey.data,
                    sepkey.size, tmpkey.str, &tmpkey.len);
      sepkey.data = tmpkey.str;
      sepkey.size = tmpkey.len;
    }

    DPRINTF("separator is [%.*s]", (int )sepkey.size, (char * )sepkey.data);

    /* Copy separator key to the parent.
     */
    if (SIZELEFT(pright->parent) < bt_branch_size(&sepkey)) {
      rc = cow_btree_split(&pright->parent, &pright->parent_index, &sepkey,
      NULL,
                           pright->pgno);

      /* Right page might now have changed parent.
       * Check if left page also changed parent.
       */
      if (pright->parent
          != mp->parent&& mp->parent_index >= NUMKEYS(mp->parent)) {
        mp->parent = pright->parent;
        mp->parent_index = pright->parent_index - 1;
      }
    } else {
      remove_prefix(&sepkey, pright->parent->prefix.len);
      rc = cow_btree_add_node(pright->parent, pright->parent_index, &sepkey,
      NULL,
                              pright->pgno, 0);
    }
    if (rc != BT_SUCCESS) {
      delete copy;
      return BT_FAIL;
    }

    /* Update prefix for right and left page, if the parent was split.
     */
    find_common_prefix(pright);
    assert(orig_pfx_len <= pright->prefix.len);
    right_pfx_diff = pright->prefix.len - orig_pfx_len;

    find_common_prefix(mp);
    assert(orig_pfx_len <= mp->prefix.len);
    left_pfx_diff = mp->prefix.len - orig_pfx_len;

    for (i = j = 0; i <= NUMKEYSP(copy); j++) {
      if (i < split_indx) {
        /* Re-insert in left sibling. */
        p = mp;
        pfx_diff = left_pfx_diff;
      } else {
        /* Insert in right sibling. */
        if (i == split_indx)
          /* Reset insert index for right sibling. */
          j = (i == newindx && ins_new);
        p = pright;
        pfx_diff = right_pfx_diff;
      }

      if (i == newindx && !ins_new) {
        /* Insert the original entry that caused the split. */
        rkey.data = newkey->data;
        rkey.size = newkey->size;
        if (IS_LEAF(mp)) {
          rdata.data = newdata->data;
          rdata.size = newdata->size;
        } else
          pgno = newpgno;
        flags = 0;
        pfx_diff = p->prefix.len;

        ins_new = 1;

        /* Update page and index for the new key. */
        *newindxp = j;
        *mpp = p;
      } else if (i == NUMKEYSP(copy)) {
        break;
      } else {
        cow_node = NODEPTRP(copy, i);
        rkey.data = NODEKEY(cow_node);
        rkey.size = cow_node->ksize;
        if (IS_LEAF(mp)) {
          rdata.data = NODEDATA(cow_node);
          rdata.size = cow_node->n_dsize;
        } else
          pgno = cow_node->n_pgno;
        flags = cow_node->flags;

        i++;
      }

      if (!IS_LEAF(mp) && j == 0) {
        /* First branch index doesn't need key data. */
        rkey.size = 0;
      } else
        remove_prefix(&rkey, pfx_diff);

      rc = cow_btree_add_node(p, j, &rkey, &rdata, pgno, flags);
    }

    delete copy;
    return rc;
  }

  int insert(struct cow_btree_txn *txn, struct cow_btval *key,
             struct cow_btval *data, unsigned int flags = 0) {
    int rc = BT_SUCCESS, exact, close_txn = 0;
    unsigned int ki;
    struct cow_node *leaf;
    struct mpage *mp;
    struct cow_btval xkey;

    assert(key != NULL);
    assert(data != NULL);

    if (txn != NULL && F_ISSET(txn->flags, BT_TXN_RDONLY)) {
      errno = EINVAL;
      return BT_FAIL;
    }

    if (key->size == 0 || key->size > MAXKEYSIZE) {
      errno = EINVAL;
      return BT_FAIL;
    }

    DPRINTF("==> put key %.*s, size %zu, data size %zu", (int )key->size,
            (char * )key->data, key->size, data->size);

    if (txn == NULL) {
      close_txn = 1;
      if ((txn = txn_begin(0)) == NULL)
        return BT_FAIL;
    }

    rc = cow_btree_search_page(txn, key, NULL, 1, &mp);
    if (rc == BT_SUCCESS) {
      leaf = cow_btree_search_node(mp, key, &exact, &ki);
      if (leaf && exact) {
        if (F_ISSET(flags, BT_NOOVERWRITE)) {
          DPRINTF("duplicate key %.*s", (int )key->size, (char * )key->data);
          errno = EEXIST;
          rc = BT_FAIL;
          goto done;
        }
        cow_btree_del_node(mp, ki);
      }
      if (leaf == NULL) { /* append if not found */
        ki = NUMKEYS(mp);
        DPRINTF("appending key at index %i", ki);
      }
    } else if (errno == ENOENT) {
      /* new file, just write a root leaf page */
      DPRINTF("allocating new root leaf page");
      if ((mp = cow_btree_new_page(P_LEAF)) == NULL) {
        rc = BT_FAIL;
        goto done;
      }
      txn->root = mp->pgno;
      meta.depth++;
      ki = 0;
    } else
      goto done;

    assert(IS_LEAF(mp));
    DPRINTF("there are %lu keys, should insert new key at index %i",
            NUMKEYS(mp), ki);

    /* Copy the key pointer as it is modified by the prefix code. The
     * caller might have allocated the data.
     */
    xkey.data = key->data;
    xkey.size = key->size;

    if (SIZELEFT(mp) < bt_leaf_size(key, data)) {
      rc = cow_btree_split(&mp, &ki, &xkey, data, P_INVALID);
    } else {
      /* There is room already in this leaf page. */
      remove_prefix(&xkey, mp->prefix.len);
      rc = cow_btree_add_node(mp, ki, &xkey, data, 0, 0);
    }

    if (rc != BT_SUCCESS)
      txn->flags |= BT_TXN_ERROR;
    else
      meta.entries++;

    done: if (close_txn) {
      if (rc == BT_SUCCESS)
        rc = txn_commit(txn);
      else
        cow_btree_txn_abort(txn);
    }
    mpage_prune();
    return rc;
  }

  pgno_t cow_btree_compact_tree(pgno_t pgno, cow_btree *btc) {
    ssize_t rc;
    indx_t i;
    pgno_t *pnext, next;
    struct cow_node *cow_node;
    struct page *p;
    struct mpage *mp;

    /* Get the page and make a copy of it.
     */
    if ((mp = cow_btree_get_mpage(pgno)) == NULL)
      return P_INVALID;
    if ((p = (page*) new char[head.psize]) == NULL)
      return P_INVALID;
    bcopy(mp->page, p, head.psize);

    /* Go through all nodes in the (copied) page and update the
     * page pointers.
     */
    if (F_ISSET(p->flags, P_BRANCH)) {
      for (i = 0; i < NUMKEYSP(p); i++) {
        cow_node = NODEPTRP(p, i);
        cow_node->n_pgno = cow_btree_compact_tree(cow_node->n_pgno, btc);
        if (cow_node->n_pgno == P_INVALID) {
          delete p;
          return P_INVALID;
        }
      }
    } else if (F_ISSET(p->flags, P_LEAF)) {
      for (i = 0; i < NUMKEYSP(p); i++) {
        cow_node = NODEPTRP(p, i);
        if (F_ISSET(cow_node->flags, F_BIGDATA)) {
          bcopy(NODEDATA(cow_node), &next, sizeof(next));
          next = cow_btree_compact_tree(next, btc);
          if (next == P_INVALID) {
            delete p;
            return P_INVALID;
          }
          bcopy(&next, NODEDATA(cow_node), sizeof(next));
        }
      }
    } else if (F_ISSET(p->flags, P_OVERFLOW)) {
      pnext = &p->p_next_pgno;
      if (*pnext > 0) {
        *pnext = cow_btree_compact_tree(*pnext, btc);
        if (*pnext == P_INVALID) {
          delete p;
          return P_INVALID;
        }
      }
    } else
      assert(0);

    pgno = p->pgno = btc->txn->next_pgno++;
    rc = write(btc->fd, p, head.psize);
    delete p;
    if (rc != (ssize_t) head.psize)
      return P_INVALID;
    mpage_prune();
    return pgno;
  }

  // Wrappers
  int at(struct cow_btval *key, struct cow_btval *data) {
    return at(NULL, key, data);
  }

  int insert(struct cow_btval *key, struct cow_btval *data,
             unsigned int flags) {
    return insert(NULL, key, data, flags);
  }

  int remove(struct cow_btval *key, struct cow_btval *data) {
    return remove(NULL, key, data);
  }

  struct cursor* cow_btree_cursor_open() {
    return cow_btree_txn_cursor_open(NULL);
  }

  int compact() {
    std::string compact_path;
    cow_btree *btc;
    struct cow_btree_txn *txn, *txnc = NULL;
    int fd;
    pgno_t root;

    DPRINTF("compacting btree with path %s", path);

    if (path == NULL) {
      errno = EINVAL;
      return BT_FAIL;
    }

    if ((txn = txn_begin(0)) == NULL)
      return BT_FAIL;

    compact_path = std::string(path) + ".compact.XXXXXX";
    fd = mkstemp((char*) compact_path.c_str());
    if (fd == -1) {
      cow_btree_txn_abort(txn);
      return BT_FAIL;
    }

    btc = new cow_btree(fd, 0);
    bcopy(&meta, &btc->meta, sizeof(meta));
    btc->meta.revisions = 0;

    if ((txnc = btc->txn_begin(0)) == NULL)
      goto failed;

    if (meta.root != P_INVALID) {
      root = cow_btree_compact_tree(meta.root, btc);
      if (root == P_INVALID)
        goto failed;
      if (btc->cow_btree_write_meta(root, 0) != BT_SUCCESS)
        goto failed;
    }

    fsync(fd);

    DPRINTF("renaming %s to %s", compact_path.c_str(), path);
    if (rename(compact_path.c_str(), path) != 0)
      goto failed;

    /* Write a "tombstone" meta page so other processes can pick up
     * the change and re-open the file.
     */
    if (cow_btree_write_meta(P_INVALID, BT_TOMBSTONE) != BT_SUCCESS)
      goto failed;

    cow_btree_txn_abort(txn);
    cow_btree_txn_abort(txnc);
    btc->cow_btree_close();
    mpage_prune();
    return 0;

    failed: cow_btree_txn_abort(txn);
    cow_btree_txn_abort(txnc);
    unlink(compact_path.c_str());
    btc->cow_btree_close();
    mpage_prune();
    return BT_FAIL;
  }

  /* Reverts the last change. Truncates the file at the last root page.
   */
  int cow_btree_revert() {
    if (cow_btree_read_meta(NULL) != 0)
      return -1;

    DPRINTF("truncating file at page %u", meta.root);
    return ftruncate(fd, head.psize * meta.root);
  }

  void cow_btree_set_cache_size(unsigned int cache_size) {
    stat.max_cache = cache_size;
  }

  unsigned int cow_btree_get_flags() {
    return (flags & ~BT_FIXPADDING);
  }

  const char* cow_btree_get_path() {
    return path;
  }

  const struct cow_btree_stat* cow_btree_stat() {
    stat.branch_pages = meta.branch_pages;
    stat.leaf_pages = meta.leaf_pages;
    stat.overflow_pages = meta.overflow_pages;
    stat.revisions = meta.revisions;
    stat.depth = meta.depth;
    stat.entries = meta.entries;
    stat.psize = head.psize;
    stat.created_at = meta.created_at;

    return &stat;
  }

};

#endif /* COW_BTREE_H_ */

