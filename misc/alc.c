#include<unistd.h>
#include<stddef.h>
#include<stdlib.h>
#include<sys/mman.h>
#include<pthread.h>
#include<string.h>

/*....................................................*/


static void ERRMSG(char *s) {
  char *p;
  for (p = s; *p; ++p)
    ;
  if (!write(2, s, (size_t) (p - s)))
    exit(1);
  return;
}

static void write_int(int a) {
  char buf[128], *pos, sign;
  int r;
  sign = a < 0 ? '-' : '\0';
  if (a == 0) {
    *(pos = &buf[127]) = '0';
    pos--;
  } else
    for ((r = a < 0 ? -a : a), pos = &buf[127]; r; r = (r - r % 10) / 10)
      *pos-- = (char) ((r % 10) + (int) '0');
  *(pos) = sign ? '-' : ' ';
  if (!write(2, pos, (size_t) (&buf[127] - pos + 1)))
    exit(1);
}

static void write_str(char *s) {
  char *p;
  if (!s)
    return;
  for (p = s; *p; ++p)
    ;
  if (!write(2, s, (size_t) (p - s)))
    exit(1);
}

#define __DBG__(s)  {} /*((s))*/

#ifdef _TRACK_
#define __TRK__(s)  ((s))
#else
#define __TRK__(s)  {}
#endif

/*....................................................*/

static pthread_mutex_t gen_mutex = PTHREAD_MUTEX_INITIALIZER;

#define _MALLOC_LOCK_INIT() {;}
#define _MALLOC_LOCK() {pthread_mutex_lock(&gen_mutex);}
#define _MALLOC_UNLOCK() {pthread_mutex_unlock(&gen_mutex);}

static size_t page_size = 0;

#define MMAP(size)  ( mmap( (void *)0, \
          (size),    \
          PROT_READ|PROT_WRITE, \
          MAP_ANON|MAP_PRIVATE, -1, (off_t)0 ) \
        )

struct heap {
  size_t signature;
  size_t chunk_size;
  int n_chunks, n_free_chunks;
  ptrdiff_t first_free_chunk_off, /* -1 if no free chunks */
  recent_chunk_off;
  size_t heap_size;
  struct heap *prev_heap, *next_heap;
  struct heap_raw *raw;
};

struct heap_raw {
  size_t signature;
  size_t chunk_size;
  int n_heaps;
  struct heap *first_heap, *first_free_heap, *recent_heap, *last_heap;
};

#define _heap_signature_     ((size_t)0x99887766)
#define _heap_raw_signature_ ((size_t)0xffeeddcc)

static
int n_raws = 20;

static struct heap_raw raws[] = { { _heap_raw_signature_, 8, 0, NULL, NULL,
    NULL, NULL }, { _heap_raw_signature_, 16, 0, NULL, NULL, NULL, NULL }, {
    _heap_raw_signature_, 24, 0, NULL, NULL, NULL, NULL }, {
    _heap_raw_signature_, 32, 0, NULL, NULL, NULL, NULL }, {
    _heap_raw_signature_, 40, 0, NULL, NULL, NULL, NULL }, {
    _heap_raw_signature_, 48, 0, NULL, NULL, NULL, NULL }, {
    _heap_raw_signature_, 56, 0, NULL, NULL, NULL, NULL }, {
    _heap_raw_signature_, 64, 0, NULL, NULL, NULL, NULL }, {
    _heap_raw_signature_, 72, 0, NULL, NULL, NULL, NULL }, {
    _heap_raw_signature_, 96, 0, NULL, NULL, NULL, NULL }, {
    _heap_raw_signature_, 128, 0, NULL, NULL, NULL, NULL }, {
    _heap_raw_signature_, 192, 0, NULL, NULL, NULL, NULL }, {
    _heap_raw_signature_, 256, 0, NULL, NULL, NULL, NULL }, {
    _heap_raw_signature_, 320, 0, NULL, NULL, NULL, NULL }, {
    _heap_raw_signature_, 384, 0, NULL, NULL, NULL, NULL }, {
    _heap_raw_signature_, 512, 0, NULL, NULL, NULL, NULL }, {
    _heap_raw_signature_, 768, 0, NULL, NULL, NULL, NULL }, {
    _heap_raw_signature_, 1024, 0, NULL, NULL, NULL, NULL }, {
    _heap_raw_signature_, 1536, 0, NULL, NULL, NULL, NULL }, {
    _heap_raw_signature_, 2048, 0, NULL, NULL, NULL, NULL } };

static struct heap*
new_heap(size_t chunk_size, void* previous_heap, void* next_heap) {
  void *h, *chunk;
  struct heap *hp;
  size_t best_chunk_size, best_heap_size, min_size, min_heap_size, heap_size;
  int i_raw, n_chunks, i_chunk;
  /* __DBG__("DBG: new_heap(): \n"); */
  if (!chunk_size) {
    ERRMSG("ERROR: new_heap(chunk_size = 0, ...)\n");
    return (struct heap*) 0;
  }
  for (i_raw = 0; i_raw < n_raws; i_raw++)
    if (raws[i_raw].chunk_size >= chunk_size)
      break;
  if (i_raw < n_raws)
    best_chunk_size = raws[i_raw].chunk_size;
  else
    best_chunk_size = chunk_size;
  min_heap_size = sizeof(struct heap) + sizeof(ptrdiff_t) + best_chunk_size /**/
      + sizeof(ptrdiff_t)/**/;
  if (min_heap_size < page_size) {
    heap_size = page_size;
    n_chunks = (page_size - sizeof(struct heap))
        / (sizeof(ptrdiff_t) + best_chunk_size /**/+ sizeof(ptrdiff_t)/**/);
  } else {
    heap_size = page_size * (min_heap_size / page_size + 1);
    n_chunks = 1;
  }
  if ((h = MMAP(heap_size)) == MAP_FAILED) {
    ERRMSG("ERROR: new_heap(): mmap() failed\n");
    return (struct heap*) 0;
  }
  hp = (struct heap*) h;
  hp->signature = _heap_signature_;
  hp->chunk_size = best_chunk_size;
  hp->n_chunks = hp->n_free_chunks = n_chunks;
  hp->first_free_chunk_off = hp->recent_chunk_off = sizeof(struct heap)
      + sizeof(ptrdiff_t);
  hp->heap_size = heap_size;
  if (i_raw < n_raws) {
    hp->raw = &raws[i_raw];
    if (!(hp->raw)->first_heap) {
      (hp->raw)->first_heap = (hp->raw)->first_free_heap = (hp->raw)
          ->recent_heap = (hp->raw)->last_heap = hp;
      hp->prev_heap = hp->next_heap = NULL;
    } else {
      (hp->raw)->recent_heap = hp;
      hp->prev_heap = (hp->raw)->last_heap;
      hp->next_heap = NULL;
      if ((hp->raw)->last_heap)
        ((hp->raw)->last_heap)->next_heap = hp;
      (hp->raw)->last_heap = hp;
      if (!(hp->raw)->first_free_heap)
        (hp->raw)->first_free_heap = hp;
    }
    (hp->raw)->n_heaps++;
  } else {
    hp->raw = (struct heap_raw*) 0;
    hp->prev_heap = hp->next_heap = NULL;
  }
  for (chunk = h + sizeof(struct heap);
      chunk + sizeof(ptrdiff_t) + best_chunk_size /**/+ sizeof(ptrdiff_t) /**/
      <= h + heap_size;
      chunk += sizeof(ptrdiff_t) + best_chunk_size /**/+ sizeof(ptrdiff_t)/**/)
    *(ptrdiff_t*) chunk = -(chunk + sizeof(ptrdiff_t) - h);
  return hp;
}

int is_valid_heap(struct heap* hp) {
  /* __DBG__("DBG: is_valid_heap(): \n"); */
  return hp->signature == _heap_signature_ && hp->heap_size >= page_size;
}

int is_free_heap(struct heap* hp) {
  /* __DBG__("DBG: is_free_heap(): \n"); */
  if (is_valid_heap(hp))
    return hp->first_free_chunk_off != -1;
  else
    return 0;
}

struct heap*
find_first_free_heap(struct heap_raw *raw) {
  struct heap *hp;
  /* __DBG__("DBG: find_first_free_heap():\n"); */
  if (!raw) {
    ERRMSG("ERROR: find_first_free_heap(raw = 0)\n");
    return (struct heap*) 0;
  }
  if (raw->signature != _heap_raw_signature_) {
    ERRMSG("ERROR: find_first_free_heap(): invalid heap row\n");
    return (struct heap*) 0;
  }
  for (hp = raw->first_heap; hp; hp = hp->next_heap) {
    if (!is_valid_heap(hp)) {
      ERRMSG("ERROR: find_first_free_heap(): invalid heap found\n");
      return (struct heap*) 0;
    }
    if (is_free_heap(hp))
      break;
  }
  return hp;
}

void*
alloc_chunk(size_t size, struct heap* hp) {
  void *recent_chunk, *first_free_chunk, *chunk, *p;
  /* __DBG__("DBG: alloc_chunk():\n"); */
  if (!is_valid_heap(hp)) {
    ERRMSG("ERROR: alloc_chunk(): invalid heap\n");
    return (void*) 0;
  }
  if (!size) {
    ERRMSG("WARNING: alloc_chunk(size = 0,...)\n");
    return (void*) 0;
  }
  if (!is_free_heap(hp)) {
    ERRMSG("ERROR: alloc_chunk(..., non-free heap)\n");
    return (void*) 0;
  }
  recent_chunk = (void*) hp + hp->recent_chunk_off;
  if (*(ptrdiff_t*) (recent_chunk - sizeof(ptrdiff_t)) < 0) {
    chunk = recent_chunk;
  } else
    chunk = (void*) hp + hp->first_free_chunk_off;
  *(ptrdiff_t*) (chunk - sizeof(ptrdiff_t)) = chunk - (void*) hp;
  hp->recent_chunk_off = chunk - (void*) hp;
  hp->n_free_chunks--;
  for (p = (void*) hp + sizeof(struct heap) + sizeof(ptrdiff_t);
      p + hp->chunk_size /**/+ sizeof(ptrdiff_t) /**/
      <= (void*) hp + hp->heap_size;
      p += hp->chunk_size + sizeof(ptrdiff_t) /**/+ sizeof(ptrdiff_t)/**/
      )
    if (*(ptrdiff_t*) (p - sizeof(ptrdiff_t)) < 0) {
      hp->first_free_chunk_off = p - (void*) hp;
      break;
    }
  if (p + hp->chunk_size/**/+ sizeof(ptrdiff_t)/**/
  > (void*) hp + hp->heap_size) {
    hp->first_free_chunk_off = -1; /* no free chunks */
    if (hp->raw) {
      (hp->raw)->recent_heap = hp;
      if (hp == (hp->raw)->first_free_heap) {
        (hp->raw)->first_free_heap = find_first_free_heap(hp->raw);
      }
    }
  }
  return chunk;
}

void release_heap(struct heap *hp) {
  struct heap_raw *raw;
  /* __DBG__("DBG: release_heap():\n"); */
  if (!hp) {
    ERRMSG("ERROR: release_heap(heap = 0)\n");
    return;
  }
  if (!is_valid_heap(hp)) {
    ERRMSG("ERROR: release_heap(): invalid heap\n");
    return;
  }
  if (hp->n_free_chunks != hp->n_chunks) {
    ERRMSG(
        "ERROR: release_heap(): n_free_chunks != n_chunks. Can't release the heap.\n");
    return;
  }
  if (raw = hp->raw) {
    if (hp->prev_heap)
      (hp->prev_heap)->next_heap = hp->next_heap;
    if (hp->next_heap)
      (hp->next_heap)->prev_heap = hp->prev_heap;
    if (raw->first_heap == hp)
      raw->first_heap = hp->next_heap;
    if (raw->last_heap == hp)
      raw->last_heap = hp->prev_heap;
    if (raw->first_free_heap == hp)
      raw->first_free_heap = find_first_free_heap(raw);
    raw->recent_heap = raw->last_heap;
  }
  if (munmap((void*) hp, hp->heap_size) != 0) {
    ERRMSG("ERROR: release_heap(): munmap() failed\n");
    return;
  }
  if (raw)
    raw->n_heaps--;
  return;
}

void release(void *chunk) {
  struct heap *hp;
  ptrdiff_t off;
  /* __DBG__("DBG: release():\n"); */
  if (!chunk) {
    ERRMSG("ERROR: release(chunk = 0)\n");
    return;
  }
  if ((off = *(ptrdiff_t*) (chunk - sizeof(ptrdiff_t))) < 0) {
    ERRMSG("ERROR: release(): the chunk is already free\n");
    return;
  }
  if (off < sizeof(struct heap) + sizeof(ptrdiff_t)) {
    ERRMSG("ERROR: release(): wrong pointer\n");
    return;
  }
  hp = (struct heap*) (chunk - off);
  if (!is_valid_heap(hp)) {
    ERRMSG("ERROR: release(): wrong pointer\n");
    return;
  }
  *(ptrdiff_t*) (chunk - sizeof(ptrdiff_t)) = -off;
  hp->recent_chunk_off = off;
  if (hp->first_free_chunk_off == -1 || hp->first_free_chunk_off > off)
    hp->first_free_chunk_off = off;
  hp->n_free_chunks++;
  if (hp->n_free_chunks == hp->n_chunks) {
    release_heap(hp);
    return;
  }
  if (hp->raw) {
    (hp->raw)->recent_heap = hp;
    if (!(hp->raw)->first_free_heap)
      (hp->raw)->first_free_heap = hp;
  }
  return;
}

static void*
alloc(size_t size) {
  size_t size_to_alloc, size_to_mmap;
  int i_raw;
  void *chunk;
  struct heap *hp;
  /* __DBG__("DBG: alloc():\n"); */
  if (size == 0)
    return (void*) 0;
  if (page_size == 0)
    page_size = getpagesize();
  for (i_raw = 0; i_raw < n_raws; i_raw++)
    if (raws[i_raw].chunk_size >= size) {
      size_to_alloc = raws[i_raw].chunk_size;
      break;
    }
  if (i_raw == n_raws) {
    if (!(hp = new_heap(size, NULL, NULL))) {
      ERRMSG("ERROR: alloc(): new_heap() returns NULL\n");
      return (void*) 0;
    }
    size_to_alloc = size;
  } else {
    if (raws[i_raw].recent_heap && is_free_heap(raws[i_raw].recent_heap)) {
      hp = raws[i_raw].recent_heap;
    } else if (raws[i_raw].first_free_heap) {
      hp = raws[i_raw].first_free_heap;
    } else {
      if (!(hp = new_heap(raws[i_raw].chunk_size, raws[i_raw].last_heap, NULL))) {
        ERRMSG("ERROR: alloc(): new_heap() returns NULL\n");
        return (void*) 0;
      }
    }
  }
  if (!(chunk = alloc_chunk(size_to_alloc, hp))) {
    ERRMSG("ERROR: alloc(): alloc_chunk() returns NULL\n");
  }
  return chunk;
}

static void*
re_alloc(void *p, size_t size) {
  struct heap *hp;
  ptrdiff_t off;
  void *p_new, *p1, *p2;
  if (!p) {
    if (!size) {
      ERRMSG("WARNING: re_alloc(0,0)\n");
      return (void*) 0;
    }
    p_new = alloc(size);
  } else {
    if (size == 0) {
      release(p);
      return (void*) 0;
    }
    off = *(ptrdiff_t*) (p - sizeof(ptrdiff_t));
    if (off < 0) {
      ERRMSG(
          "ERROR: re_alloc(): attempt to realloc already free chunk? Doing nothing and returning NULL\n");
      return (void*) 0;
    }
    hp = (struct heap*) (p - off);
    if (!is_valid_heap(hp)) {
      ERRMSG(
          "ERROR: re_alloc(): wrong pointer? can't find the heap it belongs to. Doing nothing and returning NULL\n");
      return (void*) 0;
    }
    if (size <= hp->chunk_size) /* Doing nothing */
      return p;
    if ((p_new = alloc(size)) == NULL) {
      ERRMSG("ERROR: re_alloc(): can't allocate new size\n");
      return (void*) 0;
    }
    for (p1 = p, p2 = p_new; p1 < p + hp->chunk_size; p1++, p2++)
      *(char*) p2 = *(char*) p1;
    release(p);
  }
  return p_new;
}

/*..............................................................................*/

void draw_heap(struct heap* hp) {
  int i;
  void *p;
  write_str("heap_size: ");
  write_int(hp->heap_size / page_size);
  write_str(" ");
  for (p = (void*) hp + sizeof(struct heap) + sizeof(ptrdiff_t);
      p + hp->chunk_size /**/+ sizeof(ptrdiff_t)/**/
      <= (void*) hp + hp->heap_size;
      p += hp->chunk_size + sizeof(ptrdiff_t) /**/+ sizeof(ptrdiff_t)/**/
      )
    if (*(ptrdiff_t*) (p - sizeof(ptrdiff_t)) < 0)
      write_int(0);
    else
      write_int(1);
  write_str("\n");
  return;
}

void draw(void) {
  struct heap *hp;
  int i_raw;
  write_str("heap_rows:\n");
  for (i_raw = 0; i_raw < n_raws; ++i_raw) {
    write_str("chunk_size: ");
    write_int(raws[i_raw].chunk_size);
    write_str(" heaps: ");
    write_int(raws[i_raw].n_heaps);
    write_str("\n");
    for (hp = raws[i_raw].first_heap; hp; hp = hp->next_heap) {
      draw_heap(hp);
    }
  }
  return;
}

/*..............................................................................*/

void* malloc(size_t size) {
  void* p;
  _MALLOC_LOCK()
  ;
  __DBG__(
      ( write_str("malloc("), write_int(size), write_str(") ncalls: "), write_int(n_calls), n_calls++, write_str(" ")));
  __TRK__(( n_calls % 10000 == 0 ? draw(), n_calls++ : n_calls++ ));
  if (!size)
    ++size; /* GLIBC malloc behaviour */
  p = alloc(size);
  __DBG__(( write_str(" ret: "), write_int((int)p), write_str("\n") ));
  _MALLOC_UNLOCK()
  ;
  return p;
}

void* realloc(void *p, size_t size) {
  void *r;
  _MALLOC_LOCK()
  ;
  __DBG__(
      ( write_str("realloc(..., "), write_int(size), write_str(") ncalls: "), write_int(n_calls), n_calls++, write_str("\n")));
  __TRK__(( n_calls % 10000 == 0 ? draw(), n_calls++ : n_calls++ ));
  r = re_alloc(p, size);
  _MALLOC_UNLOCK()
  ;
  return r;
}

void free(void* p) {
  _MALLOC_LOCK()
  ;
  __DBG__(
      ( write_str("free(): "), write_str(" ncalls: "), write_int(n_calls), n_calls++, write_str("\n")));
  __TRK__(( n_calls % 10000 == 0 ? draw(), n_calls++ : n_calls++ ));
  if (p)
    release(p);
  _MALLOC_UNLOCK()
  ;
  return;
}

void* calloc(size_t nmemb, size_t size) {
  size_t size_tot;
  void *ret;
  int off;
  _MALLOC_LOCK()
  ;
  __DBG__(
      ( write_str("calloc("), write_int(nmemb), write_str(" , "), write_int(size), write_str(") ncalls: "), write_int(n_calls), n_calls++, write_str("\n")));
  __TRK__(( n_calls % 10000 == 0 ? draw(), n_calls++ : n_calls++ ));
  size_tot = nmemb * size;
  ret = alloc(size_tot);
  if (ret)
    for (off = 0; off < size_tot; off++)
      *(char*) (ret + off) = '\0';
  _MALLOC_UNLOCK()
  ;
  return ret;
}
/**/

/**

 int main (void) {
 void *p [500];
 int i, s, tot;
 srand(1);
 for (i = 0, tot = 0; i < 500 ; i++) {
 s = rand() % 128 + 1;
 tot += s;
 p[i] = malloc (s);
 draw();
 ERRMSG("\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
 }
 ERRMSG ("******************* Now re_alloc ******************\n");
 for (i = 0; i < 500; i++) {
 s = rand() % 2048 + 1;
 p[i] = realloc  (p[i],s);
 draw();
 if (!p[i])
 ERRMSG("RE_ALLOC(..,0)\n");
 ERRMSG("\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
 }
 ERRMSG ("******************** Now release ********************\n");
 for (i = 0; i < 500; i++) {
 if (p[i]) {
 free (p[i]);
 draw();
 }
 else
 ERRMSG("EMPTY\n");
 ERRMSG("\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
 }
 return 0;
 }
 **/

