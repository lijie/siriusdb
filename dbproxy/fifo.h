#ifndef _FIFO_H
#define _FIFO_H

#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

typedef struct struct_fifo {
  /* TODO(lijie3): atomic_t for pt & gt */
  volatile uint32_t pt;
  volatile uint32_t gt;
  uint32_t size;
  uint32_t mask;
  char data[0];
} fifo_t __attribute__((aligned (4)));

struct fifo_head {
  uint32_t size;
  uint32_t pos;
} __attribute__((aligned (4)));

static inline uint32_t fifo_size(fifo_t *f) {
  uint32_t gt = f->gt;
  uint32_t pt = f->pt;
  return pt - gt;
}

static inline int fifo_empty(fifo_t *f) {
  uint32_t gt = f->gt;
  uint32_t pt = f->pt;
  return gt == pt;
}

static inline int fifo_full(fifo_t *f, size_t size) {
  return fifo_size(f) + size >= f->size;
}

static inline int need_malloc(fifo_t *f, size_t size) {
}

static inline void * fifo_get(fifo_t *f, size_t size) {
  uint32_t mask = f->size - 1;
  uint32_t gt = f->gt;
  uint32_t pt = f->pt;
  uint32_t off = gt & f->mask;
  void *res;

  /* not enough data */
  if (fifo_size(f) < size)
    return NULL;

  res = f->data + off;

  /* beyond boundary, need copy */
  if (off + size > f->size) {
    size_t cp = f->size - off;
    res = malloc(size);
    assert(res);
    memcpy(res, f->data + off, cp);
    memcpy(res + cp, f->data, size - cp);
  }

  gt += size;
  f->gt = gt;
  return res;
}

static inline void fifo_end(fifo_t *f, char *p, size_t size) {
  if (p + size <= f->data + f->size)
    return;
  free(p - sizeof(struct fifo_head));
}

static inline void * fifo_alloc(fifo_t *f, size_t size) {
  uint32_t gt = f->gt;
  uint32_t pt = f->pt;
  uint32_t off = pt & f->mask;
  void *res;

  /* not enough data */
  if (fifo_full(f, size))
    return NULL;

  res = f->data + off;
  if (off + size > f->size) {
    printf("malloc\n");
    struct fifo_head *h;
    /* boundary, need alloc */
    res = malloc(size + sizeof(*h));
    h = (struct fifo_head *)res;
    h->size = size;
    h->pos = pt;
    res += sizeof(*h);
  }

  pt += size;
  f->pt = pt;
  return res;
}

static inline void fifo_put(fifo_t *f, char *p, size_t size) {
  struct fifo_head *h;
  uint32_t cp;

  if (p + size <= f->data + f->size)
    return;

  h = (struct fifo_head *)(p - sizeof(*h));
  printf("%zd %d %u\n", size, h->size, h->pos);

  assert(size == h->size);

  /* copy */
  cp = f->size - (h->pos & f->mask);
  memcpy(f->data + (h->pos & f->mask), p, cp);
  memcpy(f->data, p + cp, size - cp);
  free(h);
}

/* shrink only works for last alloc */
static inline void * fifo_shrink(fifo_t *f, void *p,
                                 uint32_t oldsize, uint32_t newsize) {
  uint32_t gt = f->gt;
  uint32_t pt = f->pt;
  uint32_t offset = oldsize - newsize;
  char *old = (char *)p;

  /* malloced */
  if (old + oldsize > f->data + f->size)
    return p;

  pt = pt - offset;
  f->pt = pt;
  return p;
}

/* extend only works for last alloc */
static inline void * fifo_extend(fifo_t *f, void *p,
                                 size_t oldsize, size_t newsize) {
  uint32_t gt = f->gt;
  uint32_t pt = f->pt;
  uint32_t off = pt & f->mask;
  uint32_t offset = newsize - oldsize;
  struct fifo_head *h;
  char *old = (char *)p;

  if (fifo_full(f, offset))
    return NULL;

  /* malloced */
  if (old + oldsize > f->data + f->size) {
    printf("realloc\n");
    p = realloc(p - sizeof(*h), newsize + sizeof(*h));
    h = (struct fifo_head *)p;
    printf("old size pt %u %u\n", h->size, h->pos);
    h->size = newsize;
    p += sizeof(*h);
  } else if (off + offset > f->size) {
    printf("re alloc\n");
    pt -= oldsize;
    f->pt = pt;
    p = fifo_alloc(f, newsize);
    memcpy(p, old, oldsize);
    return p;
  }

  pt += offset;
  f->pt = pt;
  return p;
}

static inline void * fifo_resize(fifo_t *f, void *p,
                                 size_t oldsize, size_t newsize) {
  if (newsize == oldsize)
    return;
  else if (newsize > oldsize)
    return fifo_extend(f, p, oldsize, newsize);
  else
    return fifo_shrink(f, p, newsize, oldsize);
}

static inline fifo_t * fifo_new(size_t size) {
  fifo_t *f;

  /* needs pow(2, n) */
  if ((size == 0) || (size & (size - 1) != 0))
    return NULL;

  f = (fifo_t *)malloc(sizeof(*f) + size);
  memset(f, 0, sizeof(*f));

  f->size = size;
  f->mask = size - 1;
  return f;
}

#endif
