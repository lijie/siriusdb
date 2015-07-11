#include <stdio.h>
#include <assert.h>
#include "fifo.h"

static void test1() {
  fifo_t *f1 = fifo_new(1024 * 4);
  int i;
  void *p;

  for (i = 0; i < 1023; i++) {
    p = fifo_alloc(f1, 4);
    assert(p);
    *(int *)p = i;
    fifo_put(f1, p, 4);
  }

  p = fifo_alloc(f1, 4);
  assert(p == NULL);

  for (i = 0; i < 1023; i++) {
    p = fifo_get(f1, 4);
    assert(p);
    assert(*(int *)p == i);
    fifo_end(f1, p, 4);
  }

  assert(fifo_empty(f1));
}

static void test2() {
  fifo_t *f1 = fifo_new(1024 * 4);
  int i;
  void *p;

  for (i = 0; i < 1023; i++) {
    p = fifo_alloc(f1, 13);
    assert(p);
    *(int *)p = i;
    fifo_put(f1, p, 13);

    p = fifo_get(f1, 13);
    assert(p);
    assert(*(int *)p == i);
    fifo_end(f1, p, 13);
  }

  assert(fifo_empty(f1));
}

static void test3() {
  fifo_t *f1 = fifo_new(1024 * 4);
  int i;
  void *p;

  for (i = 0; i < 1023; i++) {
    p = fifo_alloc(f1, 13);
    assert(p);
    *(int *)p = i;
    fifo_put(f1, p, 13);

    p = fifo_extend(f1, p, 13, 13 * 2);
    assert(p);
    fifo_put(f1, p, 26);

    printf("%u %u\n", f1->pt, f1->gt);
    p = fifo_get(f1, 26);
    assert(p);
    assert(*(int *)p == i);
    fifo_end(f1, p, 26);
  }

  assert(fifo_empty(f1));
}

int main(int argc, char **argv) {
  test1();
  test2();
  test3();
}
