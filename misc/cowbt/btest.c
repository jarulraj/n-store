#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>

#include "btree.h"

int compare_cb(const bp_key_t* a, const bp_key_t* b) {
  uint32_t i, len = a->length < b->length ? a->length : b->length;

  for (i = 0; i < len; i++) {
    if (a->value[i] != b->value[i]) {
      return a->value[i] > b->value[i] ? 1 : -1;
    }
  }

  return a->length - b->length;
}

int main(void) {
  bp_tree_t tree;

  int r;
  r = bp_open(&tree, "./test.db");
  assert(r == 0);

  bp_set_compare_cb(&tree, compare_cb);

  const int n = 10;
  int i;

  for (i = 0; i < n; i++) {
    char key[1000];
    char val[1000];
    sprintf(key, "some key %d", i);
    sprintf(val, "some value %d", i);
    bp_sets(&tree, key, val);
  }

  printf("Checking \n");

  for (i = 0; i < n; i++) {
    char key[1000];
    char expected[1000];
    sprintf(key, "some key %d", i);
    sprintf(expected, "some value %d", i);

    char* value;
    bp_gets(&tree, key, &value);
    printf("val : %s \n", value);
    assert(strcmp(value, expected) == 0);
  }

  r = bp_close(&tree);
  assert(r == 0);
}
