#include <stdlib.h>
#include <stdio.h>

#include "btree.h"

int main(void) {
  bp_db_t db;

  /* Open database */
  bp_open(&db, "./test.db");

  const int n = 100;
  int i;
  for (i = 0; i < n; i++) {
    char key[1000];
    char val[1000];
    sprintf(key, "some key %d", i);
    sprintf(val, "some value %d", i);

    /* Set some value */
    bp_sets(&db, key, val);
  }

  for (i = 0; i < n; i++) {
    char key[1000];
    char expected[1000];
    sprintf(key, "some key %d", i);
    sprintf(expected, "some value %d", i);

    char* value;

    /* Get some value */
    bp_gets(&db, key, &value);
    assert(strcmp(value, expected) == 0);
  }

  /* Close database */
  bp_close(&db);
}
