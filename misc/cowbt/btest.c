#include <stdlib.h>
#include <stdio.h>

#include "btree.h"

int main(void) {
  bp_db_t db;

  /* Open database */
  bp_open(&db, "/tmp/1.bp");

  /* Set some value */
  bp_sets(&db, "key", "value");

  /* Get some value */
  bp_value_t value;
  bp_gets(&db, "key", &value.value);
  fprintf(stdout, "%s\n", value.value);
  free(value.value);

  /* Close database */
  bp_close(&db);
}
