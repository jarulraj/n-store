/* Simple test program for the btree database. */

#include <sys/types.h>

#include <iostream>
#include <getopt.h>
#include <stdio.h>
#include <string.h>

#include "cow_btree.h"

using namespace std;

int main(int argc, char **argv) {
  int c, rc = BT_FAIL;
  unsigned int flags = 0;
  cow_btree *bt;
  struct cursor *cursor;
  const char *filename = "test.db";
  struct cow_btval key, val, maxkey;

  while ((c = getopt(argc, argv, "rf:")) != -1) {
    switch (c) {
      case 'r':
        flags |= BT_REVERSEKEY;
        break;
      case 'f':
        filename = optarg;
        break;
    }
  }

  argc -= optind;
  argv += optind;

  if (argc == 0)
    errx(1, "missing command");

  bt = new cow_btree(filename);

  bzero(&key, sizeof(key));
  bzero(&val, sizeof(val));
  bzero(&maxkey, sizeof(maxkey));

  if (strcmp(argv[0], "put") == 0) {
    if (argc < 3)
      errx(1, "missing arguments");
    key.data = argv[1];
    key.size = strlen((char*) key.data);
    val.data = argv[2];
    val.size = strlen((char*) val.data);
    rc = bt->insert(&key, &val, 0);
    if (rc == BT_SUCCESS)
      printf("OK\n");
    else
      printf("FAIL\n");
  } else if (strcmp(argv[0], "del") == 0) {
    if (argc < 1)
      errx(1, "missing argument");
    key.data = argv[1];
    key.size = strlen((char*) key.data);
    rc = bt->remove(&key, NULL);
    if (rc == BT_SUCCESS)
      printf("OK\n");
    else
      printf("FAIL\n");
  } else if (strcmp(argv[0], "get") == 0) {
    if (argc < 2)
      errx(1, "missing arguments");
    key.data = argv[1];
    key.size = strlen((char*) key.data);
    rc = bt->at(&key, &val);
    if (rc == BT_SUCCESS) {
      printf("OK %.*s\n", (int) val.size, (char *) val.data);
    } else {
      printf("FAIL\n");
    }
  } else if (strcmp(argv[0], "scan") == 0) {
    if (argc > 1) {
      key.data = argv[1];
      key.size = strlen((char*) key.data);
      flags = BT_CURSOR;
    } else
      flags = BT_FIRST;
    if (argc > 2) {
      maxkey.data = argv[2];
      maxkey.size = strlen((char*) key.data);
    }

    cursor = bt->cow_btree_cursor_open();
    while ((rc = bt->cow_btree_cursor_get(cursor, &key, &val,
                                          cursor_op::BT_CURSOR)) == BT_SUCCESS) {
      if (argc > 2 && bt->cow_btree_cmp(&key, &maxkey) > 0)
        break;
      printf("OK %zi %.*s\n", key.size, (int) key.size, (char *) key.data);
      flags = BT_NEXT;
    }
    bt->cow_btree_cursor_close(cursor);
  } else if (strcmp(argv[0], "compact") == 0) {
    if ((rc = bt->compact()) != BT_SUCCESS)
      warn("compact");
  } else if (strcmp(argv[0], "revert") == 0) {
    if ((rc = bt->cow_btree_revert()) != BT_SUCCESS)
      warn("revert");
  } else
    errx(1, "%s: invalid command", argv[0]);

  //delete bt;

  return rc;
}

