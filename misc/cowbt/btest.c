/* Simple test program for the btree database. */

#include <sys/types.h>

#include <getopt.h>
#include <stdio.h>
#include <string.h>

#include "btree.h"

int main(int argc, char **argv) {
  int c, rc = BT_FAIL;
  unsigned int flags = 0;
  struct btree *bt;
  struct cursor *cursor;
  const char *filename = "test.db";
  struct btval key, data, maxkey;

  /*
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
   */

  bt = btree_open(filename, flags | BT_NOSYNC, 0644);
  if (bt == NULL)
    err(1, filename);

  bzero(&key, sizeof(key));
  bzero(&data, sizeof(data));
  bzero(&maxkey, sizeof(maxkey));

  int i, count = 20000;

  key.data = malloc(sizeof(char) * 100);
  data.data = malloc(sizeof(char) * 100);

  struct btree_txn *t = btree_txn_begin(bt, 0);
  for (i = 0; i < count; i++) {
    sprintf(key.data, "%d", i);
    sprintf(data.data, "%d-val", i);
    key.size = strlen(key.data);
    data.size = strlen(data.data);

    rc = btree_txn_put(bt, t, &key, &data, 0);
    assert(rc == BT_SUCCESS);
  }
  btree_txn_commit(t);

  rc = btree_get(bt, &key, &data);
  if (rc == BT_SUCCESS) {
    printf("OK %.*s\n", (int) data.size, (char *) data.data);
  } else {
    printf("FAIL\n");
  }

  t = NULL;
  t = btree_txn_begin(bt, 0);
  for (i = 0; i < count; i++) {
    sprintf(key.data, "%d", i);
    key.size = strlen(key.data);
    rc = btree_txn_del(bt, t, &key, NULL);
    assert(rc == BT_SUCCESS);
  }
  btree_txn_commit(t);

  /*
   if (strcmp(argv[0], "put") == 0) {
   if (argc < 3)
   errx(1, "missing arguments");
   key.data = argv[1];
   key.size = strlen(key.data);
   data.data = argv[2];
   data.size = strlen(data.data);
   rc = btree_put(bt, &key, &data, 0);
   if (rc == BT_SUCCESS)
   printf("OK\n");
   else
   printf("FAIL\n");
   } else if (strcmp(argv[0], "del") == 0) {
   if (argc < 1)
   errx(1, "missing argument");
   key.data = argv[1];
   key.size = strlen(key.data);
   rc = btree_del(bt, &key, NULL);
   if (rc == BT_SUCCESS)
   printf("OK\n");
   else
   printf("FAIL\n");
   } else if (strcmp(argv[0], "get") == 0) {
   if (argc < 2)
   errx(1, "missing arguments");
   key.data = argv[1];
   key.size = strlen(key.data);
   rc = btree_get(bt, &key, &data);
   if (rc == BT_SUCCESS) {
   printf("OK %.*s\n", (int) data.size, (char *) data.data);
   } else {
   printf("FAIL\n");
   }
   } else if (strcmp(argv[0], "scan") == 0) {
   if (argc > 1) {
   key.data = argv[1];
   key.size = strlen(key.data);
   flags = BT_CURSOR;
   } else
   flags = BT_FIRST;
   if (argc > 2) {
   maxkey.data = argv[2];
   maxkey.size = strlen(key.data);
   }

   cursor = btree_cursor_open(bt);
   while ((rc = btree_cursor_get(cursor, &key, &data, flags)) == BT_SUCCESS) {
   if (argc > 2 && btree_cmp(bt, &key, &maxkey) > 0)
   break;
   printf("OK %zi %.*s\n", key.size, (int) key.size, (char *) key.data);
   flags = BT_NEXT;
   }
   btree_cursor_close(cursor);
   } else if (strcmp(argv[0], "compact") == 0) {
   if ((rc = btree_compact(bt)) != BT_SUCCESS)
   warn("compact");
   } else if (strcmp(argv[0], "revert") == 0) {
   if ((rc = btree_revert(bt)) != BT_SUCCESS)
   warn("revert");
   } else
   errx(1, "%s: invalid command", argv[0]);
   */

  btree_close(bt);

  return rc;
}

