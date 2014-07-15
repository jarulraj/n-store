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

  bt = new cow_btree(filename);

  bzero(&key, sizeof(key));
  bzero(&val, sizeof(val));
  bzero(&maxkey, sizeof(maxkey));

  int i, count = 20000;

  key.data = malloc(sizeof(char) * 100);
  val.data = malloc(sizeof(char) * 100);

  struct cow_btree_txn *t = bt->txn_begin(0);
  for (i = 0; i < count; i++) {
    sprintf((char*) key.data, "%d", i);
    sprintf((char*) val.data, "%d-val", i);
    key.size = strlen((char*) key.data);
    val.size = strlen((char*) val.data);

    rc = bt->insert(t, &key, &val);
    assert(rc == BT_SUCCESS);
  }
  bt->txn_commit(t);

  rc = bt->at(&key, &val);
  if (rc == BT_SUCCESS) {
    printf("OK %.*s\n", (int) val.size, (char*) val.data);
  } else {
    printf("FAIL\n");
  }

  t = bt->txn_begin(0);
  for (i = 0; i < count; i++) {
    sprintf((char*) key.data, "%d", i);
    key.size = strlen((char*) key.data);
    rc = bt->remove(t, &key, NULL);
    assert(rc == BT_SUCCESS);
    rc = bt->at(t, &key, &val);
    assert(rc == BT_FAIL);
  }
  bt->txn_commit(t);

  cout << "size :: " << bt->size << endl;

  bt->compact();

  /*
   if (strcmp(argv[0], "put") == 0) {
   if (argc < 3)
   errx(1, "missing arguments");
   key.data = argv[1];
   key.size = strlen(key.data);
   data.data = argv[2];
   data.size = strlen(data.data);
   rc = bt->cow_btree_put( &key, &data, 0);
   if (rc == BT_SUCCESS)
   printf("OK\n");
   else
   printf("FAIL\n");
   } else if (strcmp(argv[0], "del") == 0) {
   if (argc < 1)
   errx(1, "missing argument");
   key.data = argv[1];
   key.size = strlen(key.data);
   rc = bt->cow_btree_del( &key, NULL);
   if (rc == BT_SUCCESS)
   printf("OK\n");
   else
   printf("FAIL\n");
   } else if (strcmp(argv[0], "get") == 0) {
   if (argc < 2)
   errx(1, "missing arguments");
   key.data = argv[1];
   key.size = strlen(key.data);
   rc = bt->cow_btree_get( &key, &data);
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

   cursor = bt->cow_btree_cursor_open(bt);
   while ((rc = bt->cow_btree_cursor_get(cursor, &key, &data, flags)) == BT_SUCCESS) {
   if (argc > 2 && bt->cow_btree_cmp( &key, &maxkey) > 0)
   break;
   printf("OK %zi %.*s\n", key.size, (int) key.size, (char *) key.data);
   flags = BT_NEXT;
   }
   bt->cow_btree_cursor_close(cursor);
   } else if (strcmp(argv[0], "compact") == 0) {
   if ((rc = bt->cow_btree_compact(bt)) != BT_SUCCESS)
   warn("compact");
   } else if (strcmp(argv[0], "revert") == 0) {
   if ((rc = bt->cow_btree_revert(bt)) != BT_SUCCESS)
   warn("revert");
   } else
   errx(1, "%s: invalid command", argv[0]);
   */

  delete bt;

  return rc;
}

