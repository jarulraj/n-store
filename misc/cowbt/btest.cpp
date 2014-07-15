/* Simple test program for the btree database. */

#include <sys/types.h>

#include <iostream>
#include <getopt.h>
#include <stdio.h>
#include <string.h>

#include "btree.h"

using namespace std;

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

  bt = new btree(filename, flags | BT_NOSYNC, 0644);
  if (bt == NULL)
    err(1, filename);

  bzero(&key, sizeof(key));
  bzero(&data, sizeof(data));
  bzero(&maxkey, sizeof(maxkey));

  int i, count = 20000;

  key.data = malloc(sizeof(char) * 100);
  data.data = malloc(sizeof(char) * 100);

  struct btree_txn *t = bt->btree_txn_begin( 0);
  for (i = 0; i < count; i++) {
    sprintf((char*)key.data, "%d", i);
    sprintf((char*)data.data, "%d-val", i);
    key.size = strlen((char*)key.data);
    data.size = strlen((char*)data.data);

    rc = bt->btree_txn_put( t, &key, &data, 0);
    assert(rc == BT_SUCCESS);
  }
  bt->btree_txn_commit(t);

  rc = bt->btree_get(&key, &data);
  if (rc == BT_SUCCESS) {
    printf("OK %.*s\n", (int) data.size, (char*) data.data);
  } else {
    printf("FAIL\n");
  }

  t = bt->btree_txn_begin( 0);
  for (i = 0; i < count; i++) {
    sprintf((char*)key.data, "%d", i);
    key.size = strlen((char*)key.data);
    rc = bt->btree_txn_del( t, &key, NULL);
    assert(rc == BT_SUCCESS);
    rc = bt->btree_txn_get( t, &key, &data);
    assert(rc == BT_FAIL);
  }
  bt->btree_txn_commit(t);

  cout<<"size :: "<<bt->size<<endl;

  bt->btree_compact();

  /*
   if (strcmp(argv[0], "put") == 0) {
   if (argc < 3)
   errx(1, "missing arguments");
   key.data = argv[1];
   key.size = strlen(key.data);
   data.data = argv[2];
   data.size = strlen(data.data);
   rc = bt->btree_put( &key, &data, 0);
   if (rc == BT_SUCCESS)
   printf("OK\n");
   else
   printf("FAIL\n");
   } else if (strcmp(argv[0], "del") == 0) {
   if (argc < 1)
   errx(1, "missing argument");
   key.data = argv[1];
   key.size = strlen(key.data);
   rc = bt->btree_del( &key, NULL);
   if (rc == BT_SUCCESS)
   printf("OK\n");
   else
   printf("FAIL\n");
   } else if (strcmp(argv[0], "get") == 0) {
   if (argc < 2)
   errx(1, "missing arguments");
   key.data = argv[1];
   key.size = strlen(key.data);
   rc = bt->btree_get( &key, &data);
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

   cursor = bt->btree_cursor_open(bt);
   while ((rc = bt->btree_cursor_get(cursor, &key, &data, flags)) == BT_SUCCESS) {
   if (argc > 2 && bt->btree_cmp( &key, &maxkey) > 0)
   break;
   printf("OK %zi %.*s\n", key.size, (int) key.size, (char *) key.data);
   flags = BT_NEXT;
   }
   bt->btree_cursor_close(cursor);
   } else if (strcmp(argv[0], "compact") == 0) {
   if ((rc = bt->btree_compact(bt)) != BT_SUCCESS)
   warn("compact");
   } else if (strcmp(argv[0], "revert") == 0) {
   if ((rc = bt->btree_revert(bt)) != BT_SUCCESS)
   warn("revert");
   } else
   errx(1, "%s: invalid command", argv[0]);
   */

  bt->btree_close();

  return rc;
}

