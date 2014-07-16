/* Simple test program for the btree database. */

#include <sys/types.h>

#include <iostream>
#include <getopt.h>
#include <stdio.h>
#include <string.h>

#include "cow_pbtree.h"
#include "libpm.h"

using namespace std;

extern struct static_info *sp;

int main(int argc, char **argv) {

  const char* path = "./zfile";

  long pmp_size = 10 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path << endl;

  sp = (struct static_info *) pmemalloc_static_area();

  int c, rc = BT_FAIL;
  unsigned int flags = 0;
  cow_pbtree *pbt;
  struct cursor *cursor;
  const char *filename = "test.db";
  struct cow_btval key, val, maxkey;

  pbt = new cow_pbtree(&sp->ptrs[sp->itr]);

  cow_btree* bt = pbt->t_ptr;

  bzero(&key, sizeof(key));
  bzero(&val, sizeof(val));
  bzero(&maxkey, sizeof(maxkey));

  /*
   int i, count = 2;

   key.data = new char[100];
   val.data = new char[100];

   struct cow_btree_txn *t = bt->txn_begin(0);
   for (i = 0; i < count; i++) {
   sprintf((char*) key.data, "%d", i);
   sprintf((char*) val.data, "%d-val", i);
   key.size = strlen((char*) key.data)+1;
   val.size = strlen((char*) val.data)+1;
   printf("insert :: key : %s val : %s \n", (char*) key.data, (char*) val.data);

   rc = bt->insert(t, &key, &val);
   assert(rc == BT_SUCCESS);
   }
   bt->txn_commit(t);

   t = bt->txn_begin(0);
   for (i = 0; i < count; i++) {
   sprintf((char*) key.data, "%d", i);
   key.size = strlen((char*) key.data)+1;
   rc = bt->at(t, &key, &val);
   assert(rc == BT_SUCCESS);
   printf("at :: key : %s val : %s \n", (char*) key.data, (char*) val.data);
   rc = bt->remove(t, &key, NULL);
   assert(rc == BT_SUCCESS);
   rc = bt->at(t, &key, &val);
   assert(rc == BT_FAIL);
   }
   bt->txn_commit(t);

   cout << "size :: " << bt->stat.leaf_pages << endl;

   bt->cow_btree_revert();

   //bt->compact();

   //delete bt;
   //sp->ptrs[sp->itr] = NULL;
   */

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

  return rc;
}

