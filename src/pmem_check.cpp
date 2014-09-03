/*
 * pmemalloc_check.c -- check the health of a pmem pool
 * Usage: pmemalloc_check [-FMd] path
 * This is just a simple CLI for calling pmemalloc_check().
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <stdarg.h>

#include "libpm.h"

using namespace std;

char Usage[] = "pmem_check <path>"; /* for USAGE() */

int main(int argc, char *argv[]) {
  const char *path;

  if (optind >= argc)
    printf("No path given \nUsage:: %s \n", Usage);
  path = argv[optind++];

  if (optind < argc)
    printf("Usage :: %s \n", Usage);

  storage::pmemalloc_check(path);

  exit(0);
}
