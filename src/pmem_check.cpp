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

using namespace std;

#include "libpm.h"

char Usage[] = "pmem_check <path>"; /* for USAGE() */

int main(int argc, char *argv[]) {
  const char *path;
  int opt;

  if (optind >= argc)
    printf("No path given \nUsage:: %s \n", Usage);
  path = argv[optind++];

  if (optind < argc)
    printf("Usage :: %s \n", Usage);

  pmemalloc_check(path);

  exit(0);
}

