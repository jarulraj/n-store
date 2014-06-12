#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>

int main(int argc, char *argv[]) {
  int fd, offset;
  char *data;
  struct stat sbuf;

  if (argc != 1) {
    fprintf(stderr, "usage: mmapdemo \n");
    exit(1);
  }

  if ((fd = open("log", O_RDWR)) == -1) {
    perror("open");
    exit(1);
  }

  offset = 0;

  if (stat("log", &sbuf) == -1) {
    perror("stat");
    exit(1);
  }

  printf("size : %lu\n", sbuf.st_size);

  caddr_t location = 0x01c00000;

  if ((data = mmap(location, sbuf.st_size, PROT_WRITE, MAP_SHARED, fd, 0))
      == (caddr_t) (-1)) {
    perror("mmap");
    exit(1);
  }

  printf("data :\n%s %p\n", data, data);

  data[2] = 'x';

  printf("data :\n%c\n", *(location + 2));

  msync(data, sbuf.st_size, MS_SYNC);

  return 0;
}

