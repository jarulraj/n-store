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

  fd = open("log", O_RDWR | O_CREAT);
  if (fd < 0) {
    perror("open");
    exit(1);
  }

  size_t len = 8UL * 1024;
  if (posix_fallocate(fd, 0, len) < 0) {
    perror("open");
    exit(1);
  }

  if (stat("log", &sbuf) == -1) {
    perror("stat");
    exit(1);
  }

  printf("size : %lu\n", sbuf.st_size);
  printf("fd : %d\n", fd);

  caddr_t location = (caddr_t) 0x010000000;

  if ((data = (char*) mmap(location, sbuf.st_size, PROT_WRITE, MAP_SHARED, fd,
                           0)) == (caddr_t) (-1)) {
    perror("mmap");
    exit(1);
  }

  printf("data : %s %p\n", data, data);

  char cmd[512];
  sprintf(cmd, "cat /proc/%d/maps", getpid());
  system(cmd);

  data[2] = 'x';

  char* ptr = (char*) (location + 2);

  printf("data :  %p %c \n", ptr, (*ptr));

  msync(data, sbuf.st_size, MS_SYNC);

  return 0;
}

