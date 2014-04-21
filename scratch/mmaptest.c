#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>

int main(int argc, char *argv[]){
	int fd, offset;
	char *data;
	struct stat sbuf;

	if (argc != 1) {
		fprintf(stderr, "usage: mmapdemo \n");
		exit(1);
	}

	if ((fd = open("log.txt", O_RDWR )) == -1) {
		perror("open");
		exit(1);
	}

	offset = 0;

    if (stat("log.txt", &sbuf) == -1) {
        perror("stat");
        exit(1);
    }

    printf("size : %lu\n", sbuf.st_size);

    if ((data = mmap((caddr_t)0, sbuf.st_size, PROT_READ, MAP_SHARED, fd, 0)) == (caddr_t)(-1)) {
        perror("mmap");
        exit(1);
    }

    printf("data :\n%s\n", data);

    return 0;
}

