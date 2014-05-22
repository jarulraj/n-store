#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>

#include <iostream>
#include <chrono>
#include <random>
#include <string>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <algorithm>

using namespace std;

#define VALUE_SIZE 4096
#define ITR        1024*32

#define SIZE       VALUE_SIZE*ITR

std::chrono::time_point<std::chrono::high_resolution_clock> start, finish;
std::chrono::duration<double> elapsed_seconds ;

std::string random_string( size_t length ){
    auto randchar = []() -> char
    {
        const char charset[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
        const size_t max_index = (sizeof(charset) - 1);
        return charset[ rand() % max_index ];
    };
    std::string str(length,0);
    std::generate_n( str.begin(), length, randchar );
    return str;
}

int main(int argc, char *argv[]){
	int fd, offset;
	char *data;
	struct stat sbuf;

	std::string name = "./log";

	if (argc != 1) {
		fprintf(stderr, "usage: msynctest \n");
		exit(EXIT_FAILURE);
	}

	if ((fd = open(name.c_str(), O_RDWR )) == -1) {
		perror("open");
		exit(EXIT_FAILURE);
	}

    std::string val = random_string(VALUE_SIZE);

    caddr_t location = (caddr_t) 0x01c00000;
    off_t file_len = SIZE;

    if(ftruncate(fd, file_len) == -1){
    	perror("fallocate");
    	exit(EXIT_FAILURE);
    }

    if (stat(name.c_str(), &sbuf) == -1) {
        perror("stat");
        exit(EXIT_FAILURE);
    }

    printf("start size : %lu\n", sbuf.st_size);

    if ((data = (char*) mmap(location, sbuf.st_size, PROT_WRITE, MAP_SHARED, fd, 0)) == (caddr_t)(-1)) {
        perror("mmap");
        exit(EXIT_FAILURE);
    }

    off_t len = val.size();
	offset = 0;
	int itr;

	cout<<"len :"<<len<<endl;

    start = std::chrono::high_resolution_clock::now();

	for (itr = 0; itr < ITR; itr++) {
		char* cur_offset = (data + offset);
		memcpy(cur_offset, val.c_str(), len);
		offset += len;
    
		// Part of file
		if(msync(cur_offset, len, MS_SYNC) == -1){
			perror("msync");
			exit(EXIT_FAILURE);
		}

		// Full file
		/*
		if(msync(data, offset, MS_SYNC) == -1){
			perror("msync");
			exit(EXIT_FAILURE);
		}
		*/

	}

    finish = std::chrono::high_resolution_clock::now();
    elapsed_seconds = finish - start;
    std::cout<<"Execution duration: "<< elapsed_seconds.count()<<endl;

    return 0;
}

