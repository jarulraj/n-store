#ifndef MMAP_FD_H_
#define MMAP_FD_H_

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <cstring>
#include <cassert>

#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>

#include "record.h"
#include "config.h"

using namespace std;

#define DELIM ' '
#define CHUNK_DELIM "-1 -1"

#define rounddown2(x, y) ((x)&(~((y)-1)))          /* if y is power of two */
#define roundup2(x, y)  (((x)+((y)-1))&(~((y)-1))) /* if y is powers of two */

// MMAP
class mmap_fd {
public:
	mmap_fd() :
			file_name(""),
			fd(-1),
			offset(0),
			prev_offset(0),
			data(NULL),
			page_size(0),
			max_len(0){}

	mmap_fd(std::string _name, long long len, caddr_t location, config& _conf){

		file_name = _name;
		conf = _conf;

		if ((fp = fopen(file_name.c_str(), "w+")) == NULL) {
			cout << "fopen failed " << file_name << " \n";
			exit(EXIT_FAILURE);
		}

		fd = fileno(fp);
		page_size = getpagesize();
		max_len = len;

		struct stat sbuf;

		if (stat(file_name.c_str(), &sbuf) == -1) {
			cout << "stat failed " << file_name << " \n";
			exit(EXIT_FAILURE);
		}

		// new file check
		if (sbuf.st_size == 0) {

			if (ftruncate(fd, len) == -1) {
				perror("ftruncate failed");
				cout << file_name << " \n";
				exit(EXIT_FAILURE);
			}

			if (stat(file_name.c_str(), &sbuf) == -1) {
				cout << "stat failed " << file_name << " \n";
				exit(EXIT_FAILURE);
			}

			//cout<<"after fallocate: size :"<< sbuf.st_size << endl;
		}

		// XXX Fix -- scan max pointer from clean dir
		offset = 0;
		prev_offset = 0;

		if ((data = (char*) mmap(location, sbuf.st_size, PROT_WRITE, MAP_SHARED, fd, 0)) == (caddr_t) (-1)) {
			perror(" mmap_error ");
			cout << "mmap failed " << file_name << endl;
			exit(EXIT_FAILURE);
		}

	}

	char* push_back_record(const record& rec) {
		char rec_str[conf.sz_tuple];
		int len = 0;

		sprintf(rec_str, "%u %s \n", rec.key, rec.value);
		len = strlen(rec_str);

		char* cur_offset = (data + offset);
		memcpy(cur_offset, rec_str, len);
		offset += len;

		return cur_offset;
	}

	void push_back_batch_id(int batch_id) {
		char rec_str[conf.sz_tuple];
		int len = 0;

		sprintf(rec_str, "%d ", batch_id);
		len = strlen(rec_str);

		char* cur_offset = (data + offset);
		memcpy(cur_offset, rec_str, len);

		offset += len;
	}

	std::string get_value(char* location) {
		unsigned int key;
		char val[conf.sz_value];
		char tuple[conf.sz_tuple];

		memcpy(tuple, location, sizeof(tuple));
		sscanf(tuple, " %u %s ", &key, val);

		return std::string(val);
	}

	void sync() {
		int ret = 0;


		// Partial file
		int len = 0;
		off_t round;

		len = offset - prev_offset;
		//cout << "msync :: "<<file_name<<" :: " << len << endl;

		if (len > 0) {
			round = rounddown2(prev_offset, page_size);

			ret = msync(data + round, len, MS_SYNC);
			if (ret == -1) {
				perror("msync failed");
				exit (EXIT_FAILURE);
			}
		}

		prev_offset = offset;

		// Full file
		/*
		ret = msync(data, offset, MS_SYNC);
		if (ret == -1) {
			perror("msync failed");
			exit(EXIT_FAILURE);
		}
		*/
	}


//private:
	FILE* fp = NULL;
	int fd;

	std::string file_name;
	char* data;

	off_t offset;
	off_t prev_offset;
	config conf;

	long long int max_len;
	int page_size;
};


#endif /* MMAP_FD_H_ */
