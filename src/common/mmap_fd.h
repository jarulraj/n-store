#ifndef MMAP_FD_H_
#define MMAP_FD_H_

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <cstring>

#include "record.h"
#include "config.h"

using namespace std;

#define DELIM ' '
#define CHUNK_DELIM "-1 -1"

#define roundup2(x, y)  (((x)+((y)-1))&(~((y)-1)))

// MMAP
class mmap_fd {
public:
	mmap_fd() :
			name(""), fd(-1), offset(0), data(NULL) {
	}

	mmap_fd(std::string _name, caddr_t location, config& _conf){

		name = _name;
		conf = _conf;

		if ((fp = fopen(name.c_str(), "w+")) == NULL) {
			cout << "fopen failed " << name << " \n";
			exit(EXIT_FAILURE);
		}

		fd = fileno(fp);

		struct stat sbuf;

		if (stat(name.c_str(), &sbuf) == -1) {
			cout << "stat failed " << name << " \n";
			exit(EXIT_FAILURE);
		}

		// new file check
		if (sbuf.st_size == 0) {

			// XXX Simplify
			off_t len = conf.num_keys*conf.sz_value*100 + (conf.per_writes/100)*conf.num_txns*conf.sz_value ;

			if (ftruncate(fd, len) == -1) {
				cout << "fallocate failed " << name << " \n";
				exit(EXIT_FAILURE);
			}

			if (stat(name.c_str(), &sbuf) == -1) {
				cout << "stat failed " << name << " \n";
				exit(EXIT_FAILURE);
			}

			//cout<<"after fallocate: size :"<< sbuf.st_size << endl;
			offset = 0;
		}

		// XXX Fix -- scan max pointer from clean dir
		offset = 0;

		if ((data = (char*) mmap(location, sbuf.st_size, PROT_WRITE, MAP_SHARED, fd, 0)) == (caddr_t) (-1)) {
			perror(" mmap_error ");
			cout << "mmap failed " << name << endl;
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

	void push_back_dir_entry(const sp_record& rec) {
		char rec_str[conf.sz_tuple];
		int len = 0;

		sprintf(rec_str, "%u %p ", rec.key, rec.location);
		len = strlen(rec_str);

		char* cur_offset = (data + offset);
		memcpy(cur_offset, rec_str, len);

		offset += len;
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

		ret = msync(data, offset, MS_SYNC);
		if (ret == -1) {
			perror("msync failed");
			exit(EXIT_FAILURE);
		}

	}


private:
	FILE* fp = NULL;
	int fd;

	std::string name;
	char* data;

	off_t offset;
	config conf;
};


#endif /* MMAP_FD_H_ */
