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

typedef std::unordered_map<unsigned int, char*> inner_map;
typedef std::unordered_map<unsigned int, inner_map*> outer_map;
typedef std::unordered_map<unsigned int, char*> chunk_map;
typedef std::unordered_map<unsigned int, bool> chunk_status;

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
			off_t len = conf.num_keys*conf.sz_value*10000 + conf.num_thds*conf.num_txns*conf.sz_value ;

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

		if ((data = (char*) mmap(location, sbuf.st_size, PROT_WRITE, MAP_SHARED,
				fd, 0)) == (caddr_t) (-1)) {
			perror(" mmap_error ");
			cout << "mmap failed " << name << endl;
			exit(EXIT_FAILURE);
		}

	}

	char* push_back_record(const record& rec) {
		char rec_str[conf.sz_tuple];
		int len = 0;

		sprintf(rec_str, "%u %s ", rec.key, rec.value.c_str());
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

	char* push_back_chunk(const inner_map* rec) {
		stringstream rec_stream;
		string rec_str;
		inner_map::const_iterator itr;
		size_t chunk_size = 0;

		if (rec == NULL) {
			cout << "Empty chunk" << endl;
			return NULL;
		}

		for (itr = rec->begin(); itr != rec->end(); itr++) {
			rec_stream << (*itr).first << DELIM << static_cast<void *>((*itr).second) << DELIM;
		}

		rec_stream << CHUNK_DELIM << endl;
		rec_str = rec_stream.str();
		chunk_size = rec_str.size();

		char* cur_offset = (data + offset);
		memcpy(cur_offset, rec_str.c_str(), chunk_size);

		//cout<<"chunk size: "<< chunk_size<<endl;
		offset += chunk_size;

		return cur_offset;
	}

	std::string get_value(char* location) {
		unsigned int key;
		char val[conf.sz_value];
		char tuple[conf.sz_tuple];

		memcpy(tuple, location, sizeof(tuple));
		sscanf(tuple, "%ud %s ", &key, val);

		return std::string(val);
	}

	void sync() {
		int ret = 0;

		//cout<<"msync offset: "<<offset<<endl;

		ret = msync(data, offset, MS_SYNC);
		if (ret == -1) {
			perror("msync failed");
			exit(EXIT_FAILURE);
		}

	}

	void copy(chunk_map dir) {
		chunk_map::iterator itr;

		for (itr = dir.begin(); itr != dir.end(); itr++) {
			sp_record r((*itr).first, "", (*itr).second);
			this->push_back_dir_entry(r);
		}

	}

	void set(bool dir) {
		char* cur_offset = (data);

		if (dir == false)
			*cur_offset = '0';
		else
			*cur_offset = '1';
	}

	void reset_fd() {
		offset = 0;
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
