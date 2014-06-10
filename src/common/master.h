#ifndef MASTER_H_
#define MASTER_H_

#include <string>

#include "mmap_fd.h"

using namespace std;

#define MASTER_LOC 0x02a00000
#define DIR_LOC    0x02b00000

typedef std::unordered_map<unsigned int, sp_record*> dir_map;

// MASTER

class master {
public:
	master() :
			name("") {
	}

	master(std::string table_name, config conf) {

		dir_fd = mmap_fd(conf.fs_path + table_name + "_dir", (caddr_t) DIR_LOC, conf);
		master_fd = mmap_fd(conf.fs_path + table_name + "_master",  (caddr_t) MASTER_LOC, conf);

	}

	mmap_fd& get_dir_fd() {
		return dir_fd;
	}

	dir_map& get_dir(){
		return dir;
	}

	void sync() {
		//std::cout << "Syncing master !" << endl;
		//wrlock(&table_access);

		// XXX Log the dirty dir

		// Sync master
		master_fd.push_back_batch_id(batch_id);
		master_fd.sync();
		batch_id++;

		//unlock(&table_access);
	}

	pthread_rwlock_t table_access = PTHREAD_RWLOCK_INITIALIZER;

	int batch_id = 0;

private:
	std::string name;
	config conf;

	// in-file dirs
	mmap_fd dir_fd;
	mmap_fd master_fd;

	// in-memory dirs
	dir_map dir;

};


#endif /* MASTER_H_ */
