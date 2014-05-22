#ifndef MASTER_H_
#define MASTER_H_

#include <string>

#include "mmap_fd.h"

using namespace std;

#define MASTER_LOC 0x01a00000

#define DIR_LOC    0x01c00000
#define DIR0_LOC   0x01d00000
#define DIR1_LOC   0x01e00000

typedef std::unordered_map<unsigned int, char*> inner_map;
typedef std::unordered_map<unsigned int, inner_map*> outer_map;
typedef std::unordered_map<unsigned int, char*> chunk_map;
typedef std::unordered_map<unsigned int, bool> chunk_status;

// MASTER

class master {
public:
	master() : name(""), dir_fd_ptr(0), dir_ptr(0) {}

	master(std::string table_name, config conf){
		chunk = mmap_fd(conf.fs_path + table_name + "_chunk", (caddr_t) DIR_LOC, conf);

		dir_fds[0] = mmap_fd(conf.fs_path + table_name + "_dir0", (caddr_t) DIR0_LOC, conf);
		dir_fds[1] = mmap_fd(conf.fs_path + table_name + "_dir1", (caddr_t) DIR1_LOC, conf);

		master_fd = mmap_fd(conf.fs_path + table_name, (caddr_t) MASTER_LOC, conf);

		// Initialize
		dir_fd_ptr = 0;
		dir_ptr = 0;
	}

	mmap_fd& get_dir_fd() {
		return dir_fds[dir_fd_ptr];
	}

	outer_map& get_dir() {
		return dirs[dir_ptr];
	}

	outer_map& get_clean_dir() {
		return dirs[!dir_ptr];
	}

	chunk_map& get_chunk_map() {
		return cmaps[dir_ptr];
	}

	chunk_map& get_clean_chunk_map() {
		return cmaps[!dir_ptr];
	}

	chunk_status& get_status() {
		return status;
	}

	void display_clean_dir() {
		outer_map::const_iterator o_itr;
		inner_map::const_iterator i_itr;

		for (o_itr = get_clean_dir().begin(); o_itr != get_clean_dir().end();
				o_itr++) {
			cout << (*o_itr).first << "::";
			inner_map* imap = (*o_itr).second;
			for (i_itr = (*imap).begin(); i_itr != (*imap).end(); i_itr++)
				cout << (*i_itr).first << " ";
			cout << endl;
		}
	}

	void display_dir() {
		outer_map::const_iterator o_itr;
		inner_map::const_iterator i_itr;

		for (o_itr = get_dir().begin(); o_itr != get_dir().end(); o_itr++) {
			cout << (*o_itr).first << "::";
			inner_map* imap = (*o_itr).second;
			for (i_itr = (*imap).begin(); i_itr != (*imap).end(); i_itr++)
				cout << (*i_itr).first << " ";
			cout << endl;
		}
	}

	void display_status() {
		chunk_status::const_iterator c_itr;

		for (c_itr = get_status().begin(); c_itr != get_status().end(); c_itr++)
			cout << (*c_itr).first << " : " << (*c_itr).second << endl;
	}

	void sync() {
		std::cout << "Syncing master !" << endl;

		int rc = -1;

		rc = pthread_rwlock_wrlock(&table_access);
		if (rc != 0) {
			cout << "sync:: wrlock failed \n";
			return;
		}

		// Log the dirty dir
		outer_map& outer = get_dir();
		chunk_status& status = get_status();

		outer_map::iterator itr;
		unsigned int chunk_id = 0;
		char* location;

		/*
		 for(itr = outer.begin() ; itr != outer.end() ; itr++){
		 chunk_id = (*itr).first;

		 if(status[chunk_id] == true){
		 //cout<<"Push back chunk :"<<chunk_id<<endl;
		 location = chunk.push_back_chunk(outer[chunk_id]);
		 get_chunk_map()[chunk_id] = location;
		 }
		 else{
		 //cout<<"Keep chunk from clean dir :"<<chunk_id<<endl;
		 get_chunk_map()[chunk_id] = get_clean_chunk_map()[chunk_id];
		 }
		 }

		 dir_fds[dir_fd_ptr].copy(get_chunk_map());
		 dir_fds[dir_fd_ptr].sync();

		 // Sync master
		 master_fd.set(dir_ptr);
		 master_fd.sync();
		 */

		// Toggle
		toggle();

		// Setup new dirty dir from old clean dir
		get_dir_fd().reset_fd();
		get_dir().clear();
		outer = get_clean_dir();

		for (itr = outer.begin(); itr != outer.end(); itr++) {
			chunk_id = (*itr).first;

			get_dir()[chunk_id] = get_clean_dir()[chunk_id];
			status[chunk_id] = false;
		}

		rc = pthread_rwlock_unlock(&table_access);
		if (rc != 0) {
			cout << "sync:: unlock failed \n";
			return;
		}

	}

	void toggle() {
		dir_fd_ptr = !dir_fd_ptr;
		dir_ptr = !dir_ptr;
	}

	pthread_rwlock_t table_access = PTHREAD_RWLOCK_INITIALIZER;

private:
	std::string name;

	// in-file dirs
	mmap_fd dir_fds[2];
	mmap_fd master_fd;
	mmap_fd chunk;

	// in-memory dirs
	outer_map dirs[2];

	// in-memory dirty dir chunk status
	chunk_status status;

	// in-memory chunk maps
	chunk_map cmaps[2];

	// master
	bool dir_fd_ptr;
	bool dir_ptr;

};


#endif /* MASTER_H_ */
