#include <iostream>
#include <vector>
#include <string>
#include <random>
#include <functional>
#include <algorithm>
#include <sstream>
#include <utility>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <cassert>

#include <memory>
#include <chrono>
#include <random>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <unordered_map>

#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>

using namespace std;

#define NUM_KEYS 1024*32
#define NUM_TXNS 200000
#define CHUNK_SIZE 1024

#define VALUE_SIZE 2
#define SIZE       NUM_KEYS*VALUE_SIZE*100 + NUM_TXNS*VALUE_SIZE*2

unsigned long int num_threads = 2;

long num_keys = NUM_KEYS ;
long num_txn  = NUM_TXNS ;
long num_wr   = 10 ;

#define TUPLE_SIZE 4 + 4 + VALUE_SIZE + 10

#define MASTER_LOC 0x01a00000
#define TABLE_LOC  0x01b00000

#define DIR_LOC    0x01c00000
#define DIR0_LOC   0x01d00000
#define DIR1_LOC   0x01e00000


typedef std::unordered_map<unsigned int, char*> inner_map;
typedef std::unordered_map<unsigned int, inner_map*> outer_map;
typedef std::unordered_map<unsigned int, char*> chunk_map;
typedef std::unordered_map<unsigned int, bool> chunk_status;

std::mutex gc_mutex;
std::condition_variable cv;
bool ready = false;

//std::string prefix = "/mnt/pmfs/n-store/";
std::string prefix = "./";

// UTILS
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

// TRANSACTION

class txn{
    public:
        txn(unsigned long _txn_id, std::string _txn_type, unsigned int _key, std::string _value) :
            txn_id(_txn_id),
            txn_type(_txn_type),
            key(_key),
            value(_value)
    {
        //start = std::chrono::system_clock::now();
    }

        //private:
        unsigned long txn_id;
        std::string txn_type;

        unsigned int key;
        std::string value;

        //std::chrono::time_point<std::chrono::system_clock> start, end;
};

// TUPLE + TABLE + INDEX

class record{
    public:
		record() :
			key(0),
			value(""),
			location(NULL)
    	{
		}

        record(unsigned int _key, std::string _value, char* _location) :
            key(_key),
            value(_value),
            location(_location){}

        //private:
        unsigned int key;
        std::string value;
        char* location;
};

pthread_rwlock_t  table_access;

// MMAP
class mmap_fd{
	public:
		mmap_fd() :
			name(""),
			fd(-1),
			offset(0),
			data(NULL)
		{
		}


		mmap_fd(std::string _name, caddr_t location) :
			name(_name)
		{
			if ((fp = fopen(name.c_str(), "w+")) == NULL) {
				cout<<"fopen failed "<<name<<" \n";
				exit(EXIT_FAILURE);
			}

			fd = fileno(fp);

			struct stat sbuf;

		    if (stat(name.c_str(), &sbuf) == -1) {
				cout<<"stat failed "<<name<<" \n";
		        exit(EXIT_FAILURE);
		    }

		    //cout<<"size :"<< sbuf.st_size << endl;

		    // new file check
		    if(sbuf.st_size == 0){
			    off_t len = SIZE;

		    	if(ftruncate(fd, len) == -1){
					cout<<"fallocate failed "<<name<<" \n";
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

		    if ((data = (char*) mmap(location, sbuf.st_size, PROT_WRITE, MAP_SHARED, fd, 0)) == (caddr_t)(-1)) {
		    	perror(" mmap_error ");
				cout<<"mmap failed "<<name<<endl;
		        exit(EXIT_FAILURE);
		    }

		    //cout<<"data :"<< data << endl;
		}

	char* push_back_record (const record& rec){
		char rec_str[TUPLE_SIZE];
		int len = 0;

		sprintf(rec_str, "%ud %s ", rec.key, rec.value.c_str());
		len = strlen(rec_str);

		char* cur_offset = (data + offset);
		memcpy(cur_offset, rec_str, len);
		offset += len;

		return cur_offset;
	}

	void push_back_dir_entry(const record& rec){
		char rec_str[TUPLE_SIZE];
		int len = 0;

		sprintf(rec_str, "%ud %p ", rec.key, rec.location);
		len = strlen(rec_str);

		char* cur_offset = (data + offset);
		memcpy(cur_offset, rec_str, len);

		offset += len;
	}

	char* push_back_chunk(const inner_map* rec) {
		stringstream rec_stream;
		string rec_str;
		inner_map::const_iterator itr;

		if(rec == NULL){
			cout<<"Empty chunk"<<endl;
			return NULL;
		}

		for (itr = rec->begin(); itr != rec->end(); itr++) {
			rec_stream << (*itr).first << static_cast<void *>((*itr).second);
		}

		rec_str = rec_stream.str();
		char* cur_offset = (data + offset);
		memcpy(cur_offset, rec_str.c_str(), rec_str.size());

		offset += rec_str.size();

		return cur_offset;
	}

	std::string get_value(char* location){
		unsigned int key;
		char val[VALUE_SIZE];
		char tuple[TUPLE_SIZE];

		memcpy(tuple, location, sizeof(tuple));
		sscanf(tuple,"%ud %s ", &key, val);

		return std::string(val);
	}

	void sync(){
		int ret = 0;

		//cout<<"msync offset: "<<offset<<endl;

		ret = msync(data, offset, MS_SYNC);
		if(ret == -1){
			perror("msync failed");
	        exit(EXIT_FAILURE);
		}

	}

	void copy(chunk_map dir){
		chunk_map::iterator itr;

		for(itr = dir.begin() ; itr != dir.end() ; itr++){
			record r((*itr).first, "", (*itr).second);
			this->push_back_dir_entry(r);
		}

	}

	void set(bool dir){
		char* cur_offset = (data);

		if(dir == false)
			*cur_offset = '0';
		else
			*cur_offset = '1';
	}

	void reset_fd(){
		offset = 0;
	}

	private:
		FILE* fp = NULL;
		int fd;
		std::string name;
		char* data;
		off_t offset;

};

mmap_fd table(prefix + "usertable", (caddr_t) TABLE_LOC);

// MASTER

class master{
	public:

	master(std::string table_name) :
		name(table_name)
	{
		chunk      = mmap_fd(prefix + name + "_chunk",(caddr_t) DIR_LOC);
		dir_fds[0] = mmap_fd(prefix + name + "_dir0",(caddr_t) DIR0_LOC);
		dir_fds[1] = mmap_fd(prefix + name + "_dir1",(caddr_t) DIR1_LOC);

		master_fd = mmap_fd(prefix + "master",(caddr_t) MASTER_LOC);

		// Initialize
		dir_fd_ptr = 0;
		dir_ptr = 0;
	}

	mmap_fd& get_dir_fd(){
		return dir_fds[dir_fd_ptr];
	}

	outer_map& get_dir(){
		return dirs[dir_ptr];
	}

	outer_map& get_clean_dir() {
		return dirs[!dir_ptr];
	}

	chunk_map& get_chunk_map(){
		return cmaps[dir_ptr];
	}

	chunk_map& get_clean_chunk_map() {
		return cmaps[!dir_ptr];
	}

	chunk_status& get_status(){
		return status;
	}

	void display_clean_dir(){
		outer_map::const_iterator o_itr;
		inner_map::const_iterator i_itr;

		for(o_itr = get_clean_dir().begin() ; o_itr != get_clean_dir().end() ; o_itr++){
			cout<<(*o_itr).first<<"::";
			inner_map* imap = (*o_itr).second;
			for(i_itr = (*imap).begin() ; i_itr != (*imap).end() ; i_itr++)
				cout<<(*i_itr).first<<" ";
			cout<<endl;
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

	void display_status(){
		chunk_status::const_iterator c_itr;

		for(c_itr = get_status().begin() ; c_itr != get_status().end() ; c_itr++)
			cout<<(*c_itr).first<<" : "<<(*c_itr).second<<endl;
	}

	void sync(){
        std::cout << "Syncing master !\n"<<endl;

	    int rc = -1;

	    rc = pthread_rwlock_wrlock(&table_access);
	    if(rc != 0){
	    	cout<<"sync:: wrlock failed \n";
	    	return;
	    }


	    // Log the dirty dir
		outer_map& outer = get_dir();
	    chunk_status& status = get_status();

	    outer_map::iterator itr;
		unsigned int chunk_id = 0;
	    char* location;

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

		// Toggle
		toggle();

		// Setup new dirty dir from old clean dir
		get_dir_fd().reset_fd();
		get_dir().clear();
		outer = get_clean_dir();

		for(itr = outer.begin() ; itr != outer.end() ; itr++){
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

	void toggle(){
		dir_fd_ptr = !dir_fd_ptr;
		dir_ptr = !dir_ptr;
	}

	//private:
	std::string name;

	// file dirs
	mmap_fd dir_fds[2];
	mmap_fd master_fd;
	mmap_fd chunk;

	// in-memory dirs
	outer_map dirs[2];

	// dirty dir chunk status
	chunk_status status;

	// chunk maps
    chunk_map cmaps[2];

	// master
	bool dir_fd_ptr;
	bool dir_ptr;
};

master mstr("usertable");

// LOGGING

class entry{
    public:
        entry(txn _txn, char* _before_image, char* _after_image) :
            transaction(_txn),
            before_image(_before_image),
            after_image(_after_image){}

        //private:
        txn transaction;
        char* before_image;
        char* after_image;
};


class logger {
    public:

        void push(entry e){
        	std::lock_guard<std::mutex> lock(log_access);

            log_queue.push_back(e);
        }

        void clear(){
        	log_queue.clear();
        }

    private:

        std::mutex log_access;
        vector<entry> log_queue;
};

logger _undo_buffer;

// GROUP COMMIT

void group_commit(){
    std::unique_lock<std::mutex> lk(gc_mutex);
    cv.wait(lk, []{return ready;});

    while(ready){
        table.sync();
        mstr.sync();

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

}

// TRANSACTION OPERATIONS

int update(txn t){
    int key = t.key;

    char* before_image;
    char* after_image;
    int rc = -1;
    unsigned int chunk_id = 0;

	chunk_id = key / CHUNK_SIZE;

	outer_map& outer = mstr.get_clean_dir();

	// chunk does not exist
	if (outer.count(chunk_id) == 0) {
		std::cout << "Update: chunk does not exist : " << key << endl;
		return -1;
	}

	// key does not exist
	inner_map* clean_chunk = outer[chunk_id];
	if (clean_chunk->count(key) == 0) {
		std::cout << "Update: key does not exist : " << key << endl;
		return -1;
	}

    record* rec = new record(key, t.value, NULL);
	after_image = table.push_back_record(*rec);
	rec->location = after_image;
	before_image = (*clean_chunk)[key];

    rc = pthread_rwlock_wrlock(&table_access);
    if(rc != 0){
    	cout<<"update:: wrlock failed \n";
    	return -1;
    }

	chunk_status& status = mstr.get_status();

	// New chunk
	if (status[chunk_id] == false) {
		inner_map* chunk = new inner_map;

		// Copy map from clean dir
		(*chunk).insert((*clean_chunk).begin(),(*clean_chunk).end());
		(*chunk)[key] = after_image;

		mstr.get_dir()[chunk_id] = chunk;
		status[chunk_id] = true;
	}
	// Add to existing chunk
	else{
		inner_map* chunk = mstr.get_dir()[chunk_id];

		before_image = (*chunk)[key];
		(*chunk)[key] = after_image;
	}

	rc = pthread_rwlock_unlock(&table_access);
	if (rc != 0) {
		cout << "update:: unlock failed \n";
		return -1;
	}

    // Add log entry
    entry e(t, before_image, after_image);
    _undo_buffer.push(e);

    return 0;
}

std::string read(txn t){
    int key = t.key;
    unsigned int chunk_id = 0;

	chunk_id = key / CHUNK_SIZE;

	outer_map& outer = mstr.get_clean_dir();
	if (outer.count(chunk_id) == 0){
        std::cout<<"Read: chunk does not exist : "<<key<<endl;
        return "not exists";
    }

    inner_map* clean_chunk = outer[chunk_id];
    if(clean_chunk->count(key) == 0){
        std::cout<<"Read: key does not exist : "<<key<<endl;
        return "not exists";
    }

    char* location = (*clean_chunk)[key];
    return table.get_value(location);
}


// RUNNER + LOADER

void runner(){
    std::string val;

    for(int i=0 ; i<num_txn ; i++){
        long r = rand();
        std::string val = random_string(VALUE_SIZE);
        long key = r%num_keys;

        if(r % 100 < num_wr){
            txn t(i, "Update", key, val);
            update(t);
        }
        else{
            txn t(i, "Read", key, val);
            val = read(t);
        }
    }

}


void check(){

	std::cout<<"Check :"<<std::endl;
	for(int i=0 ; i<num_keys ; i++){
	    std::string val;

        txn t(i, "Read", i, val);
		val = read(t);

		std::cout<<i<<" "<<val<<endl;
	}

}

void load(){
    size_t ret;
    char* after_image;

    for(int i=0 ; i<num_keys ; i++){
        int key = i;
        unsigned int chunk_id = 0;
        string value = random_string(VALUE_SIZE);

        record* rec = new record(key, value, NULL);

        after_image = table.push_back_record(*rec);
		rec->location = after_image;

		chunk_id = key / CHUNK_SIZE;

		// New chunk
		if (mstr.get_dir().count(chunk_id) == 0 || 	mstr.get_status()[chunk_id] == false){
			inner_map* chunk = new inner_map;
			(*chunk)[key] = after_image;

			//cout<<"New chunk :"<<chunk_id<<endl;
			mstr.get_dir()[chunk_id] = chunk;

			mstr.get_status()[chunk_id] = true;
		}
		// Add to existing chunk
		else {
			inner_map* chunk = mstr.get_dir()[chunk_id];
			//cout<<"Add to chunk :"<<chunk_id<<endl;

			if(chunk != NULL)
				(*chunk)[key] = after_image;
		}

        // Add log entry
        txn t(0, "Insert", key, value);

        entry e(t, NULL, after_image);
        _undo_buffer.push(e);
    }

    table.sync();
    mstr.sync();
}

int main(){
    std::chrono::time_point<std::chrono::system_clock> start, end;

    assert(CHUNK_SIZE < NUM_KEYS);

    // Loader
    load();
    std::cout<<"Loading finished "<<endl;
    //check();

    start = std::chrono::system_clock::now();

    // Logger start
    std::thread gc(group_commit);
    {
        std::lock_guard<std::mutex> lk(gc_mutex);
        ready = true;
    }
    cv.notify_one();

    // Runner
    std::vector<std::thread> th_group;
    for(int i=0 ; i<num_threads ; i++)
        th_group.push_back(std::thread(runner));

    for(int i=0 ; i<num_threads ; i++)
    	th_group.at(i).join();

    // Logger end
    ready = false;
    gc.join();

    //check();

    end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;
    std::cout<<"Duration: "<< elapsed_seconds.count()<<endl;

    return 0;
}

