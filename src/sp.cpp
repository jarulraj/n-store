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

#define NUM_KEYS 10000
#define NUM_TXNS 10000

#define VALUE_SIZE 128
#define SIZE       NUM_KEYS*VALUE_SIZE*100

unsigned long int num_threads = 4;

long num_keys = NUM_KEYS ;
long num_txn  = NUM_TXNS ;
long num_wr   = 10 ;

#define TUPLE_SIZE 4 + 4 + VALUE_SIZE + 1

#define MASTER_LOC 0x01a00000
#define TABLE_LOC  0x01b00000
#define DIR0_LOC   0x01c00000
#define DIR1_LOC   0x01d00000

std::mutex gc_mutex;
std::condition_variable cv;
bool ready = false;

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

        std::chrono::time_point<std::chrono::system_clock> start, end;
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

        friend ostream& operator<<(ostream& out, const record& rec){
            out << "|" << rec.key << "|" << rec.value << "|";
            return out;
        }

        friend istream& operator>>(istream& in, record& rec){
            in.ignore(1); // skip delimiter

            in >> rec.key;
            in.ignore(1);
            in >> rec.value;
            in.ignore(1);

            return in;
        }


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
			    off_t offset = 0;
			    off_t len = SIZE;

		    	if(fallocate(fd, 0, offset, len) == -1){
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
        stringstream rec_stream;
        string rec_str;

        rec_stream << rec.key <<" "<< rec.value<<" ";
        rec_str = rec_stream.str();

        char* cur_offset = (data + offset);
		memcpy(cur_offset, rec_str.c_str(), rec_str.size());

		offset += rec_str.size();

		return cur_offset;
	}

	void push_back_dir_entry(const record& rec){
		stringstream rec_stream;
		string rec_str;

		rec_stream << rec.key << static_cast<void *>(rec.location);
		rec_str = rec_stream.str();

		char* cur_offset = (data + offset);
		memcpy(cur_offset, rec_str.c_str(), rec_str.size());

		offset += rec_str.size();

	}

	record get_record(char* location){
		record r;
		unsigned int key;
		std::string value;

		char tuple[TUPLE_SIZE];
		memcpy(tuple, location, sizeof(tuple));

		istringstream ss(tuple);
		ss >> key >> value;

		r.key = key;
		r.value = value;
		r.location = location;

		return r;
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

	void copy(std::unordered_map<unsigned int, char*> dir){
		std::unordered_map<unsigned int, char*>::iterator itr;

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

mmap_fd table("usertable", (caddr_t) TABLE_LOC);

// MASTER

class master{
	public:

	master(std::string table_name) :
		name(table_name)
	{
		dir_fds[0] = mmap_fd(name+"_dir0",(caddr_t) DIR0_LOC);
		dir_fds[1] = mmap_fd(name+"_dir1",(caddr_t) DIR1_LOC);

		master_fd = mmap_fd("master",(caddr_t) MASTER_LOC);

		// Initialize
		dir_fd_ptr = 0;
		dir = 0;
	}

	mmap_fd& get_dir_fd(){
		return dir_fds[dir_fd_ptr];
	}

	unordered_map<unsigned int, char*>& get_dir(){
		return dirs[dir];
	}

	void sync(){
	    int rc = -1;

	    rc = pthread_rwlock_wrlock(&table_access);
	    if(rc != 0){
	    	cout<<"sync:: wrlock failed \n";
	    	return;
	    }

		// First copy and then flush dirty dir
		dir_fds[dir_fd_ptr].copy(get_dir());
		dir_fds[dir_fd_ptr].sync();

		// Sync master
		master_fd.set(dir);
		master_fd.sync();

		// Toggle
		toggle();

		// Clean up new dir fd and dir
		get_dir_fd().reset_fd();
		get_dir().clear();
		get_dir().insert(get_clean_dir().begin(), get_clean_dir().end());

		rc = pthread_rwlock_unlock(&table_access);
		if (rc != 0) {
			cout << "sync:: unlock failed \n";
			return;
		}

	}

	unordered_map<unsigned int, char*>& get_clean_dir(){
		// Toggle the dir bit
		return dirs[!dir];
	}

	void toggle(){
		dir_fd_ptr = !dir_fd_ptr;
		dir = !dir;
	}

	//private:
	std::string name;

	// file dirs
	mmap_fd dir_fds[2];
	mmap_fd master_fd;

	// in-memory dirs
	unordered_map<unsigned int, char*> dirs[2];

	// master
	bool dir_fd_ptr;
	bool dir;
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
        std::cout << "Syncing table and master !\n"<<endl;

        table.sync();
        mstr.sync();

        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

}

// TRANSACTION OPERATIONS

int update(txn t){
    int key = t.key;
    char* before_image;
    char* after_image;

    // key does not exist
    if(mstr.get_dir().count(t.key) == 0){
        std::cout<<"Key does not exist : "<<key<<endl;
        return -1;
    }

    record* rec = new record(t.key, t.value, NULL);

    {
        after_image = table.push_back_record(*rec);
        rec->location = after_image;

        before_image = mstr.get_dir()[t.key];
        mstr.get_dir()[t.key] = after_image;

        //std::cout<<"Updated "<<key<<endl;
    }

    // Add log entry
    entry e(t, before_image, after_image);
    _undo_buffer.push(e);

    return 0;
}

std::string read(txn t){
    int key = t.key;
    std::string val;
    int rc = -1;

	rc = pthread_rwlock_rdlock(&table_access);
	if (rc != 0) {
		cout << "read:: rdlock failed \n";
		return "";
	}

    if (mstr.get_clean_dir().count(t.key) == 0) // key does not exist
        return "not exists";


    char* location =  mstr.get_clean_dir()[key];
    record r = table.get_record(location);
    val = r.value;

	rc = pthread_rwlock_unlock(&table_access);
	if (rc != 0) {
		cout << "read:: unlock failed \n";
		return "";
	}

    return val;
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
        string value = random_string(VALUE_SIZE);

        record* rec = new record(key, value, NULL);

        {
            after_image = table.push_back_record(*rec);
            rec->location = after_image;

            mstr.get_dir()[key] = after_image;
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

