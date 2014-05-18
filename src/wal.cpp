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
#include <fstream>

using namespace std;

#define NUM_KEYS   100000
#define NUM_TXNS   100000
#define NUM_THDS   2

#define VALUE_SIZE 2

unsigned long int num_threads = NUM_THDS;

long num_keys = NUM_KEYS ;
long num_txn  = NUM_TXNS ;
long num_wr   = 10 ;

std::mutex gc_mutex;
std::condition_variable cv;
bool ready = false;

#define DELIM ' '
#define LOG_LINE_SIZE VALUE_SIZE*3

int log_enable ;

std::chrono::time_point<std::chrono::system_clock> start, finish;
std::chrono::duration<double> elapsed_seconds;

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

        std::chrono::time_point<std::chrono::system_clock> start, end;
};

// TUPLE + TABLE + INDEX

class record{
    public:
        record(unsigned int _key, std::string _value) :
            key(_key),
            value(_value){}

        friend ostream& operator<<(ostream& out, const record& rec){
            out << DELIM << rec.key << DELIM << rec.value << DELIM;
            return out;
        }

        friend istream& operator>>(istream& in, record& rec){
            in.ignore(1); // skip delimiter
            in >> rec.key;

            in.ignore(1); // skip delimiter
            in >> rec.value;

            in.ignore(1); // skip delimiter
            return in;
        }


        //private:
        unsigned int key;
        std::string value;
};

pthread_rwlock_t  table_access;
vector<record> table;

unordered_map<unsigned int, record*> table_index;

// LOGGING

class entry{
    public:
        entry(txn _txn, record* _before_image, record* _after_image) :
            transaction(_txn),
            before_image(_before_image),
            after_image(_after_image){}

        //private:
        txn transaction;
        record* before_image;
        record* after_image;
};


class logger {
    public:
        logger(std::string name, std::string mode){
            std::string logFileName = name;

            log_file = fopen(logFileName.c_str(), mode.c_str());
            if (log_file != NULL) {
                cout << "Log file: " <<logFileName<< endl;
            }

            log_file_fd = fileno(log_file);
        }


        void push(entry e){
        	std::lock_guard<std::mutex> lock(log_access);

            log_queue.push_back(e);
        }

        int write(){
            int ret ;
            stringstream buffer_stream;
            string buffer;

            {
				std::lock_guard<std::mutex> lock(log_access);

				for (std::vector<entry>::iterator it = log_queue.begin(); it != log_queue.end(); ++it) {
					if ((*it).transaction.txn_type != "")
						buffer_stream << (*it).transaction.txn_type;

					if ((*it).before_image != NULL)
						buffer_stream << *((*it).before_image);

					if ((*it).after_image != NULL)
						buffer_stream << *((*it).after_image);

					buffer_stream << endl;
				}

				buffer = buffer_stream.str();
				size_t buffer_size = buffer.size();

				ret = fwrite(buffer.c_str(), sizeof(char), buffer_size, log_file);
				ret = fsync(log_file_fd);

				// Set end time
				/*
				 for (std::vector<entry>::iterator it = log_queue.begin() ; it != log_queue.end(); ++it){
				 	 (*it).transaction.end = std::chrono::system_clock::now();
				 	 std::chrono::duration<double> elapsed_seconds = (*it).transaction.end - (*it).transaction.start;
				 	 cout<<"Duration: "<< elapsed_seconds.count()<<endl;
				 }
				 */

				// Clear queue
				log_queue.clear();
            }

            return ret;
        }

        void close(){
        	std::lock_guard<std::mutex> lock(log_access);

        	fclose(log_file);
        }

    //private:
        FILE* log_file ;
        int log_file_fd;

        std::mutex log_access;
        vector<entry> log_queue;
};

logger undo_buffer(prefix+"./log", "w");

// GROUP COMMIT

void group_commit(){
    std::unique_lock<std::mutex> lk(gc_mutex);
    cv.wait(lk, []{return ready;});

    while(ready){
        std::cout << "Syncing log !\n"<<endl;

        // sync
        undo_buffer.write();

        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

}

// TRANSACTION OPERATIONS

int update(txn t){
    int key = t.key;
    int rc = -1;

    rc = pthread_rwlock_wrlock(&table_access);
    if(rc != 0){
    	cout<<"update:: wrlock failed \n";
    	return -1;
    }

    if(table_index.count(t.key) == 0){
        rc = pthread_rwlock_unlock(&table_access);
        if(rc != 0){
        	cout<<"update:: unlock failed \n";
        	return -1;
        }
        return -1;
    }

    record* before_image;
    record* after_image = new record(t.key, t.value);

	before_image = table_index.at(t.key);
	table.push_back(*after_image);
	table_index[key] = after_image;

    rc = pthread_rwlock_unlock(&table_access);
    if(rc != 0){
    	cout<<"update:: unlock failed \n";
    	return -1;
    }

    // Add log entry
    entry e(t, before_image, after_image);
    undo_buffer.push(e);

    return 0;
}

std::string read(txn t){
    int key = t.key;
    int rc = -1;
    std::string val = "" ;

    rc = pthread_rwlock_rdlock(&table_access);
    if(rc != 0){
    	cout<<"read:: rdlock failed \n";
    	return "";
    }

	if (table_index.count(t.key) == 0){
		rc = pthread_rwlock_unlock(&table_access);
		if (rc != 0) {
			cout << "read:: rdlock failed \n";
			return "";
		}
		return "";
	}

	record* r = table_index[key];
	if (r != NULL) {
		val = r->value;
	}

	rc = pthread_rwlock_unlock(&table_access);
	if (rc != 0) {
		cout << "read:: rdlock failed \n";
		return "";
	}

    return val;
}

int insert(txn t){
    int key = t.key;
    int rc = -1;

    rc = pthread_rwlock_wrlock(&table_access);
    if(rc != 0){
    	cout<<"update:: wrlock failed \n";
    	return -1;
    }

    // check if key already exists
    if(table_index.count(t.key) != 0){
        rc = pthread_rwlock_unlock(&table_access);
        if(rc != 0){
        	cout<<"update:: unlock failed \n";
        	return -1;
        }
        return -1;
    }

    record* after_image = new record(t.key, t.value);

	table.push_back(*after_image);
	table_index[key] = after_image;

    rc = pthread_rwlock_unlock(&table_access);
    if(rc != 0){
    	cout<<"update:: unlock failed \n";
    	return -1;
    }

    // Add log entry
    entry e(t, NULL, after_image);
    undo_buffer.push(e);

    return 0;
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
	unordered_map<unsigned int, record*>::const_iterator t_itr;

    for (t_itr = table_index.begin() ; t_itr != table_index.end(); ++t_itr){
        cout << (*t_itr).first <<" "<< (*t_itr).second->value << endl;
    }

}

void load(){
    size_t ret;
    stringstream buffer_stream;
    string buffer;
    int rc = -1;

    for(int i=0 ; i<num_keys ; i++){
        int key = i;
        string value = random_string(VALUE_SIZE);
        record* after_image = new record(key, value);

        {
    	    rc = pthread_rwlock_wrlock(&table_access);
    	    if(rc != 0){
    	    	cout<<"load:: wrlock failed \n";
    	    	return;
    	    }


            table.push_back(*after_image);
            table_index[key] = after_image;

    	    rc = pthread_rwlock_unlock(&table_access);
    	    if(rc != 0){
    	    	cout<<"load:: unlock failed \n";
    	    	return;
    	    }
        }

        // Add log entry
        txn t(0, "Insert", key, value);

        entry e(t, NULL, after_image);
        undo_buffer.push(e);
    }

    // sync
    undo_buffer.write();
}

void snapshot(){
	int rc = -1;

    start = std::chrono::system_clock::now();

    rc = pthread_rwlock_wrlock(&table_access);
    if(rc != 0){
    	cout<<"update:: wrlock failed \n";
    	return;
    }

	logger snapshotter(prefix+"./snapshot", "w");
	unordered_map<unsigned int, record*>::const_iterator t_itr;

    for (t_itr = table_index.begin() ; t_itr != table_index.end(); ++t_itr){
        txn t(0, "", -1, "");
        record* tuple = (*t_itr).second;

        entry e(t, tuple, NULL);
    	snapshotter.push(e);
    }

    // sync
    snapshotter.write();
    snapshotter.close();

    // Add log entry
    txn t(0, "Snapshot", -1, "");
    entry e(t, NULL, NULL);
    undo_buffer.push(e);

    // sync
    undo_buffer.write();

    rc = pthread_rwlock_unlock(&table_access);
    if(rc != 0){
    	cout<<"update:: unlock failed \n";
    	return;
    }

    finish = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds = finish - start;
    std::cout<<"Snapshot  duration: "<< elapsed_seconds.count()<<endl;
}

void recovery(){

	// Clear stuff
	undo_buffer.close();
	table.clear();
	table_index.clear();

	cout<<"Read snapshot"<<endl;

	// Read snapshot
	logger reader(prefix+"./snapshot", "r");
	unsigned int key = -1;
	char value[VALUE_SIZE];
	char txn_type[10];

	while(fscanf(reader.log_file, "%d %s \n", &key, value) != EOF){
        txn t(0, "Insert", key, std::string(value));
        insert(t);
	}

	reader.close();

	cout<<"Apply log"<<endl;

	ifstream scanner (prefix+"./log");
	string line;
	bool apply = false;
	char before_val[VALUE_SIZE], after_val[VALUE_SIZE];
	unsigned int before_key, after_key;

	if (scanner.is_open()) {

		while (getline(scanner, line)) {
			if(apply == false){
				sscanf(line.c_str(), "%s", txn_type);
				if(strcmp(txn_type, "Snapshot") ==0)
					apply = true;
			}
			// Apply log
			else{
				sscanf(line.c_str(), "%s", txn_type);

				// Update
				if(strcmp(txn_type, "Update") ==0){
					sscanf(line.c_str(), "%s %d %s %d %s", txn_type, &before_key, before_val, &after_key, after_val);
			        txn t(0, "Update", after_key, std::string(after_val));
			        update(t);
				}

			}
		}

		scanner.close();
	}

}

int main(){
	// Loader
    load();
    std::cout<<"Loading finished "<<endl;
    //check();

    // Take snapshot
    snapshot();

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

    undo_buffer.write();

    finish = std::chrono::system_clock::now();
    elapsed_seconds = finish - start;
    std::cout<<"Execution duration: "<< elapsed_seconds.count()<<endl;

    //check();

    /*
    // Recover
    start = std::chrono::system_clock::now();

    recovery();

    finish = std::chrono::system_clock::now();
    elapsed_seconds = finish - start;
    std::cout<<"Recovery duration: "<< elapsed_seconds.count()<<endl;

    //check();
     */

    return 0;
}
