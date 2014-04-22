#include <iostream>
#include <vector>
#include <string>
#include <random>
#include <functional>
#include <algorithm>
#include <sstream>
#include <utility>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <unordered_map>
#include <memory>

#include <boost/thread.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>

#include <chrono>
#include <random>

#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>

using namespace std;

#define NUM_KEYS 1000
#define NUM_TXNS 10000

#define VALUE_SIZE 128

int log_enable = 0 ;

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
            out << "|" << rec.key << "|" << rec.value << "|";
            return out;
        }

        friend istream& operator>>(istream& in, record& rec){
            in.ignore(1); // skip delimiter

            in >> rec.key;
            in.ignore(1); // skip delimiter
            in >> rec.value;
            in.ignore(1); // skip delimiter
            //in.ignore(1); // skip endline

            return in;
        }


        //private:
        unsigned int key;
        std::string value;
};

boost::shared_mutex table_access;

unordered_map<unsigned int, record*> table_index;

// MMAP
class mmap_table{
	public:
		mmap_table();

		mmap_table(std::string table_name) :
			_table_name(table_name)
		{
			if ((_table_fd = open(_table_name.c_str(), O_RDWR | O_CREAT, 0644)) == -1) {
				cout<<"open failed "<<_table_name<<" \n";
				exit(EXIT_FAILURE);
			}

			struct stat sbuf;

		    if (stat(_table_name.c_str(), &sbuf) == -1) {
				cout<<"stat failed "<<_table_name<<" \n";
		        exit(EXIT_FAILURE);
		    }

		    cout<<"size :"<< sbuf.st_size << endl;

		    caddr_t location = (caddr_t) 0x01c00000;

		    // new file check
		    if(sbuf.st_size == 0){
			    off_t offset = 0;
			    off_t len = 1024*1024;

		    	if(fallocate(_table_fd, 0, offset, len) == -1){
					cout<<"fallocate failed "<<_table_name<<" \n";
			        exit(EXIT_FAILURE);
		    	}

		    	if (stat(_table_name.c_str(), &sbuf) == -1) {
		    		cout << "stat failed " << _table_name << " \n";
		    		exit(EXIT_FAILURE);
		    	}

		    	_table_offset = 0;
		    }

		    // XXX Fix -- scan max pointer from clean dir
	    	_table_offset = 0;

		    if ((_table_data = (char*) mmap(location, sbuf.st_size, PROT_WRITE, MAP_SHARED, _table_fd, 0)) == (caddr_t)(-1)) {
				cout<<"mmap failed "<<_table_name<<" \n";
		        exit(EXIT_FAILURE);
		    }

		    cout<<"data :"<< _table_data << endl;
		}

	char* push_back (const record& rec){
        stringstream rec_stream;
        string rec_str;

        rec_stream << rec.key << rec.value;
        rec_str = rec_stream.str();

        char* cur_offset = (_table_data + _table_offset);
		memcpy(cur_offset, rec_str.c_str(), rec_str.size());

		_table_offset += rec_str.size();

		return cur_offset;
	}

	//private:
		int _table_fd;
		std::string _table_name;
		char* _table_data;
		off_t _table_offset;

};

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
        logger(){
            //std::string logFileName = "/mnt/pmfs/n-store/log";
            std::string logFileName = "./log";

            logFile = fopen(logFileName.c_str(), "w");
            if (logFile != NULL) {
                cout << "Log file: " <<logFileName<< endl;
            }

            logFileFD = fileno(logFile);
        }


        void push(entry e){
            boost::upgrade_lock< boost::shared_mutex > lock(log_access);
            boost::upgrade_to_unique_lock< boost::shared_mutex > uniqueLock(lock);
            // exclusive access

            log_queue.push_back(e);
        }

        int write(){
            int ret ;
            stringstream buffer_stream;
            string buffer;

            //cout<<"queue size :"<<log_queue.size()<<endl;

            boost::upgrade_lock< boost::shared_mutex > lock(log_access);
            boost::upgrade_to_unique_lock< boost::shared_mutex > uniqueLock(lock);
            // exclusive access

            for (std::vector<entry>::iterator it = log_queue.begin() ; it != log_queue.end(); ++it){
                buffer_stream << (*it).transaction.txn_type ;

                if((*it).before_image != NULL)
                    buffer_stream << *((*it).before_image) ;
                // XXX Add dummy before image

                if((*it).after_image != NULL)
                    buffer_stream << *((*it).after_image) <<endl;
            }

            buffer = buffer_stream.str();
            size_t buffer_size = buffer.size();

            ret = fwrite(buffer.c_str(), sizeof(char), buffer_size, logFile);
            //cout<<"write size :"<<ret<<endl;

            ret = fsync(logFileFD);

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

            return ret;
        }

    private:
        FILE* logFile ;
        int logFileFD;

        boost::shared_mutex log_access;
        vector<entry> log_queue;
};

logger _undo_buffer;

void logger_func(const boost::system::error_code& /*e*/, boost::asio::deadline_timer* t){
    std::cout << "Syncing log !\n"<<endl;

    // sync

    t->expires_at(t->expires_at() + boost::posix_time::milliseconds(100));
    t->async_wait(boost::bind(logger_func, boost::asio::placeholders::error, t));
}

void group_commit(){
    if(log_enable){
        boost::asio::io_service io;
        boost::asio::deadline_timer t(io, boost::posix_time::milliseconds(100));

        t.async_wait(boost::bind(logger_func, boost::asio::placeholders::error, &t));
        io.run();
    }
}

// TRANSACTION OPERATIONS

int update(txn t){
    int key = t.key;

    if(table_index.count(t.key) == 0) // key does not exist
        return -1;

    record* before_image;
    record* after_image = new record(t.key, t.value);

    {
        boost::upgrade_lock< boost::shared_mutex > lock(table_access);
        // shared access
        before_image = table_index.at(t.key);

        boost::upgrade_to_unique_lock< boost::shared_mutex > uniqueLock(lock);
        // exclusive access

        //table.push_back(*after_image);
        table_index[key] = after_image;
    }

    // Add log entry
    entry e(t, before_image, after_image);
    _undo_buffer.push(e);

    return 0;
}

std::string read(txn t){
    int key = t.key;
    if (table_index.count(t.key) == 0) // key does not exist
        return "";

    std::string val = "" ;

    {
        boost::upgrade_lock<boost::shared_mutex> lock(table_access);
        // shared access

        record* r = table_index[key];
        if(r != NULL){
            val = r->value;
        }
    }

    return val;
}

// RUNNER + LOADER

int num_threads = 4;

long num_keys = NUM_KEYS ;
long num_txn  = NUM_TXNS ;
long num_wr   = 50 ;

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

    //for (std::vector<record>::iterator it = table.begin() ; it != table.end(); ++it){
    //    cout << *it << endl;
    //}

}

void load(){
    size_t ret;
    stringstream buffer_stream;
    string buffer;

    for(int i=0 ; i<num_keys ; i++){
        int key = i;
        string value = random_string(VALUE_SIZE);
        record* after_image = new record(key, value);

        {
            boost::upgrade_lock<boost::shared_mutex> lock(table_access);
            boost::upgrade_to_unique_lock<boost::shared_mutex> uniqueLock(lock);
            // exclusive access

            //table.push_back(*after_image);
            table_index[key] = after_image;
        }

        // Add log entry
        txn t(0, "Insert", key, value);

        entry e(t, NULL, after_image);
        _undo_buffer.push(e);
    }

    group_commit();

}

int main(){
    std::chrono::time_point<std::chrono::system_clock> start, end;

    // Loader
    load();
    std::cout<<"Loading finished "<<endl;

    mmap_table user_table("usertable");

    record r1(1,"v1");
    record r2(2,"v2");

    char* ret ;

    ret = user_table.push_back(r1);
    printf("ret: %p \n", ret);

    ret = user_table.push_back(r2);
    printf("ret: %p \n", ret);

    //boost::thread group_committer(group_commit);

    // Runner
    /*
    boost::thread_group th_group;
    for(int i=0 ; i<num_threads ; i++)
        th_group.create_thread(boost::bind(runner));

    th_group.join_all();
    */
    //check();


    return 0;
}

