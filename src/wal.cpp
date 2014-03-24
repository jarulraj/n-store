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
#include <boost/date_time/posix_time/posix_time.hpp>


using namespace std;

// UTILS
std::string random_string( size_t length )
{
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
			value(_value) {}

//private:
	unsigned long txn_id;
	std::string txn_type;

	unsigned int key;
	std::string value;
};

// TUPLE + TABLE + INDEX

class record{
public:
	record(unsigned int _key, std::string _value) :
		key(_key),
		value(_value){}

//private:
	unsigned int key;
	std::string value;
};

boost::shared_mutex table_access;
vector<record*> table;

unordered_map<unsigned int, record*> table_index;

// LOGGING

class entry{
public:
	entry(unsigned long _txn_id, std::string _txn_type, record* _before_image, record* _after_image) :
			txn_id(_txn_id),
			txn_type(_txn_type),
			before_image(_before_image),
			after_image(_after_image){}

//private:
	unsigned long txn_id;
	std::string txn_type;

	record* before_image;
	record* after_image;
};


class logger {
public:
	logger(){
		std::string logFileName = "log";

		logFile = fopen(logFileName.c_str(), "w");
		if (logFile != NULL) {
			cout << "Log file" <<logFileName<< endl;
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
			buffer_stream << (*it).txn_type << (*it).before_image << (*it).after_image ;
		}

		buffer = buffer_stream.str();
		size_t buffer_size = buffer.size();

		ret = fwrite(buffer.c_str(), sizeof(char), buffer_size, logFile);
		//cout<<"write size :"<<ret<<endl;

		// reset queue
		log_queue.clear();

		return ret;
	}

	int sync(){
		cout<<"fsync"<<endl;

		int ret = fsync(logFileFD);
		return ret;
	}

private:
	FILE* logFile ;
	int logFileFD;

	boost::shared_mutex log_access;
	vector<entry> log_queue;
};

logger _logger;

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

		table.push_back(after_image);
		table_index[key] = after_image;
	}

	// Add log entry
	entry e(t.txn_id, "Update", before_image, after_image);
	_logger.push(e);

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

long num_keys = 100 ;
long num_txn  = 1000 ;
long num_wr   = 10 ;

void runner(){
	std::string val;

	for(int i=0 ; i<num_txn ; i++){
		long r = rand();
		std::string val = "xxx";
		long key = r%num_keys;

		if(r % num_txn < num_wr){
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

	for (std::vector<record*>::iterator it = table.begin() ; it != table.end(); ++it){
		if(*it != NULL){
			cout << (*it)->key <<" "<< (*it)->value << endl;
		}
	}

}

void load(){
	size_t ret;
	stringstream buffer_stream;
	string buffer;

	for(int i=0 ; i<num_keys ; i++){
		int key = i;
		record* after_image = new record(key, random_string(3));

		{
			boost::upgrade_lock<boost::shared_mutex> lock(table_access);
			boost::upgrade_to_unique_lock<boost::shared_mutex> uniqueLock(lock);
			// exclusive access

			table.push_back(after_image);
			table_index[key] = after_image;
		}

		// Add log entry
		entry e(0, "Insert", NULL, after_image);
		_logger.push(e);
	}

	// sync
	_logger.write();
	_logger.sync();
}

void cleanup(){
	for (std::vector<record*>::iterator it = table.begin() ; it != table.end(); ++it){
		if(*it != NULL){
			delete (*it);
		}
	}
}

int main(){

	load();

	boost::thread_group th_group;
	for(int i=0 ; i<num_threads ; i++)
		th_group.create_thread(boost::bind(runner));

	th_group.join_all();

	//check();

	// sync
	_logger.write();
	_logger.sync();

	cleanup();

	return 0;
}
