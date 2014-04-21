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

#include <chrono>

using namespace std;

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

logger _logger;

void logger_func(const boost::system::error_code& /*e*/){
  std::cout << "Syncing log !\n"<<endl;

  // sync
  _logger.write();
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

		table.push_back(*after_image);
		table_index[key] = after_image;
	}

	// Add log entry
	entry e(t, before_image, after_image);
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

	for (std::vector<record>::iterator it = table.begin() ; it != table.end(); ++it){
			cout << *it << endl;
	}

}

void load(){
	size_t ret;
	stringstream buffer_stream;
	string buffer;

	for(int i=0 ; i<num_keys ; i++){
		int key = i;
		string value = random_string(3);
		record* after_image = new record(key, value);

		{
			boost::upgrade_lock<boost::shared_mutex> lock(table_access);
			boost::upgrade_to_unique_lock<boost::shared_mutex> uniqueLock(lock);
			// exclusive access

			table.push_back(*after_image);
			table_index[key] = after_image;
		}

		// Add log entry
		txn t(0, "Insert", key, value);

		entry e(t, NULL, after_image);
		_logger.push(e);
	}

	// sync
	_logger.write();
}

int main(){

	load();

	boost::thread_group th_group;
	for(int i=0 ; i<num_threads ; i++)
		th_group.create_thread(boost::bind(runner));

	th_group.join_all();

	//check();
	boost::asio::io_service io;
	boost::asio::deadline_timer t(io, boost::posix_time::seconds(1));

	t.async_wait(&logger_func);
	io.run();

	return 0;
}
