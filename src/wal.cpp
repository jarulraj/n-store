#include <iostream>
#include <vector>
#include <string>
#include <random>
#include <functional>
#include <algorithm>
#include <sstream>
#include <utility>

#include <unordered_map>
#include <mutex>

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>

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

private:
	unsigned long txn_id;
	std::string txn_type;

	unsigned int key;
	std::string value;
};

// TUPLE + TABLE + INDEX
static int num_keys = 10;

class tuple{
	tuple(unsigned int _key, std::string _value, std::mutex _mx) :
		key(_key),
		value(_value),
		mx(_mx){}

private:
	std::mutex mx;
	unsigned int key;
	std::string value;
};

vector<tuple> table;
unordered_map<unsigned int, tuple*> index;

// LOGGING

class entry{
public:
	entry(unsigned long _txn_id, std::string _txn_type, tuple _before_image, tuple _after_image) :
			txn_id(_txn_id),
			txn_type(_txn_type),
			before_image(_before_image),
			after_image(_after_image){}

private:
	unsigned long txn_id;
	std::string txn_type;

	tuple before_image;
	tuple after_image;
};

FILE* logFile ;
int logFileFD;

vector<entry> queue;

int log(entry e){
	int ret ;

	stringstream buffer_stream;
	string buffer;
	buffer_stream << e.txn_type << e.after_image ;
	buffer = buffer_stream.str();

	ret = fwrite(&buffer, sizeof(char), sizeof(buffer), logFile);

	return ret;
}

// TRANSACTION OPERATIONS

int update(txn t){
	int key = t.key;

	if(index.count(t.key) == 0) // key does not exist
		return -1;

	tuple before_image = index.at(t.key);
	tuple after_image = new tuple(t.key, t.value, before_image.mx);

	// grab mutex
	std::unique_lock<std::mutex> lock(val.mx);
	table.push_back(*after_image);


	// Add log entry
	entry e = new entry(t.txn_id, "Insert", NULL, *after_image);
	queue.push_back(e);


		return 0;

}

std::string read(unsigned int){
	return "";
}

void load(){
	size_t ret;
	stringstream buffer_stream;
	string buffer;

	for(int i=0 ; i<num_keys ; i++){
		tuple r = std::make_tuple(i, random_string(3));
		cout << std::get<0>(r) << ' ';
		cout << std::get<1>(r) << '\n';

		buffer_stream << "Insert"<<std::get<0>(r)<<std::get<1>(r);
		buffer = buffer_stream.str();

		// log entry
		log(buffer);

		// update table and index
		table.push_back(r);
		index[i] = &r;
	}

	// loading - so no strict ordering
	fsync(logFileFD);
}

void open_log(){
	logFile = fopen("log", "w");
	if (logFile != NULL) {
		cout << "Opened log file" << endl;
	}

	logFileFD = fileno(logFile);
	cout << "File fd " << logFileFD << endl;
}

int main(){

	open_log();

	load();

	return 0;
}
