#ifndef _LOGGER_H_
#define _LOGGER_H_

#include <stdlib.h>
#include <unistd.h>

#include <mutex>
#include <vector>
#include <sstream>
#include <string>

#include "record.h"

using namespace std;

// LOGGING

class entry {
public:
	entry(txn _txn, record* _before_image, record* _after_image) :
			transaction(_txn), before_image(_before_image), after_image(
					_after_image) {
	}

	//private:
	txn transaction;
	record* before_image;
	record* after_image;
};

class logger {
public:
	logger() :
			log_file(NULL), log_file_fd(-1) {

	}

	void set_path(std::string name, std::string mode) {
		log_file_name = name;

		log_file = fopen(log_file_name.c_str(), mode.c_str());
		if (log_file != NULL) {
			log_file_fd = fileno(log_file);
		} else {
			cout << "Log file not found : " << log_file_name << endl;
			exit(EXIT_FAILURE);
		}
	}

	void push(entry e) {
		std::lock_guard<std::mutex> lock(log_access);

		log_queue.push_back(e);
	}

	int write() {
		int ret;
		int count;
		stringstream buffer_stream;
		string buffer;

		{
			std::lock_guard<std::mutex> lock(log_access);

			for (std::vector<entry>::iterator it = log_queue.begin();
					it != log_queue.end(); ++it) {
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

			count = fwrite(buffer.c_str(), sizeof(char), buffer_size, log_file);

			if (count != buffer_size) {
				perror("fwrite failed");
				exit(EXIT_FAILURE);
			}

			ret = fsync(log_file_fd);

			if (ret == -1) {
				perror("fsync failed");
				exit(EXIT_FAILURE);
			}

			cout << "fsync :: "<<log_file_name<<" :: " << count << endl;

			// Set end time
			/*
			 for (std::vector<entry>::iterator it = log_queue.begin() ; it != log_queue.end(); ++it){
			 (*it).transaction.end = std::chrono::high_resolution_clock::now();
			 std::chrono::duration<double> elapsed_seconds = (*it).transaction.end - (*it).transaction.start;
			 cout<<"Duration: "<< elapsed_seconds.count()<<endl;
			 }
			 */

			// Clear queue
			log_queue.clear();
		}

		return ret;
	}

	void close() {
		std::lock_guard<std::mutex> lock(log_access);

		fclose(log_file);
	}

//private:
	std::string log_file_name;
	FILE* log_file;
	int log_file_fd;

	std::mutex log_access;
	vector<entry> log_queue;
};

#endif
