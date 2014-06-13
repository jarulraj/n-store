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
  entry(statement* _stmt, record* _before_image, record* _after_image)
      : stmt_ptr(_stmt),
        before_image(_before_image),
        after_image(_after_image) {
  }

  //private:
  statement* stmt_ptr;
  record* before_image;
  record* after_image;
};

class logger {
 public:
  logger()
      : log_file(NULL),
        log_file_fd(-1),
        buffer_size(0) {
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

  void push(const entry& e) {
    buffer_stream.str("");

    buffer_stream << e.stmt_ptr->type << " ";

    if (e.before_image != NULL) {
      buffer_stream << e.before_image->get_string();
      delete e.before_image;
    }

    if (e.after_image != NULL)
      buffer_stream << e.after_image->get_string();

    buffer_stream << endl;

    buffer = buffer_stream.str();
    buffer_size = buffer.size();

    fwrite(buffer.c_str(), sizeof(char), buffer_size, log_file);
  }

  int write() {
    int ret;

    ret = fsync(log_file_fd);

    if (ret == -1) {
      perror("fsync failed");
      exit(EXIT_FAILURE);
    }

    //cout << "fsync :: "<<log_file_name<<" :: " << count << endl;

    return ret;
  }

  void close() {
    fclose(log_file);
  }

  FILE* log_file;

 private:
  stringstream buffer_stream;
  string buffer;
  size_t buffer_size;

  std::string log_file_name;
  int log_file_fd;

};

#endif
