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
  entry(statement* _stmt, int _field_id, vector<field*> _before_image,
        vector<field*> _after_image)
      : stmt_ptr(_stmt),
        field_id(_field_id),
        before_image(_before_image),
        after_image(_after_image) {
  }

  //private:
  statement* stmt_ptr;
  int field_id;
  vector<field*> before_image;
  vector<field*> after_image;
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

    buffer_stream << e.stmt_ptr->op_type << " ";

    vector<field*>::const_iterator field_itr;

    if (e.field_id != -1)
      buffer_stream << std::to_string(e.field_id) << " ";

    for (field_itr = e.before_image.begin(); field_itr != e.before_image.end();
        field_itr++) {

      if ((*field_itr) != NULL) {
        buffer_stream << (*field_itr)->get_string() << " ";
        delete (*field_itr);
      }
    }

    for (field_itr = e.after_image.begin(); field_itr != e.after_image.end();
        field_itr++) {

      if ((*field_itr) != NULL) {
        buffer_stream << (*field_itr)->get_string() << " ";
      }
    }

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
