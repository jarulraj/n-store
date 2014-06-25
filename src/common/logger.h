#ifndef _LOGGER_H_
#define _LOGGER_H_

#include <stdlib.h>
#include <unistd.h>

#include <mutex>
#include <sstream>
#include <string>

#include "record.h"
#include "libpm.h"

using namespace std;

// LOGGING

class entry {
 public:
  entry(int _transaction_id, operation_type _op_type, int _table_id,
        unsigned int _num_fields, field** _after_image, int _field_id,
        field* _after_field)
      : transaction_id(_transaction_id),
        op_type(_op_type),
        table_id(_table_id),
        num_fields(_num_fields),
        field_id(_field_id),
        after_image(_after_image),
        after_field(_after_field) {
  }

  //private:
  int transaction_id;
  operation_type op_type;
  int table_id;

  unsigned int num_fields;
  field** after_image;

  // Only Update
  int field_id;
  field* after_field;
};

class logger {
 public:
  logger()
      : log_file(NULL),
        log_file_fd(-1),
        buffer_size(0),
        pmp(NULL) {
  }

  void configure(std::string _name, void* _pmp) {
    log_file_name = _name;
    pmp = _pmp;

    log_file = fopen(log_file_name.c_str(), "w");
    if (log_file != NULL) {
      log_file_fd = fileno(log_file);
    } else {
      cout << "Log file not found : " << log_file_name << endl;
      exit(EXIT_FAILURE);
    }
  }

  void push(const entry& e) {
    entries.push_back(e);

    buffer_stream.str("");

    buffer_stream << e.transaction_id << " " << e.op_type << " " << e.table_id
                  << " ";

    unsigned int field_itr;

    if (e.after_image != NULL) {
      for (field_itr = 0; field_itr < e.num_fields; field_itr++) {
        if (e.after_image[field_itr] != NULL)
          buffer_stream << PSUB(pmp, (void* ) e.after_image[field_itr]) << " ";
        else
          buffer_stream << "0x0" << " ";
      }
    }

    if (e.field_id != -1) {
      buffer_stream << std::to_string(e.field_id) << " "
                    << PSUB(pmp, e.after_field);
    }

    buffer_stream << endl;

    buffer = buffer_stream.str();
    buffer_size = buffer.size();

    fwrite(buffer.c_str(), sizeof(char), buffer_size, log_file);
  }

  int write() {
    int ret;
    vector<entry>::iterator e_itr;

    // SYNC log
    ret = fsync(log_file_fd);
    if (ret == -1) {
      perror("fsync failed");
      exit(EXIT_FAILURE);
    }

    // PERSIST pointers
    for (e_itr = entries.begin(); e_itr != entries.end(); e_itr++) {
      unsigned int field_itr;

      // Update
      if ((*e_itr).op_type == operation_type::Update) {
        cout << "Update" << endl;
        //cout << "-" << PSUB(pmp, (*e_itr).after_field) << "-" << endl;
        pmemalloc_activate(pmp, PSUB(pmp, (*e_itr).after_field));
      }

      // Insert
      if ((*e_itr).op_type == operation_type::Insert) {
        cout << "Insert" << endl;
        for (field_itr = 0; field_itr < (*e_itr).num_fields; field_itr++) {
          if ((*e_itr).after_image[field_itr] != NULL)
            pmemalloc_activate(
                pmp, PSUB(pmp, (void* ) (*e_itr).after_image[field_itr]));
        }
      }

    }

    // CLEAR log
    entries.clear();

    return ret;
  }

  void close() {
    fclose(log_file);
  }

  FILE* log_file;
  void* pmp;

 private:
  vector<entry> entries;

  stringstream buffer_stream;
  string buffer;
  size_t buffer_size;

  std::string log_file_name;
  int log_file_fd;

};

#endif
