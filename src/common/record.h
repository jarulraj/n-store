#ifndef RECORD_H_
#define RECORD_H_

#include <iostream>
#include <string>
#include "schema.h"
#include "field.h"

using namespace std;

class record {
 public:
  record(schema* _sptr)
      : sptr(_sptr),
        data(NULL),
        data_len(_sptr->ser_len) {
    data = new char[data_len];
  }

  ~record() {
    delete data;
  }

  // Free non-inlined data
  void clear_data() {
    unsigned int field_itr;
    for (field_itr = 0; field_itr < sptr->num_columns; field_itr++) {
      if (sptr->columns[field_itr].inlined == 0) {
        void* ptr = get_pointer(field_itr);
        pmemalloc_free(ptr);
      }
    }
  }

  void display() {
    std::string data;

    unsigned int field_itr;
    for (field_itr = 0; field_itr < sptr->num_columns; field_itr++)
      data += get_data(field_itr);

    printf("record : %p %s \n", this, data.c_str());

  }

  std::string get_data(const int field_id) {
    std::string field;
    field_info finfo = sptr->columns[field_id];
    char type = finfo.type;
    size_t offset = finfo.offset;

    switch (type) {
      case field_type::INTEGER:
        int ival;
        std::sscanf(&(data[offset]), "%d", &ival);
        field = std::to_string(ival) + " ";
        break;

      case field_type::DOUBLE:
        double dval;
        std::sscanf(&(data[offset]), "%lf", &dval);
        field = std::to_string(dval) + " ";
        break;

      case field_type::VARCHAR: {
        char* vcval = NULL;
        std::sscanf(&(data[offset]), "%p", &vcval);
        if (vcval != NULL) {
          //std::printf("vcval : %p \n", vcval);
          field = std::string(vcval) + " ";
        }
      }
        break;

      case field_type::LONG_INT:
        long int lival;
        std::sscanf(&(data[offset]), "%ld", &lival);
        field = std::to_string(lival) + " ";
        break;

      default:
        cout << "Invalid type : " << type << endl;
        exit(EXIT_FAILURE);
        break;
    }

    return field;
  }

  void* get_pointer(const int field_id) {
    void* field = NULL;
    if (sptr->columns[field_id].inlined == 0) {
      std::sscanf(&(data[sptr->columns[field_id].offset]), "%p", &field);
    }

    return field;
  }

  void set_data(const int field_id, record* rec_ptr) {
    char type = sptr->columns[field_id].type;
    size_t offset = sptr->columns[field_id].offset;
    size_t len = sptr->columns[field_id].ser_len;

    switch (type) {
      case field_type::INTEGER:
      case field_type::DOUBLE:
      case field_type::LONG_INT:
        memcpy(&(data[offset]), &(rec_ptr->data[offset]), len);
        break;

      case field_type::VARCHAR:
        set_pointer(field_id, rec_ptr->get_pointer(field_id));
        break;

      default:
        cout << "Invalid type : " << type << endl;
        break;
    }
  }

  void set_int(const int field_id, int ival) {
    char type = sptr->columns[field_id].type;
    if (type == field_type::INTEGER) {
      std::sprintf(&(data[sptr->columns[field_id].offset]), "%d", ival);
    } else {
      cout << "Invalid type : " << type << endl;
      exit(EXIT_FAILURE);
    }
  }

  void set_double(const int field_id, double dval) {
    char type = sptr->columns[field_id].type;
    if (type == field_type::DOUBLE) {
      std::sprintf(&(data[sptr->columns[field_id].offset]), "%lf", dval);
    } else {
      cout << "Invalid type : " << type << endl;
      exit(EXIT_FAILURE);
    }
  }

  void set_pointer(const int field_id, void* field_ptr) {
    char type = sptr->columns[field_id].type;
    if (type == field_type::VARCHAR) {
      std::sprintf(&(data[sptr->columns[field_id].offset]), "%p", field_ptr);
    } else {
      cout << "Invalid type : " << type << endl;
      exit(EXIT_FAILURE);
    }
  }

  void persist_data() {
    pmemalloc_activate(data);

    unsigned int field_itr;
    for (field_itr = 0; field_itr < sptr->num_columns; field_itr++) {
      if (sptr->columns[field_itr].inlined == 0) {
        void* ptr = get_pointer(field_itr);
        //printf("persist data :: %p \n", ptr);
        pmemalloc_activate(ptr);
      }
    }
  }

  schema* sptr;
  char* data;
  size_t data_len;
}
;

#endif /* RECORD_H_ */
