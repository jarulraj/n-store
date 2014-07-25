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

  inline void display() {
    std::string data;

    unsigned int field_itr;
    for (field_itr = 0; field_itr < sptr->num_columns; field_itr++)
      data += get_data(field_itr);

    printf("record : %p %s \n", this, data.c_str());

  }

  inline std::string get_data(const int field_id) {
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

      case field_type::VARCHAR:
        char* vcval;
        std::sscanf(&(data[offset]), "%p", &vcval);
        if (vcval != NULL) {
          //std::printf("vcval : %p \n", vcval);
          field = std::string(vcval) + " ";
        }
        break;

      default:
        cout << "Invalid type : " << type << endl;
        exit(EXIT_FAILURE);
        break;
    }

    return field;
  }

  inline void* get_pointer(const int field_id) {
    void* field = NULL;
    if (sptr->columns[field_id].inlined == 0) {
      std::sscanf(&(data[sptr->columns[field_id].offset]), "%p", &field);
    }

    return field;
  }

  inline void set_data(const int field_id, record* rec_ptr) {
    char type = sptr->columns[field_id].type;
    size_t offset = sptr->columns[field_id].offset;
    size_t len = sptr->columns[field_id].ser_len;

    switch (type) {
      case field_type::INTEGER:
        memcpy(&(data[offset]), &(rec_ptr->data[offset]), len);
        break;

      case field_type::DOUBLE:
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

  inline void set_pointer(const int field_id, void* field_ptr) {
    if (sptr->columns[field_id].inlined == 0) {
      std::sprintf(&(data[sptr->columns[field_id].offset]), "%p", field_ptr);
    }
  }

  inline void persist_data() {
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
