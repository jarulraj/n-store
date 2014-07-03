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
        data(NULL) {
    data = new char[sptr->len];
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
        field = std::to_string(ival);
        break;

      case field_type::DOUBLE:
        double dval;
        std::sscanf(&(data[offset]), "%lf", &dval);
        field = std::to_string(dval);
        break;

      case field_type::VARCHAR:
        char* vcval;
        std::sscanf(&(data[offset]), "%p", &vcval);
        if (vcval != NULL)
          field = std::string(vcval);
        break;

      default:
        cout << "Invalid type : " << type << endl;
        break;
    }

    return field;
  }

  void* get_pointer(const int field_id) {
    unsigned int num_columns = sptr->num_columns;
    void* field = NULL;

    if (sptr->columns[field_id].inlined == 0) {
      size_t offset = sptr->columns[field_id].offset;
      std::sscanf(&(data[offset]), "%p", &field);
    }

    return field;
  }

  void set_data(const int field_id, record* rec_ptr) {
    char type = sptr->columns[field_id].type;
    size_t offset = sptr->columns[field_id].offset;
    size_t len = sptr->columns[field_id].len;

    switch (type) {
      case field_type::INTEGER:
        memcpy(&(data[offset]), &(rec_ptr->data[offset]), len);
        break;

      case field_type::DOUBLE:
        memcpy(&(data[offset]), &(rec_ptr->data[offset]), len);
        break;

      case field_type::VARCHAR:
        memcpy(&(data[offset]), &(rec_ptr->data[offset]), len);
        break;

      default:
        cout << "Invalid type : " << type << endl;
        break;
    }
  }

  void set_pointer(const int field_id, void* field_ptr) {
    unsigned int num_columns = sptr->num_columns;

    if (sptr->columns[field_id].inlined == 0) {
      size_t offset = sptr->columns[field_id].offset;
      std::sprintf(&(data[offset]), "%p", field_ptr);
    }
  }

  void persist_data() {
    pmemalloc_activate(data);

    unsigned int field_itr;
    for (field_itr = 0; field_itr < sptr->num_columns; field_itr++) {
      if (sptr->columns[field_itr].inlined == 0) {
        void* ptr = get_pointer(field_itr);
        pmemalloc_activate(ptr);
      }
    }
  }

  schema* sptr;
  char* data;
}
;

#endif /* RECORD_H_ */
