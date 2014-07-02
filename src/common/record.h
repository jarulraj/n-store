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

  std::string get_data(const int field_id, schema* sptr) {
    unsigned int num_columns = sptr->num_columns;
    std::string field;

    if (field_id < num_columns) {
      char type = sptr->columns[field_id].type;
      size_t offset = sptr->columns[field_id].offset;

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
    }

    return field;
  }

  void* get_pointer(const int field_id, schema* sptr) {
    unsigned int num_columns = sptr->num_columns;
    void* field = NULL;

    if (field_id < num_columns && sptr->columns[field_id].inlined == 0) {
      size_t offset = sptr->columns[field_id].offset;
      std::sscanf(&(data[offset]), "%p", &field);
    }

    return field;
  }

  void set_data(const int field_id, record* rec_ptr, schema* sptr) {

    if (field_id < sptr->num_columns) {
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
  }

  void persist_data() {
    pmemalloc_activate(data);

    unsigned int field_itr;
    for (field_itr = 0; field_itr < sptr->num_columns; field_itr++) {
      if (sptr->columns[field_itr].inlined == 0) {
        void* ptr = get_pointer(field_itr, sptr);
        pmemalloc_activate(ptr);
      }
    }
  }

  schema* sptr;
  char* data;
}
;

#endif /* RECORD_H_ */
