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

      /*
      cout<<"field len :"<<sptr->columns[field_id].len << endl;
      cout<<"field offset :"<<sptr->columns[field_id].offset << endl;
      cout<<"field type :"<< (int)sptr->columns[field_id].type << endl;
      */

      switch (type) {
        case field_type::INTEGER:
          int ival;
          std::sscanf(&(data[sptr->columns[field_id].offset]), "%d", &ival);
          field = std::to_string(ival);
          break;

        case field_type::DOUBLE:
          double dval;
          std::sscanf(&(data[sptr->columns[field_id].offset]), "%lf", &dval);
          field = std::to_string(dval);
          break;

        case field_type::VARCHAR:
          char* vcval;
          std::sscanf(&(data[sptr->columns[field_id].offset]), "%p", &vcval);
          field = std::string(vcval);
          break;

        default:
          cout << "Invalid type : " << type << endl;
          break;
      }
    }

    return field;
  }

  ~record() {
    delete data;
  }

  schema* sptr;
  char* data;
}
;

#endif /* RECORD_H_ */
