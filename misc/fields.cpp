#include <iostream>
#include <vector>
#include <string>
#include <cstring>

#include <unordered_map>

using namespace std;

enum field_type_ {
  INVALID,
  INTEGER,
  DOUBLE,
  VARCHAR
};

struct field_info_ {
  field_info_()
      : offset(0),
        len(0),
        type(field_type_::INVALID),
        inlined(1),
        enabled(1) {
  }

  field_info_(unsigned int _offset, unsigned int _len, char _type,
              bool _inlined, bool _enabled)
      : offset(_offset),
        len(_len + 1),
        type(_type),
        inlined(_inlined),
        enabled(_enabled) {

  }

  unsigned int offset;
  unsigned int len;
  char type;
  bool inlined;
  bool enabled;
};

class schema_ {
 public:
  schema_(vector<field_info_> _columns, size_t _len)
      : columns(NULL),
        len(_len) {

    num_columns = _columns.size();
    columns = new field_info_[num_columns];
    unsigned int itr;

    for (itr = 0; itr < num_columns; itr++) {
      columns[itr] = _columns[itr];
    }

  }

  ~schema_() {
    delete[] columns;
  }

  size_t len;
  unsigned int num_columns;
  field_info_* columns;
};

class schema__;
class record_;

class table_index_ {
 public:
  table_index_(schema_* _sptr)
      : sptr(_sptr) {

  }

  schema_* sptr;
  unordered_map<std::string, record_*> map;
};

class table_ {
 public:
  table_(schema_* _sptr)
      : sptr(_sptr) {

  }

  schema_* sptr;
  vector<table_index_*> indices;
};

class record_ {
 public:
  record_(table_* _tptr)
      : tptr(_tptr),
        data(NULL) {
    data = new char[tptr->sptr->len];
  }

  std::string get_data(const int field_id, schema_* sptr) {
    unsigned int num_columns = sptr->num_columns;
    std::string field;

    if (field_id < num_columns) {
      char type = sptr->columns[field_id].type;

      switch (type) {
        case field_type_::INTEGER:
          int ival;
          std::sscanf(&(data[sptr->columns[field_id].offset]), "%d", &ival);
          field = std::to_string(ival);
          break;

        case field_type_::DOUBLE:
          double dval;
          std::sscanf(&(data[sptr->columns[field_id].offset]), "%lf", &dval);
          field = std::to_string(dval);
          break;

        case field_type_::VARCHAR:
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

  ~record_() {
    delete[] data;
  }

  table_* tptr;
  char* data;
};

class table_record_ : public record_ {
 public:

  table_record_(table_* _tptr, int f0, int f1, double d1, char* vc1)
      : record_(_tptr) {
    schema_* sptr = tptr->sptr;

    std::sprintf(&(data[sptr->columns[0].offset]), "%d", f0);
    std::sprintf(&(data[sptr->columns[1].offset]), "%d", f1);
    std::sprintf(&(data[sptr->columns[2].offset]), "%lf", d1);
    std::sprintf(&(data[sptr->columns[3].offset]), "%p", vc1);
  }
};

std::string get_data(record_* rptr, schema_* sptr) {
  unsigned int num_columns = sptr->num_columns;
  unsigned int itr;
  std::string rec_str;

  for (itr = 0; itr < num_columns; itr++) {
    rec_str += rptr->get_data(itr, sptr) + " ";
  }

  return rec_str;
}

int main() {

  size_t offset = 0, len = 0;
  field_info_ col1(offset, sizeof(int), field_type_::INTEGER, 1, 1);
  offset += col1.len;
  field_info_ col2(offset, sizeof(int), field_type_::INTEGER, 1, 1);
  offset += col2.len;
  field_info_ col3(offset, sizeof(double), field_type_::DOUBLE, 1, 1);
  offset += col3.len;
  field_info_ col4(offset, sizeof(void*), field_type_::VARCHAR, 0, 1);
  offset += col4.len;
  len = offset;

  vector<field_info_> cols;
  cols.push_back(col1);
  cols.push_back(col2);
  cols.push_back(col3);
  cols.push_back(col4);

  schema_* s = new schema_(cols, len);

  table_* t1 = new table_(s);

  char* vc1 = new char[10];
  strcpy(vc1, "abcd");

  record_* r = new table_record_(t1, 21, 56, 23.68, vc1);

  std::string key = get_data(r, s);
  cout << "key : " << key << endl;

  offset = 0, len = 0;
  cols.clear();
  cols.push_back(col1);
  offset += col1.len;
  len = offset;

  schema_* s2 = new schema_(cols, len);

  table_index_* ti1 = new table_index_(s2);

  key = get_data(r, s2);
  cout << "key : " << key << endl;

  return 0;
}

