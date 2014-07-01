#include <iostream>
#include <vector>
#include <string>
#include <cstring>

#include <unordered_map>

using namespace std;

enum field_type {
  INVALID,
  INTEGER,
  DOUBLE,
  VARCHAR
};

struct column_info {
  column_info()
      : offset(0),
        len(0),
        type(field_type::INVALID),
        inlined(1),
        enabled(1) {
  }

  column_info(unsigned int _offset, unsigned int _len, char _type,
              bool _inlined, bool _enabled)
      : offset(_offset),
        len(_len),
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

class schema {
 public:
  schema(vector<column_info> _columns, size_t _len)
      : columns(NULL),
        len(_len) {

    num_columns = _columns.size();
    columns = new column_info[num_columns];
    unsigned int itr;

    for (itr = 0; itr < num_columns; itr++) {
      columns[itr] = _columns[itr];
    }

  }

  ~schema() {
    delete[] columns;
  }

  size_t len;
  unsigned int num_columns;
  column_info* columns;
};

class schema;
class record;

class table_index {
 public:
  table_index(schema* _sptr)
      : sptr(_sptr) {

  }

  schema* sptr;
  unordered_map<std::string, record*> map;
};

class table {
 public:
  table(schema* _sptr)
      : sptr(_sptr) {

  }

  schema* sptr;
  vector<table_index*> indices;
};

class record {
 public:
  record(table* _tptr)
      : tptr(_tptr),
        data(NULL) {
    data = new char[tptr->sptr->len + 1];
  }

  virtual std::string get_string() = 0;

  virtual ~record() {
    delete data;
  }

  table* tptr;
  char* data;
};

class table_record : public record {
 public:
  table_record(table* _tptr, int f0, int f1, double d1, char* vc1)
      : record(_tptr) {
    std::string str;
    schema* sptr = tptr->sptr;

    std::sprintf(&(data[sptr->columns[0].offset]), "%d", f0);
    std::sprintf(&(data[sptr->columns[1].offset]), "%d", f1);
    std::sprintf(&(data[sptr->columns[2].offset]), "%lf", d1);
    std::sprintf(&(data[sptr->columns[3].offset]), "%p", vc1);

  }

  std::string get_string() {
    schema* sptr = tptr->sptr;
    unsigned int num_columns = sptr->num_columns;
    unsigned int itr;
    std::string rec_str;

    int f0, f1;
    double d1;
    char* vc1;

    std::sscanf(&(data[sptr->columns[0].offset]), "%d", &f0);
    rec_str += std::to_string(f0) + " ";
    std::sscanf(&(data[sptr->columns[1].offset]), "%d", &f1);
    rec_str += std::to_string(f1) + " ";
    std::sscanf(&(data[sptr->columns[2].offset]), "%lf", &d1);
    rec_str += std::to_string(d1) + " ";
    std::sscanf(&(data[sptr->columns[3].offset]), "%p", &vc1);
    rec_str += std::string(vc1) + " ";

    return rec_str;
  }
};

std::string get_data(record* rptr, schema* sptr) {
  unsigned int num_columns = sptr->num_columns;
  unsigned int itr;
  std::string rec_str;
  char* data = rptr->data;

  for (itr = 0; itr < num_columns; itr++) {
    char type = sptr->columns[itr].type;

    switch (type) {
      case field_type::INTEGER:
        int ival;
        std::sscanf(&(data[sptr->columns[itr].offset]), "%d", &ival);
        rec_str += std::to_string(ival) + " ";
        break;

      case field_type::DOUBLE:
        double dval;
        std::sscanf(&(data[sptr->columns[itr].offset]), "%lf", &dval);
        rec_str += std::to_string(dval) + " ";
        break;

      case field_type::VARCHAR:
        char* vcval;
        std::sscanf(&(data[sptr->columns[itr].offset]), "%p", &vcval);
        rec_str += std::string(vcval) + " ";
        break;
    }
  }

  return rec_str;
}

int main() {

  size_t offset = 0, len = 0;
  column_info col1(offset, sizeof(int), field_type::INTEGER, 1, 1);
  offset += col1.len;
  column_info col2(offset, sizeof(int), field_type::INTEGER, 1, 1);
  offset += col2.len;
  column_info col3(offset, sizeof(double), field_type::DOUBLE, 1, 1);
  offset += col3.len;
  column_info col4(offset, sizeof(void*), field_type::VARCHAR, 0, 1);
  offset += col4.len;
  len = offset;

  vector<column_info> cols;
  cols.push_back(col1);
  cols.push_back(col2);
  cols.push_back(col3);
  cols.push_back(col4);

  schema* s = new schema(cols, len);

  table* t1 = new table(s);

  char* vc1 = new char[10];
  strcpy(vc1, "abcd");

  record* r = new table_record(t1, 21, 56, 23.68, vc1);

  cout << "data : " << r->get_string() << endl;

  offset = 0, len = 0;
  cols.clear();
  cols.push_back(col1);
  offset += col1.len;
  len = offset;

  schema* s2 = new schema(cols, len);

  table_index* ti1 = new table_index(s2);

  std::string key = get_data(r, s2);
  cout<<"key : " <<key<<endl;

  return 0;
}

