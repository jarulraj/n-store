#ifndef FIELD_H_
#define FIELD_H_

#include <iostream>
#include <vector>
#include <string>
#include <cstring>

using namespace std;

enum field_type {
  INTEGER,
  VARCHAR
};

class field {
 public:

  virtual std::string get_string() = 0;

  field_type get_type();

  virtual ~field() {
  }

};

class integer_field : public field {
 public:
  integer_field(int _val)
      : val(_val) {
  }

  std::string get_string() {
    return std::to_string(val);
  }

  field_type get_type() {
    return field_type::INTEGER;
  }

 private:
  int val;
};

class varchar_field : public field {
 public:
  varchar_field(const std::string& str) {
    if (!str.empty()) {
      size_t len = str.size();
      data = new char[len+1];
      memcpy(data, str.c_str(), len+1);
    }
  }

  std::string get_string() {
    return std::string(data);
  }

  field_type get_type() {
    return field_type::VARCHAR;
  }

  ~varchar_field() {
    delete data;
  }

 private:
  char* data;
};

#endif /* FIELD_H_ */
