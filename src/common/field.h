#ifndef FIELD_H_
#define FIELD_H_

#include <iostream>
#include <vector>
#include <string>

using namespace std;

enum field_type {
  INTEGER,
  VARCHAR
};

class field {
 public:

  virtual std::string& get_string() = 0;

  field_type get_type();

  virtual ~field() {
  }

};

class integer_field : public field {
 public:
  integer_field(int _data)
      : str(std::to_string(_data)) {
  }

  std::string& get_string() {
    return str;
  }

  field_type get_type() {
    return field_type::INTEGER;
  }

 private:
  std::string str;
};

class varchar_field : public field {
 public:
  varchar_field(char* _data) {
    if (_data != NULL)
      str = std::string(_data);
  }

  std::string& get_string() {
    return str;
  }

  field_type get_type() {
    return field_type::VARCHAR;
  }

 private:
  std::string str;
};

#endif /* FIELD_H_ */
