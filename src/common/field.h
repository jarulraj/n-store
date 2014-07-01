#ifndef FIELD_H_
#define FIELD_H_

#include <iostream>
#include <vector>
#include <string>
#include <cstring>

using namespace std;

enum field_type {
  INVALID,
  INTEGER,
  DOUBLE,
  VARCHAR
};

struct field_info {
  field_info()
      : offset(0),
        len(0),
        type(field_type::INVALID),
        inlined(1),
        enabled(1) {
  }

  field_info(unsigned int _offset, unsigned int _len, char _type,
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

#endif /* FIELD_H_ */
