#pragma once

#include <iostream>
#include <vector>
#include <string>
#include <cstring>

namespace storage {

enum field_type {
  FD_INVALID,
  INTEGER,
  DOUBLE,
  VARCHAR,
};

struct field_info {
  field_info()
      : offset(0),
        ser_len(0),
        deser_len(0),
        type(field_type::FD_INVALID),
        inlined(1),
        enabled(1) {
  }

  field_info(off_t _offset, size_t _ser_len, size_t _deser_len, field_type _type,
              bool _inlined, bool _enabled)
      : offset(_offset),
        ser_len(_ser_len+1),
        deser_len(_deser_len+1),
        type(_type),
        inlined(_inlined),
        enabled(_enabled) {

  }

  off_t offset;
  size_t ser_len;
  size_t deser_len;
  field_type type;
  bool inlined;
  bool enabled;
};

}

