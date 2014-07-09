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

class field_info {
 public:
  field_info()
      : offset(0),
        len(0),
        type(field_type::INVALID),
        inlined(1),
        enabled(1) {
  }

  field_info(unsigned int _offset, unsigned int _len, field_type _type,
             bool _inlined, bool _enabled)
      : offset(_offset),
        len(_len + 1),
        type(_type),
        inlined(_inlined),
        enabled(_enabled) {

  }

  static void* operator new(size_t sz) throw (bad_alloc) {
    if (persistent) {
      void* ret = pmem_new(sz);
      pmemalloc_activate(ret);
      return ret;
    } else
      return ::operator new(sz);
  }

  static void operator delete(void *p) throw () {
    if (persistent)
      pmem_delete(p);
    else
      ::operator delete(p);
  }

  unsigned int offset;
  unsigned int len;
  field_type type;
  bool inlined;
  bool enabled;
  static bool persistent;

};

#endif /* FIELD_H_ */
