#ifndef RECORD_H_
#define RECORD_H_

#include <iostream>

using namespace std;

#define DELIM ' '

class record {
 public:
  record(unsigned int _key, char* _value)
      : key(_key),
        value(_value) {
  }

  ~record() {
    delete[] value;
  }

  friend ostream& operator<<(ostream& out, const record& rec) {
    out << DELIM << rec.key << DELIM << rec.value << DELIM;
    return out;
  }

  friend istream& operator>>(istream& in, record& rec) {
    in.ignore(1);  // skip delimiter
    in >> rec.key;

    in.ignore(1);  // skip delimiter
    in >> rec.value;

    in.ignore(1);  // skip delimiter
    return in;
  }

  //private:
  unsigned int key;
  char* value;
};

class sp_record {
 public:
  sp_record()
      : key(0),
        location { NULL, NULL } {
  }

  sp_record(unsigned int _key, unsigned int _batch_id, char* _location)
      : key(_key),
        batch_id { _batch_id, 0 },
        location { _location, NULL } {
  }

  //private:
  unsigned int key;
  unsigned int batch_id[2];
  char* location[2];
};

#endif /* RECORD_H_ */
