#ifndef STMT_H_
#define STMT_H_

#include <string>
#include <chrono>

#include "record.h"
#include "key.h"
#include "table.h"

using namespace std;

enum stmt_type {
  Insert,
  Delete,
  Update,
  Select
};

class statement {
 public:
  statement(unsigned int _partition, stmt_type _type, table* _tptr,
            record* _rptr, key* _kptr)
      : partition_id(_partition),
        type(_type),
        table_ptr(_tptr),
        rec_ptr(_rptr),
        rec_key(_kptr) {
  }

//private:
  unsigned int partition_id;
  stmt_type type;
  record* rec_ptr;
  table* table_ptr;
  key* rec_key;

};

#endif /* STMT_H_ */
