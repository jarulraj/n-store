#ifndef STMT_H_
#define STMT_H_

#include <string>
#include <chrono>

#include "record.h"
#include "key.h"
#include "table.h"

using namespace std;

enum partition_type {
  Single,
  Multiple
};

enum operation_type {
  Insert,
  Delete,
  Update,
  Select
};

class statement {
 public:
  statement(unsigned int _stmt_id, partition_type _ptype, unsigned int _partition_id, operation_type _otype, table* _tptr,
            record* _rptr, key* _kptr)
      : statement_id(_stmt_id),
        part_type(_ptype),
        partition_id(_partition_id),
        op_type(_otype),
        table_ptr(_tptr),
        rec_ptr(_rptr),
        rec_key(_kptr) {
  }

//private:
  unsigned int statement_id;
  partition_type part_type;
  unsigned int partition_id;
  operation_type op_type;
  record* rec_ptr;
  table* table_ptr;
  key* rec_key;

};

#endif /* STMT_H_ */
