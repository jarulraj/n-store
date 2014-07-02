#ifndef STMT_H_
#define STMT_H_

#include <string>
#include <chrono>

#include "record.h"
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

  statement()
      : transaction_id(-1),
        part_type(partition_type::Single),
        partition_id(-1),
        op_type(operation_type::Insert),
        table_id(-1),
        rec_ptr(NULL),
        field_id(-1),
        table_index_id(-1),
        projection(NULL) {
  }

  statement(unsigned int _txn_id, partition_type _ptype,
            unsigned int _partition_id, operation_type _otype,
            unsigned int _table_id, record* _rptr, int _fid,
            unsigned int _table_index_id, schema* _projection)
      : transaction_id(_txn_id),
        part_type(_ptype),
        partition_id(_partition_id),
        op_type(_otype),
        table_id(_table_id),
        rec_ptr(_rptr),
        field_id(_fid),
        table_index_id(_table_index_id),
        projection(_projection) {
  }

//private:
  int transaction_id;
  partition_type part_type;
  int partition_id;
  operation_type op_type;

  // Insert and Delete
  record* rec_ptr;
  int table_id;

  // Update
  int field_id;

  // Select
  int table_index_id;
  schema* projection;

};

#endif /* STMT_H_ */
