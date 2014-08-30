#pragma once

#include <string>
#include <chrono>

#include "record.h"
#include "table.h"

namespace storage {

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
        op_type(operation_type::Insert),
        table_id(-1),
        rec_ptr(NULL),
        table_index_id(-1),
        projection(NULL) {
  }

  // Insert and Delete
  statement(int _txn_id, operation_type _otype, int _table_id, record* _rptr)
      : transaction_id(_txn_id),
        op_type(_otype),
        table_id(_table_id),
        rec_ptr(_rptr),
        table_index_id(-1),
        projection(NULL) {
  }

  // Update
  statement(int _txn_id, operation_type _otype, int _table_id, record* _rptr,
            std::vector<int> _fid)
      : transaction_id(_txn_id),
        op_type(_otype),
        table_id(_table_id),
        rec_ptr(_rptr),
        field_ids(_fid),
        table_index_id(-1),
        projection(NULL) {
  }

  // Select
  statement(int _txn_id, operation_type _otype, int _table_id, record* _rptr,
            int _table_index_id, schema* _projection)
      : transaction_id(_txn_id),
        op_type(_otype),
        table_id(_table_id),
        rec_ptr(_rptr),
        table_index_id(_table_index_id),
        projection(_projection) {
  }

  int transaction_id;
  operation_type op_type;

  // Insert and Delete
  int table_id;
  record* rec_ptr;

  // Update
  std::vector<int> field_ids;

  // Select
  int table_index_id;
  schema* projection;

};

}
