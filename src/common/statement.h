#ifndef STMT_H_
#define STMT_H_

#include <string>
#include <chrono>

#include "record.h"
#include "table.h"

using namespace std;

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
        field_id(-1),
        table_index_id(-1),
        projection(NULL) {
  }

  statement(int _txn_id, operation_type _otype,
            int _table_id, record* _rptr, int _fid,
            int _table_index_id, schema* _projection)
      : transaction_id(_txn_id),
        op_type(_otype),
        table_id(_table_id),
        rec_ptr(_rptr),
        field_id(_fid),
        table_index_id(_table_index_id),
        projection(_projection) {
  }

//private:
  int transaction_id;
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
