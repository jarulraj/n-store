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
  statement(unsigned int _stmt_id, partition_type _ptype,
            unsigned int _partition_id, operation_type _otype, table* _tptr,
            record* _rptr,
            int _fid,
            field* _fptr,
            table_index* _tindxptr,
            vector<bool> _projection)
      : statement_id(_stmt_id),
        part_type(_ptype),
        partition_id(_partition_id),
        op_type(_otype),
        table_ptr(_tptr),
        rec_ptr(_rptr),
        field_id(_fid),
        field_ptr(_fptr),
        table_index_ptr(_tindxptr),
        projection(_projection) {
  }

//private:
  unsigned int statement_id;
  partition_type part_type;
  unsigned int partition_id;
  operation_type op_type;

  // Insert and Delete
  record* rec_ptr;
  table* table_ptr;

  // Update
  int field_id;
  field* field_ptr;

  // Select
  table_index* table_index_ptr;
  vector<bool> projection;

};

#endif /* STMT_H_ */
