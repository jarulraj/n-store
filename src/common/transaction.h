#pragma once

// TRANSACTION

#include <string>
#include <chrono>
#include <vector>

#include "statement.h"

namespace storage {

class transaction {
 public:
  transaction(unsigned int _txn_id, std::vector<statement> _stmts)
      : transaction_id(_txn_id),
        stmts(_stmts) {
  }

//private:
  unsigned int transaction_id;
  std::vector<statement> stmts;

};

}
