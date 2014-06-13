#ifndef TXN_H_
#define TXN_H_

// TRANSACTION

#include <string>
#include <chrono>
#include <vector>

#include "statement.h"

using namespace std;

enum transaction_type {
  Single,
  Multiple
};

class transaction {
 public:
  transaction(unsigned long _txn_id, transaction_type _ttype, vector<statement*> _stmts)
      : txn_id(_txn_id),
        ttype(_ttype),
        stmts(_stmts) {
  }

//private:
  unsigned long txn_id;
  transaction_type ttype;
  vector<statement*> stmts;

  //chrono::time_point<std::chrono::high_resolution_clock> start, end;
};

#endif /* TXN_H_ */
