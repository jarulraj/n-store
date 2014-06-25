#ifndef TXN_H_
#define TXN_H_

// TRANSACTION

#include <string>
#include <chrono>
#include <vector>

#include "statement.h"

using namespace std;

class transaction {
 public:
  transaction(unsigned int _txn_id, vector<statement> _stmts)
      : transaction_id(_txn_id),
        stmts(_stmts) {
  }

//private:
  unsigned int transaction_id;
  vector<statement> stmts;

  //chrono::time_point<std::chrono::high_resolution_clock> start, end;
};

#endif /* TXN_H_ */
