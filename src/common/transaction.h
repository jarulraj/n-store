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
  transaction(vector<statement*> _stmts)
      : stmts(_stmts) {
  }

//private:
  vector<statement*> stmts;

  //chrono::time_point<std::chrono::high_resolution_clock> start, end;
};

#endif /* TXN_H_ */
