#pragma once

#include <string>
#include <cstdio>
#include <cstdlib>

namespace storage {

class status {
 public:

  status(unsigned int _num_txns)
      : txn_counter(0),
        num_txns(_num_txns) {

    period = ((num_txns > 10) ? (num_txns / 10) : 1);

  }

  void display() {
    if (++txn_counter % period == 0) {
      printf("Finished :: %.2lf %% \r",
             ((double) (txn_counter * 100) / num_txns));
      fflush(stdout);
    }

    if (txn_counter == num_txns)
      printf("\n");
  }

  unsigned int txn_counter = 0;
  unsigned int num_txns;
  unsigned int period;

};

}
