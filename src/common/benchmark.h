#ifndef BENCHMARK_H_
#define BENCHMARK_H_

#include "table.h"
#include "workload.h"

using namespace std;

class benchmark {
 public:

  virtual workload& get_dataset() = 0;

  virtual workload& get_workload() = 0;

  virtual ~benchmark() {
  }
};

#endif /* BENCHMARK_H_ */
