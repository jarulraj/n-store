#ifndef BENCHMARK_H_
#define BENCHMARK_H_

#include "engine.h"

using namespace std;

class benchmark {
 public:

  virtual void load(engine* ee) = 0;
  virtual void execute(engine* ee) = 0;

  virtual ~benchmark() {
  }
};

#endif /* BENCHMARK_H_ */
