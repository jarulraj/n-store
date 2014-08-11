#ifndef BENCHMARK_H_
#define BENCHMARK_H_

#include <vector>

#include "nstore.h"
#include "engine.h"
#include "timer.h"
#include "database.h"

using namespace std;

class benchmark {
 public:
  benchmark(__attribute__((unused)) config& conf, unsigned int _tid, database* _db, timer* _tm,
            struct static_info* _sp) {
    tid = _tid;
    tm = _tm;
    db = _db;
    sp = _sp;
  }

  virtual void load() = 0;
  virtual void execute() = 0;
  virtual void sim_crash() = 0;

  virtual ~benchmark() {
  }

  unsigned int tid;
  timer* tm;
  database* db;
  struct static_info* sp;
};

#endif /* BENCHMARK_H_ */
