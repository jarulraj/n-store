#pragma once

#include <vector>

#include "config.h"
#include "engine.h"
#include "timer.h"
#include "database.h"

namespace storage {

class benchmark {
 public:
  benchmark(unsigned int _tid, database* _db, timer* _tm)
      : sp(NULL) {
    tid = _tid;
    tm = _tm;
    db = _db;
  }

  virtual ~benchmark() {
  }

  virtual void load() = 0;
  virtual void execute() = 0;
  virtual void sim_crash() = 0;
  virtual void reset() = 0;

  unsigned int tid;
  timer* tm;
  database* db;
  struct static_info* sp;
};

}
