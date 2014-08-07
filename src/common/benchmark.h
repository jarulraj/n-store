#ifndef BENCHMARK_H_
#define BENCHMARK_H_

#include <vector>
#include <thread>
#include "engine.h"
#include "nstore.h"
#include "timer.h"
#include "utils.h"
#include <map>

using namespace std;

class benchmark {
 public:
  benchmark()
      : single(true),
        num_executors(1) {
  }

  benchmark(config& conf) {
    single = conf.single;
    num_executors = conf.num_executors;
    num_txns = conf.num_txns;
  }

  virtual void load(engine* ee) = 0;
  virtual void handler(engine* ee, unsigned int tid) = 0;

  void execute(engine* ee) {
    if (single)
      execute_st(ee);
    else
      execute_mt(ee);
  }

  void execute_st(engine* ee) {
    handler(ee, 0);

    display_stats(ee, tm[0].duration(), num_txns);
  }

  void execute_mt(engine* ee) {
    std::vector<std::thread> executors;
    int num_thds = num_executors;

    for (int i = 0; i < num_thds; i++) {
      executors.push_back(std::thread(&benchmark::handler, this, ee, i));
    }

    for (int i = 0; i < num_thds; i++)
      executors[i].join();

    double max_dur = 0;
    for (int i = 0; i < num_thds; i++)
      max_dur = std::max(max_dur, tm[i].duration());

    display_stats(ee, max_dur, num_txns);
  }

  virtual void sim_crash(engine* ee) = 0;

  virtual ~benchmark() {
  }

  bool single;
  unsigned int num_executors;
  unsigned int num_txns;

  std::map<unsigned int, timer> tm;
};

#endif /* BENCHMARK_H_ */
