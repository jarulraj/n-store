#ifndef BENCHMARK_H_
#define BENCHMARK_H_

#include <vector>
#include <thread>
#include <map>

#include "nstore.h"
#include "engine.h"
#include "engine_mt.h"
#include "timer.h"
#include "utils.h"
#include "lock_manager.h"

using namespace std;

class benchmark {
 public:
  benchmark()
      : single(true),
        num_executors(1),
        num_txns(0) {
  }

  benchmark(config& conf) {
    single = conf.single;
    num_executors = conf.num_executors;
    num_txns = conf.num_txns;
  }

  virtual void load(engine* ee) = 0;
  virtual void handler(engine* ee, unsigned int tid) = 0;

  void load(config& conf) {
    engine* ee = get_engine(conf, 0, false);

    load(ee);

    delete ee;
  }

  void execute(config& conf) {
    if (single)
      execute_st(conf);
    else
      execute_mt(conf);
  }

  void execute_st(config& conf) {
    engine* ee = get_engine(conf, 0, conf.read_only);
    handler(ee, 0);
    delete ee;

    display_stats(conf.etype, tm[0].duration(), num_txns);
  }

  void execute_mt(config& conf) {
    std::vector<std::thread> executors;
    int num_thds = num_executors;
    engine** te = new engine*[num_executors];

    for (int i = 0; i < num_thds; i++) {
      te[i] = get_engine(conf, i, conf.read_only);
      executors.push_back(std::thread(&benchmark::handler, this, te[i], i));
      //cout<<"executors :: "<<i<<" id :: "<<executors[i].get_id()<<endl;
    }

    for (int i = 0; i < num_thds; i++)
      executors[i].join();

    double max_dur = 0;
    for (int i = 0; i < num_thds; i++)
      max_dur = std::max(max_dur, tm[i].duration());

    display_stats(conf.etype, max_dur, num_txns);

    for (int i = 0; i < num_thds; i++){
      //te[i]->display();
      delete te[i];
    }
    delete[] te;
  }

  virtual void sim_crash(engine* ee) = 0;

  virtual ~benchmark() {
  }

  engine* get_engine(config& state, unsigned int tid, bool read_only) {
    engine* ee = NULL;

    switch (state.etype) {
      case engine_type::WAL:
      case engine_type::SP:
      case engine_type::LSM:
      case engine_type::OPT_WAL:
      case engine_type::OPT_SP:
      case engine_type::OPT_LSM:

        if (state.single)
          ee = new engine(state, tid, read_only);
        else
          ee = new engine_mt(state, tid, read_only, &lm);
        break;

      default:
        cout << "Unknown engine type :: " << state.etype << endl;
        break;
    }

    return ee;
  }

  bool single;
  unsigned int num_executors;
  unsigned int num_txns;

  std::map<unsigned int, timer> tm;
  lock_manager lm;
};

#endif /* BENCHMARK_H_ */
