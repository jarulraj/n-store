#ifndef COORDINATOR_H_
#define COORDINATOR_H_

#include <vector>
#include <thread>
#include <map>

#include "nstore.h"
#include "engine.h"
#include "timer.h"
#include "utils.h"
#include "database.h"
#include "libpm.h"

#include "ycsb_benchmark.h"
#include "tpcc_benchmark.h"

using namespace std;

class coordinator {
 public:
  coordinator()
      : single(true),
        num_executors(1),
        num_txns(0),
        recovery_mode(true),
        load(false) {
  }

  coordinator(const config conf)
      : recovery_mode(true),
        load(false) {
    single = conf.single;
    num_executors = conf.num_executors;
    num_txns = conf.num_txns;

    for (unsigned int i = 0; i < num_executors; i++) {
      tms.push_back(timer());
    }
  }

  void execute_bh(benchmark* bh) {
    // Reset
    bh->reset();

    // Load
    if (load)
      bh->load();

    // Execute
    bh->execute();
  }

  void eval(config conf) {
    if (!conf.recovery) {
      execute(conf);
    } else {
      recover(conf);
    }

  }

  void execute(config& conf) {
    std::vector<std::thread> executors;

    if (sp->init == 0) {
      //cout << "Initialization Mode" << endl;

      benchmark** partitions = new benchmark*[num_executors];
      pmemalloc_activate(partitions);

      for (unsigned int i = 0; i < num_executors; i++) {
        database* db = new database(conf, sp, i);
        pmemalloc_activate(db);

        partitions[i] = get_benchmark(conf, i, db);
        pmemalloc_activate(partitions[i]);
      }

      sp->ptrs[0] = partitions;
      sp->itr = 1;
      conf.partitions = partitions;

      recovery_mode = false;
    } else {
      //cout << "Recovery Mode " << endl;
      recovery_mode = true;

      conf.partitions = (benchmark**) sp->ptrs[0];
    }

    load = check_load(conf);

    for (unsigned int i = 0; i < num_executors; i++)
      executors.push_back(
          std::thread(&coordinator::execute_bh, this, conf.partitions[i]));

    for (unsigned int i = 0; i < num_executors; i++)
      executors[i].join();

    double max_dur = 0;
    for (unsigned int i = 0; i < num_executors; i++) {
      cout << "dur :" << i << " :: " << tms[i].duration() << endl;
      max_dur = std::max(max_dur, tms[i].duration());
    }
    cout << "max dur :" << max_dur << endl;
    display_stats(conf.etype, max_dur, num_txns);

    if(sp->init == 0)
      sp->init = 1;

  }

  void recover(const config conf) {

    database* db = new database(conf, sp, 0);
    benchmark* bh = get_benchmark(conf, 0, db);

    // Load
    bh->load();

    // Crash and recover
    bh->sim_crash();

  }

  benchmark* get_benchmark(const config state, unsigned int tid, database* db) {
    benchmark* bh = NULL;

    // Fix benchmark
    switch (state.btype) {
      case benchmark_type::YCSB:
        LOG_INFO("YCSB");
        bh = new ycsb_benchmark(state, tid, db, &tms[tid]);
        break;

      case benchmark_type::TPCC:
        LOG_INFO("TPCC");
        bh = new tpcc_benchmark(state, tid, db, &tms[tid]);
        break;

      default:
        cout << "Unknown benchmark type :: " << state.btype << endl;
        break;
    }

    return bh;
  }

  bool check_load(const config conf) {
    if (!recovery_mode)
      return true;

    if (recovery_mode) {
      if (conf.etype == engine_type::WAL || conf.etype == engine_type::LSM)
        return true;
    }

    return false;
  }

  bool single;
  unsigned int num_executors;
  unsigned int num_txns;bool recovery_mode;bool load;

  std::vector<timer> tms;
};

#endif /* COORDINATOR_H_ */
