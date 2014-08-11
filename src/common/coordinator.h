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
        num_txns(0) {
  }

  coordinator(__attribute__((unused))    config& conf) {
    single = conf.single;
    num_executors = conf.num_executors;
    num_txns = conf.num_txns;

    for (unsigned int i = 0; i < num_executors; i++) {
      tm.push_back(timer());
      sps.push_back(static_info());
    }
  }

  void execute_bh(benchmark* bh) {
    // Load
    bh->load();

    // Execute
    bh->execute();
  }

  void execute(config& conf) {
    std::vector<std::thread> executors;
    benchmark** partitions = new benchmark*[num_executors];

    for (unsigned int i = 0; i < num_executors; i++) {
      database* db = new database(conf, &sps[i], i);
      pmemalloc_activate(db);

      partitions[i] = get_benchmark(conf, i, db);
    }

    for (unsigned int i = 0; i < num_executors; i++)
      executors.push_back(
          std::thread(&coordinator::execute_bh, this, partitions[i]));

    for (unsigned int i = 0; i < num_executors; i++)
      executors[i].join();

    double sum_dur = 0;
    for (unsigned int i = 0; i < num_executors; i++) {
      //cout<<"dur :"<<i<<" :: "<<tm[i].duration()<<endl;
      sum_dur += tm[i].duration();
    }
    //cout<<"avg dur :"<<sum_dur/num_executors<<endl;
    display_stats(conf.etype, sum_dur / num_executors, num_txns);

    /*
     for (unsigned int i = 0; i < num_executors; i++) {
     delete partitions[i];
     }

     delete[] partitions;
     */
  }

  benchmark* get_benchmark(config& state, unsigned int tid, database* db) {
    benchmark* bh = NULL;

    // Fix benchmark
    switch (state.btype) {
      case benchmark_type::YCSB:
        LOG_INFO("YCSB");
        bh = new ycsb_benchmark(state, tid, db, &tm[tid], &sps[tid]);
        break;

      case benchmark_type::TPCC:
        LOG_INFO("TPCC");
        bh = new tpcc_benchmark(state, tid, db, &tm[tid], &sps[tid]);
        break;

      default:
        cout << "Unknown benchmark type :: " << state.btype << endl;
        break;
    }

    return bh;
  }

  bool single;
  unsigned int num_executors;
  unsigned int num_txns;

  std::vector<struct static_info> sps;
  std::vector<timer> tm;
};

#endif /* COORDINATOR_H_ */
