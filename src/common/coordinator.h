#pragma once

#include <vector>
#include <thread>
#include <map>

#include "config.h"
#include "engine.h"
#include "timer.h"
#include "utils.h"
#include "database.h"
#include "libpm.h"

#include "ycsb_benchmark.h"
#include "tpcc_benchmark.h"

namespace storage {

class coordinator {
 public:
  coordinator()
      : single(true),
        num_executors(1),
        num_txns(0) {
  }

  coordinator(const config conf) {
    single = conf.single;
    num_executors = conf.num_executors;
    num_txns = conf.num_txns;

    for (unsigned int i = 0; i < num_executors; i++) {
      tms.push_back(timer());
      sps.push_back(static_info());
    }
  }

  void execute_bh(benchmark* bh) {
    // Load
    bh->load();

    // Execute
    bh->execute();

  }

  void eval(const config conf){
    if(!conf.recovery){
      execute(conf);
    }
    else{
      recover(conf);
    }

  }

  void execute(const config conf) {
    std::vector<std::thread> executors;
    benchmark** partitions = new benchmark*[num_executors];

    for (unsigned int i = 0; i < num_executors; i++) {
      database* db = new database(conf, sp, i);
      partitions[i] = get_benchmark(conf, i, db);
    }

    for (unsigned int i = 0; i < num_executors; i++)
      executors.push_back(
          std::thread(&coordinator::execute_bh, this, partitions[i]));

    for (unsigned int i = 0; i < num_executors; i++)
      executors[i].join();

    double max_dur = 0;
    for (unsigned int i = 0; i < num_executors; i++) {
      std::cout<<"dur :"<<i<<" :: "<<tms[i].duration()<<std::endl;
      max_dur = std::max(max_dur, tms[i].duration());
    }
    std::cout<<"max dur :"<<max_dur<<std::endl;
    display_stats(conf.etype, max_dur, num_txns);

    /*
     for (unsigned int i = 0; i < num_executors; i++) {
     delete partitions[i];
     }

     delete[] partitions;
     */
  }

  void recover(const config conf){

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
        bh = new ycsb_benchmark(state, tid, db, &tms[tid], &sps[tid]);
        break;

      case benchmark_type::TPCC:
        LOG_INFO("TPCC");
        bh = new tpcc_benchmark(state, tid, db, &tms[tid], &sps[tid]);
        break;

      default:
        std::cout << "Unknown benchmark type :: " << state.btype << std::endl;
        break;
    }

    return bh;
  }

  bool single;
  unsigned int num_executors;
  unsigned int num_txns;

  std::vector<struct static_info> sps;
  std::vector<timer> tms;
};

}
