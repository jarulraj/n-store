// TESTER

#include <cassert>

#include "nstore.h"
#include "wal_engine.h"
#include "opt_wal_engine.h"
#include "sp_engine.h"
#include "opt_sp_engine.h"
#include "lsm_engine.h"
#include "opt_lsm_engine.h"

#include "ycsb_benchmark.h"
#include "tpcc_benchmark.h"
#include "utils.h"

#include "libpm.h"

using namespace std;

extern struct static_info *sp;  // global persistent memory structure
int level = 2;  // verbosity level

static void usage_exit(FILE *out) {
  fprintf(out, "Command line options : nstore <options> \n"
          "   -x --num-txns          :  Number of transactions \n"
          "   -k --num-keys          :  Number of keys \n"
          "   -e --num-executors     :  Number of executors \n"
          "   -f --fs-path           :  Path for FS \n"
          "   -g --gc-interval       :  Group commit interval \n"
          "   -a --wal-enable        :  WAL enable (traditional) \n"
          "   -w --opt-wal-enable    :  OPT WAL enable \n"
          "   -s --sp-enable         :  SP enable (traditional) \n"
          "   -c --opt-sp-enable     :  OPT SP enable \n"
          "   -m --lsm-enable        :  LSM enable (traditional) \n"
          "   -l --opt-lsm-enable    :  OPT LSM enable \n"
          "   -y --ycsb              :  YCSB benchmark \n"
          "   -t --tpcc              :  TPCC benchmark \n"
          "   -h --help              :  Print help message \n"
          "   -p --per-writes        :  Percent of writes \n"
          "   -u --ycsb-update-one   :  Update one field \n"
          "   -q --ycsb_zipf_skew    :  Zipf Skew \n");
  exit(-1);
}

static void parse_arguments(int argc, char* argv[], config& state) {

  // Default Values
  state.fs_path = std::string("/mnt/pmfs/n-store/");

  state.num_keys = 10;
  state.num_txns = 10;
  state.num_executors = 1;

  state.verbose = false;

  state.gc_interval = 5;
  state.ycsb_per_writes = 0.1;

  state.merge_interval = 100000;
  state.merge_ratio = 0.2;

  state.etype = engine_type::WAL;
  state.btype = benchmark_type::YCSB;

  state.read_only = false;

  state.ycsb_skew = 1.0;
  state.ycsb_update_one = false;
  state.ycsb_field_size = 100;
  state.ycsb_tuples_per_txn = 2;
  state.ycsb_num_val_fields = 10;

  state.tpcc_num_warehouses = 2;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "f:x:k:e:p:g:q:vwascmhluyt", opts, &idx);

    if (c == -1)
      break;

    switch (c) {
      case 'f':
        state.fs_path = std::string(optarg);
        cout << "fs_path: " << state.fs_path << endl;
        break;
      case 'x':
        state.num_txns = atoi(optarg);
        cout << "num_txns: " << state.num_txns << endl;
        break;
      case 'k':
        state.num_keys = atoi(optarg);
        cout << "num_keys: " << state.num_keys << endl;
        break;
      case 'e':
        state.num_executors = atoi(optarg);
        cout << "num_executors: " << state.num_executors << endl;
        break;
      case 'v':
        state.verbose = true;
        level = 3;
        break;
      case 'p':
        state.ycsb_per_writes = atof(optarg);
        assert(state.ycsb_per_writes >= 0 && state.ycsb_per_writes <= 1);

        if (state.ycsb_per_writes == 0)
          state.read_only = true;
        cout << "per_writes: " << state.ycsb_per_writes << endl;
        break;
      case 'g':
        state.gc_interval = atoi(optarg);
        cout << "gc_interval: " << state.gc_interval << endl;
        break;
      case 'a':
        state.etype = engine_type::WAL;
        cout << "wal_enable: " << endl;
        break;
      case 'w':
        state.etype = engine_type::OPT_WAL;
        cout << "opt_wal_enable " << endl;
        break;
      case 's':
        state.etype = engine_type::SP;
        cout << "sp_enable  " << endl;
        break;
      case 'c':
        state.etype = engine_type::OPT_SP;
        cout << "opt_sp_enable " << endl;
        break;
      case 'm':
        state.etype = engine_type::LSM;
        cout << "lsm_enable " << endl;
        break;
      case 'l':
        state.etype = engine_type::OPT_LSM;
        cout << "opt_lsm_enable " << endl;
        break;
      case 'y':
        state.btype = benchmark_type::YCSB;
        cout << "ycsb_benchmark " << endl;
        break;
      case 't':
        state.btype = benchmark_type::TPCC;
        cout << "tpcc_benchmark " << endl;
        break;
      case 'q':
        state.ycsb_skew = atof(optarg);
        cout << "skew: " << state.ycsb_skew << endl;
        break;
      case 'u':
        state.ycsb_update_one = true;
        cout << "ycsb_update_one " << endl;
        break;
      case 'h':
        usage_exit(stderr);
        break;
      default:
        fprintf(stderr, "\nUnknown option: -%c-\n", c);
        usage_exit(stderr);
    }
  }
}

void execute(config& state) {

  bool generate_dataset = !sp->init;
  benchmark* bh;

  // Fix benchmark
  switch (state.btype) {
    case benchmark_type::YCSB: {
      LOG_INFO("YCSB");

      bh = new ycsb_benchmark(state);
    }
      break;

    case benchmark_type::TPCC: {
      LOG_INFO("TPCC");

      bh = new tpcc_benchmark(state);
    }
      break;

    default:
      cout << "Unknown benchmark type :: " << state.btype << endl;
      break;
  }

  // Run engine
  switch (state.etype) {
    case engine_type::WAL: {
      LOG_INFO("WAL");
      wal_engine* wal;

      wal = new wal_engine(state);
      bh->load(wal);
      delete wal;

      wal = new wal_engine(state, state.read_only);
      bh->execute(wal);
      delete wal;
    }
      break;

    case engine_type::SP: {
      LOG_INFO("SP");
      sp_engine* sp;

      if (generate_dataset) {
        sp = new sp_engine(state);
        bh->load(sp);
        delete sp;
      }

      sp = new sp_engine(state, state.read_only);
      bh->execute(sp);
      delete sp;

    }
      break;

    case engine_type::LSM: {
      LOG_INFO("LSM");
      lsm_engine* lsm;

      lsm = new lsm_engine(state);
      bh->load(lsm);
      delete lsm;

      lsm = new lsm_engine(state, false);
      bh->execute(lsm);
      delete lsm;
    }
      break;

    case engine_type::OPT_WAL: {
      LOG_INFO("OPT WAL");
      opt_wal_engine* opt_wal;

      if (generate_dataset) {
        opt_wal = new opt_wal_engine(state);
        bh->load(opt_wal);
        delete opt_wal;
      }

      opt_wal = new opt_wal_engine(state, state.read_only);
      bh->execute(opt_wal);
      delete opt_wal;
    }
      break;

    case engine_type::OPT_SP: {
      LOG_INFO("OPT SP");
      opt_sp_engine* opt_sp;

      if (generate_dataset) {
        opt_sp = new opt_sp_engine(state);
        bh->load(opt_sp);
        delete opt_sp;
      }

      opt_sp = new opt_sp_engine(state, state.read_only);
      bh->execute(opt_sp);
      delete opt_sp;
    }

      break;

    case engine_type::OPT_LSM: {
      LOG_INFO("OPT LSM");

      opt_lsm_engine* opt_lsm;

      if (generate_dataset) {
        opt_lsm = new opt_lsm_engine(state);
        bh->load(opt_lsm);
        delete opt_lsm;
      }

      opt_lsm = new opt_lsm_engine(state, state.read_only);
      bh->execute(opt_lsm);
      delete opt_lsm;
    }
      break;

    default:
      cout << "Unknown engine type :: " << state.etype << endl;
      break;
  }

}

int main(int argc, char **argv) {
  const char* path = "/mnt/pmfs/n-store/zfile";

  size_t pmp_size = 8UL * 1024 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path << endl;

  sp = (struct static_info *) pmemalloc_static_area();

// Start
  config state;
  parse_arguments(argc, argv, state);
  state.sp = sp;

  execute(state);

  pmemalloc_end(path);

  return 0;
}
