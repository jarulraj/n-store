// TESTER

#include <cassert>

#include "nstore.h"
#include "wal_engine.h"
#include "sp_engine.h"
#include "lsm_engine.h"
#include "opt_wal_engine.h"
#include "opt_sp_engine.h"
#include "opt_lsm_engine.h"

#include "ycsb_benchmark.h"
#include "utils.h"

#include "libpm.h"

using namespace std;

extern struct static_info *sp;  // global persistent memory structure
int level = 2;  // verbosity level

static void usage_exit(FILE *out) {
  fprintf(out, "Command line options : nstore <options> \n"
          "   -x --num-txns        :  Number of transactions \n"
          "   -k --num-keys        :  Number of keys \n"
          "   -e --num-executors   :  Number of executors \n"
          "   -p --per-writes      :  Percent of writes \n"
          "   -f --fs-path         :  Path for FS \n"
          "   -g --gc-interval     :  Group commit interval \n"
          "   -a --wal-enable      :  WAL enable (traditional) \n"
          "   -w --opt-wal-enable  :  OPT WAL enable \n"
          "   -s --sp-enable       :  SP enable (traditional) \n"
          "   -c --opt-sp-enable   :  OPT SP enable \n"
          "   -m --lsm-enable      :  LSM enable (traditional) \n"
          "   -l --opt-lsm-enable  :  OPT LSM enable \n"
          "   -q --skew            :  Skew \n"
          "   -h --help            :  Print help message \n");
  exit(-1);
}

static void parse_arguments(int argc, char* argv[], config& state) {

  // Default Values
  state.fs_path = std::string("/mnt/pmfs/n-store/");

  state.num_keys = 2;
  state.num_txns = 10;
  state.num_executors = 1;

  state.sz_value = 4;
  state.verbose = false;

  state.gc_interval = 5;
  state.per_writes = 0.1;

  state.merge_interval = 100;

  state.sp_enable = false;
  state.wal_enable = false;
  state.opt_wal_enable = false;
  state.lsm_enable = false;
  state.opt_sp_enable = false;
  state.opt_lsm_enable = false;

  state.skew = 1;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "f:x:k:e:p:g:q:vwascmhl", opts, &idx);

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
        state.per_writes = atof(optarg);
        cout << "per_writes: " << state.per_writes << endl;
        break;
      case 'g':
        state.gc_interval = atoi(optarg);
        cout << "gc_interval: " << state.gc_interval << endl;
        break;
      case 'a':
        state.wal_enable = true;
        cout << "WAL enable: " << state.wal_enable << endl;
        break;
      case 'w':
        state.opt_wal_enable = true;
        cout << "OPT WAL enable: " << state.opt_wal_enable << endl;
        break;
      case 's':
        state.sp_enable = true;
        cout << "SP enable: " << state.sp_enable << endl;
        break;
      case 'c':
        state.opt_sp_enable = true;
        cout << "OPT SP enable: " << state.opt_sp_enable << endl;
        break;
      case 'm':
        state.lsm_enable = true;
        cout << "LSM enable: " << state.lsm_enable << endl;
        break;
      case 'l':
        state.opt_lsm_enable = true;
        cout << "OPT LSM enable: " << state.opt_lsm_enable << endl;
        break;
      case 'q':
        state.skew = atof(optarg);
        cout << "skew: " << state.skew << endl;
        break;
      case 'h':
        usage_exit(stderr);
        break;
      default:
        fprintf(stderr, "\nUnknown option: -%c-\n", c);
        usage_exit(stderr);
    }
  }

  assert(state.per_writes >= 0 && state.per_writes <= 1);
}

int main(int argc, char **argv) {
  const char* path = "/mnt/pmfs/n-store/zfile";

  size_t pmp_size = 32UL * 1024 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path << endl;

  sp = (struct static_info *) pmemalloc_static_area();

  // Start
  config state;
  parse_arguments(argc, argv, state);
  state.sp = sp;

  if (state.opt_wal_enable == true) {
    LOG_INFO("WAL");

    bool generate_dataset = !sp->init;
    ycsb_benchmark ycsb(state);
    opt_wal_engine opt_wal(state);

    if (generate_dataset)
      opt_wal.generator(ycsb.get_dataset(), false);

    opt_wal.generator(ycsb.get_workload(), true);
  }

  if (state.wal_enable == true) {
    LOG_INFO("ARIES");

    bool generate_dataset = !sp->init;
    ycsb_benchmark ycsb(state);
    wal_engine wal(state);

    wal.generator(ycsb.get_dataset(), false);

    wal.generator(ycsb.get_workload(), true);
  }

  if (state.sp_enable == true) {
    LOG_INFO("SP");

    bool generate_dataset = !sp->init;
    ycsb_benchmark ycsb(state);
    sp_engine sp(state);

    if (generate_dataset)
      sp.generator(ycsb.get_dataset(), false);

    sp.generator(ycsb.get_workload(), true);
  }

  if (state.opt_sp_enable == true) {
    LOG_INFO("OPT SP");

    bool generate_dataset = !sp->init;
    ycsb_benchmark ycsb(state);
    opt_sp_engine opt_sp(state);

    if (generate_dataset)
      opt_sp.generator(ycsb.get_dataset(), false);

    opt_sp.generator(ycsb.get_workload(), true);
  }

  if (state.lsm_enable == true) {
    LOG_INFO("LSM");

    ycsb_benchmark ycsb(state);
    lsm_engine lsm(state);

    lsm.generator(ycsb.get_dataset(), false);
    lsm.generator(ycsb.get_workload(), true);
  }

  if (state.opt_lsm_enable == true) {
    LOG_INFO("OPT LSM");

    bool generate_dataset = !sp->init;
    ycsb_benchmark ycsb(state);
    opt_lsm_engine opt_lsm(state);

    if (generate_dataset)
      opt_lsm.generator(ycsb.get_dataset(), false);

    opt_lsm.generator(ycsb.get_workload(), true);
  }
  return 0;
}
