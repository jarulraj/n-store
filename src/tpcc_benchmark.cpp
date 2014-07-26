// TPCC BENCHMARK

#include "tpcc_benchmark.h"

using namespace std;

class tpcc_record : public record {
 public:
  tpcc_record(schema* _sptr, int _key, const std::string& _val,
              int num_val_fields, bool update_one)
      : record(_sptr) {

    std::sprintf(&(data[sptr->columns[0].offset]), "%d", _key);

  }

};

tpcc_benchmark::tpcc_benchmark(config& _conf)
    : conf(_conf),
      txn_id(0) {

  btype = benchmark_type::TPCC;

}

void tpcc_benchmark::load(engine* ee) {
}

void tpcc_benchmark::execute(engine* ee) {
}
