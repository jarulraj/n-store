#ifndef WAL_COORDINATOR_H_
#define WAL_COORDINATOR_H_

#include "coordinator.h"
#include "wal_engine.h"

using namespace std;

class wal_coordinator : public coordinator {
 public:
  wal_coordinator(const config& _conf, workload& _load);
  ~wal_coordinator();

  void runner(workload& load);

 private:
  const config& conf;
  workload& load;

  vector<wal_engine*> engines;
  std::vector<std::thread> executors;

};

#endif /* WAL_COORDINATOR_H_ */
