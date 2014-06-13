#ifndef WAL_COORDINATOR_H_
#define WAL_COORDINATOR_H_

#include "coordinator.h"

using namespace std;

class wal_coordinator : public coordinator {
 public:
  wal_coordinator();
  wal_coordinator(const config& _conf);

  void runner(workload* load);

 private:
  config conf;
  std::vector<std::thread> executors;

};

#endif /* WAL_COORDINATOR_H_ */
