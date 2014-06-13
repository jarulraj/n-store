#ifndef COORDINATOR_H_
#define COORDINATOR_H_

#include "nstore.h"
#include "workload.h"

#include <vector>
#include <thread>

using namespace std;

class coordinator {
 public:
  virtual void runner(workload* load) = 0;

  virtual ~coordinator() {}

};

#endif /* COORDINATOR_H_ */
