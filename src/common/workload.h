#ifndef WORKLOAD_H_
#define WORKLOAD_H_

#include <list>
#include <unordered_map>
#include "transaction.h"

using namespace std;


class workload {

public:
	list<transaction> txns;
	bool read_only = false;
};

#endif /* WORKLOAD_H_ */
