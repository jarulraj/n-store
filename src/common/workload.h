#ifndef WORKLOAD_H_
#define WORKLOAD_H_

#include <vector>
#include "transaction.h"

using namespace std;


class workload {

public:
	vector<transaction> txns;
	vector<table> tables;

};

#endif /* WORKLOAD_H_ */
