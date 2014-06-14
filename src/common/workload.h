#ifndef WORKLOAD_H_
#define WORKLOAD_H_

#include <vector>
#include <unordered_map>
#include "transaction.h"

using namespace std;


class workload {

public:
	vector<transaction> txns;
	unordered_map<std::string, table*> tables;

};

#endif /* WORKLOAD_H_ */
