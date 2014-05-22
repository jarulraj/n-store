#ifndef TXN_H_
#define TXN_H_

#include <string>
#include <chrono>

using namespace std;

// TRANSACTION

class txn {
public:
	txn(unsigned long _txn_id, std::string _txn_type, unsigned int _key, std::string _value) :
			txn_id(_txn_id),
			txn_type(_txn_type),
			key(_key),
			value(_value)
	{
		//start = std::chrono::high_resolution_clock::now();
	}

	//private:
	unsigned long txn_id;
	string txn_type;

	unsigned int key;
	string value;

	chrono::time_point<std::chrono::high_resolution_clock> start, end;
};


#endif /* TXN_H_ */
