#ifndef TXN_H_
#define TXN_H_

#include <string>
#include <chrono>

using namespace std;

// TRANSACTION

enum txn_type{
    Insert,
    Delete,
    Update,
    Select,
    Snapshot
};

class txn {
public:
	txn(unsigned long _txn_id, txn_type _type, unsigned int _key, char* _value) :
			txn_id(_txn_id),
			type(_type),
			key(_key),
			value(_value)
	{
		//start = std::chrono::high_resolution_clock::now();
	}

	//private:
	unsigned long txn_id;
	txn_type type;

	unsigned int key;
	char* value;

	chrono::time_point<std::chrono::high_resolution_clock> start, end;
};


#endif /* TXN_H_ */
