#ifndef TABLE_INDEX_H_
#define TABLE_INDEX_H_

#include <unordered_map>
#include "record.h"

using namespace std;

class table_index {
 public:

  table_index(vector<bool> _key)
      : key(_key) {
  }

  //private:

  vector<bool> key;
  unordered_map<std::string, record*> index;

};

#endif /* TABLE_INDEX_H_ */
