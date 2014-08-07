#ifndef ENGINE_H_
#define ENGINE_H_

#include <string>
#include "statement.h"
#include "nstore.h"

using namespace std;

class engine {
 public:
  virtual std::string select(const statement& st) = 0;
  virtual int insert(const statement& st) = 0;
  virtual int remove(const statement& st) = 0;
  virtual int update(const statement& st) = 0;

  virtual void load(const statement& st) = 0;

  virtual void txn_begin() = 0;
  virtual void txn_end(bool commit) = 0;

  virtual void recovery() = 0;

  virtual ~engine() {}

  int txn_counter;
  engine_type etype;
};

#endif  /* ENGINE_H_ */
