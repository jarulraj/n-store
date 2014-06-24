#ifndef ENGINE_H_
#define ENGINE_H_

#include "transaction.h"
#include "statement.h"

#include <boost/lockfree/queue.hpp>
#include <thread>
#include <atomic>
#include <string>

using namespace std;

class engine {
 public:
  virtual std::string select(const statement& st) = 0;
  virtual int update(const statement& st) = 0;

  virtual int test() = 0;

  virtual ~engine() {}

};

#endif  /* ENGINE_H_ */
