#ifndef ENGINE_MT_H_
#define ENGINE_MT_H_

#include <string>

#include "engine.h"
#include "lock_manager.h"

using namespace std;

class engine_mt : public engine {
 public:
  engine_mt()
      : engine() {
  }

  engine_mt(const config& conf)
      : engine(conf) {
  }

  std::string select(const statement& st) {
    return (de->select(st));
  }

  int insert(const statement& st) {
    return (de->insert(st));
  }

  int remove(const statement& st) {
    return (de->remove(st));
  }

  int update(const statement& st) {
    return (de->update(st));
  }

  void load(const statement& st) {
    de->load(st);
  }

  void txn_begin() {
    de->txn_begin();
  }

  void txn_end(bool commit) {
    de->txn_end(commit);
  }

  void recovery() {
    de->recovery();
  }
};

#endif  /* ENGINE_MT_H_ */
