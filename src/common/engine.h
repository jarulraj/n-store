#ifndef ENGINE_H_
#define ENGINE_H_

#include <string>

#include "wal_engine.h"
#include "sp_engine.h"
#include "lsm_engine.h"
#include "opt_wal_engine.h"
#include "opt_sp_engine.h"
#include "opt_lsm_engine.h"

using namespace std;

class engine {
 public:
  engine()
      : etype(engine_type::WAL),
        de(NULL) {
  }

  engine(const config& conf)
      : etype(conf.etype) {

    switch (conf.etype) {
      case engine_type::WAL:
        de = new wal_engine(conf, conf.read_only);
        break;
      /*
      case engine_type::SP:
        de = new sp_engine(conf, conf.read_only);
        break;
      case engine_type::LSM:
        de = new lsm_engine(conf, conf.read_only);
        break;
      case engine_type::OPT_WAL:
        de = new opt_wal_engine(conf, conf.read_only);
        break;
      case engine_type::OPT_SP:
        de = new opt_sp_engine(conf, conf.read_only);
        break;
      case engine_type::OPT_LSM:
        de = new opt_lsm_engine(conf, conf.read_only);
        break;
      */
      default:
        cout << "Unknown engine type :: " << etype << endl;
        exit(EXIT_FAILURE);
        break;
    }
  }

  ~engine() {
    delete de;
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

  engine_type etype;
  storage_engine* de;
};

#endif  /* ENGINE_H_ */
