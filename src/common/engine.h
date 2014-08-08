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

  engine(const config& conf, unsigned int tid, bool read_only)
      : etype(conf.etype) {

    switch (conf.etype) {
      case engine_type::WAL:
        de = new wal_engine(conf, read_only, tid);
        break;
      case engine_type::SP:
        de = new sp_engine(conf, read_only, tid);
        break;
      case engine_type::LSM:
        de = new lsm_engine(conf, read_only, tid);
        break;
      case engine_type::OPT_WAL:
        de = new opt_wal_engine(conf, read_only, tid);
        break;
      case engine_type::OPT_SP:
        de = new opt_sp_engine(conf, read_only, tid);
        break;
      case engine_type::OPT_LSM:
        de = new opt_lsm_engine(conf, read_only, tid);
        break;
      default:
        cout << "Unknown engine type :: " << etype << endl;
        exit(EXIT_FAILURE);
        break;
    }

  }

  virtual ~engine() {
    delete de;
  }

  virtual std::string select(const statement& st) {
    return (de->select(st));
  }

  virtual int insert(const statement& st) {
    return (de->insert(st));
  }

  virtual int remove(const statement& st) {
    return (de->remove(st));
  }

  virtual int update(const statement& st) {
    return (de->update(st));
  }

  virtual void display() {
    cout << "ST" << endl;
  }

  void load(const statement& st) {
    de->load(st);
  }

  virtual void txn_begin() {
    de->txn_begin();
  }

  virtual void txn_end(bool commit) {
    de->txn_end(commit);
  }

  void recovery() {
    de->recovery();
  }

  engine_type etype;
  engine_api* de;
};

#endif  /* ENGINE_H_ */
