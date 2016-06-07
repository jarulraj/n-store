#pragma once

#include <string>

#include "wal_engine.h"
#include "sp_engine.h"
#include "lsm_engine.h"
#include "opt_wal_engine.h"
#include "opt_sp_engine.h"
#include "opt_lsm_engine.h"

namespace storage {

class engine {
 public:
  engine()
      : etype(engine_type::WAL),
        de(NULL) {
  }

  engine(const config& conf, unsigned int tid, database* db, bool read_only)
      : etype(conf.etype) {

    switch (conf.etype) {
      case engine_type::WAL:
        de = new wal_engine(conf, db, read_only, tid);
        break;
      case engine_type::SP:
	die();
        de = new sp_engine(conf, db, read_only, tid);
        break;
      case engine_type::LSM:
	die();
        de = new lsm_engine(conf, db, read_only, tid);
        break;
      case engine_type::OPT_WAL:
	die();
        de = new opt_wal_engine(conf, db, read_only, tid);
        break;
      case engine_type::OPT_SP:
	die();
        de = new opt_sp_engine(conf, db, read_only, tid);
        break;
      case engine_type::OPT_LSM:
	die();
        de = new opt_lsm_engine(conf, db, read_only, tid);
        break;
      default:
        std::cout << "Unknown engine type :: " << etype << std::endl;
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
    std::cout << "ST" << std::endl;
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

}

