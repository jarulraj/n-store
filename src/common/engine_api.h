#pragma once

#include <string>
#include "statement.h"

namespace storage {

class engine_api {
 public:
  virtual ~engine_api() {}

  virtual std::string select(const statement& st) = 0;
  virtual int insert(const statement& st) = 0;
  virtual int remove(const statement& st) = 0;
  virtual int update(const statement& st) = 0;

  virtual void load(const statement& st) = 0;

  virtual void txn_begin() = 0;
  virtual void txn_end(bool commit) = 0;

  virtual void recovery() = 0;

  engine_type etype;
};

}

