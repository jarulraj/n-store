#ifndef ENGINE_MT_H_
#define ENGINE_MT_H_

#include <string>

#include "engine.h"
#include "lock_manager.h"
#include "utils.h"

using namespace std;

class engine_mt : public engine {
 public:
  engine_mt()
      : engine(),
        lm(NULL) {
  }

  engine_mt(const config& conf, int tid, bool read_only, lock_manager* _lm)
      : engine(conf, tid, read_only),
        lm(_lm) {
    //cout<<"MT engine"<<endl;
  }

  std::string select(const statement& st) {
    std::string ret;
    int tuple_id = st.rec_ptr->get_hash_id();
    unsigned long lock_id = hasher(st.table_id, tuple_id);
    int rc = lm->tuple_rdlock(lock_id);

    if (rc == 0) {
      ret = de->select(st);
      lk_vec.push_back(lock_id);
    } else
      release();

    return ret;
  }

  int insert(const statement& st) {
    int ret = -1;
    int tuple_id = st.rec_ptr->get_hash_id();
    unsigned long lock_id = hasher(st.table_id, tuple_id);
    int rc = lm->tuple_wrlock(lock_id);

    if (rc == 0) {
      ret = de->insert(st);
      lk_vec.push_back(lock_id);
    } else
      release();

    return ret;
  }

  int remove(const statement& st) {
    int ret = -1;
    int tuple_id = st.rec_ptr->get_hash_id();
    unsigned long lock_id = hasher(st.table_id, tuple_id);
    int rc = lm->tuple_wrlock(lock_id);

    if (rc == 0)
      ret = de->remove(st);
    else
      release();

    return ret;
  }

  int update(const statement& st) {
    int ret = -1;
    int tuple_id = st.rec_ptr->get_hash_id();
    unsigned long lock_id = hasher(st.table_id, tuple_id);
    int rc = lm->tuple_wrlock(lock_id);

    if (rc == 0) {
      ret = de->update(st);
      lk_vec.push_back(lock_id);
    } else
      release();

    return ret;
  }

  virtual void txn_begin() {
    de->txn_begin();
  }

  virtual void txn_end(bool commit) {
    de->txn_end(commit);

    release();
  }

  void release() {
    for (unsigned long lk : lk_vec)
      lm->tuple_unlock(lk);
    lk_vec.clear();
  }

  void display() {
    cout << "MT :: ";
    cout << "aborts :: " << lm->abort << endl;
  }

  std::vector<unsigned long> lk_vec;
  lock_manager* lm;
};

#endif  /* ENGINE_MT_H_ */
