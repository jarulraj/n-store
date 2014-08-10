#ifndef ENGINE_MT_H_
#define ENGINE_MT_H_

#include <string>
#include <set>

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
    /*
     pthread_rwlock_t* tuple_rwlock = st.rec_ptr->tuple_rwlock;

     if (lk_set.find(tuple_rwlock) != lk_set.end()) {
     ret = de->select(st);
     } else if (try_rdlock(tuple_rwlock) == 0) {
     ret = de->select(st);
     lk_set.insert(tuple_rwlock);
     } else
     release();
     */

    return ret;
  }

  int insert(const statement& st) {
    int ret = -1;
    /*
     pthread_rwlock_t* tuple_rwlock = st.rec_ptr->tuple_rwlock;

     if (lk_set.find(tuple_rwlock) != lk_set.end()) {
     ret = de->insert(st);
     } else if (try_wrlock(tuple_rwlock) == 0) {
     ret = de->insert(st);
     lk_set.insert(tuple_rwlock);
     } else
     release();
     */

    return ret;
  }

  int remove(const statement& st) {
    int ret = -1;
    /*
     pthread_rwlock_t* tuple_rwlock = st.rec_ptr->tuple_rwlock;

     if (lk_set.find(tuple_rwlock) != lk_set.end()) {
     ret = de->remove(st);
     } else if (try_wrlock(tuple_rwlock) == 0) {
     ret = de->remove(st);
     lk_set.insert(tuple_rwlock);
     } else
     release();
     */
    return ret;
  }

  int update(const statement& st) {
    int ret = -1;
    /*
     pthread_rwlock_t* tuple_rwlock = st.rec_ptr->tuple_rwlock;

     if (lk_set.find(tuple_rwlock) != lk_set.end()) {
     ret = de->update(st);
     } else if (try_wrlock(tuple_rwlock) == 0) {
     ret = de->update(st);
     lk_set.insert(tuple_rwlock);
     } else
     release();
     */
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
    for (pthread_rwlock_t* lk : lk_set)
      unlock(lk);
    lk_set.clear();
  }

  void display() {
    cout << "MT :: ";
    cout << "aborts :: " << lm->abort << endl;
  }

  std::set<pthread_rwlock_t*> lk_set;
  lock_manager* lm;
};

#endif  /* ENGINE_MT_H_ */
