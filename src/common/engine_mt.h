#ifndef ENGINE_MT_H_
#define ENGINE_MT_H_

#include <string>

#include "engine.h"
#include "lock_manager.h"

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
    int hash_id = st.rec_ptr->get_hash_id();
    int rc = lm->tuple_lock(st.table_id, hash_id);

    if (rc == 0) {
      ret = de->select(st);
      lm->tuple_unlock(st.table_id, hash_id);
    }

    return ret;
  }

  int insert(const statement& st) {
    int ret = -1;
    int hash_id = st.rec_ptr->get_hash_id();
    int rc = lm->tuple_lock(st.table_id, hash_id) ;

    if (rc == 0) {
      ret = de->insert(st);
      lm->tuple_unlock(st.table_id, hash_id);
    }

    return ret;
  }

  int remove(const statement& st) {
    int ret = -1;
    int hash_id = st.rec_ptr->get_hash_id();
    int rc = lm->tuple_lock(st.table_id, hash_id);

    if (rc == 0) {
      ret = de->remove(st);
      lm->tuple_unlock(st.table_id, hash_id);
    }

    return ret;
  }

  int update(const statement& st) {
    int ret = -1;
    int hash_id = st.rec_ptr->get_hash_id();
    int rc = lm->tuple_lock(st.table_id, hash_id);

    if (rc == 0) {
      ret = de->update(st);
      lm->tuple_unlock(st.table_id, hash_id);
    }

    return ret;
  }

  void display(){
    cout<<"MT"<<endl;
  }

  lock_manager* lm;
};

#endif  /* ENGINE_MT_H_ */
