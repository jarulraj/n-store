// ARIES LOGGING

#include "aries_engine.h"
#include <fstream>

using namespace std;

void aries_engine::group_commit() {

  while (ready) {
    //std::cout << "Syncing log !" << endl;

    // sync
    undo_log.write();

    std::this_thread::sleep_for(std::chrono::milliseconds(conf.gc_interval));
  }
}

aries_engine::aries_engine(const config& _conf)
    : conf(_conf),
      db(conf.db) {

  //for (int i = 0; i < conf.num_executors; i++)
  //  executors.push_back(std::thread(&wal_engine::runner, this));

}

aries_engine::~aries_engine() {

  // done = true;
  //for (int i = 0; i < conf.num_executors; i++)
  //  executors[i].join();

}

std::string aries_engine::select(const statement& st) {
  LOG_INFO("Select");
  std::string val;

  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  table_index* table_index = tab->indices->at(st.table_index_id);

  unsigned long key = hash_fn(st.key);

  rec_ptr = table_index->map->at(key);
  val = get_data(rec_ptr, st.projection);
  LOG_INFO("val : %s", val.c_str());

  return val;
}

void aries_engine::insert(const statement& st) {
  LOG_INFO("Insert");
  record* after_rec = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = get_data(after_rec, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);

  // Check if key exists
  if (indices->at(0)->map->contains(key) != 0) {
    return;
  }

  // Add log entry
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << serialize(after_rec, after_rec->sptr) << "\n";
  entry_str = entry_stream.str();
  undo_log.push_back(entry_str);

  tab->data->push_back(after_rec);

  // Add entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = get_data(after_rec, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->map->insert(key, after_rec);
  }
}

void aries_engine::remove(const statement& st) {
  LOG_INFO("Remove");
  record* rec_ptr = st.rec_ptr;
  table* tab = db->tables->at(st.table_id);
  plist<table_index*>* indices = tab->indices;

  unsigned int num_indices = tab->num_indices;
  unsigned int index_itr;

  std::string key_str = get_data(rec_ptr, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);

  // Check if key does not exist
  if (indices->at(0)->map->contains(key) == 0) {
    return;
  }

  record* before_rec = indices->at(0)->map->at(key);

  // Add log entry
  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " " << serialize(before_rec, before_rec->sptr) << "\n";

  entry_str = entry_stream.str();
  undo_log.push_back(entry_str);

  tab->data->erase(before_rec);

  // Remove entry in indices
  for (index_itr = 0; index_itr < num_indices; index_itr++) {
    key_str = get_data(rec_ptr, indices->at(index_itr)->sptr);
    key = hash_fn(key_str);

    indices->at(index_itr)->map->erase(key);
  }

}

void aries_engine::update(const statement& st) {
  LOG_INFO("Update");
  record* rec_ptr = st.rec_ptr;
  plist<table_index*>* indices = db->tables->at(st.table_id)->indices;

  std::string key_str = get_data(rec_ptr, indices->at(0)->sptr);
  unsigned long key = hash_fn(key_str);

  record* before_rec = indices->at(0)->map->at(key);

  // Check if key does not exist
  if (before_rec == 0)
    return;

  std::string after_data, before_data;
  int num_fields = st.field_ids.size();

  entry_stream.str("");
  entry_stream << st.transaction_id << " " << st.op_type << " " << st.table_id
               << " ";
  // before image
  //entry_stream << serialize(before_rec, before_rec->sptr) << " ";

  for (int field_itr : st.field_ids) {
    // Update existing record
    before_rec->set_data(field_itr, rec_ptr);
  }

  // after image
  entry_stream << serialize(before_rec, before_rec->sptr) << "\n";

  // Add log entry
  entry_str = entry_stream.str();
  undo_log.push_back(entry_str);
}

// RUNNER + LOADER

void aries_engine::execute(const transaction& txn) {

  for (const statement& st : txn.stmts) {
    if (st.op_type == operation_type::Select) {
      select(st);
    } else if (st.op_type == operation_type::Insert) {
      insert(st);
    } else if (st.op_type == operation_type::Update) {
      update(st);
    } else if (st.op_type == operation_type::Delete) {
      remove(st);
    }
  }

}

void aries_engine::runner() {
  bool empty = true;

  while (!done) {
    rdlock(&txn_queue_rwlock);
    empty = txn_queue.empty();
    unlock(&txn_queue_rwlock);

    if (!empty) {
      wrlock(&txn_queue_rwlock);
      const transaction& txn = txn_queue.front();
      txn_queue.pop();
      unlock(&txn_queue_rwlock);

      execute(txn);
    }
  }

  while (!txn_queue.empty()) {
    wrlock(&txn_queue_rwlock);
    const transaction& txn = txn_queue.front();
    txn_queue.pop();
    unlock(&txn_queue_rwlock);

    execute(txn);
  }
}

void aries_engine::generator(const workload& load, bool stats) {

  undo_log.configure(conf.fs_path + "log");

  // Logger start
  std::thread gc(&aries_engine::group_commit, this);
  ready = true;

  for (const transaction& txn : load.txns)
    execute(txn);

  // Logger end
  ready = false;
  gc.join();

  undo_log.write();
  undo_log.close();

}

void aries_engine::recovery() {

  int op_type, txn_id, table_id;
  table *tab;
  plist<table_index*>* indices;
  unsigned int num_indices, index_itr;

  field_info finfo;
  std::string entry_str;
  std::ifstream log_file(undo_log.log_file_name);

  while (std::getline(log_file, entry_str)) {
    //LOG_INFO("entry : %s ", entry_str.c_str());
    std::stringstream entry(entry_str);

    entry >> txn_id >> op_type >> table_id;

    switch (op_type) {
      case operation_type::Insert: {
        LOG_INFO("Redo Insert");

        tab = db->tables->at(table_id);
        schema* sptr = tab->sptr;
        record* after_rec = deserialize(entry, sptr);

        indices = tab->indices;
        num_indices = tab->num_indices;

        tab->data->push_back(after_rec);

        // Add entry in indices
        for (index_itr = 0; index_itr < num_indices; index_itr++) {
          std::string key_str = get_data(after_rec,
                                         indices->at(index_itr)->sptr);
          unsigned long key = hash_fn(key_str);

          indices->at(index_itr)->map->insert(key, after_rec);
        }
      }
        break;

      case operation_type::Delete: {
        LOG_INFO("Redo Delete");

        tab = db->tables->at(table_id);
        schema* sptr = tab->sptr;
        record* before_rec = deserialize(entry, sptr);

        indices = tab->indices;
        num_indices = tab->num_indices;

        tab->data->erase(before_rec);

        // Remove entry in indices
        for (index_itr = 0; index_itr < num_indices; index_itr++) {
          std::string key_str = get_data(before_rec,
                                         indices->at(index_itr)->sptr);
          unsigned long key = hash_fn(key_str);

          indices->at(index_itr)->map->erase(key);
        }
      }
        break;

      case operation_type::Update:
        LOG_INFO("Redo Update");
        {
          int num_fields;
          int field_itr;

          tab = db->tables->at(table_id);
          schema* sptr = tab->sptr;
          record* before_rec = deserialize(entry, sptr);
          record* after_rec = deserialize(entry, sptr);

          // Update entry in indices
          for (index_itr = 0; index_itr < num_indices; index_itr++) {
            std::string key_str = get_data(before_rec,
                                           indices->at(index_itr)->sptr);
            unsigned long key = hash_fn(key_str);

            indices->at(index_itr)->map->insert(key, after_rec);
          }

        }

        break;

      default:
        cout << "Invalid operation type" << op_type << endl;
        break;
    }

  }

}

