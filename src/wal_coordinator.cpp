#include "wal_coordinator.h"
#include "transaction.h"
#include "statement.h"
#include "message.h"

#include <vector>

using namespace std;

wal_coordinator::wal_coordinator(const config& _conf, database* _db)
    : conf(_conf),
      db(_db) {

  // Executors
  for (int i = 0; i < conf.num_parts; i++) {
    wal_engine* we = new wal_engine(i, conf, db);

    engines.push_back(we);
    executors.push_back(std::thread(&wal_engine::runner, we));
  }

}

wal_coordinator::~wal_coordinator() {

  // Turn off engines
  for (int i = 0; i < conf.num_parts; i++) {
    engines[i]->done = true;

    executors[i].join();

    delete engines[i];
  }

}

void wal_coordinator::runner(const workload& load) {

  unsigned int part_id;

  vector<transaction>::const_iterator txn_itr;
  vector<statement>::const_iterator stmt_itr;

  // Process workload
  for (txn_itr = load.txns.begin(); txn_itr != load.txns.end(); txn_itr++) {

    for (stmt_itr = (*txn_itr).stmts.begin();
        stmt_itr != (*txn_itr).stmts.end(); stmt_itr++) {

      // Single Partition
      if ((*stmt_itr).part_type == partition_type::Single) {

        part_id = (*stmt_itr).partition_id;

        message msg(message_type::Request, (*stmt_itr).transaction_id, false,
                    (*stmt_itr));

        wrlock(&engines[part_id]->msg_queue_rwlock);
        engines[part_id]->msg_queue.push(msg);
        unlock(&engines[part_id]->msg_queue_rwlock);

      }

    }

  }

}

