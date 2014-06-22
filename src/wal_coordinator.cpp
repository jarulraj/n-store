#include "wal_coordinator.h"
#include "transaction.h"
#include "statement.h"
#include "message.h"

#include <vector>

using namespace std;

wal_coordinator::wal_coordinator(const config& _conf, workload& _load)
    : conf(_conf),
      load(_load) {

  // Executors
  for (int i = 0; i < conf.num_parts; i++) {
    wal_engine* we = new wal_engine(i, conf, load);

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

void wal_coordinator::runner(workload& load) {

  vector<transaction>& txns = load.txns;
  statement* st;
  unsigned int part_id;

  vector<transaction>::iterator txn_itr;
  vector<statement*>::iterator stmt_itr;


  // Process workload
  for (txn_itr = txns.begin(); txn_itr != txns.end(); txn_itr++) {

    for (stmt_itr = (*txn_itr).stmts.begin(); stmt_itr != (*txn_itr).stmts.end(); stmt_itr++) {

      // Single Partition
      if ((*stmt_itr)->part_type == partition_type::Single) {

        st = (*stmt_itr);
        part_id = st->partition_id;

        message msg(message_type::Request, st->statement_id, false, st);
        engines[part_id]->msg_queue.push(msg);
      }

    }

  }

}

