#include "wal_coordinator.h"
#include "transaction.h"
#include "statement.h"
#include "message.h"

#include <vector>

using namespace std;

wal_coordinator::wal_coordinator(const config& _conf)
    : conf(_conf) {

  // Executors
  for (int i = 0; i < conf.num_parts; i++) {
    wal_engine* we = new wal_engine(i, conf);
    engines.push_back(we);
    executors.push_back(std::thread(&wal_engine::runner, we));
  }

}

wal_coordinator::~wal_coordinator() {

  // Turn off engines
  for (int i = 0; i < conf.num_parts; i++){
    engines[i]->done = true;

    executors[i].join();

    delete engines[i];
  }

}

void wal_coordinator::runner(workload* load) {

  vector<transaction> txns;
  vector<statement*> stmts;
  vector<transaction>::iterator txn_itr;
  vector<statement*>::iterator stmt_itr;

  txns = load->txns;

  // Process workload
  for (txn_itr = txns.begin(); txn_itr != txns.end(); txn_itr++) {
    stmts = (*txn_itr).stmts;

    for (stmt_itr = stmts.begin(); stmt_itr != stmts.end(); stmt_itr++) {

      // Single Partition
      if ((*stmt_itr)->part_type == partition_type::Single) {
        statement* s_ptr = (*stmt_itr);
        unsigned int part_id = s_ptr->partition_id;

        message msg(message_type::Request, s_ptr->statement_id, false, s_ptr);

        engines[part_id]->msg_queue.push(msg);
      }

    }

  }

}

