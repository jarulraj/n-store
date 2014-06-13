#include "wal_coordinator.h"
#include "wal_engine.h"
#include "transaction.h"
#include "statement.h"

#include <vector>

using namespace std;

wal_coordinator::wal_coordinator(const config& _conf)
    : conf(_conf) {

  vector<wal_engine*> engines;

  // Executors
  for (int i = 0; i < conf.num_parts; i++) {
    wal_engine* we = new wal_engine(conf);
    engines.push_back(we);
    executors.push_back(std::thread(&wal_engine::runner, we));
  }

  for (int i = 0; i < conf.num_parts; i++) {
    executors.at(i).join();
    delete engines.at(i);
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

    // Single Partition Txn
    if ((*txn_itr).ttype == transaction_type::Single) {

      for (stmt_itr = stmts.begin(); stmt_itr != stmts.end(); stmt_itr++) {
        statement* s_ptr = (*stmt_itr);
        unsigned int part_id = s_ptr->partition_id;

        if (s_ptr->type == stmt_type::Insert) {
          cout << "Insert on engine " << part_id << endl;
        } else if (s_ptr->type == stmt_type::Select) {
          cout << "Select on engine " << part_id << endl;
        }
      }

    }

  } // End workload

}

