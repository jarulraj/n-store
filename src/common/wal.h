#ifndef WAL_H_
#define WAL_H_

#include <vector>
#include <map>
#include <unordered_map>
#include <thread>
#include <memory>
#include <mutex>
#include <condition_variable>

#include "engine.h"
#include "logger.h"
#include "nstore.h"
#include "txn.h"
#include "record.h"
#include "utils.h"

using namespace std;

class wal_engine : public engine {
	public:
		wal_engine(config _conf) :
			conf(_conf),
			ready(false){}

		void loader() ;
		void runner(int pid) ;

		char* read(txn t);
		int update(txn t);

		int test();

		// Custom functions
		void group_commit();
		int insert(txn t);

		void check();
		void cleanup();

		void snapshot();
		void recovery();

	private:
		config conf;

		std::mutex gc_mutex;
		std::condition_variable cv;
		bool ready = false;

		pthread_rwlock_t  table_access = PTHREAD_RWLOCK_INITIALIZER;
		unordered_map<unsigned int, record*> table_index;

		logger undo_log;
};


#endif /* WAL_H_ */
