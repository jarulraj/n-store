#ifndef LSM_H_
#define LSM_H_

#include <cstring>
#include <cstdlib>
#include <fstream>
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
#include "mmap_fd.h"

using namespace std;

typedef unordered_map<unsigned int, record*> mem_map;
typedef unordered_map<unsigned int, char*> nvm_map;

class lsm_engine : public engine {
	public:
		lsm_engine(config _conf) :
			conf(_conf){}

		void loader() ;
		void runner(int pid) ;

		char* read(txn t);
		int update(txn t);

		int test();

		// Custom functions
		void group_commit();
		int insert(txn t);
		int remove(txn t);

		void check();
		void merge();

		mem_map& get_mem_index();

		pthread_rwlock_t  table_access = PTHREAD_RWLOCK_INITIALIZER;

	private:
		config conf;

		std::mutex gc_mutex;
		std::condition_variable gc_cv;
		bool gc_ready = false;

		mem_map mem_index;
		nvm_map nvm_index;

		mmap_fd table;

		logger undo_log;

};



#endif /* LSM_H_ */
