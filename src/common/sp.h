#ifndef SP_H_
#define SP_H_

#include <memory>
#include <chrono>
#include <random>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <cassert>


#include "engine.h"
#include "nstore.h"
#include "utils.h"
#include "txn.h"
#include "mem_logger.h"
#include "mmap_fd.h"
#include "master.h"

using namespace std;

class sp_engine : public engine {
	public:
		sp_engine(config _conf) :
			conf(_conf),
			ready(false),
			log_enable(0){}

		void loader() ;
		void runner(int pid) ;

		char* read(txn t);
		int update(txn t);

		int test();

		// Custom functions
		void group_commit();

		void check();
		void cleanup();

		void recovery();

	private:
		config conf;

		std::mutex gc_mutex;
		std::condition_variable cv;
		bool ready = false;

		mmap_fd table;
		master mstr;
		mem_logger undo_buffer;

		int log_enable ;

};


#endif /* SP_H_ */
