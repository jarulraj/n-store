// LOG STRUCTURED MERGE TREE

#include "lsm.h"

using namespace std;

#define TABLE_LOC  0x01a00000

void lsm_engine::group_commit(){
    std::unique_lock<std::mutex> gc_lk(gc_mutex);
    gc_cv.wait(gc_lk, [&]{return gc_ready;});

    while(gc_ready){
    	//std::cout << "Syncing log !"<<endl;

        // sync
        undo_log.write();

        std::this_thread::sleep_for(std::chrono::milliseconds(conf.gc_interval));
    }
}

void lsm_engine::merge(){
	//std::cout << "Merging !" << endl;

	mem_map::iterator itr;
	char* after_image;
	unsigned int key;

	for (itr = mem_index.begin(); itr != mem_index.end(); ++itr) {
		key = (*itr).first;
		record* rec = (*itr).second;

		if (rec != NULL) {
			after_image = table.push_back_record(*rec);
			nvm_index[key] = after_image;
		}

		delete rec;
	}

	mem_index.clear();
}


int lsm_engine::update(txn t){
	int ret = -1;

	remove(t);
    insert(t);

    return ret;
}


int lsm_engine::remove(txn t){
    int key = t.key;
    record* before_image ;

    t.txn_type = "Delete";

    // check active mem_index
    if(mem_index.count(key) != 0){
    	before_image = mem_index[key];
    	mem_index.erase(key);

        entry e(t, before_image, NULL);
        undo_log.push(e);
    }

    // check nvm_index
    if(nvm_index.count(key) != 0){
    	nvm_index.erase(key);
    }

    return 0;
}

char* lsm_engine::read(txn t){
    int key = t.key;
    record* rec ;
    char* val = NULL ;

	if (mem_index.count(key) != 0) {
		rec = mem_index[key];
		return rec->value;
	}

	if (nvm_index.count(key) != 0) {
		val = nvm_index[key];
		return val;
	}

    return val;
}

int lsm_engine::insert(txn t){
    int key = t.key;

    t.txn_type = "Insert";

    // check if key already exists
    if(mem_index.count(key) != 0){
    	delete t.value;
        return -1;
    }

    record* after_image = new record(t.key, t.value);

    mem_index[key] = after_image;

    // Add log entry
    entry e(t, NULL, after_image);
    undo_log.push(e);

    // Merge
    if(mem_index.size() > conf.lsm_size)
    	merge();

    return 0;
}

// RUNNER + LOADER

void lsm_engine::runner(int pid) {
    long range_size   = conf.num_keys/conf.num_parts;
    long range_offset = pid*range_size;
    long range_txns   = conf.num_txns/conf.num_parts;

    char* val;

    for (int i = 0; i < range_txns; i++) {
		long z = conf.zipf_dist[i];
		double u = conf.uniform_dist[i];
		long key = range_offset + z % range_size;

		if (u < conf.per_writes) {
		    char* updated_val = new char[conf.sz_value];
		    memset(updated_val, 'x', conf.sz_value);
		    updated_val[conf.sz_value-1] = '\0';

			txn t(i, "Update", key, updated_val);
			update(t);
		} else {
			txn t(i, "Read", key, val);
			val = read(t);
		}
	}

}

void lsm_engine::cleanup(){
	mem_map::const_iterator t_itr;

    for (t_itr = mem_index.begin() ; t_itr != mem_index.end(); ++t_itr){
    	delete (*t_itr).second;
    }

}

void lsm_engine::check(){

	std::cout << "Check :" << std::endl;
	for (int i = 0; i < conf.num_keys; i++) {
		char* val;

		txn t(i, "Read", i, NULL);
		val = read(t);

		std::cout << i << " " << val << endl;
	}

}

void lsm_engine::loader(){
    size_t ret;
    stringstream buffer_stream;
    string buffer;
    int rc = -1;

    for(int i=0 ; i<conf.num_keys ; i++){
        int key = i;

        char* value = new char[conf.sz_value];
        random_string(value, conf.sz_value);
        record* after_image = new record(key, value);

        mem_index[key] = after_image;

        // Add log entry
        txn t(0, "Insert", key, value);
        entry e(t, NULL, after_image);
        undo_log.push(e);
    }

    // sync and merge
    undo_log.write();
    merge();
}

int lsm_engine::test(){

	undo_log.set_path(conf.fs_path+"./log", "w");

	table = mmap_fd(conf.fs_path + "usertable", (caddr_t) TABLE_LOC, conf);

	timespec start, finish;

	// Loader
    loader();
    //std::cout<<"Loading finished "<<endl;

    //check();

    clock_gettime(CLOCK_REALTIME, &start);

    // Logger and merger start
    std::thread gc(&lsm_engine::group_commit, this);
    {
        std::lock_guard<std::mutex> gc_lk(gc_mutex);
        gc_ready = true;
    }
    gc_cv.notify_one();

    // Runner
    std::vector<std::thread> th_group;
    for(int i=0 ; i<conf.num_parts ; i++)
        th_group.push_back(std::thread(&lsm_engine::runner, this, i));

    for(int i=0 ; i<conf.num_parts ; i++)
    	th_group.at(i).join();

    // Logger end
    gc_ready = false;
    gc.join();

    undo_log.write();

    clock_gettime(CLOCK_REALTIME, &finish);
    display_stats(start, finish, conf.num_txns);

    cleanup();

	//check();

    return 0;
}
