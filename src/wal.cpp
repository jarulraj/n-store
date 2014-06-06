// WRITE-AHEAD LOGGING

#include <cstring>
#include <cstdlib>
#include <fstream>

#include "wal.h"

using namespace std;

void wal_engine::group_commit(){
    std::unique_lock<std::mutex> lk(gc_mutex);
    cv.wait(lk, [&]{return ready;});

    while(ready){
    	if(conf.verbose)
    		std::cout << "Syncing log !"<<endl;

        // sync
        undo_log.write();

        std::this_thread::sleep_for(std::chrono::milliseconds(conf.gc_interval));
    }
}

int wal_engine::update(txn t){
    int key = t.key;
    int rc = -1;

    //wrlock(&table_access);

    if(table_index.count(t.key) == 0){
        //unlock(&table_access);
        return -1;
    }

    record* before_image;
    record* after_image = new record(t.key, t.value);

	before_image = table_index.at(t.key);
	table_index[key] = after_image;

    //unlock(&table_access);

    // Add log entry
    entry e(t, before_image, after_image);
    undo_log.push(e);

    return 0;
}

char* wal_engine::read(txn t){
    int key = t.key;
    char* val = NULL ;

    //rdlock(&table_access);

	if (table_index.count(t.key) == 0){
        //unlock(&table_access);
		return NULL;
	}

	record* r = table_index[key];
	if (r != NULL) {
		val = r->value;
	}

    //unlock(&table_access);

    return val;
}

int wal_engine::insert(txn t){
    int key = t.key;
    int rc = -1;

    //wrlock(&table_access);

    // check if key already exists
    if(table_index.count(t.key) != 0){
        //unlock(&table_access);
        return -1;
    }

    record* after_image = new record(t.key, t.value);

	table_index[key] = after_image;

    //unlock(&table_access);

    // Add log entry
    entry e(t, NULL, after_image);
    undo_log.push(e);

    return 0;
}

// RUNNER + LOADER

void wal_engine::runner(int pid) {
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
		    txn t(i, "Update", key, updated_val);
			update(t);
		} else {
			txn t(i, "Read", key, val);
			val = read(t);
		}
	}

}

void wal_engine::check(){
	unordered_map<unsigned int, record*>::const_iterator t_itr;

    for (t_itr = table_index.begin() ; t_itr != table_index.end(); ++t_itr){
        cout << (*t_itr).first <<" "<< (*t_itr).second->value << endl;
    }

}

void wal_engine::cleanup(){
	unordered_map<unsigned int, record*>::const_iterator t_itr;

    for (t_itr = table_index.begin() ; t_itr != table_index.end(); ++t_itr){
    	delete (*t_itr).second;
    }

}

void wal_engine::loader(){
    size_t ret;
    stringstream buffer_stream;
    string buffer;
    int rc = -1;

    for(int i=0 ; i<conf.num_keys ; i++){
        int key = i;

        char* value = new char[conf.sz_value];
        random_string(value, conf.sz_value);
        record* after_image = new record(key, value);

        {
            //wrlock(&table_access);

            table_index[key] = after_image;

            //unlock(&table_access);
        }

        // Add log entry
        txn t(0, "Insert", key, value);

        entry e(t, NULL, after_image);
        undo_log.push(e);
    }

    // sync
    undo_log.write();
}

int wal_engine::test(){

	undo_log.set_path(conf.fs_path+"./log", "w");

	timespec start, finish;

	// Loader
    loader();
    //std::cout<<"Loading finished "<<endl;
    //check();

    // Take snapshot
    //snapshot();

    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &start);

    // Logger start
    std::thread gc(&wal_engine::group_commit, this);
    {
        std::lock_guard<std::mutex> lk(gc_mutex);
        ready = true;
    }
    cv.notify_one();

    // Runner
    std::vector<std::thread> th_group;
    for(int i=0 ; i<conf.num_parts ; i++)
        th_group.push_back(std::thread(&wal_engine::runner, this, i));

    for(int i=0 ; i<conf.num_parts ; i++)
    	th_group.at(i).join();

    // Logger end
    ready = false;
    gc.join();

    undo_log.write();

    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &finish);
	display_stats(start, finish, conf.num_txns);

	// Recovery
	//check();

	/*
	start = std::chrono::high_resolution_clock::now();

	recovery();

	finish = std::chrono::high_resolution_clock::now();
	elapsed_seconds = finish - start;
	std::cout << "Recovery duration: " << elapsed_seconds.count() << endl;

	check();
	*/

	cleanup();

    return 0;
}

void wal_engine::snapshot(){
	int rc = -1;

	std::chrono::time_point<std::chrono::high_resolution_clock> start, finish;
	std::chrono::duration<double> elapsed_seconds;

    start = std::chrono::high_resolution_clock::now();

    rc = pthread_rwlock_wrlock(&table_access);
    if(rc != 0){
    	cout<<"update:: wrlock failed \n";
    	return;
    }

	logger snapshotter ;
	snapshotter.set_path(conf.fs_path+"./snapshot", "w");

	unordered_map<unsigned int, record*>::const_iterator t_itr;

    for (t_itr = table_index.begin() ; t_itr != table_index.end(); ++t_itr){
        txn t(0, "", -1, NULL);
        record* tuple = (*t_itr).second;

        entry e(t, tuple, NULL);
    	snapshotter.push(e);
    }

    // sync
    snapshotter.write();
    snapshotter.close();

    // Add log entry
    txn t(0, "Snapshot", -1, NULL);
    entry e(t, NULL, NULL);
    undo_log.push(e);

    // sync
    undo_log.write();

    rc = pthread_rwlock_unlock(&table_access);
    if(rc != 0){
    	cout<<"update:: unlock failed \n";
    	return;
    }

    finish = std::chrono::high_resolution_clock::now();
    elapsed_seconds = finish - start;
    std::cout<<"Snapshot  duration: "<< elapsed_seconds.count()<<endl;
}

void wal_engine::recovery(){

	// Clear stuff
	undo_log.close();
	table_index.clear();

	cout<<"Read snapshot"<<endl;

	// Read snapshot
	logger reader;
	reader.set_path(conf.fs_path+"./snapshot", "r");

	unsigned int key = -1;
	char txn_type[10];

	while(1){
		char* value =  new char[conf.sz_value];
		if(fscanf(reader.log_file, "%u %s ", &key, value) != EOF){
			txn t(0, "Insert", key, value);
			insert(t);
		}
		else{
			break;
		}
	}

	reader.close();

	cout<<"Apply log"<<endl;

	ifstream scanner(conf.fs_path+"./log");
	string line;
	bool apply = false;
	char before_val[conf.sz_value];
	unsigned int before_key, after_key;

	if (scanner.is_open()) {

		while (getline(scanner, line)) {
			if(apply == false){
				sscanf(line.c_str(), "%s", txn_type);
				if(strcmp(txn_type, "Snapshot") ==0)
					apply = true;
			}
			// Apply log
			else{
				sscanf(line.c_str(), "%s", txn_type);

				// Update
				if(strcmp(txn_type, "Update") ==0){
					char* after_val=  new char[conf.sz_value];
					sscanf(line.c_str(), "%s %u %s %u %s ", txn_type, &before_key, before_val, &after_key, after_val);

			        txn t(0, "Update", after_key, after_val);
			        update(t);
				}

			}
		}

		scanner.close();
	}

}
