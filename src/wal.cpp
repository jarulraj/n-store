// WRITE-AHEAD LOGGING

#include <cstring>
#include <cstdlib>
#include <fstream>

#include "wal.h"

using namespace std;

#define VALUE_SIZE 2

void wal_engine::group_commit(){
    std::unique_lock<std::mutex> lk(gc_mutex);
    cv.wait(lk, [&]{return ready;});

    while(ready){
        std::cout << "Syncing log !\n"<<endl;

        // sync
        undo_log.write();

        std::this_thread::sleep_for(std::chrono::milliseconds(conf.gc_interval));
    }
}

int wal_engine::update(txn t){
    int key = t.key;
    int rc = -1;

    rc = pthread_rwlock_wrlock(&table_access);
    if(rc != 0){
    	cout<<"update:: wrlock failed \n";
    	return -1;
    }

    if(table_index.count(t.key) == 0){
        rc = pthread_rwlock_unlock(&table_access);
        if(rc != 0){
        	cout<<"update:: unlock failed \n";
        	return -1;
        }
        return -1;
    }

    record* before_image;
    record* after_image = new record(t.key, t.value);

	before_image = table_index.at(t.key);
	table.push_back(*after_image);
	table_index[key] = after_image;

    rc = pthread_rwlock_unlock(&table_access);
    if(rc != 0){
    	cout<<"update:: unlock failed \n";
    	return -1;
    }

    // Add log entry
    entry e(t, before_image, after_image);
    undo_log.push(e);

    return 0;
}

std::string wal_engine::read(txn t){
    int key = t.key;
    int rc = -1;
    std::string val = "" ;

    rc = pthread_rwlock_rdlock(&table_access);
    if(rc != 0){
    	cout<<"read:: rdlock failed \n";
    	return "";
    }

	if (table_index.count(t.key) == 0){
		rc = pthread_rwlock_unlock(&table_access);
		if (rc != 0) {
			cout << "read:: rdlock failed \n";
			return "";
		}
		return "";
	}

	record* r = table_index[key];
	if (r != NULL) {
		val = r->value;
	}

	rc = pthread_rwlock_unlock(&table_access);
	if (rc != 0) {
		cout << "read:: rdlock failed \n";
		return "";
	}

    return val;
}

int wal_engine::insert(txn t){
    int key = t.key;
    int rc = -1;

    rc = pthread_rwlock_wrlock(&table_access);
    if(rc != 0){
    	cout<<"update:: wrlock failed \n";
    	return -1;
    }

    // check if key already exists
    if(table_index.count(t.key) != 0){
        rc = pthread_rwlock_unlock(&table_access);
        if(rc != 0){
        	cout<<"update:: unlock failed \n";
        	return -1;
        }
        return -1;
    }

    record* after_image = new record(t.key, t.value);

	table.push_back(*after_image);
	table_index[key] = after_image;

    rc = pthread_rwlock_unlock(&table_access);
    if(rc != 0){
    	cout<<"update:: unlock failed \n";
    	return -1;
    }

    // Add log entry
    entry e(t, NULL, after_image);
    undo_log.push(e);

    return 0;
}

// RUNNER + LOADER

void wal_engine::runner(){
    std::string val;

    for(int i=0 ; i<conf.num_txns ; i++){
        long r = rand();
        std::string val = random_string(VALUE_SIZE);
        long key = r%conf.num_keys;

        if(r % 100 < conf.per_writes){
            txn t(i, "Update", key, val);
            update(t);
        }
        else{
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

void wal_engine::loader(){
    size_t ret;
    stringstream buffer_stream;
    string buffer;
    int rc = -1;

    for(int i=0 ; i<conf.num_keys ; i++){
        int key = i;
        string value = random_string(VALUE_SIZE);
        record* after_image = new record(key, value);

        {
    	    rc = pthread_rwlock_wrlock(&table_access);
    	    if(rc != 0){
    	    	cout<<"load:: wrlock failed \n";
    	    	return;
    	    }


            table.push_back(*after_image);
            table_index[key] = after_image;

    	    rc = pthread_rwlock_unlock(&table_access);
    	    if(rc != 0){
    	    	cout<<"load:: unlock failed \n";
    	    	return;
    	    }
        }

        // Add log entry
        txn t(0, "Insert", key, value);

        entry e(t, NULL, after_image);
        undo_log.push(e);
    }

    // sync
    undo_log.write();
}

void wal_engine::snapshot(){
	int rc = -1;

	std::chrono::time_point<std::chrono::system_clock> start, finish;
	std::chrono::duration<double> elapsed_seconds;

    start = std::chrono::system_clock::now();

    rc = pthread_rwlock_wrlock(&table_access);
    if(rc != 0){
    	cout<<"update:: wrlock failed \n";
    	return;
    }

	logger snapshotter ;
	snapshotter.set_path(conf.fs_path+"./snapshot", "w");

	unordered_map<unsigned int, record*>::const_iterator t_itr;

    for (t_itr = table_index.begin() ; t_itr != table_index.end(); ++t_itr){
        txn t(0, "", -1, "");
        record* tuple = (*t_itr).second;

        entry e(t, tuple, NULL);
    	snapshotter.push(e);
    }

    // sync
    snapshotter.write();
    snapshotter.close();

    // Add log entry
    txn t(0, "Snapshot", -1, "");
    entry e(t, NULL, NULL);
    undo_log.push(e);

    // sync
    undo_log.write();

    rc = pthread_rwlock_unlock(&table_access);
    if(rc != 0){
    	cout<<"update:: unlock failed \n";
    	return;
    }

    finish = std::chrono::system_clock::now();
    elapsed_seconds = finish - start;
    std::cout<<"Snapshot  duration: "<< elapsed_seconds.count()<<endl;
}

void wal_engine::recovery(){

	// Clear stuff
	undo_log.close();
	table.clear();
	table_index.clear();

	cout<<"Read snapshot"<<endl;

	// Read snapshot
	logger reader;
	reader.set_path(conf.fs_path+"./snapshot", "r");

	unsigned int key = -1;
	char value[VALUE_SIZE];
	char txn_type[10];

	while(fscanf(reader.log_file, "%d %s \n", &key, value) != EOF){
        txn t(0, "Insert", key, std::string(value));
        insert(t);
	}

	reader.close();

	cout<<"Apply log"<<endl;

	ifstream scanner(conf.fs_path+"./log");
	string line;
	bool apply = false;
	char before_val[VALUE_SIZE], after_val[VALUE_SIZE];
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
					sscanf(line.c_str(), "%s %d %s %d %s", txn_type, &before_key, before_val, &after_key, after_val);
			        txn t(0, "Update", after_key, std::string(after_val));
			        update(t);
				}

			}
		}

		scanner.close();
	}

}

int wal_engine::test(){

	undo_log.set_path(conf.fs_path+"./log", "w");

	std::chrono::time_point<std::chrono::system_clock> start, finish;
	std::chrono::duration<double> elapsed_seconds;

	unsigned long int num_threads = conf.num_thds;

	long num_keys = conf.num_keys ;
	long num_txn  = conf.num_txns ;
	long num_wr   = conf.per_writes ;

	// Loader
    loader();
    std::cout<<"Loading finished "<<endl;
    //check();

    // Take snapshot
    snapshot();

    start = std::chrono::system_clock::now();
    
    // Logger start
    std::thread gc(&wal_engine::group_commit, this);
    {
        std::lock_guard<std::mutex> lk(gc_mutex);
        ready = true;
    }
    cv.notify_one();

    // Runner
    std::vector<std::thread> th_group;
    for(int i=0 ; i<num_threads ; i++)
        th_group.push_back(std::thread(&wal_engine::runner, this));

    for(int i=0 ; i<num_threads ; i++)
    	th_group.at(i).join();

    // Logger end
    ready = false;
    gc.join();

    undo_log.write();

    finish = std::chrono::system_clock::now();
    elapsed_seconds = finish - start;
    std::cout<<"Execution duration: "<< elapsed_seconds.count()<<endl;

    //check();

    // Recover
    start = std::chrono::system_clock::now();

    recovery();

    finish = std::chrono::system_clock::now();
    elapsed_seconds = finish - start;
    std::cout<<"Recovery duration: "<< elapsed_seconds.count()<<endl;

    //check();

    return 0;
}
