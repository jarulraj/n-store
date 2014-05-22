// SHADOW PAGING

#include "sp.h"

using namespace std;

#define TABLE_LOC  0x01b00000

void sp_engine::group_commit() {
	std::unique_lock<std::mutex> lk(gc_mutex);
	cv.wait(lk, [&] {return ready;});

	while (ready) {
		table.sync();
		mstr.sync();

		std::this_thread::sleep_for(std::chrono::milliseconds(conf.gc_interval));
	}

}

int sp_engine::update(txn t) {
	int key = t.key;

	char* before_image;
	char* after_image;
	int rc = -1;
	unsigned int chunk_id = 0;

	chunk_id = key / conf.sz_partition;

	outer_map& outer = mstr.get_clean_dir();

	// chunk does not exist
	if (outer.count(chunk_id) == 0) {
		std::cout << "Update: chunk does not exist : " << key << endl;
		return -1;
	}

	// key does not exist
	inner_map* clean_chunk = outer[chunk_id];
	if (clean_chunk->count(key) == 0) {
		std::cout << "Update: key does not exist : " << key << endl;
		return -1;
	}

	sp_record* rec = new sp_record(key, t.value, NULL);
	after_image = table.push_back_record(*rec);
	rec->location = after_image;
	before_image = (*clean_chunk)[key];

	rc = pthread_rwlock_wrlock(&mstr.table_access);
	if (rc != 0) {
		cout << "update:: wrlock failed \n";
		return -1;
	}

	chunk_status& status = mstr.get_status();

	// New chunk
	if (status[chunk_id] == false) {
		inner_map* chunk = new inner_map;

		// Copy map from clean dir
		(*chunk).insert((*clean_chunk).begin(), (*clean_chunk).end());
		(*chunk)[key] = after_image;

		mstr.get_dir()[chunk_id] = chunk;
		status[chunk_id] = true;
	}
	// Add to existing chunk
	else {
		inner_map* chunk = mstr.get_dir()[chunk_id];

		before_image = (*chunk)[key];
		(*chunk)[key] = after_image;
	}

	rc = pthread_rwlock_unlock(&mstr.table_access);
	if (rc != 0) {
		cout << "update:: unlock failed \n";
		return -1;
	}

	// Add log entry
	mem_entry e(t, before_image, after_image);
	undo_buffer.push(e);

	return 0;
}

std::string sp_engine::read(txn t) {
	int key = t.key;
	unsigned int chunk_id = 0;

	chunk_id = key / conf.sz_partition;

	outer_map& outer = mstr.get_clean_dir();
	if (outer.count(chunk_id) == 0) {
		std::cout << "Read: chunk does not exist : " << key << endl;
		return "not exists";
	}

	inner_map* clean_chunk = outer[chunk_id];
	if (clean_chunk->count(key) == 0) {
		std::cout << "Read: key does not exist : " << key << endl;
		return "not exists";
	}

	char* location = (*clean_chunk)[key];
	return table.get_value(location);
}

// RUNNER + LOADER

void sp_engine::runner() {
	std::string val;

	for (int i = 0; i < conf.num_txns; i++) {
		long r = rand();
		std::string val = random_string(conf.sz_value);
		long key = r % conf.num_keys;

		if (r % 100 < conf.per_writes) {
			txn t(i, "Update", key, val);
			update(t);
		} else {
			txn t(i, "Read", key, val);
			val = read(t);
		}
	}

}

void sp_engine::check() {

	std::cout << "Check :" << std::endl;
	for (int i = 0; i < conf.num_keys; i++) {
		std::string val;

		txn t(i, "Read", i, val);
		val = read(t);

		std::cout << i << " " << val << endl;
	}

}

void sp_engine::loader() {
	size_t ret;
	char* after_image;

	for (int i = 0; i < conf.num_keys; i++) {
		int key = i;
		unsigned int chunk_id = 0;
		string value = random_string(conf.sz_value);

		sp_record* rec = new sp_record(key, value, NULL);

		after_image = table.push_back_record(*rec);
		rec->location = after_image;

		chunk_id = key / conf.sz_partition;

		// New chunk
		if (mstr.get_dir().count(chunk_id) == 0
				|| mstr.get_status()[chunk_id] == false) {
			inner_map* chunk = new inner_map;
			(*chunk)[key] = after_image;

			//cout<<"New chunk :"<<chunk_id<<endl;
			mstr.get_dir()[chunk_id] = chunk;

			mstr.get_status()[chunk_id] = true;
		}
		// Add to existing chunk
		else {
			inner_map* chunk = mstr.get_dir()[chunk_id];
			//cout<<"Add to chunk :"<<chunk_id<<endl;

			if (chunk != NULL)
				(*chunk)[key] = after_image;
		}

		// Add log entry
		txn t(0, "Insert", key, value);

		mem_entry e(t, NULL, after_image);
		undo_buffer.push(e);
	}

	table.sync();
	mstr.sync();
}

void sp_engine::recovery() {

	// Clear in-memory structures
	mstr.get_chunk_map().clear();
	mstr.get_clean_chunk_map().clear();
	mstr.get_status().clear();

	mstr.get_dir().clear();
	mstr.get_clean_dir().clear();

	cout << "Rebuild clean dir" << endl;

}

int sp_engine::test() {

	assert(conf.sz_partition <= conf.num_keys);

	table = mmap_fd(conf.fs_path + "usertable", (caddr_t) TABLE_LOC, conf);
	mstr = master("usertable", conf);

	std::chrono::time_point<std::chrono::system_clock> start, finish;
	std::chrono::duration<double> elapsed_seconds;

	// Loader
	loader();
	std::cout << "Loading finished " << endl;
	//check();

	start = std::chrono::system_clock::now();

	// Logger start
	std::thread gc(&sp_engine::group_commit, this);
	{
		std::lock_guard<std::mutex> lk(gc_mutex);
		ready = true;
	}
	cv.notify_one();

	// Runner
	std::vector<std::thread> th_group;
	for (int i = 0; i < conf.num_thds; i++)
		th_group.push_back(std::thread(&sp_engine::runner, this));

	for (int i = 0; i < conf.num_thds; i++)
		th_group.at(i).join();

	// Logger end
	ready = false;
	gc.join();

	finish = std::chrono::system_clock::now();
	elapsed_seconds = finish - start;
	std::cout << "Execution duration: " << elapsed_seconds.count() << endl;

	//check();

	// Recover
	/*
	 start = std::chrono::system_clock::now();

	 recovery();

	 finish = std::chrono::system_clock::now();
	 elapsed_seconds = finish - start;
	 std::cout<<"Recovery duration: "<< elapsed_seconds.count()<<endl;
	 */

	//check();
	return 0;
}

