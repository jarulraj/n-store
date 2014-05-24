// SHADOW PAGING

#include "sp.h"

using namespace std;

#define TABLE_LOC  0x02c00000

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

	dir_map& dir = mstr.get_dir();

	if (dir.count(key) == 0) {
		std::cout << "Update: key does not exist : " << key << endl;
		return -1;
	}

	record* rec = new record(key, t.value);
	after_image = table.push_back_record(*rec);

	sp_record* sp_rec = dir[key];

	//wrlock(&mstr.table_access);

	// older batch
	int index = (sp_rec->batch_id[0] < sp_rec->batch_id[1]) ? 0 : 1;

	sp_rec->location[index] = after_image;
	sp_rec->batch_id[index] = mstr.batch_id;

	//unlock(&mstr.table_access);

	// Add log entry
	mem_entry e(t, before_image, after_image);
	undo_buffer.push(e);

	return 0;
}

char* sp_engine::read(txn t) {
	int key = t.key;

	dir_map& dir = mstr.get_dir();

	if (dir.count(key) == 0) {
		std::cout << "Read: key does not exist : " << key << endl;
		return NULL;
	}

	sp_record* sp_rec =  dir[key];
	int index = (sp_rec->batch_id[0] >= sp_rec->batch_id[1]) ? 0 : 1;
	char* location = sp_rec->location[index];

	//char* val = strtok(location, "\n");

	return location;
}

// RUNNER + LOADER

void sp_engine::runner(int pid) {
    long range_size   = conf.num_keys/conf.num_parts;
    long range_offset = pid*range_size;
    long range_txns   = conf.num_txns/conf.num_parts;

    char* updated_val = new char[conf.sz_value];
    memset(updated_val, 'x', conf.sz_value);
    char* val;

    for (int i = 0; i < range_txns; i++) {
		long r = rand();
		long key = range_offset + r % range_size;

		if (r % 100 < conf.per_writes) {
			txn t(i, "Update", key, updated_val);
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
		char* val;

		txn t(i, "Read", i, val);
		val = read(t);

		std::cout << i << " " << val << endl;
	}

}

void sp_engine::loader() {
	size_t ret;
	char* after_image;

	int batch_id = mstr.batch_id;

	for (int i = 0; i < conf.num_keys; i++) {
		int key = i;

		char* value = new char[conf.sz_value];
        random_string(value, conf.sz_value);

		record* rec = new record(key, value);
		after_image = table.push_back_record(*rec);

		sp_record* sp_rec = new sp_record(key, batch_id, after_image);
		mstr.get_dir()[key] = sp_rec;

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
	cout << "Rebuild clean dir" << endl;

}

int sp_engine::test() {

	table = mmap_fd(conf.fs_path + "usertable", (caddr_t) TABLE_LOC, conf);
	mstr = master("usertable", conf);

	std::chrono::time_point<std::chrono::high_resolution_clock> start, finish;
	std::chrono::duration<double> elapsed_seconds;

	// Loader
	loader();
	std::cout << "Loading finished " << endl;
	//check();

	start = std::chrono::high_resolution_clock::now();

	// Logger start
	std::thread gc(&sp_engine::group_commit, this);
	{
		std::lock_guard<std::mutex> lk(gc_mutex);
		ready = true;
	}
	cv.notify_one();

	// Runner
	std::vector<std::thread> th_group;
	for (int i = 0; i < conf.num_parts; i++)
		th_group.push_back(std::thread(&sp_engine::runner, this, i));

	for (int i = 0; i < conf.num_parts; i++)
		th_group.at(i).join();

	// Logger end
	ready = false;
	gc.join();

	finish = std::chrono::high_resolution_clock::now();
	std::cout << "Execution duration (ms): " << chrono::duration_cast<chrono::milliseconds>(finish-start).count() << endl;

	//check();

	// Recovery
	/*
	start = std::chrono::high_resolution_clock::now();

	recovery();

	finish = std::chrono::high_resolution_clock::now();
	elapsed_seconds = finish - start;
	std::cout << "Recovery duration: " << elapsed_seconds.count() << endl;

	check();
	*/

	return 0;
}

