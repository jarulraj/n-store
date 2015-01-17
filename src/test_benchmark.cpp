// TEST BENCHMARK

#include "test_benchmark.h"

namespace storage {

class testtable_record : public record {
public:
	testtable_record(schema* _sptr, int key, const std::string& val,
			int num_val_fields, bool update_one)
: record(_sptr) {

		set_int(0, key);

		if (val.empty())
			return;

		if (!update_one) {
			for (int itr = 1; itr <= num_val_fields; itr++) {
				set_varchar(itr, val);
			}
		} else
			set_varchar(1, val);
	}

};

// TESTTABLE
table* create_testtable(config& conf) {

	std::vector<field_info> cols;
	off_t offset;

	offset = 0;
	field_info key(offset, 10, 10, field_type::INTEGER, 1, 1);
	offset += key.ser_len;
	cols.push_back(key);

	for (int itr = 1; itr <= conf.ycsb_num_val_fields; itr++) {
		field_info val = field_info(offset, 12, conf.ycsb_field_size,
				field_type::VARCHAR, 0, 1);
		offset += val.ser_len;
		cols.push_back(val);
	}

	// SCHEMA
	schema* test_table_schema = new schema(cols);
	pmemalloc_activate(test_table_schema);

	table* test_table = new table("test", test_table_schema, 1, conf, sp);
	pmemalloc_activate(test_table);

	// PRIMARY INDEX
	for (int itr = 1; itr <= conf.ycsb_num_val_fields; itr++) {
		cols[itr].enabled = 0;
	}

	schema* test_table_index_schema = new schema(cols);
	pmemalloc_activate(test_table_index_schema);

	table_index* key_index = new table_index(test_table_index_schema,
			conf.ycsb_num_val_fields + 1, conf,
			sp);
	pmemalloc_activate(key_index);
	test_table->indices->push_back(key_index);

	return test_table;
}

test_benchmark::test_benchmark(config _conf, unsigned int tid, database* _db,
		timer* _tm, struct static_info* _sp)
: benchmark(tid, _db, _tm, _sp),
  conf(_conf),
  txn_id(0) {

	btype = benchmark_type::TEST;

	// Partition workload
	num_keys = conf.num_keys / conf.num_executors;
	num_txns = conf.num_txns / conf.num_executors;

	// Initialization mode
	if (sp->init == 0) {
		//cout << "Initialization Mode" << endl;
		sp->ptrs[0] = _db;

		table* testtable = create_testtable(conf);
		db->tables->push_back(testtable);

		sp->init = 1;
	} else {
		//cout << "Recovery Mode " << endl;
		database* db = (database*) sp->ptrs[0];
		db->reset(conf, tid);
	}

	test_table_schema = db->tables->at(TEST_TABLE_ID)->sptr;

	if (conf.recovery) {
		num_txns = conf.num_txns;
		num_keys = 1000;
		conf.ycsb_per_writes = 0.5;
		conf.ycsb_tuples_per_txn = 20;
	}

	if (conf.ycsb_update_one == false) {
		for (int itr = 1; itr <= conf.ycsb_num_val_fields; itr++)
			update_field_ids.push_back(itr);
	} else {
		// Update only first field
		update_field_ids.push_back(1);
	}

	// Generate skewed dist
	simple_skew(zipf_dist, conf.ycsb_skew, num_keys,
			num_txns * conf.ycsb_tuples_per_txn);
	uniform(uniform_dist, num_txns);

}

void test_benchmark::load() {
	engine* ee = new engine(conf, tid, db, false);

	schema* testtable_schema = db->tables->at(TEST_TABLE_ID)->sptr;
	unsigned int txn_itr;

	ee->txn_begin();

	for (txn_itr = 0; txn_itr < num_keys; txn_itr++) {

		if (txn_itr % conf.load_batch_size == 0) {
			ee->txn_end(true);
			txn_id++;
			ee->txn_begin();
		}

		// LOAD
		int key = txn_itr;
		std::string value = get_rand_astring(conf.ycsb_field_size);

		record* rec_ptr = new testtable_record(testtable_schema, key, value,
				conf.ycsb_num_val_fields, false);

		statement st(txn_id, operation_type::Insert, TEST_TABLE_ID, rec_ptr);

		ee->load(st);
	}

	ee->txn_end(true);

	delete ee;
}

void test_benchmark::do_update(engine* ee) {

	// UPDATE
	std::string updated_val(conf.ycsb_field_size, 'x');
	int zipf_dist_offset = txn_id * conf.ycsb_tuples_per_txn;
	txn_id++;
	int rc;

	TIMER(ee->txn_begin())

	for (int stmt_itr = 0; stmt_itr < conf.ycsb_tuples_per_txn; stmt_itr++) {

		int key = zipf_dist[zipf_dist_offset + stmt_itr];

		record* rec_ptr = new testtable_record(test_table_schema, key, updated_val,
				conf.ycsb_num_val_fields,
				conf.ycsb_update_one);

		statement st(txn_id, operation_type::Update, TEST_TABLE_ID, rec_ptr,
				update_field_ids);

		TIMER(rc = ee->update(st))
		if (rc != 0) {
			TIMER(ee->txn_end(false))
    		  return;
		}
	}

	TIMER(ee->txn_end(true))
}

void test_benchmark::do_read(engine* ee) {

	// SELECT
	int zipf_dist_offset = txn_id * conf.ycsb_tuples_per_txn;
	txn_id++;
	std::string empty;
	std::string rc;

	TIMER(ee->txn_begin())

	for (int stmt_itr = 0; stmt_itr < conf.ycsb_tuples_per_txn; stmt_itr++) {

		int key = zipf_dist[zipf_dist_offset + stmt_itr];

		record* rec_ptr = new testtable_record(test_table_schema, key, empty,
				conf.ycsb_num_val_fields, false);

		statement st(txn_id, operation_type::Select, TEST_TABLE_ID, rec_ptr, 0,
				test_table_schema);

		TIMER(ee->select(st))
	}

	TIMER(ee->txn_end(true))
}

void test_benchmark::do_insert(engine* ee) {

	// INSERT
	txn_id++;
	int rc;
	schema* testtable_schema = db->tables->at(TEST_TABLE_ID)->sptr;

	TIMER(ee->txn_begin())

	for (int stmt_itr = 0; stmt_itr < conf.ycsb_tuples_per_txn; stmt_itr++) {

		int key = num_keys + txn_id;
		std::string value = get_rand_astring(conf.ycsb_field_size);

		record* rec_ptr = new testtable_record(testtable_schema, key, value,
				conf.ycsb_num_val_fields, false);

		statement st(txn_id, operation_type::Insert, TEST_TABLE_ID, rec_ptr);

		TIMER(rc = ee->insert(st));

		if (rc != 0) {
			TIMER(ee->txn_end(false))
    		  return;
		}
	}

	TIMER(ee->txn_end(true))
}

int delete_counter = 0;

void test_benchmark::do_delete(engine* ee) {

	// DELETE
	txn_id++;
	int rc;
	schema* testtable_schema = db->tables->at(TEST_TABLE_ID)->sptr;
	std::string empty;

	TIMER(ee->txn_begin())

	for (int stmt_itr = 0; stmt_itr < conf.ycsb_tuples_per_txn; stmt_itr++) {

		int key = delete_counter;
		delete_counter++;

		record* del_rec_ptr = new testtable_record(testtable_schema, key, empty,
						conf.ycsb_num_val_fields, false);

		statement st = statement(txn_id, operation_type::Delete, TEST_TABLE_ID, del_rec_ptr);

		TIMER(rc = ee->remove(st));

		if (rc != 0) {
			TIMER(ee->txn_end(false))
    		  return;
		}
	}

	TIMER(ee->txn_end(true))
}

void test_benchmark::sim_crash() {
	engine* ee = new engine(conf, tid, db, conf.read_only);
	unsigned int txn_itr;

	// UPDATE
	std::vector<int> field_ids;
	for (int itr = 1; itr <= conf.ycsb_num_val_fields; itr++)
		field_ids.push_back(itr);

	std::string updated_val(conf.ycsb_field_size, 'x');
	int zipf_dist_offset = 0;

	// No recovery needed
	if (conf.etype == engine_type::SP || conf.etype == engine_type::OPT_SP) {
		ee->recovery();
		return;
	}

	// Always in sync
	if (conf.etype == engine_type::OPT_WAL || conf.etype == engine_type::OPT_LSM)
		num_txns = 1;

	ee->txn_begin();

	for (txn_itr = 0; txn_itr < num_txns; txn_itr++) {
		for (int stmt_itr = 0; stmt_itr < conf.ycsb_tuples_per_txn; stmt_itr++) {

			int key = zipf_dist[zipf_dist_offset + stmt_itr];

			record* rec_ptr = new testtable_record(test_table_schema, key,
					updated_val,
					conf.ycsb_num_val_fields,
					conf.ycsb_update_one);

			statement st(txn_id, operation_type::Update, TEST_TABLE_ID, rec_ptr,
					field_ids);

			ee->update(st);
		}
	}

	// Recover
	ee->recovery();
	delete ee;
}

void test_benchmark::execute() {
	engine* ee = new engine(conf, tid, db, conf.read_only);
	unsigned int txn_itr;

	std::cout << "num_txns :: " << num_txns << std::endl;

	for (txn_itr = 0; txn_itr < num_txns; txn_itr++) {
		switch(conf.test_benchmark_mode) {
		case 0:
			do_read(ee);
			break;

		case 1:
			do_insert(ee);
			break;

		case 2:
			do_update(ee);
			break;

		case 3:
			do_delete(ee);
			break;

		default:
			LOG_INFO("unknown test_benchmark_mode : %d", conf.test_benchmark_mode);
			break;
		}
	}

	if(tid == 0)
	{
		std::cout <<"---------------------------------------------------"<<std::endl;

		switch(conf.test_benchmark_mode) {
		case 0:
			std::cout <<"SELECT ::" <<std::endl;
			break;

		case 1:
			std::cout <<"INSERT ::" <<std::endl;
			break;

		case 2:
			std::cout <<"UPDATE ::" <<std::endl;
			break;

		case 3:
			std::cout <<"DELETE ::" <<std::endl;
			break;

		default:
			LOG_INFO("unknown test_benchmark_mode : %d", conf.test_benchmark_mode);
			break;
		}

		std::cout <<"TEST TABLE STATS ::" <<std::endl;
		std::cout<<"Index Size : "<<db->tables->at(TEST_TABLE_ID)->indices->at(0)->pm_map->size()<<std::endl;

		//std::cout << "duration :: " << tm->duration() << std::endl;
	}

	delete ee;
}

}




