#include <iostream>
#include <map>
#include <string>

#include <boost/thread.hpp>

using namespace std;

boost::shared_mutex _access;
map<long,long> table;

long num_keys = 1000000;
long num_rd = 10000000;
long num_wr = 1000000;

void reader()
{
	long val;
	for(long i=0 ; i<num_rd ; i++){
		long key = i%num_keys;

		{
			boost::shared_lock< boost::shared_mutex > lock(_access);
			// do work here, without anyone having exclusive access

			val = table.at(key);
		}

	}
}

void conditional_writer()
{
	for(long i=0 ; i<num_wr ; i++){
		long r = rand();
		long val = r%1000;
		long key = r%num_keys;

		{
			boost::upgrade_lock< boost::shared_mutex > lock(_access);
			// do work here, without anyone having exclusive access

			boost::upgrade_to_unique_lock< boost::shared_mutex > uniqueLock(lock);
			// do work here, but now you have exclusive access

			table[key] = -1;
		}

	}

}

void init(){
	for(long i=0 ; i<num_keys ; i++)
		table[i] = i;
}

int main(){

	init();

	boost::thread t1(conditional_writer);
	boost::thread t2(reader);
	boost::thread t3(reader);

	t1.join();
	t2.join();
	t3.join();

    return 0;
}
