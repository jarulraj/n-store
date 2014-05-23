#ifndef NSTORE_H_
#define NSTORE_H_

#include <string>
#include <getopt.h>

using namespace std;

class config {

    public:
        std::string fs_path;

        int num_keys;
        int num_txns;
        int num_parts;

        int sz_value;
        int sz_tuple;

        int per_writes;

        int gc_interval;

        bool verbose;
        bool sp_only;
        bool log_only;
};


#endif /* NSTORE_H_ */
