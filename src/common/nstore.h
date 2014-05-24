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
        int lsm_interval;

        bool verbose;
        bool log_only;
        bool sp_only;
        bool lsm_only;
};


#endif /* NSTORE_H_ */
