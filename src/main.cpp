// TESTER

#include "nstore.h"
#include "wal.h"
#include "sp.h"

static void usage_exit(FILE *out){
    fprintf(out,
            "Command Line: \n"
            "   -f --fs-path         :  The path for PMFS \n"
            "   -x --num-txns        :  Number of transactions to execute \n"
            "   -k --num-keys        :  Number of keys \n"
            "   -t --num-thds        :  Number of threads \n"
            "   -p --partition-size  :  Partition size \n"
    		);
    exit(-1);
}

static struct option opts[] =
{
    { "fs-path",		    optional_argument,		NULL,  'f' },
    { "num-txns",		 	optional_argument,		NULL,  'x' },
    { "num-keys",		    optional_argument,		NULL,  'k' },
    { "num-thds",			optional_argument,		NULL,  't' },
    { "partition-size",	    optional_argument,		NULL,  'p' },
    { "verbose", 			no_argument,      		NULL,  'v' },
    { NULL,					0,					    NULL,   0	}
};

static void parse_arguments(int argc, char* argv[], config& state) {

	// Default Values

	state.fs_path =  std::string("./");

    state.num_keys      =  10000;
    state.num_txns      =  20000;
    state.num_thds      =  2;

    state.sz_value      =  2;
    state.sz_partition  =  10;
    state.verbose       =  false;

    state.sz_tuple      = 4 + 4 + state.sz_value + 10;

    state.gc_interval   =  5; // ms
    state.per_writes    =  10;

    // Parse args
    while (1) {
        int idx = 0;
        int c = getopt_long(argc, argv, "f:x:k:t:p:v", opts, &idx);

        if (c == -1)
            break;

        switch (c) {
            case 'f':
                state.fs_path = std::string(optarg);
                cout<<"fs_path: "<<state.fs_path<<endl;
                break;
            case 'x':
                state.num_txns = atoi(optarg);
                cout<<"num_txns: "<<state.num_txns<<endl;
                break;
            case 'k':
                state.num_keys = atoi(optarg);
                cout<<"num_keys: "<<state.num_keys<<endl;
                break;
            case 't':
                state.num_thds = atoi(optarg);
                cout<<"num_thds: "<<state.num_thds<<endl;
                break;
            case 'p':
                state.sz_partition = atoi(optarg);
                cout<<"sz_partition: "<<state.sz_partition<<endl;
                break;
            case 'v':
                 state.verbose = true;
                 break;
            default:
                fprintf(stderr, "\nUnknown option: -%c-\n", c);
                usage_exit(stderr);
        }
    }
}


int main(int argc, char **argv){
    class config state;
    parse_arguments(argc, argv, state);

    wal_engine wal(state);
    wal.test();

    sp_engine sp(state);
    sp.test();

    return 0;
}
