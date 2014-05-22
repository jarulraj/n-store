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
    { "fs-path",		    no_argument,			0,  'f' },
    { "num-txns",			no_argument,			0,  'x' },
    { "num-keys",		    no_argument,			0,  'k' },
    { "num-thds",			no_argument,			0,  't' },
    { "partition-size",	    no_argument,			0,  'p' },
    { "verbose", 			no_argument,      		0,  'v' },
    { 0,					0,					    0,   0	}
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

    state.gc_interval   =   5; // ms

    state.per_writes    =  10;

    // Parse args
    while (1) {
        int idx = 0;
        int c = getopt_long(argc, argv, "fxktp", opts, &idx);

        if (c == -1) 
            break;

        switch (c) {
            case 'f':
                state.fs_path = std::string(optarg);
                break;
            case 'x': 
                state.num_txns = atoi(optarg);
                break; 
            case 'k': 
                state.num_keys = atoi(optarg);
                break;
            case 't':
                state.num_thds = atoi(optarg);
                break;
            case 'p': 
                state.sz_partition = atoi(optarg);
                break;
            case 'v':
                 state.verbose = (bool)(atoi(optarg));
                 break;
            default:
                fprintf(stderr, "\nERROR: Unknown option: -%c-\n", c);
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
