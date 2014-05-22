
#include <algorithm>

#include "utils.h"

std::string random_string( size_t length ){
    auto randchar = []() -> char
    {
        const char charset[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";

        const size_t max_index = (sizeof(charset) - 1);
        return charset[ rand() % max_index ];
    };

    std::string str(length,0);
    std::generate_n( str.begin(), length, randchar );

    return str;
}

// PTHREAD WRAPPERS

void wrlock(pthread_rwlock_t* access){
	int rc = -1;
	rc = pthread_rwlock_wrlock(access);

	if(rc != 0){
		printf("wrlock failed \n");
		exit(EXIT_FAILURE);
	}
}

void rdlock(pthread_rwlock_t* access){
	int rc = -1;
	rc = pthread_rwlock_rdlock(access);

	if(rc != 0){
		printf("rdlock failed \n");
		exit(EXIT_FAILURE);
	}
}

void unlock(pthread_rwlock_t* access){
	int rc = -1;
	rc = pthread_rwlock_unlock(access);

	if(rc != 0){
		printf("unlock failed \n");
		exit(EXIT_FAILURE);
	}
}

