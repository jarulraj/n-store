
#include <algorithm>

#include "utils.h"

void random_string(char* str, size_t len ){
	static const char alphanum[] = "0123456789"
			"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
			"abcdefghijklmnopqrstuvwxyz";

	for (int i = 0; i < len; ++i) {
		str[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
	}

	str[len] = 0;
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

