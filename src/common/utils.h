#ifndef _UTILS_H_
#define _UTILS_H_

// UTILS

void random_string(char* str, size_t len );

void wrlock(pthread_rwlock_t* access);

void unlock(pthread_rwlock_t* access);

void rdlock(pthread_rwlock_t* access);

#endif /* UTILS_H_ */
