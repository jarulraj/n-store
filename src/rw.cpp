#define _MULTI_THREADED

#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

#include <unistd.h>
#include <pthread.h>

using namespace std;

pthread_rwlock_t       rwlock;

/* Simple function to check the return code and exit the program
 *    if the function call failed
 *       */
static void compResults(std::string str, int rc) {
    if (rc) {
        printf("Error on : %s, rc=%d", str.c_str(), rc);
        exit(EXIT_FAILURE);
    }
    return;
}

void *rdlockThread()
{
    int rc;

    printf("Entered thread, getting read lock\n");
    rc = pthread_rwlock_rdlock(&rwlock);
    compResults("pthread_rwlock_rdlock()\n", rc);
    printf("got the rwlock read lock\n");

    sleep(1);

    printf("unlock the read lock\n");
    rc = pthread_rwlock_unlock(&rwlock);
    compResults("pthread_rwlock_unlock()\n", rc);
    printf("Secondary thread unlocked\n");
    return NULL;
}

void *wrlockThread()
{
    int rc;

    printf("Entered thread, getting write lock\n");
    rc = pthread_rwlock_wrlock(&rwlock);
    compResults("pthread_rwlock_wrlock()\n", rc);

    printf("Got the rwlock write lock, now unlock\n");
    rc = pthread_rwlock_unlock(&rwlock);
    compResults("pthread_rwlock_unlock()\n", rc);
    printf("Secondary thread unlocked\n");
    return NULL;
}



int main(int argc, char **argv)
{
    int                   rc=0;
    std::thread             t1, t2;

    printf("Enter Testcase - %s\n", argv[0]);

    printf("Main, initialize the read write lock\n");
    rc = pthread_rwlock_init(&rwlock, NULL);
    compResults("pthread_rwlock_init()\n", rc);

    printf("Main, grab a read lock\n");
    rc = pthread_rwlock_rdlock(&rwlock);
    compResults("pthread_rwlock_rdlock()\n",rc);

    printf("Main, grab the same read lock again\n");
    rc = pthread_rwlock_rdlock(&rwlock);
    compResults("pthread_rwlock_rdlock() second\n", rc);

    printf("Main, create the read lock thread\n");
    t1 = std::thread(rdlockThread);

    printf("Main - unlock the first read lock\n");
    rc = pthread_rwlock_unlock(&rwlock);
    compResults("pthread_rwlock_unlock()\n", rc);

    printf("Main, create the write lock thread\n");
    t2 = std::thread(wrlockThread);

    sleep(1);
    printf("Main - unlock the second read lock\n");
    rc = pthread_rwlock_unlock(&rwlock);
    compResults("pthread_rwlock_unlock()\n", rc);

    printf("Main, wait for the threads\n");

    t1.join();
    t2.join();

    rc = pthread_rwlock_destroy(&rwlock);
    compResults("pthread_rwlock_destroy()\n", rc);

    printf("Main completed\n");
    return 0;
}
