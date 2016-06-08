#pragma once

#include <vector>
#include <ctime>
#include <sstream>

#include "config.h"

void* pmalloc(size_t sz);
void pfree(void *p);


namespace storage {

class schema;

// UTILS

#define TIMER(expr) \
  { tm->start(); \
    {expr;} \
    tm->end(); }

#define die()	assert(0)
// RAND GEN
std::string get_rand_astring(size_t len);

double get_rand_double(double d_min, double d_max);

bool get_rand_bool(double ratio);

int get_rand_int(int i_min, int i_max);

int get_rand_int_excluding(int i_min, int i_max, int excl);

std::string get_tuple(std::stringstream& entry, schema* sptr);

// szudzik hasher
inline unsigned long hasher(unsigned long a, unsigned long b) {
  if (a >= b)
    return (a * a + a + b);
  else
    return (a + b * b);
}

inline unsigned long hasher(unsigned long a, unsigned long b, unsigned long c) {
  unsigned long a_sq = a * a;
  unsigned long ret = (a_sq + b) * (a_sq + b) + c;
  return ret;
}

void simple_skew(std::vector<int>& simple_dist, double alpha, int n, int num_values);

void zipf(std::vector<int>& zipf_dist, double alpha, int n, int num_values);

void uniform(std::vector<double>& uniform_dist, int num_values);

void display_stats(engine_type etype, double duration, int num_txns);

void wrlock(pthread_rwlock_t* access);
void rdlock(pthread_rwlock_t* access);
void unlock(pthread_rwlock_t* access);

// PCOMMIT
// PCOMMIT helpers

#define CPU_FREQ_MHZ (2593)

static inline unsigned long read_tsc(void){
    unsigned long var;
    unsigned int hi, lo;

    asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
    var = ((unsigned long long int) hi << 32) | lo;

    return var;
}

static inline void cpu_pause(){
    __asm__ volatile ("pause" ::: "memory");
}

static inline void clwb(__attribute__((unused)) void *addr){
    cpu_pause();
}

static inline void pcommit(unsigned long lat){
    unsigned long etsc = read_tsc() + (unsigned long)(lat*CPU_FREQ_MHZ/1000);
    while (read_tsc() < etsc)
        cpu_pause();
}

#define DELAY_IN_NS (800)

static inline void test_pcommit(){
    unsigned long stsc, etsc;

    stsc = read_tsc();
    pcommit(DELAY_IN_NS);
    etsc = read_tsc();
    printf ("pm_wbarrier latency: %lu ns\n",
            (unsigned long)((etsc-stsc)*1000/CPU_FREQ_MHZ));

    stsc = read_tsc();
    pcommit(2*DELAY_IN_NS);
    etsc = read_tsc();
    printf ("pm_wbarrier latency: %lu ns\n",
            (unsigned long)((etsc-stsc)*1000/CPU_FREQ_MHZ));

    stsc = read_tsc();
    clwb(NULL);
    etsc = read_tsc();
    printf ("clwb latency: %lu ns\n",
            (unsigned long)((etsc-stsc)*1000/CPU_FREQ_MHZ));
}

}
