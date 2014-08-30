#pragma once

#include <vector>
#include <ctime>
#include <sstream>

#include "config.h"
#include "record.h"

namespace storage {

// UTILS

#define TIMER(expr) \
  { tm->start(); \
    {expr;} \
    tm->end(); }

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

}
