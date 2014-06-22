#ifndef _UTILS_H_
#define _UTILS_H_

#include <vector>
#include <ctime>

#include "record.h"

using namespace std;

// UTILS

inline void random_string(char* str, size_t len) {
  static const char alphanum[] = "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  char rep = alphanum[rand() % (sizeof(alphanum) - 1)];
  for (int i = 0; i < len; ++i)
    str[i] = rep;
  str[len - 1] = '\0';
}

inline std::string get_data(record* rec_ptr, vector<bool> key){

  vector<field*> fields = rec_ptr->data;
  vector<field*>::iterator field_itr;

  std::string data;
  int field_id = 0;

  for(field_itr = fields.begin() ; field_itr != fields.end() ; field_itr++)
    if(key[field_id++] == true)
     data += (*field_itr)->get_string() + " ";

  return data;
}

void simple_skew(vector<int>& zipf_dist, int n, int num_values);

void zipf(vector<int>& zipf_dist, double alpha, int n, int num_values);

void uniform(vector<double>& uniform_dist, int num_values);

void display_stats(timespec start, timespec finish, int num_txns);

void wrlock(pthread_rwlock_t* access);

void unlock(pthread_rwlock_t* access);

void rdlock(pthread_rwlock_t* access);

#endif /* UTILS_H_ */
