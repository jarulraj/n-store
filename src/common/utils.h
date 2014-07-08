#ifndef _UTILS_H_
#define _UTILS_H_

#include <vector>
#include <ctime>
#include <sstream>

#include "record.h"

using namespace std;

// UTILS

inline std::string random_string(size_t len) {
  static const char alphanum[] = "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  char rep = alphanum[rand() % (sizeof(alphanum) - 1)];
  std::string str(len, rep);
  return str;
}

inline std::string get_data(record* rptr, schema* sptr) {
  unsigned int num_columns = sptr->num_columns;
  unsigned int itr;
  std::string rec_str;

  if (rptr == NULL || sptr == NULL)
    return rec_str;

  for (itr = 0; itr < num_columns; itr++) {
    if (sptr->columns[itr].enabled)
      rec_str += rptr->get_data(itr);
  }

  return rec_str;
}

inline std::string serialize(record* rptr, schema* sptr) {
  unsigned int num_columns = sptr->num_columns;
  unsigned int itr;
  std::string rec_str;

  rec_str += std::to_string(num_columns) + " ";

  if (rptr == NULL || sptr == NULL)
    return rec_str;

  for (itr = 0; itr < num_columns; itr++) {
    if (sptr->columns[itr].enabled) {
      rec_str += std::to_string(itr) + " ";
      rec_str += rptr->get_data(itr);
    }
  }

  return rec_str;
}

inline record* deserialize(std::stringstream& entry, schema* sptr) {
  unsigned int num_columns;
  unsigned int itr, field_id;
  std::string rec_str;

  record* rec_ptr = new record(sptr);

  entry >> num_columns;

  for (itr = 0; itr < num_columns; itr++) {
    entry >> field_id;

    char type = sptr->columns[field_id].type;
    size_t offset = sptr->columns[field_id].offset;
    size_t len = sptr->columns[field_id].len;

    switch (type) {
      case field_type::INTEGER: {
        int ival;
        entry >> ival;
        std::sprintf(&(rec_ptr->data[offset]), "%d", ival);
      }
        break;

      case field_type::DOUBLE: {
        double dval;
        entry >> dval;
        std::sprintf(&(rec_ptr->data[offset]), "%lf", dval);
      }
        break;

      case field_type::VARCHAR: {
        char* vc = new char[sptr->columns[field_id].len];
        entry >> vc;
        std::sprintf(&(rec_ptr->data[offset]), "%p", vc);
      }
        break;

      default:
        cout << "Invalid field type : " << type << endl;
        break;
    }
  }

  return rec_ptr;
}

void simple_skew(vector<int>& zipf_dist, int n, int num_values);

void zipf(vector<int>& zipf_dist, double alpha, int n, int num_values);

void uniform(vector<double>& uniform_dist, int num_values);

void display_stats(timespec start, timespec finish, int num_txns);

void wrlock(pthread_rwlock_t* access);

void unlock(pthread_rwlock_t* access);

void rdlock(pthread_rwlock_t* access);

#endif /* UTILS_H_ */
