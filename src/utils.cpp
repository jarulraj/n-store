#include <algorithm>
#include <random>
#include <iostream>
#include <vector>
#include <chrono>
#include <iomanip>
#include <ctime>
#include <cassert>

#include "utils.h"

using namespace std;

// RAND GEN
std::string get_rand_astring(size_t len) {
  static const char alphanum[] = "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  char rep = alphanum[rand() % (sizeof(alphanum) - 1)];
  std::string str(len, rep);
  return str;
}

double get_rand_double(double d_min, double d_max) {
  double f = (double) rand() / RAND_MAX;
  return d_min + f * (d_max - d_min);
}

bool get_rand_bool(double ratio) {
  double f = (double) rand() / RAND_MAX;
  return (f < ratio) ? true : false;
}

int get_rand_int(int i_min, int i_max) {
  double f = (double) rand() / RAND_MAX;
  return i_min + (int) (f * (double) (i_max - i_min));
}

int get_rand_int_excluding(int i_min, int i_max, int excl) {
  int ret;
  double f;

  if (i_max == i_min + 1) {
    if (excl == i_max)
      return i_min;
    else
      return i_max;
  }

  while (1) {
    f = (double) rand() / RAND_MAX;
    ret = i_min + (int) (f * (double) (i_max - i_min));
    if (ret != excl)
      break;
  }

  return ret;
}

// SER + DESER
std::string serialize(record* rptr, schema* sptr) {
  if (rptr == NULL || sptr == NULL)
    return "";

  char* data = rptr->data;
  std::stringstream output;
  unsigned int num_columns = sptr->num_columns;

  for (unsigned int itr = 0; itr < num_columns; itr++) {
    field_info finfo = sptr->columns[itr];
    bool enabled = finfo.enabled;

    if (enabled) {
      char type = finfo.type;
      size_t offset = finfo.offset;

      switch (type) {
        case field_type::INTEGER:
          int ival;
          memcpy(&ival, &(data[offset]), sizeof(int));
          output << ival;
          break;

        case field_type::DOUBLE:
          double dval;
          memcpy(&dval, &(data[offset]), sizeof(double));
          output << dval;
          break;

        case field_type::VARCHAR: {
          char* vcval = NULL;
          memcpy(&vcval, &(data[offset]), sizeof(char*));
          if (vcval != NULL) {
            output << vcval;
          }
        }
          break;

        default:
          cout << "invalid type : " << type << endl;
          exit(EXIT_FAILURE);
          break;
      }

      output << " ";
    }
  }

  return output.str();
}

record* deserialize(std::string entry_str, schema* sptr) {
  if (entry_str.empty())
    return NULL;

  unsigned int num_columns = sptr->num_columns;
  record* rec_ptr = new record(sptr);
  std::stringstream input(entry_str);

  for (unsigned int itr = 0; itr < num_columns; itr++) {
    field_info finfo = sptr->columns[itr];
    bool enabled = finfo.enabled;

    if (enabled) {
      char type = finfo.type;
      size_t offset = finfo.offset;

      switch (type) {
        case field_type::INTEGER: {
          int ival;
          input >> ival;
          memcpy(&(rec_ptr->data[offset]), &ival, sizeof(int));
        }
          break;

        case field_type::DOUBLE: {
          double dval;
          input >> dval;
          memcpy(&(rec_ptr->data[offset]), &dval, sizeof(double));
        }
          break;

        case field_type::VARCHAR: {
          char* vc = new char[finfo.deser_len];
          input >> vc;
          memcpy(&(rec_ptr->data[offset]), &vc, sizeof(char*));
        }
          break;

        default:
          cout << "invalid type : --" << type << "--" << endl;
          cout << "entry : --" << entry_str << "--" << endl;
          exit(EXIT_FAILURE);
          break;
      }
    }
  }

  return rec_ptr;
}

std::string deserialize_to_string(std::string entry_str, schema* sptr) {
  if (entry_str.empty())
    return "";

  unsigned int num_columns = sptr->num_columns;
  std::stringstream entry(entry_str), output;
  std::string vc_str;

  for (unsigned int itr = 0; itr < num_columns; itr++) {
    field_info finfo = sptr->columns[itr];
    bool enabled = finfo.enabled;

    if (enabled) {
      char type = finfo.type;

      switch (type) {
        case field_type::INTEGER: {
          int ival;
          entry >> ival;
          output << ival;
        }
          break;

        case field_type::DOUBLE: {
          double dval;
          entry >> dval;
          output << dval;
        }
          break;

        case field_type::VARCHAR: {
          entry >> vc_str;
          output << vc_str;
        }
          break;

        default:
          cout << "invalid type : --" << type << "--" << endl;
          cout << "entry : --" << entry_str << "--" << endl;
          exit(EXIT_FAILURE);
          break;
      }

      output << " ";
    }
  }

  return output.str();
}

std::string get_tuple(std::stringstream& entry, schema* sptr) {
  if (sptr == NULL)
    return "";

  std::string tuple, field;

  for (unsigned int col = 0; col < sptr->num_columns; col++) {
    entry >> field;
    tuple += field + " ";
  }

  return tuple;
}

// TIMER

void display_stats(engine_type etype, double duration, int num_txns) {
  double throughput;

  switch (etype) {
    case engine_type::WAL:
      cout << "WAL :: ";
      break;

    case engine_type::SP:
      cout << "SP :: ";
      break;

    case engine_type::LSM:
      cout << "LSM :: ";
      break;

    case engine_type::OPT_WAL:
      cout << "OPT_WAL :: ";
      break;

    case engine_type::OPT_SP:
      cout << "OPT_SP :: ";
      break;

    case engine_type::OPT_LSM:
      cout << "OPT_LSM :: ";
      break;

    default:
      cout << "Unknown engine type " << endl;
      break;
  }

  cout << std::fixed << std::setprecision(2);
  cout << "Duration(s) : " << (duration / 1000.0) << " ";

  throughput = (num_txns * 1000.0) / duration;
  cout << "Throughput  : " << throughput << endl;
}

// RANDOM DIST
void zipf(vector<int>& zipf_dist, double alpha, int n, int num_values) {
  static double c = 0;          // Normalization constant
  double z;          // Uniform random number (0 < z < 1)
  double sum_prob;          // Sum of probabilities
  double zipf_value = 0;          // Computed exponential value to be returned
  int i, j;

  double* powers = new double[n + 1];
  int seed = 0;
  std::mt19937 gen(seed);
  std::uniform_real_distribution<> dis(0.001, 0.099);

  for (i = 1; i <= n; i++)
    powers[i] = 1.0 / pow((double) i, alpha);

  // Compute normalization constant on first call only
  for (i = 1; i <= n; i++)
    c = c + powers[i];
  c = 1.0 / c;

  for (i = 1; i <= n; i++)
    powers[i] = c * powers[i];

  for (j = 1; j <= num_values; j++) {

    // Pull a uniform random number in (0,1)
    z = dis(gen);

    // Map z to the value
    sum_prob = 0;
    for (i = 1; i <= n; i++) {
      sum_prob = sum_prob + powers[i];
      if (sum_prob >= z) {
        zipf_value = i;
        break;
      }
    }

    //cout << "zipf_val :: " << zipf_value << endl;
    zipf_dist.push_back(zipf_value);
  }

  delete[] powers;
}

// Simple skew generator
void simple_skew(vector<int>& simple_dist, double alpha, int n,
                 int num_values) {
  int seed = 0;
  std::mt19937 gen(seed);
  std::uniform_real_distribution<> dis(0, 1);
  double i, z;
  long bound = ((double) n / (100 * alpha));  // 90% from 10% of values
  long val, diff;

  diff = n - bound;

  for (i = 0; i < num_values; i++) {
    z = dis(gen);

    if (z < 0.9)
      val = z * bound;
    else
      val = bound + z * diff;

    //cout << "simple_val :: " << val << endl;
    simple_dist.push_back(val);
  }
}

void uniform(vector<double>& uniform_dist, int num_values) {
  int seed = 0;
  std::mt19937 gen(seed);
  std::uniform_real_distribution<> dis(0, 1);
  double i;

  for (i = 0; i < num_values; i++)
    uniform_dist.push_back(dis(gen));
}

// PTHREAD WRAPPERS

void wrlock(pthread_rwlock_t* access) {
  int rc = pthread_rwlock_wrlock(access);
  if (rc != 0) {
    perror("wrlock failed \n");
    exit(EXIT_FAILURE);
  }
}

void rdlock(pthread_rwlock_t* access) {
  int rc = pthread_rwlock_rdlock(access);
  if (rc != 0) {
    perror("rdlock failed \n");
    exit(EXIT_FAILURE);
  }
}

int try_wrlock(pthread_rwlock_t* access) {
  int rc = pthread_rwlock_trywrlock(access);
  if (rc != 0) {
    perror("trywrlock failed \n");
    exit(EXIT_FAILURE);
  }

  return rc;
}

int try_rdlock(pthread_rwlock_t* access) {
  int rc = pthread_rwlock_tryrdlock(access);
  if (rc != 0) {
    perror("tryrdlock failed \n");
  }

  return rc;
}

void unlock(pthread_rwlock_t* access) {
  int rc = pthread_rwlock_unlock(access);
  if (rc != 0) {
    perror("unlock failed \n");
  }
}

