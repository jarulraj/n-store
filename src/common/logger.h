#ifndef _LOGGER_H_
#define _LOGGER_H_

#include <stdlib.h>
#include <unistd.h>
#include <sstream>
#include <string>

#include "record.h"
#include "libpm.h"

using namespace std;

// FS LOGGING

class logger {
 public:
  logger()
      : log_file(NULL),
        log_file_fd(-1) {
  }

  void configure(std::string _name) {
    log_file_name = _name;

    log_file = fopen(log_file_name.c_str(), "a");
    if (log_file != NULL) {
      log_file_fd = fileno(log_file);
    } else {
      std::cout << "Log file not found : " << log_file_name << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  void push_back(std::string entry) {
    int ret;

    ret = fwrite(entry.c_str(), sizeof(char), entry.size(), log_file);
    if (ret < 0) {
      perror("fwrite failed");
      exit(EXIT_FAILURE);
    }

  }

  int write() {
    int ret;

    // sync log
    ret = fsync(log_file_fd);
    if (ret != 0) {
      perror("fsync failed");
      exit(EXIT_FAILURE);
    }

    return ret;
  }

  void close() {
    fclose(log_file);
  }

  //private:
  FILE* log_file;

  std::string log_file_name;
  int log_file_fd;
};

#endif
