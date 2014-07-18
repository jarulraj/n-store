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
        log_file_fd(-1),
        log_offset(0) {
  }

  void configure(std::string _name) {
    log_file_name = _name;

    // append/update mode
    log_file = fopen(log_file_name.c_str(), "a+");

    if (log_file != NULL) {
      log_file_fd = fileno(log_file);
      fseek(log_file, 0, SEEK_END);
      log_offset = ftell(log_file);
    } else {
      std::cout << "Log file not found : " << log_file_name << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  off_t push_back(std::string entry) {
    int ret;
    off_t prev_offset;

    ret = fwrite(entry.c_str(), sizeof(char), entry.size(), log_file);
    if (ret < 0) {
      perror("fwrite failed");
      exit(EXIT_FAILURE);
    }

    prev_offset = log_offset;
    log_offset += ret;
    return prev_offset;
  }

  int sync() {
    int ret;

    // sync log
    ret = fsync(log_file_fd);
    if (ret != 0) {
      perror("fsync failed");
      exit(EXIT_FAILURE);
    }

    return ret;
  }

  std::string at(off_t log_offset) {
    std::string entry_str;
    char* line = NULL;
    size_t len = 0;
    int rc;

    fseek(log_file, log_offset, SEEK_SET);
    rc = getline(&line, &len, log_file);
    if(rc == -1){
      perror("getline");
    }

    if(rc != -1){
      entry_str = std::string(line);
      free(line);
    }
    return entry_str;
  }

  void close() {
    fclose(log_file);
  }

  //private:
  FILE* log_file;
  off_t log_offset;

  std::string log_file_name;
  int log_file_fd;
};

#endif
