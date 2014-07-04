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

  void push_back(const char* entry, int len) {
    fwrite(entry, sizeof(char), len, log_file);
  }

  void push_back(std::string entry) {
    buffer_stream << entry << "\n";
  }

  int write() {
    int ret;

    buffer = buffer_stream.str();

    fwrite(buffer.c_str(), sizeof(char), buffer.size(), log_file);

    // sync log
    ret = fsync(log_file_fd);
    if (ret != 0) {
      perror("fsync failed");
      exit(EXIT_FAILURE);
    }

    // clear buffer
    buffer_stream.str("");
    buffer_stream.clear();

    return ret;
  }

  void close() {
    fclose(log_file);
  }

  FILE* log_file;

 private:
  std::stringstream buffer_stream;
  std::string buffer;

  std::string log_file_name;
  int log_file_fd;

};

#endif
