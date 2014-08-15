#ifndef _STORAGE_H_
#define _STORAGE_H_

#include <stdlib.h>
#include <unistd.h>
#include <sstream>
#include <string>
#include <unistd.h>

#include "record.h"

using namespace std;

// FS STORAGE

class storage {
 public:
  storage()
      : storage_file(NULL),
        storage_file_fd(-1),
        storage_offset(0),
        max_tuple_size(0) {
  }

  void configure(std::string _name, size_t _tuple_size, bool append) {
    storage_file_name = _name + ".nvm";
    max_tuple_size = _tuple_size;

    // file exists - read/update mode
    if (access(storage_file_name.c_str(), F_OK) != -1) {
      if (!append) {
        storage_file = fopen(storage_file_name.c_str(), "r+");
      } else {
        storage_file = fopen(storage_file_name.c_str(), "a+");
      }
    } else {
      // new file - write/update mode
      storage_file = fopen(storage_file_name.c_str(), "w+");
    }

    if (storage_file != NULL) {
      storage_file_fd = fileno(storage_file);
      storage_offset = 0;

      //fseek(storage_file, 0, SEEK_END);
      //size_t sz = ftell(storage_file);
      //printf("File size :: %s %lu \n", storage_file_name.c_str(), sz);
    } else {
      std::cout << "File not found : " << storage_file_name << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  off_t push_back(std::string entry) {
    int ret;
    off_t prev_offset;

    if (entry.size() > max_tuple_size) {
      printf("Entry size exceeds tuple size : %lu  %lu \n", entry.size(),
             max_tuple_size);
      exit(EXIT_FAILURE);
    }

    entry.resize(max_tuple_size);

    fseek(storage_file, storage_offset, SEEK_SET);
    ret = fwrite(entry.c_str(), sizeof(char), max_tuple_size, storage_file);
    if (ret < 0) {
      perror("fwrite failed");
      exit(EXIT_FAILURE);
    }

    prev_offset = storage_offset;
    storage_offset += ret;
    return prev_offset;
  }

  off_t update(off_t storage_offset, std::string entry) {
    int ret;

    if (entry.size() > max_tuple_size) {
      printf("Entry size exceeds tuple size : %lu  %lu \n", entry.size(),
             max_tuple_size);
      exit(EXIT_FAILURE);
    }

    entry.resize(max_tuple_size);

    fseek(storage_file, storage_offset, SEEK_SET);
    ret = fwrite(entry.c_str(), sizeof(char), max_tuple_size, storage_file);
    if (ret < 0) {
      perror("fwrite failed");
      exit(EXIT_FAILURE);
    }

    return storage_offset;
  }

  int sync() {
    int ret;

    // sync storage
    ret = fsync(storage_file_fd);
    if (ret != 0) {
      perror("fsync failed");
      exit(EXIT_FAILURE);
    }

    return ret;
  }

  std::string at(off_t storage_offset) {
    std::string entry_str;
    char* buf = new char[max_tuple_size + 1];
    int rc;

    fseek(storage_file, storage_offset, SEEK_SET);
    rc = fread(buf, sizeof(char), max_tuple_size, storage_file);
    if (rc == -1) {
      perror("fread");
      exit(EXIT_FAILURE);
    }

    entry_str = std::string(buf);
    delete[] buf;

    return entry_str;
  }

  void close() {
    fclose(storage_file);
  }

//private:
  FILE* storage_file;
  int storage_file_fd;
  off_t storage_offset;
  size_t max_tuple_size;
  std::string storage_file_name;
};

#endif
