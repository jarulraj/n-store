#ifndef SERIALIZER_H_
#define SERIALIZER_H_

#include <sstream>

using namespace std;

class serializer {
 public:

  std::stringstream output, input, iput;

  // SER + DESER
  std::string serialize(record* rptr, schema* sptr) {
    if (rptr == NULL || sptr == NULL)
      return "";

    char* data = rptr->data;
    unsigned int num_columns = sptr->num_columns;

    output.clear();
    output.str(std::string());

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
    input.clear();
    input.str(entry_str);

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

  std::string project(std::string entry_str, schema* sptr) {
    if (entry_str.empty())
      return "";

    input.clear();
    input.str(entry_str);
    std::string field_str, tuple_str;
    unsigned int itr = 0;

    while (getline(input, field_str, ' ')) {
      field_info finfo = sptr->columns[itr++];
      bool enabled = finfo.enabled;

      if (enabled)
        tuple_str += field_str + " ";
    }

    return tuple_str;
  }

};

#endif /* SERIALIZER_H_ */
