#ifndef MESSAGE_H_
#define MESSAGE_H_

enum message_type {
  Request,
  Response
};

class message {
 public:

  message()
      : msg_type(message_type::Request),
        statement_id(-1),
        status(false),
        st_ptr(NULL) {
  }

  message(message_type _msg_type, unsigned int _st_id, bool _status,
          statement* _st_ptr)
      : msg_type(_msg_type),
        statement_id(_st_id),
        status(_status),
        st_ptr(_st_ptr) {
  }

  message(const message &other)
      : msg_type(other.msg_type),
        statement_id(other.statement_id),
        status(other.status),
        st_ptr(other.st_ptr) {
  }

  //private:
  message_type msg_type;
  unsigned int statement_id;
  bool status;
  statement* st_ptr;
};

#endif /* MESSAGE_H_ */
