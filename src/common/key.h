#ifndef KEY_H_
#define KEY_H_

class key {
 public:
  key()
      : id(0) {
  }

  bool operator==(const key &other) const {
    cout << "key : " << id << " " << other.id << endl;

    return (id == other.id);
  }

  unsigned int id;
};

class key_hasher {
 public:
  std::size_t operator()(const key& k) const {
    using std::size_t;
    using std::hash;
    using std::string;

    cout << "key hasher used" << endl;

    return (std::hash<unsigned int>()(k.id));
  }
};

#endif /* KEY_H_ */
