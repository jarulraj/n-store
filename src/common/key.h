#ifndef KEY_H_
#define KEY_H_

class key {
 public:
  unsigned int id;

  bool operator==(const key &other) const {
    return (id == other.id);
  }
};

class key_hasher
{
 public:
  std::size_t operator()(const key& k) const
  {
    using std::size_t;
    using std::hash;
    using std::string;

    return (std::hash<unsigned int>()(k.id));
  }
};


#endif /* KEY_H_ */
