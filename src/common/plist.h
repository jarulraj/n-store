#ifndef PMEM_LIST_H_
#define PMEM_LIST_H_

#include <vector>
#include "libpm.h"

using namespace std;

// Persistent list for storing pointers
template<typename V>
class plist {
 public:
  struct node {
   public:
    node(const V& _val)
        : next(NULL),
          val(_val) {
    }

    node* next;
    V val;

    void* operator new(size_t sz) throw (bad_alloc) {
      if (persistent) {
        void* ret = pmem_new(sz);
        pmemalloc_activate(ret);
        return ret;
      } else
        return ::operator new(sz);
    }

    void operator delete(void *p) throw () {
      if (persistent)
        pmem_delete(p);
      else
        ::operator delete(p);
    }
  };

  node** head;
  node** tail;
  static bool persistent;

  plist()
      : head(NULL),
        tail(NULL) {
  }

  plist(void** _head, void** _tail) {
    head = (node**) _head;
    tail = (node**) _tail;
  }

  ~plist() {
    clear();
  }

  void* operator new(size_t sz) throw (bad_alloc) {
    if (persistent) {
      void* ret = pmem_new(sz);
      pmemalloc_activate(ret);
      return ret;
    } else
      return ::operator new(sz);
  }

  void operator delete(void *p) throw () {
    if (persistent)
      pmem_delete(p);
    else
      ::operator delete(p);
  }

  friend std::ostream& operator<<(std::ostream& os, const plist& list) {
    os << "head : " << list.head << " tail : " << list.tail << "\n";
    return os;
  }

  node* init(V val) {
    node* np = new node(val);
    np->next = (*head);

    (*head) = np;
    (*tail) = np;

    if (persistent)
      pmemalloc_activate(np);

    return np;
  }

  void push_back(V val) {
    if ((*head) == NULL) {
      init(val);
      return;
    }

    node* tailp = NULL;
    node* np = new node(val);
    // Link it in at the end of the list
    np->next = NULL;

    tailp = (*tail);
    (*tail) = np;

    if (persistent)
      pmemalloc_activate(np);

    tailp->next = np;
    pmem_persist(&tailp->next, sizeof(*np), 0);
  }

  // Returns the absolute pointer value
  V at(const int index) const {
    node * np = (*head);
    unsigned int itr = 0;

    while (np != NULL) {
      if (itr == index) {
        return np->val;
      } else {
        itr++;
        np = np->next;
      }
    }

    return NULL;
  }

  node* find(V val, node** prev) {
    node* np = (*head);
    node* tmp = NULL;
    bool found = false;

    while (np != NULL) {
      if (np->val == val) {
        found = true;
        break;
      } else {
        tmp = np;
        np = np->next;
      }
    }

    if (found) {
      if (prev) {
        *prev = tmp;
      }
      return np;
    } else {
      return NULL;
    }
  }

  void update(const int index, V val) {
    node * np = (*head);
    unsigned int itr = 0;

    while (np != NULL) {
      if (itr == index) {
        np->val = val;
        break;
      } else {
        itr++;
        np = np->next;
      }
    }
  }

  bool erase(V val) {
    node* prev = NULL;
    node* np = NULL;

    if ((*head) == NULL) {
      return false;
    }

    np = find(val, &prev);

    if (np == NULL) {
      return -1;
    } else {
      if (prev != NULL) {
        prev->next = np->next;
        pmem_persist(&prev->next, sizeof(*np), 0);
      }

      // Update head and tail
      if (np == (*head)) {
        (*head) = np->next;
      } else if (np == (*tail)) {
        (*tail) = prev;
      }
    }

    delete np;

    return true;
  }

  void display(void) {
    node* np = (*head);

    if (np == NULL) {
      cout << "empty list" << endl;
      return;
    }

    while (np) {
      cout << np->val;
      np = np->next;
    }
    cout << endl;

  }

  void clear(void) {
    node* np = (*head);
    node* prev = NULL;

    while (np) {
      prev = np;
      np = np->next;
      delete prev;
    }

    (*head) = NULL;
    (*tail) = NULL;
  }

  vector<V> get_data(void) {
    node* np = (*head);
    vector<V> data;

    while (np) {
      data.push_back(np->val);
      np = np->next;
    }

    return data;
  }
};

//template<typename V> bool plist<V>::persistent;

#endif /* PMEM_LIST_H_ */
