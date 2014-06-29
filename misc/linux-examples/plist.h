#ifndef PMEM_LIST_H_
#define PMEM_LIST_H_

#include "libpm.h"

using namespace std;

template<typename V>
class plist {
 public:
  struct node {
    struct node* next;
    V val;
  };

  struct node** head;
  struct node** tail;

  plist()
      : head(NULL),
        tail(NULL) {
  }

  plist(void** _head, void** _tail) {

    head = (struct node**) OFF(_head);
    tail = (struct node**) OFF(_tail);

    cout << "head : " << head << " tail : " << tail << "\n";
  }

  ~plist() {
    clear();
  }

  friend std::ostream& operator<<(std::ostream& os, const plist& list) {
    os << "head : " << list.head << " tail : " << list.tail << "\n";
    return os;
  }

  struct node* init(V val) {
    struct node* np = NULL;
    if ((np = (struct node*) pmemalloc_reserve(pmp, sizeof(*np))) == NULL) {
      cout << "pmemalloc_reserve failed " << endl;
      return NULL;
    }

    // Link it in at the beginning of the list
    PMEM(np)->next = (*PMEM(head));
    PMEM(np)->val = val;

    pmemalloc_onactive(pmp, np, (void **) PMEM(head), np);
    pmemalloc_onactive(pmp, np, (void **) PMEM(tail), np);
    pmemalloc_activate(pmp, np);

    return np;
  }

  void push_back(V val) {
    if ((*PMEM(head)) == NULL) {
      init(val);
      return;
    }

    struct node* np = NULL;
    struct node* tailp = NULL;

    if ((np = (struct node*) pmemalloc_reserve(pmp, sizeof(*np))) == NULL) {
      cout << "pmemalloc_reserve failed " << endl;
      return;
    }

    // Link it in at the end of the list
    PMEM(np)->next = NULL;
    PMEM(np)->val = val;

    tailp = (*PMEM(tail));

    pmemalloc_onactive(pmp, np, (void **) PMEM(tail), np);
    pmemalloc_activate(pmp, np);

    PMEM(tailp)->next = np;
    pmem_persist(&PMEM(tailp)->next, sizeof(*np), 0);
  }

  V at(const int index) const {
    struct node * np = (*PMEM(head));
    unsigned int itr = 0;
    bool found = false;

    while (np != NULL) {
      if (itr == index) {
        found = true;
        break;
      } else {
        itr++;
        np = PMEM(np)->next;
      }
    }

    if (found) {
      return PMEM(np)->val;
    }

    return 0;
  }

  struct node* find(V val, struct node** prev) {
    struct node* np = (*PMEM(head));
    struct node* tmp = NULL;
    bool found = false;

    while (np != NULL) {
      if (PMEM(np)->val == val) {
        found = true;
        break;
      } else {
        tmp = np;
        np = PMEM(np)->next;
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

  bool erase(V val) {
    struct node* prev = NULL;
    struct node* np = NULL;

    if ((*PMEM(head)) == NULL) {
      return false;
    }

    np = find(val, &prev);

    if (np == NULL) {
      return -1;
    } else {
      if (prev != NULL) {
        PMEM(prev)->next = PMEM(np)->next;
        pmem_persist(&PMEM(prev)->next, sizeof(*np), 0);
      }

      // Update head and tail
      if (np == (*PMEM(head))) {
        pmemalloc_onfree(pmp, np, (void **) PMEM(head),
        PMEM(np)->next);
      } else if (np == (*PMEM(tail))) {
        pmemalloc_onfree(pmp, np, (void **) PMEM(tail), prev);
      }
    }

    pmemalloc_free(pmp, np);

    return true;
  }

  void display(void) {
    struct node* np = (*PMEM(head));

    while (np) {
      cout << PMEM(np)->val << " -> ";
      np = PMEM(np)->next;
    }
    cout << endl;

  }

  void clear(void) {
    struct node* np = (*PMEM(head));
    struct node* prev = NULL;

    while (np) {
      prev = np;
      np = PMEM(np)->next;
      pmemalloc_free(pmp, prev);
    }

    (*PMEM(head)) = NULL;
    (*PMEM(tail)) = NULL;
  }

  vector<V> get_data(void) {
    struct node* np = (*PMEM(head));
    vector<V> data;

    while (np) {
      data.push_back(PMEM(np)->val);
      np = PMEM(np)->next;
    }

    return data;
  }

};

#endif /* PMEM_LIST_H_ */
