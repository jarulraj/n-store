#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <string>

#include <thread>
#include <mutex>

#include "libpm.h"

using namespace std;

void* pmp;
std::mutex pmp_mutex;

#define MAX_PTRS 128

struct static_info {
  void* ptrs[MAX_PTRS];
};

struct static_info *sp;

class ptree {
 private:
  struct node {
    int key;
    int val;

    node *left;
    node *right;
    node *parent;
    char bal;
  };

  struct node** root;

 public:
  ptree()
      : root(NULL) {
  }

  ptree(void** _root) {

    root = (struct node**) OFF(_root);

    cout << "root : " << root << "\n";
  }

  ~ptree() {
    clear();

  }

  void clear() {
    node* temp = (*PMEM(root));
    if (temp == NULL) {
      cout << "Empty tree \n";
      return;
    }

    cout << "clearing ::" << PMEM(temp)->key << endl;
    clear_node(temp);

    (*PMEM(root)) = NULL;
  }

  void clear_node(node* n) {
    if (n != NULL) {
      cout << "clearing ::" << PMEM(n)->key << endl;
      clear_node(PMEM(n)->left);
      clear_node(PMEM(n)->right);
      pmemalloc_free(pmp, n);
    }
  }

  void insert(int key, int val) {
    node *temp, *back, *ancestor;
    node* np = NULL;

    if ((np = (node*) pmemalloc_reserve(pmp, sizeof(*np))) == NULL) {
      cout << "pmemalloc_reserve failed " << endl;
      return;
    }

    PMEM(np)->key = key;
    PMEM(np)->val = val;
    PMEM(np)->left = NULL;
    PMEM(np)->right = NULL;
    PMEM(np)->parent = NULL;
    PMEM(np)->bal = '=';

    // Check for empty tree first
    if ((*PMEM(root)) == NULL) {
      pmemalloc_onactive(pmp, np, (void **) PMEM(root), np);
      pmemalloc_activate(pmp, np);
      return;
    }

    temp = (*PMEM(root));
    back = NULL;
    ancestor = NULL;

    // Tree is not empty so search for place to insert
    while (temp != NULL) {
      back = temp;
      // Mark ancestor that will be out of balance after this node is inserted
      if (PMEM(temp)->bal != '=')
        ancestor = temp;
      if (PMEM(np)->key < PMEM(temp)->key)
        temp = PMEM(temp)->left;
      else if (PMEM(np)->key > PMEM(temp)->key) {
        temp = PMEM(temp)->right;
      } else
        break;
    }

    // Update if key already exists
    if (temp != NULL) {
      PMEM(temp)->val = PMEM(np)->val;
      pmem_persist(&PMEM(temp)->val, sizeof(int), 0);
      return;
    }

    // Insert if key does not exist
    // temp is now NULL
    // back points to parent node to attach np to
    // ancestor points to most recent out of balance ancestor
    PMEM(np)->parent = back;   // Set parent
    pmemalloc_activate(pmp, np);

    if (PMEM(np)->key < PMEM(back)->key) {
      PMEM(back)->left = np;
      pmem_persist(&PMEM(back)->left, sizeof(node*), 0);
    } else {
      PMEM(back)->right = np;
      pmem_persist(&PMEM(back)->right, sizeof(node*), 0);
    }

    // Now call function to restore the tree's AVL property
    restoreAVL(ancestor, np);
  }

  void restoreAVL(node* ancestor, node* np) {
    node* temp = (*PMEM(root));

    //--------------------------------------------------------------------------------
    // Case 1: ancestor is NULL, i.e. balanceFactor of all ancestors' is '='
    //--------------------------------------------------------------------------------
    if (ancestor == NULL) {
      if (PMEM(np)->key < PMEM(temp)->key)  // np inserted to left of root
        PMEM(temp)->bal = 'L';
      else
        PMEM(temp)->bal = 'R';  // np inserted to right of root
      // Adjust the balanceFactor for all nodes from np back up to root
      adjustBalanceFactors(temp, np);
    }

    //--------------------------------------------------------------------------------
    // Case 2: Insertion in opposite subtree of ancestor's balance factor, i.e.
    //  ancestor.balanceFactor = 'L' AND  Insertion made in ancestor's right subtree
    //     OR
    //  ancestor.balanceFactor = 'R' AND  Insertion made in ancestor's left subtree
    //--------------------------------------------------------------------------------
    else if (((PMEM(ancestor)->bal == 'L')
        && (PMEM(np)->key > PMEM(ancestor)->key))
        || ((PMEM(ancestor)->bal == 'R')
            && (PMEM(np)->key < PMEM(ancestor)->key))) {
      PMEM(ancestor)->bal = '=';
      // Adjust the balanceFactor for all nodes from np back up to ancestor
      adjustBalanceFactors(ancestor, np);
    }
    //--------------------------------------------------------------------------------
    // Case 3: ancestor.balanceFactor = 'R' and the node inserted is
    //      in the right subtree of ancestor's right child
    //--------------------------------------------------------------------------------
    else if ((PMEM(ancestor)->bal == 'R')
        && (PMEM(np)->key > PMEM(PMEM(ancestor)->right)->key)) {
      PMEM(ancestor)->bal = '=';  // Reset ancestor's balanceFactor
      rotateLeft(ancestor);       // Do single left rotation about ancestor
      // Adjust the balanceFactor for all nodes from np back up to ancestor's parent
      adjustBalanceFactors(PMEM(ancestor)->parent, np);
    }

    //--------------------------------------------------------------------------------
    // Case 4: ancestor.balanceFactor is 'L' and the node inserted is
    //      in the left subtree of ancestor's left child
    //--------------------------------------------------------------------------------
    else if ((PMEM(ancestor)->bal == 'L')
        && (PMEM(np)->key < PMEM(PMEM(ancestor)->left)->key)) {
      PMEM(ancestor)->bal = '=';  // Reset ancestor's balanceFactor
      rotateRight(ancestor);       // Do single right rotation about ancestor
      // Adjust the balanceFactor for all nodes from np back up to ancestor's parent
      adjustBalanceFactors(PMEM(ancestor)->parent, np);
    }

    //--------------------------------------------------------------------------------
    // Case 5: ancestor.balanceFactor is 'L' and the node inserted is
    //      in the right subtree of ancestor's left child
    //--------------------------------------------------------------------------------
    else if ((PMEM(ancestor)->bal == 'L')
        && (PMEM(np)->key > PMEM(PMEM(ancestor)->left)->key)) {
      // Perform double right rotation (actually a left followed by a right)
      rotateLeft(PMEM(ancestor)->left);
      rotateRight(ancestor);
      // Adjust the balanceFactor for all nodes from np back up to ancestor
      adjustLeftRight(ancestor, np);
    }

    //--------------------------------------------------------------------------------
    // Case 6: ancestor.balanceFactor is 'R' and the node inserted is
    //      in the left subtree of ancestor's right child
    //--------------------------------------------------------------------------------
    else {
      // Perform double left rotation (actually a right followed by a left)
      rotateRight(PMEM(ancestor)->right);
      rotateLeft(ancestor);
      adjustRightLeft(ancestor, np);
    }
  }

  //------------------------------------------------------------------
  // Adjust the balance factor in all nodes from the inserted node's
  //   parent back up to but NOT including a designated end node.
  // @param end– last node back up the tree that needs adjusting
  // @param start – node just inserted
  //------------------------------------------------------------------
  void adjustBalanceFactors(node* end, node* start) {
    node* temp = PMEM(start)->parent;  // Set starting point at start's parent

    while (temp != end) {
      if (PMEM(start)->key < PMEM(temp)->key)
        PMEM(temp)->bal = 'L';
      else
        PMEM(temp)->bal = 'R';
      temp = PMEM(temp)->parent;
    }
  }

  //------------------------------------------------------------------
  // rotateLeft()
  // Perform a single rotation left about n.  This will rotate n's
  //   parent to become n's left child.  Then n's left child will
  //   become the former parent's right child.
  //------------------------------------------------------------------
  void rotateLeft(node* n) {
    node* temp = PMEM(n)->right;   //Hold pointer to n's right child
    PMEM(n)->right = PMEM(temp)->left;  // Move temp 's left child to right child of n

    if (PMEM(temp)->left != NULL)      // If the left child does exist
      PMEM(PMEM(temp)->left)->parent = n;      // Reset the left child's parent

    if (PMEM(n)->parent == NULL)       // If n was the root
    {
      (*PMEM(root)) = temp;      // Make temp the new root
      PMEM(temp)->parent = NULL;   // Root has no parent
    } else if (PMEM(PMEM(n)->parent)->left == n)  // If n was the left child of its' parent
      PMEM(PMEM(n)->parent)->left = temp;   // Make temp the new left child
    else
      // If n was the right child of its' parent
      PMEM(PMEM(n)->parent)->right = temp;      // Make temp the new right child

    PMEM(temp)->left = n;         // Move n to left child of temp
    PMEM(n)->parent = temp;         // Reset n's parent
  }

  //------------------------------------------------------------------
  // rotateRight()
  // Perform a single rotation right about n.  This will rotate n's
  //   parent to become n's right child.  Then n's right child will
  //   become the former parent's left child.
  //------------------------------------------------------------------
  void rotateRight(node* n) {
    node* temp = PMEM(n)->left;   //Hold pointer to temp
    PMEM(n)->left = PMEM(temp)->right;  // Move temp's right child to left child of n

    if (PMEM(temp)->right != NULL)      // If the right child does exist
      PMEM(PMEM(temp)->right)->parent = n;      // Reset right child's parent

    if (PMEM(n)->parent == NULL)       // If n was root
    {
      (*PMEM(root)) = temp;      // Make temp the root
      PMEM(temp)->parent = NULL;   // Root has no parent
    } else if (PMEM(PMEM(n)->parent)->left == n)  // If was the left child of its' parent
      PMEM(PMEM(n)->parent)->left = temp;   // Make temp the new left child
    else
      // If n was the right child of its' parent
      PMEM(PMEM(n)->parent)->right = temp;      // Make temp the new right child

    PMEM(temp)->right = n;         // Move n to right child of temp
    PMEM(n)->parent = temp;         // Reset n's parent
  }

  //------------------------------------------------------------------
  // adjustLeftRight()
  // @param end- last node back up the tree that needs adjusting
  // @param start - node just inserted
  //------------------------------------------------------------------
  void adjustLeftRight(node* end, node* start) {
    if (end == (*PMEM(root)))
      PMEM(end)->bal = '=';
    else if (PMEM(start)->key < PMEM(PMEM(end)->parent)->key) {
      PMEM(end)->bal = 'R';
      adjustBalanceFactors(PMEM(PMEM(end)->parent)->left, start);
    } else {
      PMEM(end)->bal = '=';
      PMEM(PMEM(PMEM(end)->parent)->left)->bal = 'L';
      adjustBalanceFactors(end, start);
    }
  }

  //------------------------------------------------------------------
  // adjustRightLeft
  // @param end- last node back up the tree that needs adjusting
  // @param start - node just inserted
  //------------------------------------------------------------------
  void adjustRightLeft(node *end, node *start) {
    if (end == (*PMEM(root)))
      PMEM(end)->bal = '=';
    else if (PMEM(start)->key > PMEM(PMEM(end)->parent)->key) {
      PMEM(end)->bal = 'L';
      adjustBalanceFactors(PMEM(PMEM(end)->parent)->right, start);
    } else {
      PMEM(end)->bal = '=';
      PMEM(PMEM(PMEM(end)->parent)->right)->bal = 'R';
      adjustBalanceFactors(end, start);
    }
  }

  void display() {
    node* temp = (*PMEM(root));
    if (temp == NULL) {
      cout << "Empty tree \n";
      return;
    }

    cout << "Tree \n";
    display_node(temp);
    cout << "----------------------------- \n";
  }

  void display_node(node* n) {
    if (n != NULL) {
      cout << "key: " << PMEM(n)->key << " val : " << PMEM(n)->val << "\n";

      if (PMEM(n)->left != NULL) {
        cout << "left :" << "\n";
        display_node(PMEM(n)->left);
      }
      if (PMEM(n)->right != NULL) {
        cout << "right :" << "\n";
        display_node(PMEM(n)->right);
      }
    }
  }

};

void* operator new(size_t sz) throw (bad_alloc) {
  std::lock_guard<std::mutex> lock(pmp_mutex);
  return PMEM(pmemalloc_reserve(pmp, sz));
}

void operator delete(void *p) throw () {
  std::lock_guard<std::mutex> lock(pmp_mutex);
  pmemalloc_free_absolute(pmp, p);
}

int main() {
  const char* path = "./testfile";

  long pmp_size = 10 * 1024 * 1024;
  if ((pmp = pmemalloc_init(path, pmp_size)) == NULL)
    cout << "pmemalloc_init on :" << path << endl;

  sp = (struct static_info *) pmemalloc_static_area(pmp);

  ptree* tree;

  // Create a tree and test case 1
  tree = new ptree(&sp->ptrs[0]);
  cout << "-----------------------------------------------\n";
  cout << "TESTING CASE 1\n\n";
  cout << "Adding Node 50\n";
  tree->insert(50, 0);
  cout << "Adding Node 20\n";
  tree->insert(20, 0);
  cout << "Adding Node 70\n";
  tree->insert(70, 0);
  cout << "Adding Node 30\n";
  tree->insert(30, 0);
  cout << "Adding Node 10\n";
  tree->insert(10, 0);
  cout << "Adding Node 90\n";
  tree->insert(90, 0);
  cout << "Adding Node 60\n";
  tree->insert(60, 0);
  cout << "***** Adding Node to trigger test of case 1\n";
  cout << "Adding Node 55\n";
  tree->insert(55, 0);
  cout << "END TESTING CASE 1\n";
  tree->display();
  delete tree;

  // Create a tree and test case 2
  tree = new ptree(&sp->ptrs[0]);
  cout << "-----------------------------------------------\n";
  cout << "TESTING CASE 2\n\n";
  cout << "Adding Node 50\n";
  tree->insert(50, 0);
  cout << "Adding Node 20\n";
  tree->insert(20, 0);
  cout << "Adding Node 70\n";
  tree->insert(70, 0);
  cout << "Adding Node 30\n";
  tree->insert(30, 0);
  cout << "Adding Node 10\n";
  tree->insert(10, 0);
  cout << "Adding Node 90\n";
  tree->insert(90, 0);
  cout << "Adding Node 60\n";
  tree->insert(60, 0);
  cout << "Adding Node 55\n";
  tree->insert(55, 0);
  cout << "***** Adding Node to trigger test of case 2\n";
  cout << "Adding Node 28\n";
  tree->insert(28, 0);
  cout << "END TESTING CASE 2\n";
  tree->display();
  delete tree;

  // Create a tree and test case 3
  tree = new ptree(&sp->ptrs[0]);
  cout << "-----------------------------------------------\n";
  cout << "TESTING CASE 3\n\n";
  cout << "Adding Node 50\n";
  tree->insert(50, 0);
  cout << "Adding Node 20\n";
  tree->insert(20, 0);
  cout << "Adding Node 70\n";
  tree->insert(70, 0);
  cout << "Adding Node 10\n";
  tree->insert(10, 0);
  cout << "Adding Node 90\n";
  tree->insert(90, 0);
  cout << "Adding Node 60\n";
  tree->insert(60, 0);
  cout << "Adding Node 80\n";
  tree->insert(80, 0);
  cout << "Adding Node 98\n";
  tree->insert(98, 0);
  cout << "***** Adding Node to trigger test of case 3\n";
  cout << "Adding Node 93\n";
  tree->insert(93, 0);
  cout << "END TESTING CASE 3\n";
  tree->display();
  delete tree;

  // Create a tree and test case 4
  tree = new ptree(&sp->ptrs[0]);
  cout << "-----------------------------------------------\n";
  cout << "TESTING CASE 4\n\n";
  cout << "Adding Node 50\n";
  tree->insert(50, 0);
  cout << "Adding Node 20\n";
  tree->insert(20, 0);
  cout << "Adding Node 70\n";
  tree->insert(70, 0);
  cout << "Adding Node 10\n";
  tree->insert(10, 0);
  cout << "Adding Node 30\n";
  tree->insert(30, 0);
  cout << "Adding Node 90\n";
  tree->insert(90, 0);
  cout << "Adding Node 60\n";
  tree->insert(60, 0);
  cout << "Adding Node 5\n";
  tree->insert(5, 0);
  cout << "Adding Node 15\n";
  tree->insert(15, 0);
  cout << "Adding Node 25\n";
  tree->insert(25, 0);
  cout << "Adding Node 35\n";
  tree->insert(35, 0);
  cout << "***** Adding Node to trigger test of case 4\n";
  cout << "Adding Node 13\n";
  tree->insert(13, 0);
  cout << "END TESTING CASE 4\n";
  tree->display();
  delete tree;

  // Create a tree and test case 5
  tree = new ptree(&sp->ptrs[0]);
  cout << "-----------------------------------------------\n";
  cout << "TESTING CASE 5\n\n";
  cout << "Adding Node 50\n";
  tree->insert(50, 0);
  cout << "Adding Node 20\n";
  tree->insert(20, 0);
  cout << "Adding Node 90\n";
  tree->insert(90, 0);
  cout << "Adding Node 10\n";
  tree->insert(10, 0);
  cout << "Adding Node 40\n";
  tree->insert(40, 0);
  cout << "Adding Node 70\n";
  tree->insert(70, 0);
  cout << "Adding Node 100\n";
  tree->insert(100, 0);
  cout << "Adding Node 5\n";
  tree->insert(5, 0);
  cout << "Adding Node 15\n";
  tree->insert(15, 0);
  cout << "Adding Node 30\n";
  tree->insert(30, 0);
  cout << "Adding Node 45\n";
  tree->insert(45, 0);
  cout << "***** Adding Node to trigger test of case 5\n";
  cout << "Adding Node 35\n";
  tree->insert(35, 0);
  cout << "END TESTING CASE 5\n";
  tree->display();
  delete tree;

  // Create a tree and test case 6
  tree = new ptree(&sp->ptrs[0]);
  cout << "-----------------------------------------------\n";
  cout << "TESTING CASE 6\n\n";
  cout << "Adding Node 50\n";
  tree->insert(50, 0);
  cout << "Adding Node 20\n";
  tree->insert(20, 0);
  cout << "Adding Node 80\n";
  tree->insert(80, 0);
  cout << "Adding Node 70\n";
  tree->insert(70, 0);
  cout << "Adding Node 90\n";
  tree->insert(90, 0);
  cout << "***** Adding Node to trigger test of case 6\n";
  cout << "Adding Node 75\n";
  tree->insert(75, 0);
  cout << "END TESTING CASE 6\n";
  tree->display();
  delete tree;

  cout << "All testing complete.\n";
}

