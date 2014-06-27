#include <iostream>

using namespace std;

struct node {
  int key;

  node *left;
  node *right;
  node *parent;
  char balanceFactor;
};

class ptree {
 private:
  node *root;

 public:
  ptree() {
    root = NULL;
  }

  ~ptree() {
    clear(root);
  }

  void clear(node *n) {
    if (n != NULL) {
      clear(n->left);
      clear(n->right);
      delete n;
    }
  }

  void insert(node *newNode) {
    node *temp, *back, *ancestor;

    temp = root;
    back = NULL;
    ancestor = NULL;

    // Check for empty tree first
    if (root == NULL) {
      root = newNode;
      return;
    }
    // Tree is not empty so search for place to insert
    while (temp != NULL)  // Loop till temp falls out of the tree
    {
      back = temp;
      // Mark ancestor that will be out of balance after
      //   this node is inserted
      if (temp->balanceFactor != '=')
        ancestor = temp;
      if (newNode->key < temp->key)
        temp = temp->left;
      else
        temp = temp->right;
    }
    // temp is now NULL
    // back points to parent node to attach newNode to
    // ancestor points to most recent out of balance ancestor

    newNode->parent = back;   // Set parent
    if (newNode->key < back->key)  // Insert at left
        {
      back->left = newNode;
    } else     // Insert at right
    {
      back->right = newNode;
    }

    // Now call function to restore the tree's AVL property
    restoreAVL(ancestor, newNode);
  }

  void restoreAVL(node *ancestor, node *newNode) {
    //--------------------------------------------------------------------------------
    // Case 1: ancestor is NULL, i.e. balanceFactor of all ancestors' is '='
    //--------------------------------------------------------------------------------
    if (ancestor == NULL) {
      if (newNode->key < root->key)       // newNode inserted to left of root
        root->balanceFactor = 'L';
      else
        root->balanceFactor = 'R';   // newNode inserted to right of root
      // Adjust the balanceFactor for all nodes from newNode back up to root
      adjustBalanceFactors(root, newNode);
    }

    //--------------------------------------------------------------------------------
    // Case 2: Insertion in opposite subtree of ancestor's balance factor, i.e.
    //  ancestor.balanceFactor = 'L' AND  Insertion made in ancestor's right subtree
    //     OR
    //  ancestor.balanceFactor = 'R' AND  Insertion made in ancestor's left subtree
    //--------------------------------------------------------------------------------
    else if (((ancestor->balanceFactor == 'L') && (newNode->key > ancestor->key))
        || ((ancestor->balanceFactor == 'R') && (newNode->key < ancestor->key))) {
      ancestor->balanceFactor = '=';
      // Adjust the balanceFactor for all nodes from newNode back up to ancestor
      adjustBalanceFactors(ancestor, newNode);
    }
    //--------------------------------------------------------------------------------
    // Case 3: ancestor.balanceFactor = 'R' and the node inserted is
    //      in the right subtree of ancestor's right child
    //--------------------------------------------------------------------------------
    else if ((ancestor->balanceFactor == 'R')
        && (newNode->key > ancestor->right->key)) {
      ancestor->balanceFactor = '=';  // Reset ancestor's balanceFactor
      rotateLeft(ancestor);       // Do single left rotation about ancestor
      // Adjust the balanceFactor for all nodes from newNode back up to ancestor's parent
      adjustBalanceFactors(ancestor->parent, newNode);
    }

    //--------------------------------------------------------------------------------
    // Case 4: ancestor.balanceFactor is 'L' and the node inserted is
    //      in the left subtree of ancestor's left child
    //--------------------------------------------------------------------------------
    else if ((ancestor->balanceFactor == 'L')
        && (newNode->key < ancestor->left->key)) {
      ancestor->balanceFactor = '=';  // Reset ancestor's balanceFactor
      rotateRight(ancestor);       // Do single right rotation about ancestor
      // Adjust the balanceFactor for all nodes from newNode back up to ancestor's parent
      adjustBalanceFactors(ancestor->parent, newNode);
    }

    //--------------------------------------------------------------------------------
    // Case 5: ancestor.balanceFactor is 'L' and the node inserted is
    //      in the right subtree of ancestor's left child
    //--------------------------------------------------------------------------------
    else if ((ancestor->balanceFactor == 'L')
        && (newNode->key > ancestor->left->key)) {
      // Perform double right rotation (actually a left followed by a right)
      rotateLeft(ancestor->left);
      rotateRight(ancestor);
      // Adjust the balanceFactor for all nodes from newNode back up to ancestor
      adjustLeftRight(ancestor, newNode);
    }

    //--------------------------------------------------------------------------------
    // Case 6: ancestor.balanceFactor is 'R' and the node inserted is
    //      in the left subtree of ancestor's right child
    //--------------------------------------------------------------------------------
    else {
      // Perform double left rotation (actually a right followed by a left)
      rotateRight(ancestor->right);
      rotateLeft(ancestor);
      adjustRightLeft(ancestor, newNode);
    }
  }

  //------------------------------------------------------------------
  // Adjust the balance factor in all nodes from the inserted node's
  //   parent back up to but NOT including a designated end node.
  // @param end– last node back up the tree that needs adjusting
  // @param start – node just inserted
  //------------------------------------------------------------------
  void adjustBalanceFactors(node *end, node *start) {
    node *temp = start->parent;  // Set starting point at start's parent
    while (temp != end) {
      if (start->key < temp->key)
        temp->balanceFactor = 'L';
      else
        temp->balanceFactor = 'R';
      temp = temp->parent;
    }  // end while
  }

  //------------------------------------------------------------------
  // rotateLeft()
  // Perform a single rotation left about n.  This will rotate n's
  //   parent to become n's left child.  Then n's left child will
  //   become the former parent's right child.
  //------------------------------------------------------------------
  void rotateLeft(node *n) {
    node *temp = n->right;   //Hold pointer to n's right child
    n->right = temp->left;      // Move temp 's left child to right child of n
    if (temp->left != NULL)      // If the left child does exist
      temp->left->parent = n;      // Reset the left child's parent
    if (n->parent == NULL)       // If n was the root
    {
      root = temp;      // Make temp the new root
      temp->parent = NULL;   // Root has no parent
    } else if (n->parent->left == n)  // If n was the left child of its' parent
      n->parent->left = temp;   // Make temp the new left child
    else
      // If n was the right child of its' parent
      n->parent->right = temp;               // Make temp the new right child

    temp->left = n;         // Move n to left child of temp
    n->parent = temp;         // Reset n's parent
  }

  //------------------------------------------------------------------
  // rotateRight()
  // Perform a single rotation right about n.  This will rotate n's
  //   parent to become n's right child.  Then n's right child will
  //   become the former parent's left child.
  //------------------------------------------------------------------
  void rotateRight(node *n) {
    node *temp = n->left;   //Hold pointer to temp
    n->left = temp->right;      // Move temp's right child to left child of n
    if (temp->right != NULL)      // If the right child does exist
      temp->right->parent = n;      // Reset right child's parent
    if (n->parent == NULL)       // If n was root
    {
      root = temp;      // Make temp the root
      temp->parent = NULL;   // Root has no parent
    } else if (n->parent->left == n)  // If was the left child of its' parent
      n->parent->left = temp;   // Make temp the new left child
    else
      // If n was the right child of its' parent
      n->parent->right = temp;               // Make temp the new right child

    temp->right = n;         // Move n to right child of temp
    n->parent = temp;         // Reset n's parent
  }

  //------------------------------------------------------------------
  // adjustLeftRight()
  // @param end- last node back up the tree that needs adjusting
  // @param start - node just inserted
  //------------------------------------------------------------------
  void adjustLeftRight(node *end, node *start) {
    if (end == root)
      end->balanceFactor = '=';
    else if (start->key < end->parent->key) {
      end->balanceFactor = 'R';
      adjustBalanceFactors(end->parent->left, start);
    } else {
      end->balanceFactor = '=';
      end->parent->left->balanceFactor = 'L';
      adjustBalanceFactors(end, start);
    }
  }

  //------------------------------------------------------------------
  // adjustRightLeft
  // @param end- last node back up the tree that needs adjusting
  // @param start - node just inserted
  //------------------------------------------------------------------
  void adjustRightLeft(node *end, node *start) {
    if (end == root)
      end->balanceFactor = '=';
    else if (start->key > end->parent->key) {
      end->balanceFactor = 'L';
      adjustBalanceFactors(end->parent->right, start);
    } else {
      end->balanceFactor = '=';
      end->parent->right->balanceFactor = 'R';
      adjustBalanceFactors(end, start);
    }
  }

  void display() {
    cout << "\nPrinting the tree...\n";
    cout << "Root Node: " << root->key << " balanceFactor is "
         << root->balanceFactor << "\n\n";
    display_node(root);
  }

  void display_node(node *n) {
    if (n != NULL) {
      cout << "Node: " << n->key << " balanceFactor is " << n->balanceFactor
           << "\n";
      if (n->left != NULL) {
        cout << "\t moving left\n";
        display_node(n->left);
        cout << "Returning to Node" << n->key << " from its' left subtree\n";
      } else {
        cout << "\t left subtree is empty\n";
      }
      cout << "Node: " << n->key << " balanceFactor is " << n->balanceFactor
           << "\n";
      if (n->right != NULL) {
        cout << "\t moving right\n";
        display_node(n->right);
        cout << "Returning to Node" << n->key << " from its' right subtree\n";
      } else {
        cout << "\t right subtree is empty\n";
      }
    }
  }

};

#include <iostream>

using namespace std;

//---------------------------------------------
// Create a new tree node with the given key
//---------------------------------------------
node* createNewNode(int key) {
  node *temp = new node();
  temp->key = key;
  temp->left = NULL;
  temp->right = NULL;
  temp->parent = NULL;
  temp->balanceFactor = '=';
  return temp;
}

int main() {
  ptree* tree;
  char ans[32];

  // Test each case by adding some nodes to the tree then
  //  printing the tree after each insertion

  // Create a tree and test case 1
  tree = new ptree();
  cout << "-----------------------------------------------\n";
  cout << "TESTING CASE 1\n\n";
  cout << "Adding Node 50\n";
  tree->insert(createNewNode(50));
  cout << "Adding Node 20\n";
  tree->insert(createNewNode(20));
  cout << "Adding Node 70\n";
  tree->insert(createNewNode(70));
  cout << "Adding Node 30\n";
  tree->insert(createNewNode(30));
  cout << "Adding Node 10\n";
  tree->insert(createNewNode(10));
  cout << "Adding Node 90\n";
  tree->insert(createNewNode(90));
  cout << "Adding Node 60\n";
  tree->insert(createNewNode(60));
  cout << "***** Adding Node to trigger test of case 1\n";
  cout << "Adding Node 55\n";
  tree->insert(createNewNode(55));
  cout << "END TESTING CASE 1\n";
  delete tree;

  // Create a tree and test case 2
  tree = new ptree();
  cout << "-----------------------------------------------\n";
  cout << "TESTING CASE 2\n\n";
  cout << "Adding Node 50\n";
  tree->insert(createNewNode(50));
  cout << "Adding Node 20\n";
  tree->insert(createNewNode(20));
  cout << "Adding Node 70\n";
  tree->insert(createNewNode(70));
  cout << "Adding Node 30\n";
  tree->insert(createNewNode(30));
  cout << "Adding Node 10\n";
  tree->insert(createNewNode(10));
  cout << "Adding Node 90\n";
  tree->insert(createNewNode(90));
  cout << "Adding Node 60\n";
  tree->insert(createNewNode(60));
  cout << "Adding Node 55\n";
  tree->insert(createNewNode(55));
  cout << "***** Adding Node to trigger test of case 2\n";
  cout << "Adding Node 28\n";
  tree->insert(createNewNode(28));
  cout << "END TESTING CASE 2\n";
  delete tree;

  // Create a tree and test case 3
  tree = new ptree();
  cout << "-----------------------------------------------\n";
  cout << "TESTING CASE 3\n\n";
  cout << "Adding Node 50\n";
  tree->insert(createNewNode(50));
  cout << "Adding Node 20\n";
  tree->insert(createNewNode(20));
  cout << "Adding Node 70\n";
  tree->insert(createNewNode(70));
  cout << "Adding Node 10\n";
  tree->insert(createNewNode(10));
  cout << "Adding Node 90\n";
  tree->insert(createNewNode(90));
  cout << "Adding Node 60\n";
  tree->insert(createNewNode(60));
  cout << "Adding Node 80\n";
  tree->insert(createNewNode(80));
  cout << "Adding Node 98\n";
  tree->insert(createNewNode(98));
  cout << "***** Adding Node to trigger test of case 3\n";
  cout << "Adding Node 93\n";
  tree->insert(createNewNode(93));
  cout << "END TESTING CASE 3\n";
  delete tree;
  cout << "-----------------------------------------------\n";
  cout << "-----------------------------------------------\n";

  // Create a tree and test case 4
  tree = new ptree();
  cout << "-----------------------------------------------\n";
  cout << "TESTING CASE 4\n\n";
  cout << "Adding Node 50\n";
  tree->insert(createNewNode(50));
  cout << "Adding Node 20\n";
  tree->insert(createNewNode(20));
  cout << "Adding Node 70\n";
  tree->insert(createNewNode(70));
  cout << "Adding Node 10\n";
  tree->insert(createNewNode(10));
  cout << "Adding Node 30\n";
  tree->insert(createNewNode(30));
  cout << "Adding Node 90\n";
  tree->insert(createNewNode(90));
  cout << "Adding Node 60\n";
  tree->insert(createNewNode(60));
  cout << "Adding Node 5\n";
  tree->insert(createNewNode(5));
  cout << "Adding Node 15\n";
  tree->insert(createNewNode(15));
  cout << "Adding Node 25\n";
  tree->insert(createNewNode(25));
  cout << "Adding Node 35\n";
  tree->insert(createNewNode(35));
  cout << "***** Adding Node to trigger test of case 4\n";
  cout << "Adding Node 13\n";
  tree->insert(createNewNode(13));
  cout << "END TESTING CASE 4\n";
  delete tree;

  // Create a tree and test case 5
  tree = new ptree();
  cout << "-----------------------------------------------\n";
  cout << "TESTING CASE 5\n\n";
  cout << "Adding Node 50\n";
  tree->insert(createNewNode(50));
  cout << "Adding Node 20\n";
  tree->insert(createNewNode(20));
  cout << "Adding Node 90\n";
  tree->insert(createNewNode(90));
  cout << "Adding Node 10\n";
  tree->insert(createNewNode(10));
  cout << "Adding Node 40\n";
  tree->insert(createNewNode(40));
  cout << "Adding Node 70\n";
  tree->insert(createNewNode(70));
  cout << "Adding Node 100\n";
  tree->insert(createNewNode(100));
  cout << "Adding Node 5\n";
  tree->insert(createNewNode(5));
  cout << "Adding Node 15\n";
  tree->insert(createNewNode(15));
  cout << "Adding Node 30\n";
  tree->insert(createNewNode(30));
  cout << "Adding Node 45\n";
  tree->insert(createNewNode(45));
  cout << "***** Adding Node to trigger test of case 5\n";
  cout << "Adding Node 35\n";
  tree->insert(createNewNode(35));
  cout << "END TESTING CASE 5\n";
  delete tree;

  // Create a tree and test case 6
  tree = new ptree();
  cout << "-----------------------------------------------\n";
  cout << "TESTING CASE 6\n\n";
  cout << "Adding Node 50\n";
  tree->insert(createNewNode(50));
  cout << "Adding Node 20\n";
  tree->insert(createNewNode(20));
  cout << "Adding Node 80\n";
  tree->insert(createNewNode(80));
  cout << "Adding Node 70\n";
  tree->insert(createNewNode(70));
  cout << "Adding Node 90\n";
  tree->insert(createNewNode(90));
  cout << "***** Adding Node to trigger test of case 6\n";
  cout << "Adding Node 75\n";
  tree->insert(createNewNode(75));
  cout << "END TESTING CASE 6\n";
  delete tree;
  cout << "All testing complete.\n";
}

