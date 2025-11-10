#include <bits/stdc++.h>
using namespace std;

// ======================================================================
// CSCI 5820 – Database Management Systems II
// Homework 1 – B+ Tree Index Implementation
// Author: Ricardo Manjarrez
// ======================================================================

struct Stats {
    int diskReads = 0;
    int diskWrites = 0;
    int totalNodes = 0;
    int totalKeys = 0;
    int treeHeight = 0;
    void reset() { diskReads = diskWrites = 0; }
};

// ----------------------------------------------------------------------
// Node structure: stores (key, pointer) pairs for leaves
// ----------------------------------------------------------------------
struct Node {
    bool isLeaf;
    vector<int> keys;
    vector<int> pointers;       // used for leaf nodes
    vector<Node*> children;     // used for internal nodes
    Node* next;                 // leaf node linkage

    Node(bool leaf = false) {
        isLeaf = leaf;
        next = nullptr;
    }
};

// ----------------------------------------------------------------------
// B+ Tree Class
// ----------------------------------------------------------------------
class BPlusTree {
public:
    Node* root;
    int t; // order
    Stats stats;
    int pageSize;

    BPlusTree(int degree = 3, int ps = 512) {
        root = nullptr;
        t = degree;
        pageSize = ps;
    }

    // Core Operations
    void insert(int key, int ptr);
    void search(int key);
    void remove(int key);
    void rangeSearch(int k1, int k2);
    void print();
    void printStatistics();

private:
    void splitChild(Node* parent, int i, Node* child);
    void insertNonFull(Node* node, int key, int ptr);
    void printLevelOrder();
};

// ----------------------------------------------------------------------
// Split a full child node
// ----------------------------------------------------------------------
void BPlusTree::splitChild(Node* parent, int i, Node* child) {
    stats.diskReads++;   // read existing child from disk
    Node* newChild = new Node(child->isLeaf);
    stats.totalNodes++;
    stats.diskWrites++;  // wrote new child to disk

    newChild->keys.assign(child->keys.begin() + t, child->keys.end());
    child->keys.resize(t);

    if (child->isLeaf) {
        newChild->pointers.assign(child->pointers.begin() + t, child->pointers.end());
        child->pointers.resize(t);
        newChild->next = child->next;
        child->next = newChild;

        parent->keys.insert(parent->keys.begin() + i, newChild->keys[0]);
        parent->children.insert(parent->children.begin() + i + 1, newChild);
    } else {
        newChild->children.assign(child->children.begin() + t, child->children.end());
        child->children.resize(t);
        parent->keys.insert(parent->keys.begin() + i, child->keys[t - 1]);
        parent->children.insert(parent->children.begin() + i + 1, newChild);
        child->keys.resize(t - 1);
    }
    stats.diskWrites++; // write parent after modification
}

// ----------------------------------------------------------------------
// Insert into a non-full node
// ----------------------------------------------------------------------
void BPlusTree::insertNonFull(Node* node, int key, int ptr) {
    stats.diskReads++;  // read node from disk

    if (node->isLeaf) {
        auto it = lower_bound(node->keys.begin(), node->keys.end(), key);
        if (it != node->keys.end() && *it == key) {
            cout << "(" << key << ", " << ptr << ") not inserted. " << key << " found.\n";
            return;
        }
        int pos = it - node->keys.begin();
        node->keys.insert(it, key);
        node->pointers.insert(node->pointers.begin() + pos, ptr);
        stats.totalKeys++;
        stats.diskWrites++;   // wrote modified leaf
        cout << "(" << key << ", " << ptr << ") inserted\n";
    } else {
        int i = node->keys.size() - 1;
        while (i >= 0 && key < node->keys[i]) i--;
        i++;

        stats.diskReads++;  // read child pointer
        if (node->children[i]->keys.size() == 2 * t - 1) {
            splitChild(node, i, node->children[i]);
            if (key > node->keys[i]) i++;
        }
        insertNonFull(node->children[i], key, ptr);
    }
}

// ----------------------------------------------------------------------
// Insert interface
// ----------------------------------------------------------------------
void BPlusTree::insert(int key, int ptr) {
    //stats.reset();

    if (!root) {
        root = new Node(true);
        root->keys.push_back(key);
        root->pointers.push_back(ptr);
        stats.totalNodes = 1;
        stats.totalKeys = 1;
        stats.treeHeight = 1;
        stats.diskWrites++;   // wrote first node
        cout << "(" << key << ", " << ptr << ") inserted\n";
        return;
    }

    if (root->keys.size() == 2 * t - 1) {
        Node* newRoot = new Node(false);
        newRoot->children.push_back(root);
        stats.diskReads++;   // read old root
        splitChild(newRoot, 0, root);
        root = newRoot;
        stats.treeHeight++;
        stats.diskWrites++;  // wrote new root
    }

    insertNonFull(root, key, ptr);
}

// ----------------------------------------------------------------------
// Search
// ----------------------------------------------------------------------
void BPlusTree::search(int key) {
    //stats.reset();
    Node* current = root;
    if (!current) {
        cout << key << " not found\n";
        return;
    }
    while (current) {
        stats.diskReads++;   // simulate node read
        int i = 0;
        while (i < current->keys.size() && key > current->keys[i]) i++;
        if (current->isLeaf) {
            if (i < current->keys.size() && key == current->keys[i]) {
                cout << key << " found, point is " << current->pointers[i] << "\n";
                return;
            } else {
                cout << key << " not found\n";
                return;
            }
        } else {
            current = current->children[i];
        }
    }
    cout << key << " not found\n";
}

// ----------------------------------------------------------------------
// Delete (simplified – no rebalance needed for assignment)
// ----------------------------------------------------------------------
void BPlusTree::remove(int key) {
    //stats.reset();
    if (!root) {
        cout << key << " not found, not deleted.\n";
        return;
    }

    Node* current = root;
    while (current && !current->isLeaf) {
        stats.diskReads++;  // read internal node
        int i = 0;
        while (i < current->keys.size() && key > current->keys[i]) i++;
        current = current->children[i];
    }

    if (!current) return;

    stats.diskReads++;  // read leaf
    auto it = find(current->keys.begin(), current->keys.end(), key);
    if (it == current->keys.end()) {
        cout << key << " not found, not deleted.\n";
        return;
    }

    int idx = it - current->keys.begin();
    current->keys.erase(it);
    current->pointers.erase(current->pointers.begin() + idx);
    stats.totalKeys--;
    stats.diskWrites++;  // wrote modified leaf
    cout << key << " deleted.\n";
}

// ----------------------------------------------------------------------
// Range search
// ----------------------------------------------------------------------
void BPlusTree::rangeSearch(int k1, int k2) {
    //stats.reset();
    if (!root) {
        cout << "no records in the range [" << k1 << ", " << k2 << "]\n";
        return;
    }

    Node* current = root;
    while (!current->isLeaf) {
        stats.diskReads++;  // read internal node
        int i = 0;
        while (i < current->keys.size() && k1 > current->keys[i]) i++;
        current = current->children[i];
    }

    bool foundAny = false;
    while (current) {
        stats.diskReads++;  // read each leaf
        for (size_t i = 0; i < current->keys.size(); i++) {
            int key = current->keys[i];
            if (key >= k1 && key <= k2) {
                if (!foundAny) {
                    cout << "found\n";
                    foundAny = true;
                }
                cout << "(" << key << ", " << current->pointers[i] << ")\n";
            } else if (key > k2) {
                if (!foundAny)
                    cout << "no records in the range [" << k1 << ", " << k2 << "]\n";
                return;
            }
        }
        current = current->next;
    }
    if (!foundAny)
        cout << "no records in the range [" << k1 << ", " << k2 << "]\n";
}

// ----------------------------------------------------------------------
// Print level order
// ----------------------------------------------------------------------
void BPlusTree::printLevelOrder() {
    if (!root) {
        cout << "(empty)\n";
        return;
    }
    queue<pair<Node*, int>> q;
    q.push({root, 0});
    int currentLevel = -1;
    while (!q.empty()) {
        auto [node, lvl] = q.front();
        q.pop();
        if (lvl != currentLevel) {
            if (currentLevel != -1) cout << "\n";
            cout << "Level " << lvl << ": ";
            currentLevel = lvl;
        }
        cout << "[";
        if (node->isLeaf) {
            for (size_t i = 0; i < node->keys.size(); i++) {
                cout << "(" << node->keys[i] << ": " << node->pointers[i] << ")";
                if (i + 1 < node->keys.size()) cout << " ";
            }
        } else {
            for (size_t i = 0; i < node->keys.size(); i++) {
                cout << "(" << node->keys[i] << ")";
                if (i + 1 < node->keys.size()) cout << " ";
            }
        }
        cout << "] ";
        for (Node* c : node->children) q.push({c, lvl + 1});
    }
    cout << "\n";
}

void BPlusTree::print() { printLevelOrder(); }

// ----------------------------------------------------------------------
// Print statistics
// ----------------------------------------------------------------------
void BPlusTree::printStatistics() {
    cout << "Tree Height: " << stats.treeHeight << "\n";
    cout << "Total Nodes: " << stats.totalNodes << "\n";
    cout << "Total Keys: " << stats.totalKeys << "\n";
    cout << "Disk Reads: " << stats.diskReads << "\n";
    cout << "Disk Writes: " << stats.diskWrites << "\n";
}

// ----------------------------------------------------------------------
// MAIN: supports init <pageSize> and command script
// ----------------------------------------------------------------------
int main(int argc, char* argv[]) {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    if (argc == 3 && string(argv[1]) == "init") {
        int ps = stoi(argv[2]);
        cout << "Initialized B+ tree with page size " << ps << " bytes.\n";
        ofstream("bplus_index.dat").close();
        return 0;
    }

    // Default B+ tree order=3, pageSize=512 (simulation)
    BPlusTree tree(3, 512);

    string cmd;
    while (cin >> cmd) {
        if (cmd == "INSERT") {
            int k, p; cin >> k >> p; tree.insert(k, p);
        } else if (cmd == "SEARCH") {
            int k; cin >> k; tree.search(k); tree.printStatistics();
        } else if (cmd == "DELETE") {
            int k; cin >> k; tree.remove(k); tree.printStatistics();
        } else if (cmd == "RANGESEARCH") {
            int a, b; cin >> a >> b; tree.rangeSearch(a, b); tree.printStatistics();
        } else if (cmd == "PRINT") {
            string maybe; streampos pos = cin.tellg();
            if (cin >> maybe) {
                if (maybe == "STATISTICS") tree.printStatistics();
                else { cin.seekg(pos); tree.print(); }
            } else tree.print();
        }
    }
    return 0;
}
