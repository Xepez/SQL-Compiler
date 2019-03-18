#ifndef _QUERY_OPTIMIZER_H
#define _QUERY_OPTIMIZER_H

#include "Schema.h"
#include "Catalog.h"
#include "ParseTree.h"
#include "RelOp.h"

#include <string>
#include <vector>

using namespace std;

struct joinDT{  // Join Data Type
    // Table Name of the join
    string tableName;
    // Att Name of the join
    string attName;
    // Cost of the join w/ select factors
    int amt;
};

struct joinRE {
    // Left and Right Childs
    joinDT* left;
    joinDT* right;
    // total value of amounts
    int total;
    // Position in Join Tree based on amt
    int pos;
};

// data structure used by the optimizer to compute join ordering
struct OptimizationTree {
	// list of tables joined up to this node
	vector<string> tables;
	// number of tuples in each of the tables (after selection predicates)
	vector<int> tuples;
	// number of tuples at this node
	int noTuples;

	// connections to children and parent
	OptimizationTree* parent;
	OptimizationTree* leftChild;
	OptimizationTree* rightChild;
};

class QueryOptimizer {
private:
	Catalog* catalog;
	vector<Scan*> scanTables;
	vector<Scan*> scanTuples;
    
    TableList* tables;
    AndList* predicate;
    
    // Holds all our joinRE's created
    vector<joinRE*> JRE;
    // Holds all the optimization trees
    vector<OptimizationTree*> OT;
    // Holds table names
    vector<string> tblName;
    
    // Delete Vectors
    vector<joinDT*> deleteMe1;
    vector<joinRE*> deleteMe2;
    vector<OptimizationTree*> deleteMe3;
    
public:
	QueryOptimizer(Catalog& _catalog);
	virtual ~QueryOptimizer();
    
    bool getTable(string _att, string& _tableName, unsigned int& _distinct);
    
    void getOptimizedTotal(string tableName, unsigned int &tableTuple);
    
    joinRE* getJoinRE(int _position);
    
    void singleTable(OptimizationTree* _root, string tableName);
    
    void printTree(OptimizationTree* _root);
    
    bool getAvailableTable(vector<string> _root, string &fName);

	void Optimize(TableList* _tables, AndList* _predicate, OptimizationTree* _root);
};

#endif // _QUERY_OPTIMIZER_H
