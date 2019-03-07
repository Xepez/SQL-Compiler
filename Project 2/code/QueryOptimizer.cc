#include <string>
#include <vector>
#include <iostream>

#include "Schema.h"
#include "Comparison.h"
#include "QueryOptimizer.h"

using namespace std;


QueryOptimizer::QueryOptimizer(Catalog& _catalog) : catalog(&_catalog) {

	catalog = &_catalog;

}

QueryOptimizer::~QueryOptimizer() {
}

bool QueryOptimizer::getTable(TableList* _tables, string _att, string& _tableName, unsigned int& _distinct) {
    // Gets table name related to the attribute and the number of distinct for it as well
    TableList* tableFind = _tables;
    while (tableFind != NULL) {
        string temp = tableFind->tableName;
        if (catalog->GetNoDistinct(temp, _att, _distinct)) {
            _tableName = temp;
            return true;
        }
        tableFind = tableFind->next;
    }
    
    return false;
}

void QueryOptimizer::Optimize(TableList* _tables, AndList* _predicate,
	OptimizationTree* _root) {
	// Computes the optimal join order
    
//    vector<string> tableVector;
//    vector<int> tuplesVector;
//    int numTup = 0;
    
	// Counts amount of tables we have
	TableList* tableCount = _tables;
    int count = 0;
    while (tableCount != NULL) {
        count++;
        tableCount = tableCount->next;
    }
    
    // If only one table we dont have a join
    if (count == 1) {
        //cout << "Only one count" << endl;
        // Pushes each table name into the root vector for names
        string tempName = _tables->tableName;
        _root->tables.push_back(tempName);
        
        // Number of tuples
        unsigned int tup = 0;
        catalog->GetNoTuples(tempName, tup);
        _root->tuples.push_back(tup);
        
        // Number of tuples
        _root->noTuples = 0;
    }
    // If we do have a join (mulitple tables)
    else {
        //cout << "Multiple Counts" << endl;
        
        unsigned int tDist;
        string tName;
        string left = _predicate->left->left->value;
        string right = _predicate->left->right->value;
        
        // Left
        if (getTable(_tables, left, tName, tDist)) {
            _root->tables.push_back(tName);
            _root->tuples.push_back(tDist);
        }
        // Right
        if (getTable(_tables, right, tName, tDist)) {
            _root->tables.push_back(tName);
            _root->tuples.push_back(tDist);
        }
        _root->noTuples = 0;
        
        char dir = 'l';
        
        //cout << "Searching for childern" << endl;
        
        AndList* currAnd = _predicate->rightAnd;
        OptimizationTree* temp = new OptimizationTree();
        while (currAnd != NULL) {
            left = currAnd->left->left->value;
            right = currAnd->left->right->value;
            
            // Left
            if (getTable(_tables, left, tName, tDist)) {
                temp->tables.push_back(tName);
                temp->tuples.push_back(tDist);
            }
            // Right
            if (getTable(_tables, right, tName, tDist)) {
                temp->tables.push_back(tName);
                temp->tuples.push_back(tDist);
            }
            temp->noTuples = 1;
            
            if (dir == 'l') {
                _root->leftChild = temp;
                dir = 'r';
            }
            else if (dir == 'r') {
                _root->rightChild = temp;
                dir = 'l';
            }
            
            currAnd = currAnd->rightAnd;
        }
    }
}
