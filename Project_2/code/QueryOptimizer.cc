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

void QueryOptimizer::getOptimizedTotal(TableList* _tables, AndList* _predicate, string tableName, unsigned int &tableTuple) {
    /*
     Determines the actual tuple value by taking into consideration
     previous selects
    */
    
    //cout << "Started finding the value" << endl;
     
    AndList* tempPred = _predicate;
    // Gets total number of tuples
    catalog->GetNoTuples(tableName, tableTuple);
    //cout << "Beginning Value: " << tableTuple << endl;
    
    while (tempPred != NULL) {
        if (tempPred->left != NULL) {
            string tempAtt;
            
            if (tempPred->left->left->code != 3 && tempPred->left->right->code == 3) {
                // If one side is a name and the other is a value
                // Indicates was a select
                tempAtt = tempPred->left->right->value;
            }
            else if (tempPred->left->left->code == 3 && tempPred->left->right->code != 3) {
                // If one side is a name and the other is a value
                // Indicates was a select
                tempAtt = tempPred->left->left->value;
            }
            
            if (tempAtt.empty()) {
                // No Value Found
                continue;
            }
            
            unsigned int tempDist;
            string tempTable;
            getTable(_tables, tempAtt, tempTable, tempDist);
            
            if (tempTable == tableName){
                if (tempPred->left->code == 7) {
                    // Value = 7 - "="
                    tableTuple /= tempDist;
                }
                else {
                    tableTuple /= 3;
                }
            }
        }
        
        tempPred = tempPred->rightAnd;
    }
    //cout << "End Value: " << tableTuple << endl;
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
        cout << "Only one count" << endl;
        // Pushes each table name into the root vector for names
        string tempName = _tables->tableName;
        _root->tables.push_back(tempName);
        
        //cout << "Got our table name " << tempName << endl;
        
        // Number of tuples
        unsigned int tup;
        // Get actual value of tuples w/ select factor
        getOptimizedTotal(_tables, _predicate, tempName, tup);
        // Adds to our _root
        _root->tuples.push_back(tup);
        
        // Number of tuples
        _root->noTuples = 0;
    }
    // If we do have a join (mulitple tables)
    else {
        cout << "Multiple Counts" << endl;
        
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
    }
}
