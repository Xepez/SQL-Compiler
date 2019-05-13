#include <string>
#include <vector>
#include <iostream>
#include <algorithm>

#include "Schema.h"
#include "Comparison.h"
#include "QueryOptimizer.h"

using namespace std;


QueryOptimizer::QueryOptimizer(Catalog& _catalog) : catalog(&_catalog) {

	catalog = &_catalog;

}

QueryOptimizer::~QueryOptimizer() {
    for (int a = 0; a < deleteMe1.size(); a++) {
        delete deleteMe1[a];
    }
    for (int b = 0; b < deleteMe2.size(); b++) {
        delete deleteMe2[b];
    }
    for (int c = 0; c < deleteMe3.size(); c++) {
        delete deleteMe3[c];
    }
}

bool QueryOptimizer::getTable(string _att, string& _tableName, unsigned int& _distinct) {
    // Gets table name related to the attribute and the number of distinct for it as well
    TableList* tableFind = tables;
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

void QueryOptimizer::getOptimizedTotal(string tableName, unsigned int &tableTuple) {
    /*
     Determines the actual tuple value by taking into consideration
     previous selects
    */
    
    //cout << "Started finding the value" << endl;
     
    AndList* tempPred = predicate;
    // Reset tuple value
    tableTuple = 0;
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
                tempPred = tempPred->rightAnd;
                continue;
            }
            
            unsigned int tempDist;
            string tempTable;
            getTable(tempAtt, tempTable, tempDist);
            
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

joinRE* QueryOptimizer::getJoinRE(int _position) {
    for (int x = 0; x < JRE.size(); x++) {
        if (JRE[x]->pos == _position)
            return JRE[x];
    }
}

void QueryOptimizer::singleTable(OptimizationTree* _root, string tableName) {
    
    // Pushes each table name into the root vector for names
    _root->tables.push_back(tableName);
    
    //cout << "Got our table name " << tempName << endl;
    
    // Number of tuples
    unsigned int tup;
    // Get actual value of tuples w/ select factor
    getOptimizedTotal(tableName, tup);
    // Adds to our _root
    _root->tuples.push_back(tup);
    
    // Number of tuples
    _root->noTuples = 0;
    
    // Set to Null cuz c++ hates me
    _root->leftChild = NULL;
    _root->rightChild = NULL;
}

void QueryOptimizer::printTree(OptimizationTree* _root) {

    // Prints Optimization Tree to ensure correctness
    if (_root->leftChild != NULL) {
        printTree(_root->leftChild);
    }
    if (_root->rightChild != NULL) {
        printTree(_root->rightChild);
    }
    
    cout << "\n---Branch" << endl;
    
    for (int x = 0; x < _root->tables.size(); x++) {
        cout << _root->tables[x] << endl;
    }
    
    for (int y = 0; y < _root->tuples.size(); y++) {
        cout << _root->tuples[y] << endl;
    }
    
    cout << _root->noTuples << endl;
    
    cout << "\n---End" << endl;
}

bool QueryOptimizer::getAvailableTable(vector<string> _root, string &fName) {
    
    string r1 = _root[0];
    string r2 = _root[1];
    
//    cout << "Checking for an avaialbe table for " << r1 << " and " << r2 << endl;
//    cout << "Current Available Tables" << endl;
//    for (int x = 0; x < tblName.size(); x++) {
//        cout << tblName[x] << endl;
//    }
    
    // Figures out available tables based on unused tables
    if (find(tblName.begin(), tblName.end(), r1) != tblName.end()) {
        //cout << r1 << " found" << endl;
        tblName.erase(find(tblName.begin(), tblName.end(), r1));
        fName = r1;
        return true;
    }
    else if (find(tblName.begin(), tblName.end(), r2) != tblName.end()) {
        //cout << r2 << " found" << endl;
        tblName.erase(find(tblName.begin(), tblName.end(), r2));
        fName = r2;
        return true;
    }
    else if (tblName.size() == 1) { // Not Right?
        fName = tblName[0];
        return true;
    }
    else {
        //cout << "FALSE" << endl;
        return false;
    }
}

void QueryOptimizer::Optimize(TableList* _tables, AndList* _predicate,
	OptimizationTree* _root) {
	// Computes the optimal join order
    
    tables = _tables;
    predicate = _predicate;
    
	// Counts amount of tables we have
	TableList* tableCount = tables;
    int count = 0;
    while (tableCount != NULL) {
        string tttemp = tableCount->tableName;
        tblName.push_back(tttemp);
        count++;
        tableCount = tableCount->next;
    }
    
    // If only one table we dont have a join
    if (count == 1) {
        //cout << "Only one count" << endl;
        string ttn = tables->tableName;
        singleTable(_root, ttn);
    }
    // If we do have a join (mulitple tables)
    else {
        //cout << "Multiple Counts" << endl;
        
        // Get all the req values
        //cout << "Getting Required Values" << endl;
        AndList* tempPred = predicate;
        int count = 0;
        while (tempPred != NULL) {
            // If our and looks like this : "NAME" "=" "NAME"
            // Indicates its a join
            if (tempPred->left->left->code == 3 && tempPred->left->right->code == 3 && tempPred->left->code == 7) {
                
                // Left Side
                string leftAtt = tempPred->left->left->value;
                string leftTable;
                unsigned int leftDist;
                int numDistinct;
                joinDT* leftJ = new joinDT;
                if (getTable(leftAtt, leftTable, leftDist)){
                    numDistinct = leftDist;
                    getOptimizedTotal(leftTable, leftDist);
                    
                    leftJ->tableName = leftTable;
                    leftJ->attName = leftAtt;
                    leftJ->amt = leftDist;
                    deleteMe1.push_back(leftJ);
                    //cout << "Got a Left of Att: " << leftAtt << " and Table: " << leftTable << " and Dist: " << leftDist << endl;
                }
                else {
                    cout << "ERROR Getting Left Table in QueryOptimizer.cc" << endl;
                }
                
                // Right Side
                string rightAtt = tempPred->left->right->value;
                string rightTable;
                unsigned int rightDist;
                joinDT* rightJ = new joinDT;
                if (getTable(rightAtt, rightTable, rightDist)){
                    getOptimizedTotal(rightTable, rightDist);
                    
                    rightJ->tableName = rightTable;
                    rightJ->attName = rightAtt;
                    rightJ->amt = rightDist;
                    deleteMe1.push_back(rightJ);
                    //cout << "Got a Right of Att: " << rightAtt << " and Table: " << rightTable << " and Dist: " << rightDist << endl;
                }
                else {
                    cout << "ERROR Getting Right Table in QueryOptimizer.cc" << endl;
                }
                
                // Combine
                joinRE* exp = new joinRE;
                exp->left = leftJ;
                exp->right = rightJ;
                exp->total = ((rightDist * leftDist) / numDistinct);
                exp->pos = count;
                JRE.push_back(exp);
                deleteMe2.push_back(exp);
                //cout << "New joinRE of position: " << count << " and total: " << exp->total << endl;
            }
            
            count++;
            tempPred = tempPred->rightAnd;
        }
        
        /*
         * Sort Through our Join Data Type Vector and sort by amt
         * Uses Bubble Sort cuz we want to be as efficient as possible
         * As can be see by the rest of my code, going for that O(n^n)
        */
        //cout << "Sorting" << endl;
        for (int a = 0; a < JRE.size()-1; a++) {
            for (int b = 0; b < JRE.size()-a-1; b++) {
                //cout << "A : " << a << "/ B : " << b << " / " << (JRE[b]->total) << " > " << (JRE[b+1]->total) << "?" << endl;
                if ((JRE[b]->total) > (JRE[b+1]->total)) {
                    //cout << "passed" << endl;
                    int temp = JRE[b]->pos;
                    JRE[b]->pos = JRE[b+1]->pos;
                    JRE[b+1]->pos = temp;
                }
            }
        }
        //cout << "End of sort Alg" << endl;
        
        // Build Optimization Tree based on positions in JRE vector
        //cout << "Building Optimization Tree" << endl;
        
        // Build the root first and then branch out
        // First get our joinRE w/ largest total
        joinRE* tempJoinRE = getJoinRE(JRE.size()-1);
        // Second push in table names
        _root->tables.push_back(tempJoinRE->left->tableName);
        _root->tables.push_back(tempJoinRE->right->tableName);
        // Third push in amounts post selection to tuples
        _root->tuples.push_back(tempJoinRE->left->amt);
        _root->tuples.push_back(tempJoinRE->right->amt);
        // Fourth save total number of tuples
        _root->noTuples = tempJoinRE->total;
        // Finally set children to Null
        _root->leftChild = NULL;
        _root->rightChild = NULL;
        // Save our Optimization Tree to the vector
        OT.push_back(_root);
        
        // Now starts creating childern to attach to the root
        int position = JRE.size()-2;
        for (int x = 0; x < JRE.size()-1; x++) {
            OptimizationTree* tempOptTree = new OptimizationTree;
            
            // First get our joinRE w/ next largest total
            joinRE* tempJoinRE = getJoinRE(position);
            // Second push in table names
            tempOptTree->tables.push_back(tempJoinRE->left->tableName);
            tempOptTree->tables.push_back(tempJoinRE->right->tableName);
            // Third push in amounts post selection to tuples
            tempOptTree->tuples.push_back(tempJoinRE->left->amt);
            tempOptTree->tuples.push_back(tempJoinRE->right->amt);
            // Fourth save total number of tuples
            tempOptTree->noTuples = tempJoinRE->total;
            // Finally set children to Null
            tempOptTree->leftChild = NULL;
            tempOptTree->rightChild = NULL;
            
            // Assign Spot on tree
            for (int y = 0; y < OT.size(); y++) {
                if (OT[y]->leftChild == NULL) {
                    OT[y]->leftChild = tempOptTree;
                    tempOptTree->parent = OT[y];
                    break;
                }
                else if (OT[y]->rightChild == NULL) {
                    OT[y]->rightChild = tempOptTree;
                    tempOptTree->parent = OT[y];
                    break;
                }
            }
            
            // Save our Optimization Tree to the vector
            OT.push_back(tempOptTree);
            
            position--;
            deleteMe3.push_back(tempOptTree);
        }
        
        // Attach Select/Scan Tables as children
        for (vector<OptimizationTree*>::iterator it = OT.begin(); it != OT.end(); it++) {
            // Checks if there are any empty children
            if ((*it)->leftChild == NULL) {
                // Initializes need variabless
                string tempNameL;
                if (getAvailableTable((*it)->tables, tempNameL)) {
                    OptimizationTree* tempTreeL = new OptimizationTree;
                    
                    // Creates Opt Tree for our scan/select
                    singleTable(tempTreeL, tempNameL);
                    // Saves our Opt Tree as a child
                    (*it)->leftChild = tempTreeL;
                    
                    // Saved for deletion later
                    deleteMe3.push_back(tempTreeL);
                }
                
            }
            if ((*it)->rightChild == NULL) {
                // Initializes need variabless
                string tempNameR;
                if (getAvailableTable((*it)->tables, tempNameR)) {
                    OptimizationTree* tempTreeR = new OptimizationTree;

                    // Creates Opt Tree for our scan/select
                    singleTable(tempTreeR, tempNameR);
                    // Saves our Opt Tree as a child
                    (*it)->rightChild = tempTreeR;
                    
                    // Saved for deletion later
                    deleteMe3.push_back(tempTreeR);
                }
            }
        }

        
        //cout << "Finished Building Optimization Tree" << endl;
        
        // Print for testing
        //cout << "Printing Tree" << endl;
        //printTree(_root);
    }
}
