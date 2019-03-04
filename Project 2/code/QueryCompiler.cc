#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"

#include <vector>

using namespace std;


QueryCompiler::QueryCompiler(Catalog& _catalog, QueryOptimizer& _optimizer) : catalog(&_catalog), optimizer(&_optimizer) {
    
    catalog = &_catalog;
    optimizer = &_optimizer;
}

QueryCompiler::~QueryCompiler() {
}

void QueryCompiler::Compile(TableList* _tables, NameList* _attsToSelect, FuncOperator* _finalFunction, AndList* _predicate, NameList* _groupingAtts, int& _distinctAtts, QueryExecutionTree& _queryTree) {
    
    // TODO: ADD TO .h if correct
    vector<Scan> scanVector;
    vector<Select> selectVector;
    
	
    // create a SCAN operator for each table in the query
    TableList* scanTable = _tables;
    while (scanTable != NULL) {
        Schema scanSchema;
        DBFile db;
        RelationalOp* ropScan;
        string tableName = scanTable->tableName;
        
        if (catalog->GetSchema(tableName, scanSchema)) {
            Scan scan(scanSchema, db);
            // May Cause Problems
            ropScan = &scan;
            scanVector.push_back(scan);
        }
        else {
            cout << "\n\nERROR GETTING SCHEMA IN SCAN - QueryCompiler.cc\n\n" << endl;
            continue;
        }
        
        // push-down selections: create a SELECT operator wherever necessary
//        AndList* selectWhere = _predicate;
//        while (selectWhere != NULL) {
        Record selectConst;
        CNF selectPredicate;
        
        if (selectPredicate.ExtractCNF(*_predicate, scanSchema, selectConst) != 0) {
            cout << "\n\nERROR GETTING CNF IN SELECT - QueryCompiler.cc\n\n" << endl;
        }
    
        if (selectPredicate.numAnds > 0) {
            //cout << "WHERE SELECTION WORKING - " << endl;
            //cout << scanSchema << endl << endl;
            
            Select select(scanSchema, selectPredicate, selectConst, ropScan);
            selectVector.push_back(select);
        }
//            selectWhere = selectWhere->rightAnd;
//        }
        
        scanTable = scanTable->next;
    }
    

//    // push-down selections: create a SELECT operator wherever necessary
//    AndList* currAnd = _predicate;
//    while(currAnd != NULL) {
//        Schema selectSchema;
//        Record selectConst;
//        CNF selectPredicate;
//        RelationalOp* selectProducer;
//
//        // Finds schema used
//        // TODO
//        TableList* selectTable = _tables;
//        string selName = selectTable->tableName;
//        while (selectTable != NULL) {
//            if (catalog->GetSchema(selName, selectSchema)) {
//                string selLeft = currAnd->left->left->value;
//                string selRight = currAnd->left->right->value;
//                if (selectSchema.Index(selLeft) != -1 || selectSchema.Index(selRight) != -1) {
//                    break;
//                }
//            }
//            selectTable = selectTable->next;
//        }
//
//        if (selectPredicate.ExtractCNF(*currAnd, selectSchema, selectConst) != 0) {
//            cout << "ERROR GETTING CNF IN SELECT - QueryCompiler.cc";
//        }
//
//        cout << "WE GOT HERE WOOO!" << endl;
//        cout << selectSchema << endl << endl;
//
//        // TODO
//        Select select(selectSchema, selectPredicate, selectConst, selectProducer);
//
//        currAnd = currAnd->rightAnd;
//    }
    

	// call the optimizer to compute the join order
    OptimizationTree* root;
    optimizer->Optimize(_tables, _predicate, root);

	// create join operators based on the optimal order computed by the optimizer
    // TODO
//    AndList* joinWhere = _predicate;
//    while (joinWhere != NULL) {
    Schema joinSchemaL, joinSchemaR, joinSchemaOut;
    CNF joinPredicate;
    RelationalOp* joinROPL;
    RelationalOp* joinROPR;
    
    
    if (joinPredicate.ExtractCNF(*_predicate, joinSchemaL, joinSchemaR) != 0) {
        cout << "\n\nERROR GETTING CNF IN JOIN - QueryCompiler.cc\n\n" << endl;
    }
    
    if (joinPredicate.numAnds > 0) {
        cout << "JOIN WORKING - " << endl;
        cout << joinSchemaL << "\t" << joinSchemaR << endl << endl;
        
        Join join(joinSchemaL, joinSchemaR, joinSchemaOut, joinPredicate, joinROPL, joinROPR);
    }
    
    Schema joinSchema; // Set equal to final join
    RelationalOp* joinOp;
//        joinWhere = joinWhere->rightAnd;
//    }
    
    
    // create the remaining operators based on the query
    
    // Pre-WriteOut Initialization
    // So we can save our write out choices depending on Project/Sum/Distinct/GroupBy
    Schema schemaWO;
    string outFile = "out.txt";
    RelationalOp* woProducer;
    
    // SUM
    if (_finalFunction != NULL) {
        // Input Schema and operator
        Schema sumSchemaIn = joinSchema;
        RelationalOp* sumProducer = joinOp;
        
        // Creates Criteria for Output Schema
        vector<string> sumOutAtt;
        sumOutAtt.push_back("sum");
        vector<string> sumOutType;
        sumOutType.push_back("FLOAT");
        vector<unsigned int> sumDistinct;
        sumDistinct.push_back(1);
        Schema sumSchemaOut(sumOutAtt, sumOutType, sumDistinct);

        // Creates function for sum
        Function sumCompute;
        sumCompute.GrowFromParseTree(_finalFunction, sumSchemaIn);
        
        // Creates Sum Operator
        Sum sum(sumSchemaIn, sumSchemaOut, sumCompute, sumProducer);
        
        // Sets Write Out Variables
        schemaWO = sumSchemaOut;
        // May Cause Problems
        woProducer = &sum;
    }
    // GROUP BY
    else if (_groupingAtts != NULL) {
        
        // Basic Initializations
        Schema groupSchemaIn = joinSchema;
        string* groupName;
        int* groupAtts;
        int noGroupAtts = 0;
        
        // Gets number of distincts, grouping attributes, and
        // sets up our vector to config our grouping atts
        unsigned int groupDist = 1;
        vector<int> gbIndex;
        NameList* tempGB = _groupingAtts;
        while(tempGB != NULL) {
            string temp = tempGB->name;
            groupDist *= groupSchemaIn.GetDistincts(temp);
            gbIndex.push_back(groupSchemaIn.Index(temp));
            noGroupAtts++;
        }
        
        // Configures our grouping attributes for order maker
        for (int i = gbIndex.size()-1; i >= 0; i--) {
            groupAtts[gbIndex.size()-i-1] = gbIndex[i];
        }
        
        // Creates Criteria for Output Schema
        vector<string> groupOutAtt;
        groupOutAtt.push_back("sum");
        vector<string> groupOutType;
        groupOutType.push_back("FLOAT");
        vector<unsigned int> groupDistinct;
        groupDistinct.push_back(groupDist);
        Schema groupSchemaOut(groupOutAtt, groupOutType, groupDistinct);
        
        // Creates function for group by and initializes remaining variables
        RelationalOp* groupByProducer;
        OrderMaker groupOM(groupSchemaIn, groupAtts, noGroupAtts);
        Function groupCompute;
        groupCompute.GrowFromParseTree(_finalFunction, groupSchemaIn);

        // Create Group By Operator
        GroupBy groupBy(groupSchemaIn, groupSchemaOut, groupOM, groupCompute, joinOp);
        
        // Sets Write Out Variables
        schemaWO = groupSchemaOut;
        // May Cause Problems
        woProducer = &groupBy;
    }
    // PROJECT W/ or W/O DISTINCT
    else {
        // PROJECT
        // Basic Initializations
        Schema projectSchemaIn = joinSchema;
        Schema projectSchemaOut = joinSchema;
        int* keepMe;
        int numAttsInput = projectSchemaIn.GetAtts().size();
        
        // Gets number of output attributes and
        // sets up our vector to config our project atts
        int numAttsOutput = 0;
        vector<int> projectIndex;
        NameList* projectSelect = _attsToSelect;
        while(projectSelect != NULL) {
            string temp = projectSelect->name;
            projectIndex.push_back(projectSchemaIn.Index(temp));
            numAttsOutput++;
        }
        
        // Configures our project attributes for project operator
        for (int i = projectIndex.size()-1; i >= 0; i--) {
            keepMe[projectIndex.size()-i-1] = projectIndex[i];
        }
        
        // Creates Project Operator
        Project project(projectSchemaIn, projectSchemaOut, numAttsInput, numAttsOutput, keepMe, joinOp);
        
        // DISTINCT
        if (_distinctAtts == 1) {
            // Sets projects out schema as distincts schema
            Schema distinctSchema = projectSchemaOut;
            // May Cause Problems
            RelationalOp* distinctOp = &project;
            
            // Create Distinct Operator
            DuplicateRemoval distinct(distinctSchema, distinctOp);
            
            // If distinct op then write out set to distinct
            // Sets Write Out Variables
            schemaWO = distinctSchema;
            // May Cause Problems
            woProducer = &distinct;
        }
        else {
            // If no distinct op then write out set to project
            // Sets Write Out Variables
            schemaWO = projectSchemaOut;
            // May Cause Problems
            woProducer = &project;
        }
    }

    // WRITE OUT
    WriteOut writeOut(schemaWO, outFile, woProducer);
    
    
	// connect everything in the query execution tree and return
    _queryTree.SetRoot(writeOut);
    
    
	// free the memory occupied by the parse tree since it is not necessary anymore
    // NO!
}
