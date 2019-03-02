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
        sumOutType.push_back("INTEGER");
        vector<unsigned int> sumDistinct;
        sumDistinct.push_back(1);
        Schema sumSchemaOut(sumOutAtt, sumOutType, sumDistinct);

        // Creates function for sum
        Function sumCompute;
        sumCompute.GrowFromParseTree(_finalFunction, sumSchemaIn);
        
        // Creates sum and save info to write out
        Sum sum(sumSchemaIn, sumSchemaOut, sumCompute, sumProducer);
        schemaWO = sumSchemaOut;
        // May Cause Problems
        woProducer = &sum;
    }
    // GROUP BY
    // TODO vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
    else if (_groupingAtts != NULL) {
        Schema groupSchema, schemaIn, schemaOut;
        string groupName = _groupingAtts->name;
        int* groupAtts;
        int noGroupAtts = 0;
        RelationalOp* groupByProducer;
        Function compute;
        OrderMaker om;
        
        // Get schema and make OrderMaker
        if (catalog->GetSchema(groupName, groupSchema)) {
            // TODO
            OrderMaker om(groupSchema, groupAtts, noGroupAtts);
        }
        else {
            cout << "\n\nERROR GETTING SCHEMA IN GROUP BY - QueryCompiler.cc\n\n" << endl;
        }
        
        // TODO
        GroupBy groupBy(schemaIn, schemaOut, om, compute, groupByProducer);
    }
    // PROJECT W/ or W/O DISTINCT
    else {
        // PROJECT
        NameList* projectSelect = _attsToSelect;
        while (projectSelect != NULL) {
            Schema projectSchemaIn, projectSchemaOut;
            int numAttsInput, numAttsOutput;
            int* keepMe;
            RelationalOp* projectProducer;
            
            // TODO
            Project(projectSchemaIn, projectSchemaOut, numAttsInput, numAttsOutput, keepMe, projectProducer);
            
            projectSelect = projectSelect->next;
        }
        
        // DISTINCT
        if (_distinctAtts == 1) {
            Schema distinctSchema;
            RelationalOp* distinctProducer;
            
            // TODO
            DuplicateRemoval distinct(distinctSchema, distinctProducer);
        }
    }

    // WRITE OUT
    WriteOut writeOut(schemaWO, outFile, woProducer);
    
    
	// connect everything in the query execution tree and return
    _queryTree.SetRoot(writeOut);
    
    
	// free the memory occupied by the parse tree since it is not necessary anymore
    // NO!
}
