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
    for (int x = 0; x < deleteMe.size(); x++) {
        delete deleteMe[x];
    }
}

RelationalOp* QueryCompiler::joiner(OptimizationTree* tempRoot, Schema &parentSchema, AndList* _predicate) {
    
    Schema joinSchemaL, joinSchemaR, joinSchemaOut;
    CNF joinPredicate;
    RelationalOp* joinROPL;
    RelationalOp* joinROPR;
    
    /*
     Both checks continue to recursively check each child
     allowing us to traverse the optimized join tree and create
     joins and build our execution tree.
    */
    if (tempRoot->leftChild != NULL) {
        joinROPL = joiner(tempRoot->leftChild, joinSchemaL, _predicate);
    }
    if (tempRoot->rightChild != NULL) {
        joinROPR = joiner(tempRoot->rightChild, joinSchemaR, _predicate);
    }
    
    if (tempRoot->tables.size() <= 1) {
        // Not a Join Op
        // Finds the scan or select the parent join will connect to
        
        // Gets schema for our table
        catalog->GetSchema(tempRoot->tables[0], parentSchema);
        
        // Run through select first so we are farthest in the tree
        for (vector<Select*>::iterator x = selectVector.begin(); x != selectVector.end(); x++) {
            if ((*x)->tableCheck(tempRoot->tables[0])) {
                // TODO: REMOVE
                //cout << "Got in Select - " << tempRoot->tables[0] << endl;
//                cout << (*x) << endl;
                return (*x);
            }
        }
        
        // If not found in select
        // Runs through scan as a last resort
        for (vector<Scan*>::iterator x = scanVector.begin(); x != scanVector.end(); x++) {
            if ((*x)->tableCheck(tempRoot->tables[0])) {
                // TODO: REMOVE
                //cout << "Got in Scan - " << tempRoot->tables[0] << endl;
//                cout << (*x) << endl;
                return (*x);
            }
        }
    }
    
    // Gets CNF using left and right schemas and prexisting where predicate
    if (joinPredicate.ExtractCNF(*_predicate, joinSchemaL, joinSchemaR) != 0) {
        // Check to ensure runs correctly
        cout << "\n\nERROR GETTING CNF IN JOIN - QueryCompiler.cc\n\n" << endl;
    }
    
    // Checks to make sure there are joins
    //if (joinPredicate.numAnds > 0) {
        // TODO: REMOVE
        //cout << "JOIN WORKING - " << endl;
        //cout << joinSchemaL << "\t" << joinSchemaR << endl << endl;
        
        // Creates our out schema by unioning the two input schemas
        // TODO: If works keep if not uncomment
        //joinSchemaOut = joinSchemaR;
        joinSchemaOut.Append(joinSchemaR);
        joinSchemaOut.Append(joinSchemaL);
        
        // Creates our join op
        Join* join = new Join(joinSchemaL, joinSchemaR, joinSchemaOut, joinPredicate, joinROPL, joinROPR);
        deleteMe.push_back(join);
        
        // Saves this schema
        parentSchema = joinSchemaOut;
        // Returns our current join op
        return join;
    //}
}

void QueryCompiler::Compile(TableList* _tables, NameList* _attsToSelect, FuncOperator* _finalFunction, AndList* _predicate, NameList* _groupingAtts, int& _distinctAtts, QueryExecutionTree& _queryTree) {
    
    // create a SCAN operator for each table in the query
    TableList* scanTable = _tables;
    while (scanTable != NULL) {
        // Basic initialization
        Schema scanSchema;
        DBFile db;
        RelationalOp* ropScan;
        string tableName = scanTable->tableName;
        
        // Gets Schema of table
        if (catalog->GetSchema(tableName, scanSchema)) {
            // TODO: REMOVE
            //cout << "Got a scan" << endl;
            
            // If get schema successful
            // Creates scan and saves to our scan vector
            Scan* scan = new Scan(scanSchema, db, tableName);
            deleteMe.push_back(scan);
            ropScan = scan;
            scanVector.push_back(scan);
        }
        else {
            // Unsuccessful in gettin schema
            cout << "\n\nERROR GETTING SCHEMA IN SCAN - QueryCompiler.cc\n\n" << endl;
        }
        
        // push-down selections: create a SELECT operator wherever necessary
        Record selectConst;
        CNF selectPredicate;
        
        // Gets CNF and Record using scanSchema and prexisting where predicate
        if (selectPredicate.ExtractCNF(*_predicate, scanSchema, selectConst) != 0) {
            // Check to ensure runs correctly
            cout << "\n\nERROR GETTING CNF IN SELECT - QueryCompiler.cc\n\n" << endl;
        }
    
        // Makes sure there are where statements that aren't joins
        if (selectPredicate.numAnds > 0) {
            // TODO: REMOVE
            //cout << "Got a Select" << endl;
            
            // Creates select operateor and saves to a select vector
            Select* select = new Select(scanSchema, selectPredicate, selectConst, ropScan, tableName);
            deleteMe.push_back(select);
            selectVector.push_back(select);
        }
        
        // Moves on to next table to create a scan of it
        scanTable = scanTable->next;
    }
    

	// call the optimizer to compute the join order
    OptimizationTree* root = new OptimizationTree();
    //cout << "Optimizing" << endl;
    optimizer->Optimize(_tables, _predicate, root);
    //cout << "Optimizing finished" << endl;

	// create join operators based on the optimal order computed by the optimizer
    // Is set equal to final join op and schema;
    //cout << "Starting Join" << endl;
    Schema joinSchema;
    RelationalOp* joinOp = joiner(root, joinSchema, _predicate);
    //cout << "Finishing join" << endl;
    
//    cout << joinSchema << endl;
    
//    if (joinOp == NULL) {
//        cout << "This is why testing is important" << endl;
//    }

    
    // create the remaining operators based on the query
    // Pre-WriteOut Initialization
    // So we can save our write out choices depending on Project/Sum/Distinct/GroupBy
    Schema schemaWO;
    string outFile = "out.txt";
    RelationalOp* woProducer;
    
    // SUM
    if (_finalFunction != NULL) {
        //cout << "Sum Function" << endl;
        
        // Input Schema and operator
        Schema sumSchemaIn = joinSchema;
        RelationalOp* sumProducer = joinOp;
        
        // Creates Criteria for Output Schema
        vector<string> sumOutAtt;
        vector<string> sumOutType;
        vector<unsigned int> sumDistinct;
        sumOutAtt.push_back("sum");
        sumOutType.push_back("FLOAT");
        sumDistinct.push_back(1);
        Schema sumSchemaOut(sumOutAtt, sumOutType, sumDistinct);

        // Creates function for sum
        Function sumCompute;
        sumCompute.GrowFromParseTree(_finalFunction, sumSchemaIn);
        
        // Creates Sum Operator
        Sum* sum = new Sum(sumSchemaIn, sumSchemaOut, sumCompute, sumProducer);
        deleteMe.push_back(sum);
        
        // Sets Write Out Variables
        schemaWO = sumSchemaOut;
        woProducer = sum;
    }
    // GROUP BY
    else if (_groupingAtts != NULL) {
        //cout << "Group By Function" << endl;
        
        // Basic Initializations
        Schema groupSchemaIn = joinSchema;
        string* groupName;
        int noGroupAtts = 0;
        int groupAtts[groupSchemaIn.GetAtts().size()];
        
        // Gets number of distincts, grouping attributes, and
        // sets up our vector to config our grouping atts
        unsigned int groupDist = 1;
        NameList* tempGB = _groupingAtts;
        while(tempGB != NULL) {
            string temp = tempGB->name;
            groupDist *= groupSchemaIn.GetDistincts(temp);
            groupAtts[noGroupAtts] = groupSchemaIn.Index(temp);
            noGroupAtts++;
            tempGB = tempGB->next;
        }
        
        // Creates Criteria for Output Schema
        vector<string> groupOutAtt;
        vector<string> groupOutType;
        vector<unsigned int> groupDistinct;
        groupOutAtt.push_back("sum");
        groupOutType.push_back("FLOAT");
        groupDistinct.push_back(groupDist);
        Schema groupSchemaOut(groupOutAtt, groupOutType, groupDistinct);
        
        // Creates function for group by and initializes remaining variables
        RelationalOp* groupByProducer;
        OrderMaker groupOM(groupSchemaIn, groupAtts, noGroupAtts);
        Function groupCompute;
        groupCompute.GrowFromParseTree(_finalFunction, groupSchemaIn);

        // Create Group By Operator
        GroupBy* groupBy = new GroupBy(groupSchemaIn, groupSchemaOut, groupOM, groupCompute, joinOp);
        deleteMe.push_back(groupBy);
        
        // Sets Write Out Variables
        schemaWO = groupSchemaOut;
        woProducer = groupBy;
    }
    // PROJECT W/ or W/O DISTINCT
    else {
        // PROJECT
        
        //cout << "Project" << endl;
        
        // Basic Initializations
        Schema projectSchemaIn = joinSchema;
        Schema projectSchemaOut = joinSchema;
        int numAttsInput = projectSchemaIn.GetAtts().size();
        int keepMe[numAttsInput];
        
        // Gets number of output attributes and
        // sets up our vector to config our project atts
        int numAttsOutput = 0;
        NameList* projectSelect = _attsToSelect;
        while(projectSelect != NULL) {
            string temp = projectSelect->name;
            cout << "give me the mula" << endl;
            keepMe[numAttsOutput] = projectSchemaOut.Index(temp);
            numAttsOutput++;
            projectSelect = projectSelect->next;
        }
        
        // Creates Project Operator
        Project* project = new Project(projectSchemaIn, projectSchemaOut, numAttsInput, numAttsOutput, keepMe, joinOp);
        deleteMe.push_back(project);
        
        // DISTINCT
        if (_distinctAtts == 1) {
            //cout << "w/ Distinct" << endl;
            
            // Sets projects out schema as distincts schema
            Schema distinctSchema = projectSchemaOut;
            RelationalOp* distinctOp = project;
            
            // Create Distinct Operator
            DuplicateRemoval* distinct = new DuplicateRemoval(distinctSchema, distinctOp);
            deleteMe.push_back(distinct);
            
            // If distinct op then write out set to distinct
            // Sets Write Out Variables
            schemaWO = distinctSchema;
            woProducer = distinct;
        }
        else {
            //cout << "w/o Distinct" << endl;
            
            // If no distinct op then write out set to project
            // Sets Write Out Variables
            schemaWO = projectSchemaOut;
            woProducer = project;
        }
    }

    // WRITE OUT
    //cout << "Write Out" << endl;
    WriteOut* writeOut = new WriteOut(schemaWO, outFile, woProducer);
    deleteMe.push_back(writeOut);
    
    
	// connect everything in the query execution tree and return
    _queryTree.SetRoot(*writeOut);
    
    
	// free the memory occupied by the parse tree since it is not necessary anymore
    // No!
}
