#ifndef _REL_OP_H
#define _REL_OP_H

#include <iostream>
#include <map>

#include "Schema.h"
#include "Record.h"
#include "DBFile.h"
#include "Function.h"
#include "Comparison.h"
#include "EfficientMap.h"
#include "EfficientMap.cc"
#include "Keyify.h"
#include "Keyify.cc"
#include "TwoWayList.h"
#include "TwoWayList.cc"
#include "Swapify.h"

using namespace std;


class RelationalOp {
protected:
	// the number of pages that can be used by the operator in execution
	int noPages;
public:
	// empty constructor & destructor
	RelationalOp() : noPages(-1) {}
	virtual ~RelationalOp() {}

	// set the number of pages the operator can use
	void SetNoPages(int _noPages) {noPages = _noPages;}

	// every operator has to implement this method
	virtual bool GetNext(Record& _record) = 0;

	/* Virtual function for polymorphic printing using operator<<.
	 * Each operator has to implement its specific version of print.
	 */
    virtual ostream& print(ostream& _os) = 0;

    /* Overload operator<< for printing.
     */
    friend ostream& operator<<(ostream& _os, RelationalOp& _op);
};

class Scan : public RelationalOp {
private:
	// schema of records in operator
	Schema schema;
	// physical file where data to be scanned are stored
	DBFile file;
	string tableName;

public:
	Scan(Schema& _schema, DBFile& _file, string _tableName);

	virtual ~Scan();

    virtual bool GetNext(Record& _record);
	
	string getTableName();
	DBFile& getfile();
	Schema& getSchema();

	virtual ostream& print(ostream& _os);
};

class Select : public RelationalOp {
private:
	// schema of records in operator
	Schema schema;

	// selection predicate in conjunctive normal form
	CNF predicate;
	// constant values for attributes in predicate
	Record constants;

	// operator generating data
	RelationalOp* producer;

	string tableName;

public:
	Select(Schema& _schema, CNF& _predicate, Record& _constants,
		RelationalOp* _producer, string _tableName);

	virtual ~Select();

    bool tableCheck(string _table);

	Schema& getSchema();
	CNF& getPredicate();
	Record& getRecords();
	RelationalOp* getProducer();
	string getTableName();

    virtual bool GetNext(Record& _record);

	virtual ostream& print(ostream& _os);
};

class Project : public RelationalOp {
private:
	// schema of records input to operator
	Schema schemaIn;
	// schema of records output by operator
	Schema schemaOut;

	// number of attributes in input records
	int numAttsInput;
	// number of attributes in output records
	int numAttsOutput;
	// index of records from input to keep in output
	// size given by numAttsOutput
	int* keepMe;

	// operator generating data
	RelationalOp* producer;

public:
	Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
		int _numAttsOutput, int* _keepMe, RelationalOp* _producer);
	virtual ~Project();

    virtual bool GetNext(Record& _record);
	
	Schema& getSchemaIn();
	Schema& getSchemaOut();
	int getNumAttsInput();
	int getNumAttsOutput();
	int* getKeepMe();
	RelationalOp* getProducer();

	virtual ostream& print(ostream& _os);
};

class Join : public RelationalOp {
private:
	// schema of records in left operand
	Schema schemaLeft;
	// schema of records in right operand
	Schema schemaRight;
	// schema of records output by operator
	Schema schemaOut;

	// selection predicate in conjunctive normal form
	CNF predicate;

	// operators generating data
	RelationalOp* left;
	RelationalOp* right;
    
    // Efficient Map for hash join
    EfficientMap<Record, SwapInt> hashMapJ;
    // Efficient Maps for Symetric Hash Join
    EfficientMap<Record, SwapInt> hashLeft;
    EfficientMap<Record, SwapInt> hashRight;
    // Two Way List for all joins
    TwoWayList<Record> joinList;
    // List of Records that needs to be put back
    TwoWayList<Record> putBackList;
    // List of Records that needs to be put back for either hash map in SHJ
    TwoWayList<Record> putBackLeft;
    TwoWayList<Record> putBackRight;
    // So many Booleans lol
    // Determines if we have run through this join yet
    bool hashAdded;
    bool firstLeft;
    // Swap between left and right maps
    bool swap;
    // Identifies if the left and right sides are empty in SHJ
    bool leftEmpty;
    bool rightEmpty;
    // Determines when to switch in SHJ
    int shjCount;
    int leftCount;
    int rightCount;
    // Ordermakers for each side
    OrderMaker omL;
    OrderMaker omR;

public:
	Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
		CNF& _predicate, RelationalOp* _left, RelationalOp* _right, int lCnt, int rCnt);
	virtual ~Join();

	Schema& getLeftSchema();
	Schema& getRightSchema();
	Schema& getSchemaOut();
	CNF& getPredicate();
	RelationalOp* getLeftRelationalOp();
	RelationalOp* getRightRelationalOp();
    
    // Get Next Algorithms
    bool NLJ(Record& _record);
    bool HJ(Record& _record);
    bool SHJ(Record& _record);

    virtual bool GetNext(Record& _record);

	virtual ostream& print(ostream& _os);
};

class DuplicateRemoval : public RelationalOp {
private:
	// schema of records in operator
	Schema schema;

    // Set Data Structure to store our Records
    map<string, Record> distinctSet;
    
	// operator generating data
	RelationalOp* producer;

public:
	DuplicateRemoval(Schema& _schema, RelationalOp* _producer);
	virtual ~DuplicateRemoval();

	Schema& getSchema();
	RelationalOp* getProducer();
    virtual bool GetNext(Record& _record);

	virtual ostream& print(ostream& _os);
};

class Sum : public RelationalOp {
private:
	// schema of records input to operator
	Schema schemaIn;
	// schema of records output by operator
	Schema schemaOut;

	// function to compute
	Function compute;

    // Stores if the sum has been computed already
    bool hasComp;
    
	// operator generating data
	RelationalOp* producer;

public:
	Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
		RelationalOp* _producer);
	virtual ~Sum();

	Schema& getSchemaIn();
	Schema& getSchemaOut();
	Function& getCompute();
	RelationalOp* getProducer();

    virtual bool GetNext(Record& _record);

	virtual ostream& print(ostream& _os);
};

class GroupBy : public RelationalOp {
private:
	// schema of records input to operator
	Schema schemaIn;
	// schema of records output by operator
	Schema schemaOut;

	// grouping attributes
	OrderMaker groupingAtts;
	// function to compute
	Function compute;

	// operator generating data
	RelationalOp* producer;

	EfficientMap<Record, SwapDouble> hashtable;

	bool atBeginning;

	int count;

public:
	GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
		Function& _compute,	RelationalOp* _producer);
	virtual ~GroupBy();

	Schema& getSchemaIn();
	Schema& getSchemaOut();
	OrderMaker& getGroupingAtts();
	Function& getCompute();
	RelationalOp* getProducer();

    virtual bool GetNext(Record& _record);

	virtual ostream& print(ostream& _os);
};

class WriteOut : public RelationalOp {
private:
	// schema of records in operator
	Schema schema;

	// output file where to write the result records
	string outFile;

	// operator generating data
	RelationalOp* producer;



public:
	WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer);
	virtual ~WriteOut();
	Schema & getSchema();
	string& getOutFile();
	RelationalOp* getProducer();

    virtual bool GetNext(Record& _record);

	virtual ostream& print(ostream& _os);
};

class QueryExecutionTree {
private:
	RelationalOp* root;

public:
	QueryExecutionTree() {}
	virtual ~QueryExecutionTree() {}

    void ExecuteQuery();
	void SetRoot(RelationalOp& _root) {root = &_root;}

    friend ostream& operator<<(ostream& _os, QueryExecutionTree& _op);
};

#endif //_REL_OP_H
