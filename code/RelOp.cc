#include <iostream>
#include <sstream>
#include "RelOp.h"

using namespace std;


//---------------------------------------------------------------------------------------------------------------------------------------------
ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}


//-------------------------------------------------------SCAN --------------------------------------------------------------------------------------
Scan::Scan(Schema& _schema, DBFile& _file, string _tableName) {
	schema = _schema;
	file = _file;
	tableName = _tableName;
}
Scan::~Scan() {
}

bool Scan::GetNext(Record& _record) {
    //cout << "Scan GetNext" << endl;
    if (file.GetNext(_record) == 1) {
        //cout << "Got Scan Record" << endl;
        return true;
    }
    else {
    	return false;
    }
}

string Scan::getTableName() {
	return tableName;
}
DBFile& Scan::getfile() {
	return file;
}
Schema& Scan::getSchema() {
	return schema;
}
ostream& Scan::print(ostream& _os) {
	return _os << "SCAN: SCHEMA IN/OUT: " << schema << endl;
}

//-------------------------------------------------------SELECT --------------------------------------------------------------------------------------
Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer, string _tableName) {
	schema = _schema;
	predicate = _predicate;
	constants = _constants;
	producer = _producer;
	tableName = _tableName;
}
Select::~Select() {
}

bool Select::GetNext(Record& _record) {
    //cout << "Select GetNext" << endl;
    while(producer->GetNext(_record)) {
        //cout << "Back to select" << endl;
	    if (predicate.Run(_record, constants)) {
	        //cout << "Success at Select" << endl;

            return true;
        }
    }
    //cout << "Failed Select" << endl;
	return false;

}

bool Select::tableCheck(string _table) {
    if (tableName == _table)
        return true;
    else
        return false;
}

string Select::getTableName() {
	return tableName;
}
Schema& Select::getSchema() {
	return schema;
}
CNF& Select::getPredicate() {
	return predicate;
}
Record& Select::getRecords() {
	return constants;
}
RelationalOp* Select::getProducer() {
	return producer;
}
ostream& Select::print(ostream& _os) {
	return _os << "SELECT: SCHEMA IN/OUT: " << schema << endl;
}


//-------------------------------------------------------PROJECT --------------------------------------------------------------------------------------
Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer) {

	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	numAttsInput = _numAttsInput;
	numAttsOutput = _numAttsOutput;
	keepMe = _keepMe;
	producer = _producer;

}
Project::~Project() {
}

bool Project::GetNext(Record& _record) {
    //cout << "Project GetNext" << endl;
    if (producer->GetNext(_record)) {
        //cout << "Out: " << numAttsOutput << " In: " << numAttsInput << endl;
        //cout << "Keep Me: " << keepMe[0] << endl;
        
        //cout << "Back to Project" << endl;
        _record.Project(keepMe, numAttsOutput, numAttsInput);
        //cout << "Success at Project" << endl;
        return true;

    }
    else {
        //cout << "Failed Project" << endl;
        return false;
    }
}

Schema& Project::getSchemaIn() {
	return schemaIn;
}
Schema& Project::getSchemaOut() {
	return schemaOut;
}
int Project::getNumAttsInput() {
	return numAttsInput;
}
int Project::getNumAttsOutput() {
	return numAttsOutput;
}
int* Project::getKeepMe() {
	return keepMe;
}
RelationalOp* Project::getProducer() {
	return producer;
}
ostream& Project::print(ostream& _os) {
	return _os << "PROJECT: SCHEMA IN: " << schemaIn << " SCHEMA OUT: " << schemaOut <<  endl;
}


//-------------------------------------------------------JOIN --------------------------------------------------------------------------------------
Join:: Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) {
	schemaLeft = _schemaLeft;
	schemaRight = _schemaRight;
	schemaOut = _schemaOut;
	predicate = _predicate;
	left = _left;
	right = _right;

}
Schema& Join::getLeftSchema() {
	return schemaLeft;
}
Schema& Join::getRightSchema() {
	return schemaRight;
}
Schema& Join::getSchemaOut() {
	return schemaOut;
}
CNF& Join::getPredicate() {
	return predicate;
}
RelationalOp* Join::getLeftRelationalOp() {
	return left;
}
RelationalOp* Join::getRightRelationalOp() {
	return right;
}
Join::~Join() {

}
ostream& Join::print(ostream& _os) {
	return _os << "JOIN: SCHEMA LEFT: " << schemaLeft << " SCHEMA RIGHT: " << schemaRight << " SCHEMA OUT:" << schemaOut << endl;
}


//-------------------------------------------------------DUPLICATE REMOVAL --------------------------------------------------------------------------------------
DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) {
	schema = _schema;
	producer = _producer;
}
DuplicateRemoval::~DuplicateRemoval() {
}

bool DuplicateRemoval::GetNext(Record& _record) {
    
    while (producer->GetNext(_record)) {
        
        stringstream currKey;           // Our Key
        _record.print(currKey, schema); // Fills our unique key
        
        if (distinctSet.find(currKey.str()) == distinctSet.end()) {
            // Dont have this in our set
            distinctSet[currKey.str()] = _record;
            return true;
        }
    }
    
    // No More records to check
    return false;
}

Schema& DuplicateRemoval::getSchema() {
	return schema;
}
RelationalOp* DuplicateRemoval::getProducer() {
	return producer;
}
ostream& DuplicateRemoval::print(ostream& _os) {
	return _os << "DISTINCT: SCHEMA: " << schema << endl;
}


//-------------------------------------------------------SUM --------------------------------------------------------------------------------------
Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
	RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	compute = _compute;
	producer = _producer;
    hasComp = false;
}
Sum::~Sum() {
}

bool Sum::GetNext(Record& _record) {
    
    Record temp;
    double runSum = 0;
    
    // If sum not computed
    if (!hasComp) {
        while (producer->GetNext(temp)) {
            int runIntSum = 0;
            double runDoubSum = 0.0;
            Type retType = compute.Apply(temp, runIntSum, runDoubSum);
            
            // Gets return type and adds to the running sum depending on that type
            if (retType == Float)
                runSum += runDoubSum;
            else if (retType == Integer)
                runSum += (double)runIntSum;
        }
        
        // Creates a record of only the sum to pass
        //_record
        
        // We have now comuted the sum once
        hasComp = true;
    }
    
    return hasComp;
}

Schema& Sum::getSchemaIn() {
	return schemaIn;
}
Schema& Sum::getSchemaOut() {
	return schemaOut;
}
Function& Sum::getCompute() {
	return compute;
}
RelationalOp* Sum::getProducer() {
	return producer;
}
ostream& Sum::print(ostream& _os) {
	return _os << "SUM: SCHEMA IN: " << schemaIn << " SCHEMA OUT: " << schemaOut << endl;
}


//-------------------------------------------------------GROUP BY --------------------------------------------------------------------------------------
GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute,	RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	groupingAtts = _groupingAtts;
	compute = _compute;
	producer = _producer;
}
GroupBy::~GroupBy() {
}

bool GroupBy::GetNext(Record& _record) {
    
}

Schema& GroupBy::getSchemaIn() {
	return schemaIn;
}
Schema& GroupBy::getSchemaOut() {
	return schemaOut;
}
OrderMaker& GroupBy::getGroupingAtts() {
	return groupingAtts;
}
Function& GroupBy::getCompute() {
	return compute;
}
RelationalOp* GroupBy::getProducer() {
	return producer;
}
ostream& GroupBy::print(ostream& _os) {
	return _os << "GROUP BY: SCHEMA IN: " << schemaIn << " SCHEMA OUT: " << schemaOut << endl;
}


//-------------------------------------------------------WRITE OUT --------------------------------------------------------------------------------------
WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {
	schema = _schema;
	outFile = _outFile;
	producer = _producer;
}
WriteOut::~WriteOut() {
}

bool WriteOut::GetNext(Record& _record) {
    //cout << "Write Out GetNext" << endl;
    if (producer->GetNext(_record)) {
        //cout << "Back to Write Out" << endl;
        _record.print(cout, schema);
        cout << "}" << endl;
        //cout << "Success at Write Out" << endl;
        return true;
    }
    else{
        //cout << "Failed Write Out" << endl;
        return false;
    }
}

Schema & WriteOut::getSchema() {
	return schema;
}
string& WriteOut::getOutFile() {
	return outFile;
}
RelationalOp* WriteOut::getProducer() {
	return producer;
}
ostream& WriteOut::print(ostream& _os) {
	return _os << "WRITEOUT: FILE: " << outFile << " SCHEMA: " << schema << endl;
}


//---------------------------------------------------------------------------------------------------------------------------------------------
void QueryExecutionTree::ExecuteQuery() {

	int count = 0;  // Here to just have something inside the while loop
    cout << "---------------------------------------------------" << endl;
    cout << "Executing Query" << endl;
    Record rec;
    while(root->GetNext(rec)){ count++; }
    cout << count << endl;
}

ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	return _os << "QUERY EXECUTION TREE";
}
