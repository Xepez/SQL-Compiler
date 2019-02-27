#include <iostream>
#include "RelOp.h"

using namespace std;


ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}


Scan::Scan(Schema& _schema, DBFile& _file) {    // Peyton

    // Saves Data for printing
    schema = _schema;
    file = _file;
}

Scan::~Scan() {

}

ostream& Scan::print(ostream& _os) {
    return _os << "SCAN: " << schema;
}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants, RelationalOp* _producer) { // Michael

}

Select::~Select() {

}

ostream& Select::print(ostream& _os) {
	return _os << "SELECT";
}


Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput, int _numAttsOutput, int* _keepMe, RelationalOp* _producer) {    // Peyton
    
    // Saves Data for printing
    schemaIn = _schemaIn;
    schemaOut = _schemaOut;
    numAttsInput = _numAttsInput;
    numAttsOutput = _numAttsOutput;
    keepMe = _keepMe;
    producer = _producer;
}

Project::~Project() {

}

ostream& Project::print(ostream& _os) {
    
    // TODO FIX IF NEEDED
    _os << "PROJECT:";
    _os << " IN: " << schemaIn << ", " << numAttsInput << " / ";
    _os << " OUT: " << schemaOut;
    for (int x = 0; x < numAttsOutput; x++) {
        _os << " KEEPING" << " = " << keepMe[x];
        if ((x+1) != numAttsOutput)
            _os << ",";
    }
    
    return _os;
}


Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut, CNF& _predicate, RelationalOp* _left, RelationalOp* _right) { // Michael

}

Join::~Join() {

}

ostream& Join::print(ostream& _os) {
	return _os << "JOIN";
}


DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) {  // Peyton
    
    // Saves Data for printing
    schema = _schema;
    producer = _producer;
}

DuplicateRemoval::~DuplicateRemoval() {

}

ostream& DuplicateRemoval::print(ostream& _os) {
    return _os << "DISTINCT: " << schema;
}


Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute, RelationalOp* _producer) {  // Michael

}

Sum::~Sum() {

}

ostream& Sum::print(ostream& _os) {
	return _os << "SUM";
}


GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts, Function& _compute,	RelationalOp* _producer) {  // Peyton

    // Saves Data for printing
    schemaIn = _schemaIn;
    schemaOut = _schemaOut;
    groupingAtts = _groupingAtts;
    compute = _compute;
    producer = _producer;
}

GroupBy::~GroupBy() {

}

ostream& GroupBy::print(ostream& _os) {
    
    _os << "GROUP BY:";
    _os << " IN: " << schemaIn;
    _os << " OUT: " << schemaOut;
    // TODO FIX IF NEEDED
    _os << " GROUPING BY: " << groupingAtts;
    //_os << " FUNCTION: " << compute;
    return _os;
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {    // Michael

}

WriteOut::~WriteOut() {

}

ostream& WriteOut::print(ostream& _os) {
	return _os << "OUTPUT";
}


ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
    
    // Print tree using preorder tree traversal
    // From root to each child till whole tree complete
    
	return _os << "QUERY EXECUTION TREE";
}
