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
    
    _os << "SCAN" << endl;
    _os << "Schema : " << schema << endl;
    return _os;
}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants, RelationalOp* _producer) { // Michael
	
    schema = _schema;
    predicate = _predicate;
    constants = _constants;
    producer = _producer;
}

Select::~Select() {

}

ostream& Select::print(ostream& _os) {
    
	_os << "SELECT" << endl;
	_os << "Schema : " << schema << "\t";
    _os << "Predicate : " << predicate << /*"\t"*/endl;
    // TODO Figure Out What to Print Here
    //_os << "Constants : " << constants << endl;
	_os << *producer ;
	 return _os;
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
    
    _os << "PROJECT" << endl;
    _os << "Schema IN : " << schemaIn << "\tNumber IN Atts : " << numAttsInput << "\t";
    _os << "Schema OUT : " << schemaOut << "\tNumber OUT Atts : " << numAttsOutput << "\t";
    _os << "Keeping : ";
    for (int x = 0; x < numAttsOutput; x++) {
        // Goes through KeepMe array and shows which atts are going to be kept
        _os << keepMe[x] << "\t";
    }
    _os << "\n" << *producer;
    return _os;
}


Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut, CNF& _predicate, RelationalOp* _left, RelationalOp* _right) { // Michael

    schemaLeft = _schemaLeft;
    schemaRight = _schemaRight;
    schemaOut = _schemaOut;
    predicate = _predicate;
    left = _left;
    right = _right;

}

Join::~Join() {

}

ostream& Join::print(ostream& _os) {
    
    _os << "JOIN" << endl;
	_os << "Schema Left IN : " << schemaLeft << "\t";
	_os << "Schema Right IN : " << schemaRight << "\t";
    _os << "Schema OUT : " << schemaOut << "\t";
    _os << "Predicate : " << predicate << endl;
    _os << *left;
    _os << *right;
	return _os;
}


DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) {  // Peyton
    
    // Saves Data for printing
    schema = _schema;
    producer = _producer;
}

DuplicateRemoval::~DuplicateRemoval() {

}

ostream& DuplicateRemoval::print(ostream& _os) {
    
    _os << "DISTINCT" << endl;
    _os << "Schema : " << schema << endl;
    _os << *producer;
    return _os;
}


Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute, RelationalOp* _producer) {  // Michael

    schemaIn = _schemaIn;
    schemaOut = _schemaOut;
    compute = _compute;
    producer = _producer;
}

Sum::~Sum() {

}

ostream& Sum::print(ostream& _os) {
	
    _os << "SUM" << endl;
    _os << "Schema IN : " << schemaIn << "\t";
    _os << "Schema OUT : " << schemaOut << /*"\t"*/endl;
    // TODO Figure Out What to Print Here
    //_os << "Compute : " << compute << endl;
    _os << *producer;
	return _os;
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
    
    _os << "GROUP BY" << endl;
    _os << "Schema IN : " << schemaIn << "\t";
    _os << "Schema OUT : " << schemaOut << "\t";
    _os << "Grouping Atts : " << groupingAtts << /*"\t"*/endl;
    // TODO Figure Out What to Print Here
    //_os << "Compute : " << compute << endl;
    _os << *producer;
    return _os;
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {    // Michael

	schema = _schema;
	outFile = _outFile;
	producer = _producer;

}

WriteOut::~WriteOut() {

}

ostream& WriteOut::print(ostream& _os) {
	
	_os << "WRITE OUT" << endl;
	_os << "Final Schema : " << schema << "\t";
 	_os << "File : " << outFile << endl;
    _os << *producer;
	return _os;
}


ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
    
    // TODO DELETE COMMENT HERE
    // Print tree using preorder tree traversal
    // From root to each child till whole tree complete
    
    return _os << "QUERY EXECUTION TREE:\n" << *_op.root << "\n";
}
