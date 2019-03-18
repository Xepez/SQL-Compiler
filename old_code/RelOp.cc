#include <iostream>
#include "RelOp.h"

using namespace std;

ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}

Scan::Scan(Schema& _schema, DBFile& _file, string _table) {    // Peyton

	// Saves Data for printing
	schema = _schema;
	file = _file;
	table = _table;
}

Scan::~Scan() {

}

bool Scan::tableCheck(string _table) {
	if (table == _table)
		return true;
	else
		return false;
}

ostream& Scan::print(ostream& _os) {

	_os << "SCAN {";
	_os << "Schema : " << schema << "}\n" << endl;
	return _os;
}

Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
		RelationalOp* _producer, string _table) { // Michael

	schema = _schema;
	predicate = _predicate;
	constants = _constants;
	producer = _producer;
	table = _table;
}

Select::~Select() {

}

bool Select::GetNext(Record& _record) {

	while (producer->GetNext(_record)) {

		if (predicate.Run(_record, constants)) {

			return true;

		}

	}

	return false;

}

bool Select::tableCheck(string _table) {
	if (table == _table)
		return true;
	else
		return false;
}

ostream& Select::print(ostream& _os) {

	_os << "SELECT {";
	_os << "Schema : " << schema << " ";
	_os << "Predicate : " << predicate << "}\n" << endl;
	// TODO Figure Out What to Print Here
	//_os << "Constants : " << constants << endl;
	_os << *producer;
	return _os;
}

Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
		int _numAttsOutput, int* _keepMe, RelationalOp* _producer) {   // Peyton

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

bool Project::GetNext(Record& _record) {

	if (producer->GetNext(_record)) {

		_record.Project(keepMe, numAttsOutput, numAttsInput);
		return true;

	}

	else {

		return false;

	}
}

ostream& Project::print(ostream& _os) {

	_os << "PROJECT {";
	_os << "Schema IN : " << schemaIn << " Number IN Atts : " << numAttsInput
			<< " ";
	_os << "Schema OUT : " << schemaOut << " Number OUT Atts : "
			<< numAttsOutput << " ";
	_os << "Keeping : ";
	for (int x = 0; x < numAttsOutput; x++) {
		// Goes through KeepMe array and shows which atts are going to be kept
		_os << keepMe[x] << " ";
	}
	_os << "}\n\n" << *producer;
	return _os;
}

Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
		CNF& _predicate, RelationalOp* _left, RelationalOp* _right) { // Michael

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

	_os << "JOIN {";
	_os << "Schema Left IN : " << schemaLeft << " ";
	_os << "Schema Right IN : " << schemaRight << " ";
	_os << "Schema OUT : " << schemaOut << " ";
	_os << "Predicate : " << predicate << "}\n" << endl;
	_os << *left;
	_os << *right;
	return _os;
}

DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) { // Peyton

	// Saves Data for printing
	schema = _schema;
	producer = _producer;
}

DuplicateRemoval::~DuplicateRemoval() {

}

ostream& DuplicateRemoval::print(ostream& _os) {

	_os << "DISTINCT {";
	_os << "Schema : " << schema << "}\n" << endl;
	_os << *producer;
	return _os;
}

Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
		RelationalOp* _producer) {  // Michael

	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	compute = _compute;
	producer = _producer;
}

Sum::~Sum() {

}

ostream& Sum::print(ostream& _os) {

	_os << "SUM {";
	_os << "Schema IN : " << schemaIn << " ";
	_os << "Schema OUT : " << schemaOut << "}\n" << endl;
	// TODO Figure Out What to Print Here
	//_os << "Compute : " << compute << endl;
	_os << *producer;
	return _os;
}

GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut,
		OrderMaker& _groupingAtts, Function& _compute,
		RelationalOp* _producer) {  // Peyton

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

	_os << "GROUP BY {";
	_os << "Schema IN : " << schemaIn << " ";
	_os << "Schema OUT : " << schemaOut << " ";
	_os << "Grouping Atts : " << groupingAtts << "}\n" << endl;
	// TODO Figure Out What to Print Here
	//_os << "Compute : " << compute << endl;
	_os << *producer;
	return _os;
}

WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) { // Michael

	schema = _schema;
	outFile = _outFile;
	producer = _producer;

}

WriteOut::~WriteOut() {

}

bool WriteOut::GetNext(Record& _record) {

	//does not compile with this _os

	//ostream _os;

	if (producer->GetNext(_record)) {
		_record.print(_os, schema);
		return true;

	}

	return false;
}

ostream& WriteOut::print(ostream& _os) {

	Record rec;

	_os << "WRITE OUT {";
	_os << "Final Schema : " << schema << " ";
	_os << "File : " << outFile << "}\n" << endl;

	//rec.print(_os, schema); // ????

	_os << *producer;
	return _os;
}

ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {

	// TODO DELETE COMMENT HERE
	// Print tree using preorder tree traversal
	// From root to each child till whole tree complete

	return _os << "QUERY EXECUTION TREE:\n" << *_op.root << "\n";
}
