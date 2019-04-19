#include <iostream>
#include <sstream>
#include "RelOp.h"
#include "Keyify.h"
#include "EfficientMap.h"
#include "TwoWayList.h"

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

bool Project::GetNext(Record& _record) {    // TODO: May be a problem here
    //cout << "Project GetNext" << endl;
    //cout << "Out: " << numAttsOutput << " In: " << numAttsInput << endl;
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
    ran = false;
    
    if (predicate.GetSortOrders(omL, omR) == 0) {
        cout << "Error getting OrderMaker from predicate" << endl;
    }
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

void Join::NLJ(Record& _record) {
    // Nested-Loop Join
    
    // Build Phase -------------------------------------------------------
    Record tempRec;
    
    // Inserting (TODO:smaller?) one side of relation into memory
    while (left->GetNext(tempRec)) {
        joinList.Insert(tempRec);
    }
    
    // Probe Phase -------------------------------------------------------
    
}

void Join::HJ(Record& _record) {
    cout << "Hash Join" << endl;
    
    // Hash Join
    Record tempRec;
    SwapInt insertCount = 0;
    
    // Inserting (TODO:smaller?) one side of relation into memory
    while (left->GetNext(tempRec)) {
        tempRec.SetOrderMaker(&omL);
        hashMapJ.Insert(tempRec, insertCount);
    }
    
//    // Print Current Data
//    int accc = 0;
//    cout << "Printing" << endl;
//    hashMapJ.MoveToStart();
//    for (EfficientMap<Record, SwapInt> it = hashMapJ; !it.AtEnd(); it.Advance()) {
//        //cout << "Key " << it.CurrentKey() << " Data " << it.CurrentData() << endl;
//        it.CurrentKey().print(cout, schemaLeft);
//        cout << endl;
//        accc++;
//    }
//    cout << "End Print / Count = " << accc << endl;
    
    // Go through other side after left side has been inserted
    while (right->GetNext(tempRec)) {
        tempRec.SetOrderMaker(&omR);
        
        // Probe
        while(true) {
            if (hashMapJ.IsThere(tempRec)) {
                cout << "Found Same" << endl;
                //tempRec.print(cout, schemaRight);
                //cout << endl;
                
                //Temp values to store removed data from map
                Record removedRec;
                SwapInt removedData;
                // If a value is found remove from map
                hashMapJ.Remove(tempRec, removedRec, removedData);
                
                Record newRec;
                // Append the two records
                newRec.AppendRecords(removedRec, tempRec, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
                cout << "appended" << endl;
                
                cout << "Old -" << endl;
                removedRec.print(cout, schemaLeft);
                
                cout << "\nNew -"  << endl;
                Record r1;
                Record r2;
                
                r2.AppendRecords(newRec, r1, schemaLeft.GetNumAtts(), 0);
                r2.print(cout, schemaLeft);
                
                // And set it into a two way list
                joinList.Insert(newRec);
            }
            else {
                // When no more records match
                cout << "No More Records" << endl;
                joinList.AtStart();
                for (int x = 0; x < joinList.Length(); x++) {
                    SwapInt tempSI = 0;
                    // Current Record
                    Record r = joinList.Current();
                    // Blank Record to get Left side from output record
                    Record r1;
                    Record r2;
                    // Creates left record from out record
                    r2.AppendRecords(r, r1, schemaLeft.GetNumAtts(), 0);
                    r2.print(cout, schemaLeft);
                    hashMapJ.Insert(r2, tempSI);
                    joinList.Advance();
                }
                break;
            }
        }
    
    }
//    int ccc = 0;
//    cout << "Printing" << endl;
//    //joinList.MoveToStart();
//    TwoWayList<Record> it;
//    for (it.Swap(joinList); !it.AtEnd(); it.Advance()) {
//        it.Current().print(cout, schemaOut);
//        cout << endl;
//        ccc++;
//    }
//    cout << "End Print / Count = " << ccc << endl;
}

void Join::SHJ(Record& _record) {
    // Symmetric Hash Join
    
}

bool Join::GetNext(Record& _record) {
    cout << "Join GetNext" << endl;

    if (!ran) {
        HJ(_record);
        ran = true;
        return true;
//        // Check to see if there are any inequality conditions
//        for (int x = 0; x < predicate.numAnds; x++) {
//            if (predicate.andList[x].op == '>' || predicate.andList[x].op == '<') {
//                NLJ(_record);
//                ran = true;
//                return true;
//            }
//        }
//
//        // Gets Record count
//        countLeft = 0;
//        countRight = 0;
//        Record tempRec;
//
//        while (left->GetNext(tempRec)) {
//            countLeft++;
//        }
//        while (right->GetNext(tempRec)) {
//            countRight++;
//        }
//
//        cout << "Left# = " << countLeft << " Right# = " << countRight << endl;
//
//        // If both children have larger records than 1000
//        if (countLeft >= 1000 && countRight >= 1000) {
//            SHJ(_record);
//            ran = true;
//            return true;
//        }
//        else {
//            HJ(_record);
//            ran = true;
//            return true;
//        }
    }

    return false;
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
    //cout << "Distinct GetNext" << endl;
    while (producer->GetNext(_record)) {
        
        stringstream currKey;           // Our Key
        _record.print(currKey, schema); // Fills our unique key
        //cout << currKey.str() << endl;
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
    //cout << "Sum GetNext" << endl;
    Record tempRec;
    double runSum = 0.0;

    // If sum not computed
    if (!hasComp) {
        while (producer->GetNext(tempRec)) {
            int runIntSum = 0;
            double runDoubSum = 0.0;
            Type retType = compute.Apply(tempRec, runIntSum, runDoubSum);
            
            // Gets return type and adds to the running sum depending on that type
            if (retType == Float) {
                runSum += runDoubSum;
                //cout << "Double: " << runDoubSum << endl;
            }
            if (retType == Integer) {
                runSum += (double)runIntSum;
                //cout << "Int: " << runIntSum << endl;
            }
            
            //cout << "Sum = " << runSum << endl;
        }
        
        // Creates a record of only the sum to pass
        // recContent creates record of our runnning sum
        char* recContent = new char[(2*sizeof(int))+sizeof(double)];
        ((int *) recContent)[0] = 2*sizeof(int)+sizeof(double);
        //cout << "A: " << ((int *) recContent)[0] << endl;
        ((int *) recContent)[1] = 2*sizeof(int);
        //cout << "B: " << ((int *) recContent)[1] << endl;
        ((double *) (recContent+2*sizeof(int)))[0] = runSum;
        //cout << ((double *) (recContent+2*sizeof(int)))[0] << endl;
        _record.Consume(recContent);
        
        // We have now comuted the sum once
        // Make sure we dont run again
        hasComp = true;
        return true;
		
    }
    
    // We have already ran through sum before
	//cout << schemaOut << endl;
    return false;
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
//    groupingAtts = _groupingAtts;
    groupingAtts.Swap(_groupingAtts);
	compute = _compute;
	producer = _producer;
    atBeginning = true;
}
GroupBy::~GroupBy() {
}

bool GroupBy::GetNext(Record& _record) {
    //cout << "Group By GetNext" << endl;
	if(atBeginning){

		//Creating dummy record variables
		Record rec;
//        Record temp;

		//For all getnexts
		while(producer->GetNext(rec)){
            //cout << "CHECK" << endl;
            
            //Apply and set as runningSum
            int intResult = 0;
            double doubleResult = 0.0;
			int applied = compute.Apply(rec, intResult, doubleResult);
			double runningSum = intResult + doubleResult;
            
//             Create new temp Record
//            temp = rec;
//            temp.SetOrderMaker(&groupingAtts);
            rec.SetOrderMaker(&groupingAtts);
			KeyDouble kd = KeyDouble(runningSum);
//            cout << kd << endl;

			//Check if this record exists
//            if(hashtable.IsThere(temp)){ //KeyDouble it
//                hashtable.Find(temp) = KeyDouble(hashtable.Find(temp) + runningSum);
//                cout << hashtable.Find(temp) << endl;
//            }
//            else //Insert it into table
//                hashtable.Insert(temp, kd);
            if(hashtable.IsThere(rec)){ //KeyDouble it
                hashtable.Find(rec) = KeyDouble(hashtable.Find(rec) + runningSum);
                //cout << hashtable.Find(rec) << endl;
            }
            else //Insert it into table
                hashtable.Insert(rec, kd);
		}

		//No longer beginning of table
		atBeginning = false;
		//Reset the hashtable
		hashtable.MoveToStart();
        //cout << "done" << endl;
	}

	if(hashtable.AtEnd()) //counldnt get any more
		return false;
	
	double tempdouble = hashtable.CurrentData();
    //cout << "Temp Dbl: " << tempdouble << endl;
	
	char* recContent = new char[(2*sizeof(int))+sizeof(double)];
	((int *) recContent)[0] = 2*sizeof(int)+sizeof(double);
	//cout << "A: " << ((int *) recContent)[0] << endl;
	((int *) recContent)[1] = 2*sizeof(int);
	//cout << "B: " << ((int *) recContent)[1] << endl;
	((double *) (recContent+2*sizeof(int)))[0] = tempdouble;

	Record r;
	r.Consume(recContent);
    
    hashtable.CurrentKey().Project(groupingAtts.whichAtts, groupingAtts.numAtts, schemaIn.GetNumAtts());
//    for (int xxx = 0; xxx < (sizeof(groupingAtts.whichAtts)/sizeof(groupingAtts.whichAtts[0])); xxx++) {
//        cout << xxx << ": Checking " << groupingAtts.whichAtts[xxx] << " " << groupingAtts.numAtts << " " << schemaIn.GetNumAtts() << endl;
//    }
    
	char* bits = hashtable.CurrentKey().GetBits();
	int size = hashtable.CurrentKey().GetSize();
    //cout << "Bits " << ((double *) (bits+2*sizeof(int)))[0] << "\tSize " << size << endl;
	Record r2;
	r2.CopyBits(bits, size);
    
	_record.AppendRecords(r, r2, 1, groupingAtts.numAtts);
    //_record.AppendRecords(r, r2, 1, schemaOut.GetNumAtts() - 1);
	hashtable.Advance();
	return true;

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
        cout << endl;
        //cout << "Success at Write Out" << endl;
        return true;
    }
    else{
        //cout << "WO Schema " << schema << endl;
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


