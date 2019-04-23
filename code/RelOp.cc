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
    hashAdded = false;
    swap = true;
    firstLeft = true;
    
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

bool Join::NLJ(Record& _record) {
    // Nested-Loop Join
    
    // Build Phase -------------------------------------------------------
    Record tempRec;
    
    // Inserting (TODO:smaller?) one side of relation into memory
    while (left->GetNext(tempRec)) {
        joinList.Insert(tempRec);
    }
    
    // Probe Phase -------------------------------------------------------
    
}

bool Join::HJ(Record& _record) {
    // Hash Join
    cout << "Hash Join" << endl;
    
    // Temp varialbes to hold our inserted data
    Record tempRec;
    SwapInt insertCount = 0;

    // Insert left side into the hashmap
    if (!hashAdded) {
        cout << "Adding Left Side" << endl;
        while (left->GetNext(tempRec)) {
            // Set left side ordermaker
            tempRec.SetOrderMaker(&omL);
            // Insert into our hashmap
            hashMapJ.Insert(tempRec, insertCount);
            insertCount = insertCount + 1;
        }
        // Ensure we run this again
        hashAdded = true;
    }
    
    // If list is not empty keep returning records till it is
    if (!joinList.AtEnd()) {
        cout << "List Not Empty" << endl;
        joinList.MoveToStart();
        _record = joinList.Current();
        joinList.Remove(_record);
        return true;
    }
    else {
        // If list is empty get right record and probe
        
        // Putting used records back into hashmap
        cout << "Putting back into hashmap from list" << endl;
        putBackList.MoveToStart();
        SwapInt tempSI = 0;
        for (int x = 0; x < putBackList.Length(); x++) {
            Record r = putBackList.Current();
            hashMapJ.Insert(r, tempSI);
            tempSI = tempSI + 1;
            putBackList.Advance();
        }
        
        // Get right record and probe left side
        if (right->GetNext(tempRec)) {
            // Set right ordermaker
            tempRec.SetOrderMaker(&omR);
            
//            cout << "Right Record:" << endl;
//            tempRec.print(cout, schemaRight);
//            cout << endl;
//            cout << "L: " << schemaLeft << endl;
//            cout << "R: " << schemaRight << endl;
            
            cout << "check we got a right record" << endl;
            // Probe
            while (hashMapJ.IsThere(tempRec)) {
                cout << "Found same record in hashmap" << endl;
                
                //Temp values to store removed data from map
                Record removedRec;
                SwapInt removedData;
                
                // If a value is found remove from map
                hashMapJ.Remove(tempRec, removedRec, removedData);
                
                // New Record to store our appended right and left records
                Record newRec;
                // Append the two records
                newRec.AppendRecords(removedRec, tempRec, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
                cout << "Append Succeeded" << endl;
                
                // Print Records
//                cout << "LEFT RECORD:" << endl;
//                removedRec.print(cout, schemaLeft);
//                cout << "\n" << endl;
//
//                cout << "RIGHT RECORD:" << endl;
//                tempRec.print(cout, schemaRight);
//                cout << "\n" << endl;
//
//                cout << "OUR NEW RECORD:" << endl;
//                newRec.print(cout, schemaOut);
//                cout << "\n" << endl;
                
                // And appended record to our list of matched records
                joinList.Insert(newRec);
                // Also need to save old records to be put back
                putBackList.Insert(removedRec);
                cout << "Inserted properly to lists" << endl;
            }
            
            // Return First Appended Record
            cout << "Done in while loop" << endl;
            joinList.MoveToStart();
            _record = joinList.Current();
            joinList.Remove(_record);
            cout << "Finished and returning True w/ appeneded record" << endl;
            return true;
        }
        else {
            cout << "Returned False" << endl;
            return false;
        }
    }
}

bool Join::SHJ(Record& _record) {
    // Symmetric Hash Join
    cout << "Symmetric Hash Join" << endl;
    
    Record tempRec;
    SwapInt leftCount = 0;
    SwapInt rightCount = 0;
    
    // IF LIST IS NOT EMPTY RETURN TUPLE
    if (!joinList.AtEnd()) {
        cout << "List Not Empty" << endl;
        joinList.MoveToStart();
        _record = joinList.Current();
        joinList.Remove(_record);
        return true;
    }
    else if (swap) {
        // Swap from left to right
        swap = false;
        
        // Make sure right hashmap is full
        cout << "Putting Back from Right List" << endl;
        // When no more records match
        putBackRight.MoveToStart();
        SwapInt tempSI = 0;
        for (int x = 0; x < putBackRight.Length(); x++) {
            // Current Record
            Record r = putBackRight.Current();
            hashLeft.Insert(r, tempSI);
            tempSI = tempSI + 1;
            putBackRight.Advance();
        }
        
        if (left->GetNext(tempRec)) {
            // Build
            cout << "Build Left" << endl;
            tempRec.SetOrderMaker(&omL);
            hashLeft.Insert(tempRec, leftCount);
            leftCount = leftCount + 1;
            
            if (!firstLeft) {
                // Probe Right Side
                while (hashRight.IsThere(tempRec)) {
                    cout << "Found Same in Right Hashmap" << endl;
                    
                    //Temp values to store removed data from map
                    Record removedRec;
                    SwapInt removedData;
                    // If a value is found remove from map
                    hashRight.Remove(tempRec, removedRec, removedData);
                    
                    Record newRec;
                    // Append the two records
                    newRec.AppendRecords(tempRec, removedRec, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
                    //cout << "Appended" << endl;
                    
                    // And set it into a two way list
                    joinList.Insert(newRec);
                    // Also need to save records to be put back
                    putBackRight.Insert(removedRec);
                    //cout << "Inserted Properly to lists" << endl;
                }
            }
            else {
                // After our first rotation
                firstLeft = false;
            }
            
            cout << "Done now Return First Record" << endl;
            joinList.MoveToStart();
            _record = joinList.Current();
            joinList.Remove(_record);
            return true;
        }
        else {
            return false;
        }
    }
    else {
        // Swap from right to left
        swap = true;
        
        // Make sure left hashmap is full
        cout << "Putting Back from Left List" << endl;
        // When no more records match
        putBackLeft.MoveToStart();
        SwapInt tempSI = 0;
        for (int x = 0; x < putBackLeft.Length(); x++) {
            // Current Record
            Record r = putBackLeft.Current();
            hashLeft.Insert(r, tempSI);
            tempSI = tempSI + 1;
            putBackLeft.Advance();
        }
        
        if (right->GetNext(tempRec)) {
            // Build
            cout << "Build Right" << endl;
            tempRec.SetOrderMaker(&omR);
            hashRight.Insert(tempRec, rightCount);
            rightCount = rightCount + 1;
            
            // Probe Left Side
            while (hashLeft.IsThere(tempRec)) {
                cout << "Found Same in Left Hashmap" << endl;
                
                //Temp values to store removed data from map
                Record removedRec;
                SwapInt removedData;
                // If a value is found remove from map
                hashLeft.Remove(tempRec, removedRec, removedData);
                
                Record newRec;
                // Append the two records
                newRec.AppendRecords(removedRec, tempRec, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
                //cout << "Appended" << endl;

                // And set it into a two way list
                joinList.Insert(newRec);
                // Also need to save records to be put back
                putBackLeft.Insert(removedRec);
                //cout << "Inserted Properly to lists" << endl;
            }
            
            cout << "Done now Return First Record" << endl;
            joinList.MoveToStart();
            _record = joinList.Current();
            joinList.Remove(_record);
            return true;
        }
        else {
            return false;
        }
    }
}

bool Join::GetNext(Record& _record) {
    cout << "Join GetNext" << endl;

    return HJ(_record);
//        // Check to see if there are any inequality conditions
//        for (int x = 0; x < predicate.numAnds; x++) {
//            if (predicate.andList[x].op == '>' || predicate.andList[x].op == '<')
//                return NLJ(_record);
//        }
//
//        // If both children have larger records than 1000
//        if (countLeft >= 1000 && countRight >= 1000)
//            return SHJ(_record);
//        else
//            return HJ(_record);
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
    cout << "Write Out GetNext" << endl;
    if (producer->GetNext(_record)) {
        cout << "Back to Write Out" << endl;
        _record.print(cout, schema);
        cout << endl;
        cout << "Success at Write Out" << endl;
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


