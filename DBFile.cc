#include <string>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"

using namespace std;


DBFile::DBFile () : fileName("") {
}

DBFile::~DBFile () {
}

DBFile::DBFile(const DBFile& _copyMe) :
	file(_copyMe.file),	fileName(_copyMe.fileName) {}

DBFile& DBFile::operator=(const DBFile& _copyMe) {
	// handle self-assignment first
	if (this == &_copyMe) return *this;

	file = _copyMe.file;
	fileName = _copyMe.fileName;

	return *this;
}

int DBFile::Create (char* f_path, FileType f_type) {
    if (f_type == Heap) { // TODO: Not sure if correct
        // Success
        fileName = f_path;
        return file.open(0, f_path);
    }
    else {
        // Fail
        //cout << "DB Type Not Heap" << endl;
        return -1;
    }
}

int DBFile::Open (char* f_path) {
    if (file.open(1, f_path) == 0) {
        // Success at opening file
        cout << "Success Opening DB File" << endl;
        
        // Initialize all needed variables
        currLen = 0;
        page = NULL;
        file.GetPage(page, currLen);
        
        return 0;
    }
    else {
        // Fail
        cout << "Failure to Open DB File" << endl;
        return -1;
    }
}

void DBFile::Load (Schema& schema, char* textFile) {
    
    File* tempFile = fopen(textFile, "r");
    Record tempRec;
    //file.open(0, fileName);
    
    while(tempRec.ExtractNextRecord(schema, tempFile) != 0) {
        page.Append(rec);
    }
}

int DBFile::Close () {
    return file.Close();
}

void DBFile::MoveFirst () { // TODO: Not sure if correct
    // Resets to beginning
    currLen = 0;
    file.GetPage(page, currLen);
}

void DBFile::AppendRecord (Record& rec) {
    page.Append(rec);
}

int DBFile::GetNext (Record& rec) {
    
    // Checks if there is a record in the current page
    if (page.GetFirst(rec) == 0) {
        
        if (currLen == file.GetLength()) {
            // End of the road
            return -1;
        }
        else {
            // Gets next page w/ current length
            file.GetPage(page, currLen) == 0
            // Increase length by one
            currLen++;
            // Retry's Getting rec w/ new page
            page.GetFirst(rec) == 0;
            return 0;
        }
    }
    else {
        return 0;
    }
}
