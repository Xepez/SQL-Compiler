#include <string>
#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"

using namespace std;

DBFile::DBFile() :
		fileName("") {
}

DBFile::~DBFile() {
}

DBFile::DBFile(const DBFile& _copyMe) :
		file(_copyMe.file), fileName(_copyMe.fileName) {
}

DBFile& DBFile::operator=(const DBFile& _copyMe) {
	// handle self-assignment first
	if (this == &_copyMe)
		return *this;

	file = _copyMe.file;
	fileName = _copyMe.fileName;

	return *this;
}

int DBFile::Create(char* f_path, FileType f_type) {

    // Couldnt figure out f_type kept getting 0 value
//    if (f_type != Heap) {
    fileName = f_path;
    fileType = f_type;

    return file.Open(0, f_path);
//    }
//    else {
//        cout << "Failed Create - " << f_type << endl;
//        return -1;
//    }

}

int DBFile::Open(char* f_path) {
    if (file.Open(1, f_path) == 0) {
        // Success at opening file
        cout << "Success Opening DB File" << endl;
        
        // Initialize all needed variables
        filecount = 0;
        file.AddPage(currPage, 0);
        
        return 0;
    }
    else {
        // Fail
        cout << "Failure to Open DB File" << endl;
        return Create(f_path, Heap);
    }

}

void DBFile::Load(Schema& schema, char* textFile) {
    
	MoveFirst();
    filecount = 0;
	FILE* newfile = fopen(textFile, "r");

	while(true){

		Record record;
        //cout << "Check" << endl;
		if(record.ExtractNextRecord(schema, *newfile)){
            //cout << "Got" << endl;
            if (currPage.Append(record) == 0) {
                file.AddPage(currPage, filecount);
                filecount++;
                currPage.EmptyItOut();
            }

		}
		else{
            file.AddPage(currPage, filecount);
            filecount++;
            currPage.EmptyItOut();
			break;
		}

	}
    
	fclose(newfile);
}

int DBFile::Close() {

	return file.Close();

}

void DBFile::MoveFirst() {

	filecount = 0;
	currPage.EmptyItOut();

}

void DBFile::AppendRecord(Record& rec) {

	if (!currPage.Append(rec)) {

		file.AddPage(currPage, filecount++);
		currPage.EmptyItOut();
		currPage.Append(rec);

	}

}

int DBFile::GetNext(Record& rec) {


	
	if(currPage.GetFirst(rec)){
		
		return 1;
		
	}
	else{
	
		if(filecount == file.GetLength()){
		
			return -1;
			
		}
		else{
			
			file.GetPage(currPage, filecount++);
			
			return 0;
		}
	}



}
