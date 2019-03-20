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

	fileName = f_path;
	fileType = f_type;

	return file.Open(0, f_path);

}

int DBFile::Open(char* f_path) {

	// do not know how to implement a check for this

	fileName = f_path;
	//if file not exist create one

	int result = file.Open(fileName.length(), f_path);

	if (result == -1) {

		cout << "Could not open file, Creating New one: " << endl;
		return Create(f_path, Heap);

	}

	else if (result == 0) {

		cout << "File successfully opened" << endl;

	}

	return result;

}

void DBFile::Load(Schema& schema, char* textFile) {

	char * txt = textFile;

	MoveFirst();
	FILE* newfile = fopen(txt, "r");

	while(true){

		Record record;

		if(record.ExtractNextRecord(schema, *newfile)){

			AppendRecord(record);

		}

		else{

			break;

		}

	}

	file.AddPage(currPage,filecount);
	currPage.EmptyItOut();
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
