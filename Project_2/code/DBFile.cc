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

	fileName = f_path;
	fileType = f_type;

	return file.Open(0,f_path);

}

int DBFile::Open (char* f_path) {

	// do not know how to implement a check for this

	fileName = f_path;
	//if file not exist create one
	if(fileName){

		return Create(f_path, Heap);

	}
	//if it does open
	else{

		return file.Open(fileName.length(),f_path);

	}
}

void DBFile::Load (Schema& schema, char* textFile) {

	//need more reading

}

int DBFile::Close () {

	return file.Close();

}

void DBFile::MoveFirst () {

	filePointer = 0;
	currPage.EmptyItOut();

}

void DBFile::AppendRecord (Record& rec) {

	if(currPage.Append(rec) == 0){

		file.AddPage(currPage,filePointer++);
		currPage.EmptyItOut();
		currPage.Append(rec);

	}

}

int DBFile::GetNext (Record& rec) {

	//need more reading

}
