#include <string>
#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"

using namespace std;

DBFile::DBFile() : fileName("") {
	//currPage = 0;
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
        fileName = f_path;
        cout << "Success Opening DB File - " << fileName << endl;

        // Initialize all needed variables
        currPage = 0;
        //file.GetPage(page, 0);
        //page.EmptyItOut();
        //file.AddPage(page, 0);
        
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
	FILE* newfile = fopen(textFile, "r");

    //cout << "Load" << endl;
    
//    int tempPointer = 0;
//    Page tempPage;
//    while(true){
//        Record record;
//        if(record.ExtractNextRecord(schema, *newfile) == 1){
//            if (tempPage.Append(record) == 0) {
//                file.AddPage(tempPage, tempPointer);
//                tempPointer++;
//                tempPage.EmptyItOut();
//            }
//
//        }
//        else{
//            file.AddPage(tempPage, tempPointer);
//            tempPointer++;
//            tempPage.EmptyItOut();
//            break;
//        }
//
//    }
//    cout << "File Len: " << file.GetLength() << " \ PointerLen: " << tempPointer << endl;
    
    while(true){
        
        Record rec;
        if(rec.ExtractNextRecord(schema, *newfile) == 1){
            if (page.Append(rec) == 0) {
                file.AddPage(page, currPage++);
                page.EmptyItOut();
                page.Append(rec);
            }
            else
                continue;
        }
        else{
            break;
        }

    }

    file.AddPage(page, currPage++);
    page.EmptyItOut();
    //cout << "File Len: " << file.GetLength() << " / PointerLen: " << currPage << endl;
    fclose(newfile);
}

int DBFile::Close() {

	return file.Close();
}

void DBFile::MoveFirst() {

	currPage = 0;
	page.EmptyItOut();
//    file.GetPage(page, currPage);
}

void DBFile::AppendRecord(Record& rec) {

	if (!page.Append(rec)) {

		file.AddPage(page, currPage++);
		page.EmptyItOut();
		page.Append(rec);

	}

}

int DBFile::GetNext(Record& rec) {
    cout << "Geddet" << endl;
    //MoveFirst();
    if(page.GetFirst(rec) != 0){
        cout << "F" << endl;
        return 1;
    }
    else if(currPage == file.GetLength() || file.GetPage(page, currPage) == -1){
        cout << "S" << endl;
        return 0;
    }
//    else if(currPage == file.GetLength()){
//        cout << "S " << currPage << " = " << file.GetLength() << endl;
//        return 0;
//    }
//    else if (file.GetPage(page, currPage) == -1) {
//        cout << "F" << endl;
//        return 0;
//    }
    else{
        cout << "T" << endl;
        page.GetFirst(rec);
        currPage++;
        return 1;
    }

//    int ret = page.GetFirst(rec);
//
//    if(ret != 0){
//        cout << "F ret = true " << endl;
//        return 1;
//
//    }else{
//
//        if(currPage == file.GetLength()){
//            cout << "ret = false " << endl;
//            cout << "S " << currPage << " = " << file.GetLength() << endl;
//            return 0;
//
//        }else{
//
//            currPage++;
//            ret = page.GetFirst(rec);
//            cout << "T ret = true " << endl;
//
//            return 1;
//        }
//
//    }
}
