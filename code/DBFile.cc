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
        cout << "Success Opening DB File - " << fileName << endl;
        
        // Initialize all needed variables
        currPage = 0;
        //file.GetPage(currPage, 0);
        //currPage.EmptyItOut();
        //file.AddPage(currPage, 0);
        
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
    
//	while(true){
//        //cout << "REE " << filecount << endl;
//		Record record;
//		if(record.ExtractNextRecord(schema, *newfile) == 1){
//            if (Page.Append(record) == 0) {
//                file.AddPage(Page, currPage);
//                currPage++;
//                Page.EmptyItOut();
//            }
//
//		}
//		else{
//            file.AddPage(Page, currPage);
//            currPage++;
//            Page.EmptyItOut();
//			break;
//		}
//
//	}
//    cout << file.GetLength() << endl;
//	fclose(newfile);

	while(true){

		Record rec;
		if(rec.ExtractNextRecord(schema, *newfile)){

			AppendRecord(rec);

		}
		else{

			break;

		}

	}

	file.AddPage(page, currPage);
	page.EmptyItOut();

}

int DBFile::Close() {

	return file.Close();

}

void DBFile::MoveFirst() {

	currPage = 0;
	page.EmptyItOut();

}

void DBFile::AppendRecord(Record& rec) {

	if (!page.Append(rec)) {

		file.AddPage(page, currPage++);
		page.EmptyItOut();
		page.Append(rec);

	}

}

int DBFile::GetNext(Record& rec) {
//    cout << "Geddet" << endl;
//    if(currPage.GetFirst(rec) != 0){
//        cout << "F" << endl;
//        return 1;
//    }
////    else if(filecount == file.GetLength() || file.GetPage(currPage, filecount) == -1){
////        cout << "S" << endl;
////        return 0;
////    }
//    else if(filecount == file.GetLength()){
//        cout << "S " << filecount << " = " << file.GetLength() << endl;
//        return 0;
//    }
//    else if (file.GetPage(currPage, filecount) == -1) {
//        cout << "F" << endl;
//        return 0;
//    }
//    else{
//        cout << "T" << endl;
//        currPage.GetFirst(rec);
//        filecount++;
//        return 1;
//    }

	int ret = page.GetFirst(rec);

	if(ret != 0){
		cout << "ret = true " << endl;
		return 1;

	}else{

		if(currPage == file.GetLength()){
			cout << "ret = false " << endl;
			cout << "S " << currPage << " = " << file.GetLength() << endl;
			return 0;

		}else{

			currPage++;
			ret = page.GetFirst(rec);
			cout << "ret = true " << endl;

			return 1;
		}

	}




















}
