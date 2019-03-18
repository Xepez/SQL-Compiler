#ifndef DBFILE_H
#define DBFILE_H

#include <string>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"

using namespace std;


class DBFile {
private:
	File file;
	string fileName;

    // Current page we are on
    Page page;
    // Current length in the page
    off_t currLen;
    
public:
	DBFile ();
	virtual ~DBFile ();
	DBFile(const DBFile& _copyMe);
	DBFile& operator=(const DBFile& _copyMe);

    // Creates new File : Returns 0 for success and -1 for failure
	int Create (char* fpath, FileType f_type);
    // Opens the File : Returns 0 for success and -1 for failure
	int Open (char* fpath);
    // Closes File : Returns length in number of pages
	int Close ();

	void Load (Schema& _schema, char* textFile);

	void MoveFirst ();
	void AppendRecord (Record& _addMe);
	int GetNext (Record& _fetchMe);
};

#endif //DBFILE_H
