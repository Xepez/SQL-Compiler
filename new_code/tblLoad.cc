#include <iostream>
#include <string>

#include "DBFile.h"
#include "Catalog.h"
#include "Schema.h"
#include "File.h"

using namespace std;

void loadTables(string tblName, string heapLoc, char* txtFile) {
    
    // Open our catalog
    string db = "catalog.sqlite";
    Catalog catalog(db);
    
    // Get the tables schema
    //cout << "Getting table Schema" << endl;
    Schema schema;
    catalog.GetSchema(tblName, schema);
    
    // Creates Heap File for Table
    //cout << "Creating File" << endl;
    DBFile dbFile;
    FileType ftype = Heap;
    dbFile.Create(txtFile, ftype);
    
    // Opens Head File
    //cout << "Opening File" << endl;
    dbFile.Open(txtFile);
    
    // Loads Data
    //cout << "Loading Data" << endl;
    dbFile.Load(schema, txtFile);
    
    // Updates Catalog Info
    //cout << "Updating Catalog" << endl;
    catalog.SetDataFile(tblName, heapLoc);
    
    // Closes File
    //cout << "Closing File" << endl;
    dbFile.Close();
}
