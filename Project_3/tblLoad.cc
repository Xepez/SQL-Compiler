#include <iostream>
#include <string>

#include "DBFile.h"
#include "Catalog.h"
#include "Schema.h"
#include "File.h"

using namespace std;

/*
        IGNORE THIS FILE. IT HAS BEEN COMBINED WITH main.cc TO TRY AND FIX
        ANNOYING ERRORS. - Peyton
*/

// Compile:
// g++ tblLoad.cc DBFile.cc File.cc Schema.cc Catalog.cc Record.cc -o tblLoad.out -lsqlite3
// -----------------------------------------------------------------------------------
// Run:
// ./tblLoad.out

void loadTables(string tblName, char* heapLoc, char* txtFile) {
    
    // Open our catalog
    string db = "catalog.sqlite";
    Catalog catalog(db);
    
    // Get the tables schema
    //cout << "Getting table Schema" << endl;
    Schema schema;
    catalog.GetSchema(tblName, schema);
    
    // Creates Heap File for Table
    //cout << "Creating File" << endl;
    DBFile dbF = DBFile();
    dbF.Create(heapLoc, Heap);
    
    // Opens Head File
    //cout << "Opening File" << endl;
    dbF.Open(heapLoc);
    
    // Loads Data
    //cout << "Loading Data" << endl;
    dbF.Load(schema, txtFile);
    
    // Updates Catalog Info
    //cout << "Updating Catalog" << endl;
    string stringLoc = heapLoc;
    catalog.SetDataFile(tblName, stringLoc);
    
    // Closes File
    //cout << "Closing File" << endl;
    dbF.Close();
}

int main() {
    cout << "---------------------------------------------------" << endl;
    cout << "Loading Table information" << endl;
    loadTables("customer", "heap/customer.heap", "tables/customer.tbl");
    loadTables("lineitem", "heap/lineitem.heap", "tables/lineitem.tbl");
    loadTables("nation", "heap/nation.heap", "tables/nation.tbl");
    loadTables("orders", "heap/orders.heap", "tables/orders.tbl");
    loadTables("part", "heap/part.heap", "tables/part.tbl");
    loadTables("partsupp", "heap/partsupp.heap", "tables/partsupp.tbl");
    loadTables("region", "heap/region.heap", "tables/region.tbl");
    loadTables("supplier", "heap/supplier.heap", "tables/supplier.tbl");
//    loadTables("customer", "customer.heap", "customer.tbl");
//    loadTables("lineitem", "lineitem.heap", "lineitem.tbl");
//    loadTables("nation", "nation.heap", "nation.tbl");
//    loadTables("orders", "orders.heap", "orders.tbl");
//    loadTables("part", "part.heap", "part.tbl");
//    loadTables("partsupp", "partsupp.heap", "partsupp.tbl");
//    loadTables("region", "region.heap", "region.tbl");
//    loadTables("supplier", "supplier.heap", "supplier.tbl");
    cout << "Finshed loading Data" << endl;
    cout << "---------------------------------------------------" << endl;
    
    return 0;
}
