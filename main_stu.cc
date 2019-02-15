#include <vector>
#include <string>
#include <iostream>

#include "Schema.h"
#include "Catalog.h"

using namespace std;


int main () {
     
//    string table = "region", attribute, type;
//    vector<string> attributes, types;
//    vector<unsigned int> distincts;
//
//    attribute = "r_regionkey"; attributes.push_back(attribute);
//    type = "INTEGER"; types.push_back(type);
//    distincts.push_back(5);
//    attribute = "r_name"; attributes.push_back(attribute);
//    type = "STRING"; types.push_back(type);
//    distincts.push_back(5);
//    attribute = "r_comment"; attributes.push_back(attribute);
//    type = "STRING"; types.push_back(type);
//    distincts.push_back(5);
//
//    Schema s(attributes, types, distincts);
//    Schema s1(s), s2; s2 = s1;
//
//    string a1 = "r_regionkey", b1 = "regionkey";
//    string a2 = "r_name", b2 = "name";
//    string a3 = "r_commen", b3 = "comment";
//
//    s1.RenameAtt(a1, b1);
//    s1.RenameAtt(a2, b2);
//    s1.RenameAtt(a3, b3);
//
//    s2.Append(s1);
//
//    vector<int> keep;
//    keep.push_back(5);
//    keep.push_back(0);
//    s2.Project(keep);
//
//    cout << s << endl;
//    cout << s1 << endl;
//    cout << s2 << endl;
//
    cout << "-----------------------------------------------------" << endl;
    cout << "Project 1:" << endl;
    
	string dbFile = "catalog.sqlite";
	Catalog c(dbFile);
    
    // Testing Purposes May Remove Later
    string table = "new";
    string attribute = "new_key";
    vector<string> attributes;
    vector<string> types;
    attributes.push_back(attribute);
    types.push_back("INTEGER");
    attributes.push_back("new_profile");
    types.push_back("STRING");
    attributes.push_back("new_cal");
    types.push_back("STRING");
    // -----------------------------------
    
    // Creates a Table
    cout << "-----------------------------------" << endl;
    c.CreateTable(table, attributes, types);
    cout << "-----------------------------------" << endl;

    // Display Current Catalog
    cout << "-----------------------------------" << endl;
    cout << c << endl;
    cout << "-----------------------------------" << endl;

    // Changes # of Tuples
    unsigned int temp = 9;
    c.SetNoTuples(table, temp);

    // If Table Exists Display # of Tuples
    unsigned int tupleCount = 0;
    cout << "-----------------------------------" << endl;
    if (c.GetNoTuples(table, tupleCount))
        cout << "# of Tuples: " << tupleCount << endl;
    cout << "-----------------------------------" << endl;

    // Changes Data Path
    string setPath = table + ".dat";
    c.SetDataFile(table, setPath);

    // If Table Exists Display File Path
    string path;
    cout << "-----------------------------------" << endl;
    if (c.GetDataFile(table, path))
        cout << "Path: " << path << endl;
    cout << "-----------------------------------" << endl;

    // Changes # of Distinct Elements
    unsigned int setNoDist = 5;
    for (vector<string>::iterator itD = attributes.begin(); itD != attributes.end(); itD++) {
        c.SetNoDistinct(table, *itD, setNoDist);
        setNoDist++;
    }

    // If Table Exists Display # of Distinct Elements
    cout << "-----------------------------------" << endl;
    cout << "# of Distinct: ";
    unsigned int noDistinct = 0;
    for (vector<string>::iterator itD = attributes.begin(); itD != attributes.end(); itD++) {
        if (c.GetNoDistinct(table, *itD, noDistinct))
            cout << noDistinct << " from attribute " << *itD << ", ";
    }
    cout << endl;
    cout << "-----------------------------------" << endl;

    // Displays all Tables in DB
    vector<string> listOfTables;
    c.GetTables(listOfTables);
    cout << "-----------------------------------" << endl;
    cout << "List of Tables: ";
    for (vector<string>::iterator itT = listOfTables.begin(); itT != listOfTables.end(); itT++) {
        cout << *itT << ", ";
    }
    cout << endl;
    cout << "-----------------------------------" << endl;

    // Displays all Attributes in a Table
    vector<string> listOfAtts;
    c.GetAttributes(table, listOfAtts);
    cout << "-----------------------------------" << endl;
    cout << "List of Attributes: ";
    for (vector<string>::iterator itA = listOfAtts.begin(); itA != listOfAtts.end(); itA++) {
        cout << *itA << ", ";
    }
    cout << endl;
    cout << "-----------------------------------" << endl;

    // Gets the Schema of the table
    Schema schema;
    c.GetSchema(table, schema);
    cout << "-----------------------------------" << endl;
    cout << "Schema for " << table << " : " << schema << endl;
    cout << "-----------------------------------" << endl;
    
    // Print Table
    cout << "-----------------------------------" << endl;
    cout << c << endl;
    cout << "-----------------------------------" << endl;

    // Saves altered contents
    c.Save();

    table = "testDropTable";
    // Drops the Table
    cout << "-----------------------------------" << endl;
    c.DropTable(table);
    cout << "-----------------------------------" << endl;

    // Display Current Catalog
    cout << "-----------------------------------" << endl;
    cout << c << endl;
    cout << "-----------------------------------" << endl;

    return 0;
}
