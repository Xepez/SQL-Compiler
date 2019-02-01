#include <vector>
#include <string>
#include <iostream>

#include "Schema.h"
#include "Catalog.h"

using namespace std;


int main () {
    
    /* Old Code
     
	string table = "region", attribute, type;
	vector<string> attributes, types;
	vector<unsigned int> distincts;

	attribute = "r_regionkey"; attributes.push_back(attribute);
	type = "INTEGER"; types.push_back(type);
	distincts.push_back(5);
	attribute = "r_name"; attributes.push_back(attribute);
	type = "STRING"; types.push_back(type);
	distincts.push_back(5);
	attribute = "r_comment"; attributes.push_back(attribute);
	type = "STRING"; types.push_back(type);
	distincts.push_back(5);

	Schema s(attributes, types, distincts);
	Schema s1(s), s2; s2 = s1;

	string a1 = "r_regionkey", b1 = "regionkey";
	string a2 = "r_name", b2 = "name";
	string a3 = "r_commen", b3 = "comment";

	s1.RenameAtt(a1, b1);
	s1.RenameAtt(a2, b2);
	s1.RenameAtt(a3, b3);

	s2.Append(s1);

	vector<int> keep;
	keep.push_back(5);
	keep.push_back(0);
	s2.Project(keep);

	cout << s << endl;
	cout << s1 << endl;
	cout << s2 << endl;


	string dbFile = "catalog.sqlite";
	Catalog c(dbFile);

	c.CreateTable(table, attributes, types);

	cout << c << endl;
    */

    // Choice Menu
    cout << "Choose a Number:" << endl;
    cout << "1. Create a Table" << endl;
    cout << "2. Drop a Table" << endl;
    cout << "3. Display Contant" << endl;
    cout << "4. Save Data" << endl;
    
    // What the user chooses
    int choice;
    cin >> choice;
    
    if (choice == 1) {
        // Create Table
        
    }
    else if (choice == 2) {
        // Drop Table
        
    }
    else if (choice == 3) {
        // Display Content of a Table
        
    }
    else if (choice == 4) {
        // Save Data
        
    }
    else {
        cout << "Invalid Input!" << endl;
    }
	return 0;
}
