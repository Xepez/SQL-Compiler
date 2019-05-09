#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <map>
#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>

#include "Schema.h"
#include "Catalog.h"

using namespace std;

sqlite3 *db;
vector<string> sql;
string currentTable;
string currentPath;
int currentNoTuples;

struct table{
	string name;
	unsigned int numOfTuples;
	string pathToData;
	vector<string> attribute;
	vector<string> type;
	vector<unsigned int> noDistinct;
};

vector<table> catalog;

int callback(void *data, int argc, char **argv, char **azColName){ //Default callback function from sqlite3

   fprintf(stderr, "%s: ", (const char*)data);
   
   for(int i = 0; i<argc; i++){
      printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
   }
   
   printf("\n");
   return 0;
}

int listAtts(void *data, int argc, char **argv, char **azColName){
	//cout << "\nAttribute: " << endl;
	vector<table>::iterator it;

	for(it = catalog.begin(); it != catalog.end(); it++){
		if(it->name == currentTable)
			break;
	}

		//printf("%s = %s\n", azColName[0], argv[0] ? argv[1] : "NULL");
		string currentAtt = ("%s", argv[0]); //Save attribute as string

		for(vector<string>::iterator its = it->attribute.begin(); its != it->attribute.end(); its++){
			if(*its == currentAtt) //if attribute exists, quit
				return 0;
		}

		it->attribute.push_back(currentAtt);
		//printf("%s = %s\n", azColName[2], argv[2] ? argv[2] : "NULL");
		string currentType = ("%s", argv[1]); //Save type as string
		it->type.push_back(currentType); //Push type
		
		it->noDistinct.push_back(atoi(argv[2])); //Initializing distinct column

	return 0;
}

int listTables(void *data, int argc, char **argv, char **azColName){

	char* error;
	bool found = false;

		// printf("FOUND TABLE: %s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
		string tableName = ("%s", argv[0]);
		currentTable = tableName;

		currentNoTuples = atoi(argv[2]);

		for(vector<table>::iterator it = catalog.begin(); it != catalog.end(); it++){ //check if it already exists, if it does don't re-add it.
			if(it->name == tableName)
				found = true;
		}

		if(!found){ //If it doesn't exist, add it to the catalog
			table temp;
			temp.name = currentTable;
			temp.pathToData = currentTable + ".dat";
			temp.numOfTuples = currentNoTuples;
			catalog.push_back(temp);
		}
		else{
			//MAKE IT EXIST
		}

		string pragmaStatement = "SELECT attribute, type, noDistinct FROM tableAtts WHERE tableName = '" + tableName + "';";
		int exit = sqlite3_exec(db, pragmaStatement.c_str(), listAtts, (void*)data, &error);
		cout << "\n";

	return 0;
}

Catalog::Catalog(string &_fileName)
{
	int exit = sqlite3_open(_fileName.c_str(), &db); //Start connection

	if (exit != SQLITE_OK) { //Print if successfull or failure
        std::cerr << "Error Connection" << std::endl; 
    } 
    else
        std::cout << "Connected Succesfully" << std::endl; 

	///////////////////////////////////////////////////////////////////////////////////

	// string tableStatement = "select name From sqlite_master where type = 'table';"; //SQL for all table names
	string tableStatement = "select tableName, path, noTuples from tables;"; //Get table names from table

	char* error;	//Error pointer
	const char* data; //Data pointer
	currentPath = _fileName; //Set the name of this database as path

	table temp; //Make sure catalog isn't empty
	temp.name = "--CURRENT CATALOG CONTENTS--";
	catalog.push_back(temp);

	exit = sqlite3_exec(db, tableStatement.c_str(), listTables, (void*)data, &error); //Get all table names

}

Catalog::~Catalog()
{
	Save();
	sqlite3_close(db);	//Close connection

}

bool Catalog::Save()
{
	int exit;
	char* error;
	const char* data;

	for (int i = 0; i < sql.size(); i++) {

		exit = sqlite3_exec(db, sql[i].c_str(), callback, (void*)data, &error);

		/*if (exit != SQLITE_OK) { 
        	std::cerr << "SQL Runtime Error" << std::endl; 
    	} 
    	else{
        	std::cout << "Executed function successfully" << std::endl;
		}*/
	}

	string tableStatement = "select tableName, path, noTuples from tables;"; //Refresh catalog
	exit = sqlite3_exec(db, tableStatement.c_str(), listTables, (void*)data, &error); //Refresh catalog

	sql.clear();
	
}

bool Catalog::GetNoTuples(string &_table, unsigned int &_noTuples)
{

	//GET NUMBER OF TUPLES
	vector<table>::iterator it;
	for(it = catalog.begin(); it != catalog.end(); it++){
		if(it->name == _table){
			_noTuples = it->numOfTuples;
			return true;
		}
	}

	return false;
	
}

void Catalog::SetNoTuples(string &_table, unsigned int &_noTuples)
{
	
	vector<table>::iterator it;
	for(it = catalog.begin(); it != catalog.end(); it++){

		if(it->name == _table){
			it->numOfTuples = _noTuples;

			string setStatement = "UPDATE tables SET noTuples = ";

			string unsint; //I absolutely hate c++ unsigned int -> string conversion.
			stringstream i;
			i << _noTuples;
			unsint = i.str();
			
			setStatement += unsint;
			setStatement += " WHERE tableName = '";
			setStatement += _table;
			setStatement += "';";
			
			sql.push_back(setStatement);
		}
	}

	return;
}

bool Catalog::GetDataFile(string &_table, string &_path)
{

	//GET FILE LOCATION
	vector<table>::iterator it;
	for(it = catalog.begin(); it != catalog.end(); it++){
		if(it->name == _table){
			_path = it->pathToData;
			return true;
		}
	}

	return false;
}

void Catalog::SetDataFile(string &_table, string &_path)
{
	vector<table>::iterator it;
	for(it = catalog.begin(); it != catalog.end(); it++){

		if(it->name == _table){
			it->pathToData = _path;

			string setStatement = "UPDATE tables SET path = '" + _path + "' WHERE tableName = '" + _table + "';";
			sql.push_back(setStatement);
		}
	}

	return;
}

bool Catalog::GetNoDistinct(string &_table, string &_attribute,
							unsigned int &_noDistinct)
{
	vector<table>::iterator it;
	for(it = catalog.begin(); it != catalog.end(); it++){
		if(it->name == _table){
			int i = 0;
			for(vector<string>::iterator its = it->attribute.begin(); its != it->attribute.end(); its++){
				if(*its == _attribute){
					_noDistinct = it->noDistinct.at(i);
					return true;
				}
				i++;
			}
		}
	}

	return false;
}
void Catalog::SetNoDistinct(string &_table, string &_attribute,
							unsigned int &_noDistinct)
{
	vector<table>::iterator it = catalog.begin(); //Beginning of catalog

	for(it = catalog.begin(); it != catalog.end(); it++){ //For every table in the catalog
		if(it->name == _table){ //If this is the one
			int i = 0;
			vector<string>::iterator it2;

			for(it2 = it->attribute.begin(); it2 != it->attribute.end(); it2++){
				if(*it2 == _attribute){ //is this the attribute?
					it->noDistinct.at(i) = _noDistinct; //Set values

					string setStatement = "UPDATE tableAtts SET noDistinct = ";

					string unsint; //I absolutely hate c++ unsigned int -> string conversion.
					stringstream i;
					i << _noDistinct;
					unsint = i.str();
					
					setStatement += unsint + " WHERE tableName = '" + _table + "' AND attribute = '" + _attribute + "';";
					
					sql.push_back(setStatement);

					break;
				}
				i++;		
			}
			break;
		}
	}
	
	return;

}

void Catalog::GetTables(vector<string> &_tables)
{

	
}

bool Catalog::GetAttributes(string &_table, vector<string> &_attributes)
{

	//Get name, type, and num of Distincts
	for(vector<table>::iterator it = catalog.begin(); it != catalog.end(); it++){
		if(it->name == _table){
			int size = it->attribute.size();
			for(int i = 0; i < size; i++){
				cout << it->attribute.at(i) << " - ";
				cout << it->type.at(i) << " -> ";
				cout << "No. Distincts: " << it->noDistinct.at(i) << endl;
			}
		}
	}

	return true;
}

bool Catalog::GetSchema(string &_table, Schema &_schema)
{

	//Get Attributes for schema

	return true;
}

bool Catalog::CreateTable(string &_table, vector<string> &_attributes,
						  vector<string> &_attributeTypes)
{
	//CREATE TABLE HERE

	if((_attributes.size() == 0)||(_attributeTypes.size() == 0))
		return false;

	for(vector<table>::iterator it = catalog.begin(); it != catalog.end(); it++){
		if(it->name == _table)
			return false;
	}


	table temp;
	temp.name = _table;

	vector<string>::iterator iterType = _attributeTypes.begin();
	vector<string>::iterator iterAtt = _attributes.begin();

	string createTable = "INSERT INTO  tables VALUES('" + _table + "', '" + currentPath + "/" + _table + "', 0);"; //Begin Create Table SQL Cmd
	sql.push_back(createTable); //Push to queue

	while(iterAtt != _attributes.end()){

		string tableAtts = "INSERT INTO  tableAtts VALUES('" + _table + "', '" + *iterAtt + "', '" + *iterType + "', 0);"; //Insert all atts into tableAtts
		temp.attribute.push_back(*iterAtt);
		temp.type.push_back(*iterType);
		temp.noDistinct.push_back(0);

		iterAtt++; //Increment pointers
		iterType++;
		sql.push_back(tableAtts); //Push to queue
	}

	catalog.push_back(temp);

	Save();

	return true;
}

bool Catalog::DropTable(string &_table)
{

	//DROP TABLE HERE
	
	for(vector<table>::iterator it = catalog.begin(); it != catalog.end(); it++){
		if(it->name == _table){
			catalog.erase(it);
			string dropTable = "DELETE FROM tables WHERE tableName = '" + _table + "';";
			sql.push_back(dropTable);
			dropTable = "DELETE FROM tableAtts WHERE tableName = '" + _table + "';";
			sql.push_back(dropTable);
			return true;
		}
	}

	//Save();

	return false;
}

ostream &operator<<(ostream &_os, Catalog &_c)
{

	for(vector<table>::iterator it = catalog.begin(); it != catalog.end(); it++){

		// if(it->name == "--CURRENT CATALOG CONTENTS--")
			// cout << it->name << endl;
		// else
			cout << it->name << " -> No. Tuples: " << it->numOfTuples << " | FOUND AT PATH: " << it->pathToData << endl;

		int size = it->attribute.size();
		
		for(int i = 0; i < size; i++){
			cout << "Attribute: " << it->attribute.at(i);
			cout  << " Type: " << it->type.at(i) << " -> ";
			cout << "No. Distincts: " << it->noDistinct.at(i) << endl;
		}

		cout << "----------------------------" << endl;
	}
	return _os;
}
