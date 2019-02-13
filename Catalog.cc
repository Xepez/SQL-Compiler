#include <iostream>
#include "sqlite3.h"

#include "Schema.h"
#include "Catalog.h"

using namespace std;

Catalog::Catalog(string& _fileName) {

	int rc = sqlite3_open(_fileName.c_str(), &db);

		if(rc){
			
			fprintf("Cannot open: ", sqlite3_errmsg(&db);
		}

		else{
			printf(" Connection successful ");

}

Catalog::~Catalog() {

	sqlite3_close(db);

	printf("Database Closed");

}

bool Catalog::Save() {

	save();

}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {

    sqlite3_stmt *stmt;

    // Prepares Select for tablename and numTuples
    sqlite3_prepare_v2(db, "SELECT tablename, numTuples FROM table WHERE tablename = (?)", -1, &stmt, NULL);
    // Binds our the table we are looking for
    sqlite3_bind_int(stmt, 1, _table);
    
    int step = sqlite3_step(stmt);
    
	if(step == SQLITE_ROW){
        _noTuples = sqlite3_column_text(stmt, 1);
        
        sqlite3_finalize(stmt);
		return true;
	} else {
        sqlite3_finalize(stmt);
        return false;
	}
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
}

bool Catalog::GetDataFile(string& _table, string& _path) {
	return true;
}

void Catalog::SetDataFile(string& _table, string& _path) {
}

bool Catalog::GetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	return true;
}
void Catalog::SetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
}

void Catalog::GetTables(vector<string>& _tables) {
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
	return true;
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {
	return true;
}

bool Catalog::CreateTable(string& _table, vector<string>& _attributes,
	vector<string>& _attributeTypes) {
	
	sqlite3_stmt *stmt;
	char *query = NULL;
	
	asprintf()
	
	
	
	//need to include att,and atttype
	sqlite3_prepare_v2(db, " CREATE TABLE ?1 ()", -1, &stmt, NULL)


	//add to see if table exits
	if(_table == ){
	
		printf("Table exists");	
		
	}
	else{


		sqlite3_bind_string(stmt, 1, _table,SQLITE_STATIC);
	
		rc = sqlite3_step(stmt);
	
			if(rc){

				printf("Table Not Added");
		
				return false;
		
			}
			else{
				
				sqlite3_finalize(stmt);
				printf("Table successfully added");
			
				return true;
		
			}
		
		}
	
	}
	
}

bool Catalog::DropTable(string& _table) {

	sqlite3_stmt *stmt;
	
	char *query = NULL;
	
	asprint(&query, "DROP TABLE ?1 ");
	
	sqlite3_prepare_v2(db, query, strlen(query), &stmt, NULL);
	
	sqlite3_bind_string(stmt, 1, _table, SQLITE_STATIC);
	
	rc = sqlite3_step(stmt);
	
		if(rc){
		
			printf("Error dropping table");
			
			return false;
		
		}
	
		else{
		
			sqlite3_finalize(stmt);
			printf("Table dropped");
			free(query);
			
			return true;
		
		}
	
	
}

ostream& operator<<(ostream& _os, Catalog& _c) {
	return _os;
