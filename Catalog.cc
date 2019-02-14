#include <iostream>

#include "Schema.h"
#include "Catalog.h"

using namespace std;

Catalog::Catalog(string& _fileName) {

	int rc = sqlite3_open(_fileName.c_str(), &db);

    if(rc){
        cout << "Cannot open database" << endl;
    }
    else{
        cout << "Connection successful" << endl;
    }
}

Catalog::~Catalog() {

	sqlite3_close(db);

	cout << "Database Closed" << endl;

}

bool Catalog::Save() {

	//save();
    return false;

}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {

    sqlite3_stmt *stmt;
    
    // Prepares Select for tablename and numTuples
    int rc = sqlite3_prepare_v2(db, "SELECT numTuples FROM table_info WHERE tablename = ?", -1, &stmt, NULL);
    
    if (rc == SQLITE_OK) {
        // Binds the table we are looking for
        sqlite3_bind_text(stmt, 1, _table.c_str(), -1, NULL);
    } else {
        cout << "Failed to execute statement: " << sqlite3_errmsg(db) << endl;
    }
    
    int step = sqlite3_step(stmt);
    
	if(step == SQLITE_ROW){
        _noTuples = sqlite3_column_int(stmt, 0);
        sqlite3_finalize(stmt);
		return true;
	} else {
        sqlite3_finalize(stmt);
        return false;
	}
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
    
    sqlite3_stmt *stmt;
    
    // Prepares Insert for numTuples in table
    sqlite3_prepare_v2(db, "UPDATE table_info SET numTuples = ? WHERE tablename = ?", -1, &stmt, NULL);
    // Binds the value and table we are looking for
    sqlite3_bind_int(stmt, 1, _noTuples);
    sqlite3_bind_text(stmt, 2, _table.c_str(), -1, NULL);
    
    int rc = sqlite3_step(stmt);

    if (rc != SQLITE_DONE) {
        cout << "ERROR inserting data -> " << sqlite3_errmsg(db) << endl;
    }
    
    sqlite3_finalize(stmt);
}

bool Catalog::GetDataFile(string& _table, string& _path) {
	
    sqlite3_stmt *stmt;
    
    // Prepares Select for tablename and numTuples
    int rc = sqlite3_prepare_v2(db, "SELECT path FROM table_info WHERE tablename = ?", -1, &stmt, NULL);
    
    if (rc == SQLITE_OK) {
        // Binds the table we are looking for
        sqlite3_bind_text(stmt, 1, _table.c_str(), -1, NULL);
    } else {
        cout << "Failed to execute statement: " << sqlite3_errmsg(db) << endl;
    }
    
    int step = sqlite3_step(stmt);
    
    if(step == SQLITE_ROW){
        _path = string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0)));
        sqlite3_finalize(stmt);
        return true;
    } else {
        sqlite3_finalize(stmt);
        return false;
    }
}

void Catalog::SetDataFile(string& _table, string& _path) {
    
    sqlite3_stmt *stmt;
    
    // Prepares Insert for numTuples in table
    sqlite3_prepare_v2(db, "UPDATE table_info SET path = ? WHERE tablename = ?", -1, &stmt, NULL);
    // Binds the value and table we are looking for
    sqlite3_bind_text(stmt, 1, _path.c_str(), -1, NULL);
    sqlite3_bind_text(stmt, 2, _table.c_str(), -1, NULL);
    
    int rc = sqlite3_step(stmt);
    
    if (rc != SQLITE_DONE) {
        cout << "ERROR inserting data -> " << sqlite3_errmsg(db) << endl;
    }
    
    sqlite3_finalize(stmt);
}

bool Catalog::GetNoDistinct(string& _table, string& _attribute, unsigned int& _noDistinct) {

    sqlite3_stmt *stmt;
    
    // Prepares Select for numDistinct
    int rc = sqlite3_prepare_v2(db, "SELECT numDistinct FROM table_info, attribute WHERE tablename = ? AND attribute.tableid = table_info.tableid AND attributename = ?", -1, &stmt, NULL);
    
    if (rc == SQLITE_OK) {
        // Binds the table we are looking for
        sqlite3_bind_text(stmt, 1, _table.c_str(), -1, NULL);
        sqlite3_bind_text(stmt, 2, _attribute.c_str(), -1, NULL);
    } else {
        cout << "Failed to execute statement: " << sqlite3_errmsg(db) << endl;
    }
    
    int step = sqlite3_step(stmt);
    
    if(step == SQLITE_ROW){
        _noDistinct = sqlite3_column_int(stmt, 0);
        sqlite3_finalize(stmt);
        return true;
    } else {
        sqlite3_finalize(stmt);
        return false;
    }
}
void Catalog::SetNoDistinct(string& _table, string& _attribute, unsigned int& _noDistinct) {
    
    sqlite3_stmt *stmt;
    
    // Prepares Insert for numDistinct in table
    sqlite3_prepare_v2(db, "UPDATE attribute SET numDistinct = ? WHERE attribute.tableid = (SELECT tableid FROM table_info WHERE tablename = ?) AND attributename = ?;", -1, &stmt, NULL);
    // Binds the value and table we are looking for
    sqlite3_bind_int(stmt, 1, _noDistinct);
    sqlite3_bind_text(stmt, 2, _table.c_str(), -1, NULL);
    sqlite3_bind_text(stmt, 3, _attribute.c_str(), -1, NULL);
    
    int rc = sqlite3_step(stmt);
    
    if (rc != SQLITE_DONE) {
        cout << "ERROR inserting data -> " << sqlite3_errmsg(db) << endl;
    }
    
    sqlite3_finalize(stmt);
}

void Catalog::GetTables(vector<string>& _tables) {
    
    sqlite3_stmt *stmt;
    int step;
    
    // Prepares Select for tablename
    sqlite3_prepare_v2(db, "SELECT tablename FROM table_info", -1, &stmt, NULL);
    
    while ((step = sqlite3_step(stmt)) == SQLITE_ROW){
        // Convert to a string
        string temp = string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0)));
        // Gets each table name
        _tables.push_back(temp);
    }
    
    sqlite3_finalize(stmt);
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
    
    sqlite3_stmt *stmt;
    int step;
    
    // Prepares Select for attributename
    sqlite3_prepare_v2(db, "SELECT attributename FROM attribute", -1, &stmt, NULL);
    
    while ((step = sqlite3_step(stmt)) == SQLITE_ROW){
        // Convert to a string
        string temp = string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0)));
        // Gets each attribute name
        _attributes.push_back(temp);
    }
    
    sqlite3_finalize(stmt);
	return true;
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {
    // Will work on tomorrow
	return true;
}

//bool Catalog::CreateTable(string& _table, vector<string>& _attributes, vector<string>& _attributeTypes) {

//    sqlite3_stmt *stmt;
//    char *query = NULL;

//    asprintf()



//    need to include att,and atttype
//    sqlite3_prepare_v2(db, " CREATE TABLE ?1 ()", -1, &stmt, NULL);


//    add to see if table exits
//    if(_table == ){

//        printf("Table exists");

//    }
//    else{


//        sqlite3_bind_text(stmt, 1, _table,SQLITE_STATIC);

//       int rc = sqlite3_step(stmt);

//            if(rc){

//                printf("Table Not Added");

//                return false;

//            }
//            else{

//                sqlite3_finalize(stmt);
//                printf("Table successfully added");

//                return true;

//            }

//        }

//    

//}

bool Catalog::DropTable(string& _table) {

    sqlite3_stmt *stmt;
    
    sqlite3_prepare_v2(db, "DROP TABLE ?", -1, &stmt, NULL);
    
    sqlite3_bind_text(stmt, 1, _table.c_str(), -1, NULL);
    
    int rc = sqlite3_step(stmt);
    
        if(rc){
        
            printf("Error dropping table");
            
            return false;
        
        }
    
        else{
        
            sqlite3_finalize(stmt);
            printf("Table dropped");

            return true;
        
        }

	
}

ostream& operator<<(ostream& _os, Catalog& _c) {
    // Will work on tomorrow
	return _os;
}
