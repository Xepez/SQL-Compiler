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
    // Will always be success since we are doing SQL statements
    return true;

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
    
    sqlite3_stmt *stmt;
    vector<string> attName;
    vector<string> attType;
    vector<unsigned int> attNoDist;
    
    // Prepares Select for attribute's attributes
    int rc = sqlite3_prepare_v2(db, "SELECT attributename, attType, numDistinct FROM attribute, table_info WHERE table_info.tableid = attribute.tableid AND tablename = ?", -1, &stmt, NULL);
    
    if (rc == SQLITE_OK) {
        // Binds the table we are looking for
        sqlite3_bind_text(stmt, 1, _table.c_str(), -1, NULL);
    } else {
        cout << "Failed to execute statement: " << sqlite3_errmsg(db) << endl;
    }
    
    int step = sqlite3_step(stmt);
    
    if(step != SQLITE_ROW){
        sqlite3_finalize(stmt);
        return false;
    }
    
    while(step == SQLITE_ROW) {
        attName.push_back(string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0))));
        attType.push_back(string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1))));
        attNoDist.push_back(sqlite3_column_int(stmt, 2));
        
        // Take a step to the next row
        step = sqlite3_step(stmt);
    }
    
    sqlite3_finalize(stmt);
    Schema insertSchema(attName, attType, attNoDist);
    _schema = insertSchema;
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
        
            printf("Error dropping table: ");
            cout << sqlite3_errmsg(db) << endl;
            sqlite3_finalize(stmt);
            return false;
        
        }
    
        else{
        
            sqlite3_finalize(stmt);
            printf("Table dropped");
            cout << endl;
            return true;
        
        }

	
}

ostream& operator<<(ostream& _os, Catalog& _c) {
    
    sqlite3_stmt *stmt;
    sqlite3_stmt *stmt2;
    int rc;
    int step2;
    
    // Prepares Select for all of catalog
    sqlite3_prepare_v2(_c.db, "SELECT tablename, numTuples, path, tableid FROM table_info ORDER BY tablename ASC", -1, &stmt, NULL);
    // Runs Query
    int step = sqlite3_step(stmt);
    
    // Prints out Info
    while(step == SQLITE_ROW){
        cout << sqlite3_column_text(stmt, 0) << "\t" << sqlite3_column_int(stmt, 1) << "\t" << sqlite3_column_text(stmt, 2) << endl;
        
        
        rc = sqlite3_prepare_v2(_c.db, "SELECT attributename, attType, numDistinct FROM attribute WHERE tableid = ? ORDER BY attributename ASC", -1, &stmt2, NULL);
        
        if (rc == SQLITE_OK) {
            // Binds the table we are looking for
            sqlite3_bind_int(stmt2, 1, sqlite3_column_int(stmt, 3));
        } else {
            cout << "Failed to execute statement: " << sqlite3_errmsg(_c.db) << endl;
        }
        
        step2 = sqlite3_step(stmt2);
        
        while(step2 == SQLITE_ROW) {
            cout << "\t" << sqlite3_column_text(stmt2, 0) << "\t" << sqlite3_column_text(stmt2, 1) << "\t" << sqlite3_column_int(stmt2, 2) << endl;
            
            // Take a step to the next row
            step2 = sqlite3_step(stmt2);
        }
        
        // Take a step to the next row
        step = sqlite3_step(stmt);
        sqlite3_finalize(stmt2);
        cout << endl;
    }
    
    //table_1 \tab noTuples \tab pathToFile
    //* \tab attribute_1 \tab type \tab noDistinct
    //* \tab attribute_2 \tab type \tab noDistinct
    
    sqlite3_finalize(stmt);
	return _os;
}
