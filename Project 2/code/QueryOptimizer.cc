#include <string>
#include <vector>
#include <iostream>

#include "Schema.h"
#include "Comparison.h"
#include "QueryOptimizer.h"

using namespace std;


QueryOptimizer::QueryOptimizer(Catalog& _catalog) : catalog(&_catalog) {

	catalog = &_catalog;

}

QueryOptimizer::~QueryOptimizer() {
}

void QueryOptimizer::Optimize(TableList* _tables, AndList* _predicate,
	OptimizationTree* _root) {
	// compute the optimal join order
	CNF joinPredicate;
	//Scan for the tables
	TableList* scanTable = _tables;


	while(scanTable != NULL){

		Schema scanSchema;
		DBFile db;
		string tableName = scanTable->tableName;
		unsigned int noTuples;
		if(catalog->GetSchema(tableName,scanSchema)){


			Scan* scan = new Scan(scanSchema,db,tableName);
			scanTables.push_back(scan);

		}
		else{
			continue;
		}





	}




}
