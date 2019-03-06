#ifndef _QUERY_COMPILER_H
#define _QUERY_COMPILER_H

/* Take as input the query tokens produced by the query parser and generate
 * the query execution tree. This requires instantiating relational operators
 * with the correct parameters, based on the query.
 * Two issues have to be addressed:
 *  1) Identify the schema(s) for each operator.
 *  2) Identify the parameters of the operation the operator is executing.
 *     For example, identify the predicate in a SELECT. Or the JOIN PREDICATE.
 */
#include "Catalog.h"
#include "ParseTree.h"
#include "QueryOptimizer.h"
#include "RelOp.h"

using namespace std;


class QueryCompiler {
private:
	Catalog* catalog;
	QueryOptimizer* optimizer;

    // Vectors that save our scan and select ops for later use w/ joins
    vector<Scan*> scanVector;
    vector<Select*> selectVector;
    vector<RelationalOp*> deleteMe;
public:
	QueryCompiler(Catalog& _catalog, QueryOptimizer& _optimizer);
	virtual ~QueryCompiler();

    // Custom fcn to traverse a join tree and initialize the joins in proper order
    RelationalOp* joinMeDaddy(OptimizationTree* tempRoot, Schema &parentSchema, AndList* _predicate);
    
	void Compile(TableList* _tables, NameList* _attsToSelect,
		FuncOperator* _finalFunction, AndList* _predicate,
		NameList* _groupingAtts, int& _distinctAtts,
		QueryExecutionTree& _queryTree);
};

#endif // _QUERY_COMPILER_H
