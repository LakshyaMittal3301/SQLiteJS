import QueryParser from "./QueryParser.js";
import SchemaTable from "./SchemaTable.js";

function handleDBInfoCommand(database){
	console.log(`database page size: ${database.pageSize}`);
	console.log(`number of tables: ${database.firstPage.numOfCells}`);
}

function handleTablesCommand(database){
	let tableNames = [];

	for(const row of database.firstPage.cells){
		const columns = row.values;
		tableNames.push(columns[2]);
	}
	console.log(tableNames.join(' '));
}

async function handleSelectCommand(queryObj, database){
    let result = await database.getData(queryObj);
    if(queryObj.selectColumns[0] === 'count(*)'){
        console.log(result);
        return;
    }

    for(const row of result){
        console.log(row.join('|'));
    }

}

export async function handleQuery(query, database){
	let queryObj = new QueryParser(query);
	let command = queryObj.command;

	switch(command){
		case ".dbinfo":
			handleDBInfoCommand(database);
			break;
		case ".tables":
			handleTablesCommand(database);
			break;
		case "select":
			await handleSelectCommand(queryObj, database);
			break;
		default:
			console.log(`Unknown command: ${command}`);
			break;
	}

}
