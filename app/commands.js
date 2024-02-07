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

async function handleSelectCommand(argsArray, database){
	let columnName = argsArray[0];
	let tableName = argsArray[argsArray.length - 1];
	let schemaTableEntry = null;
	
	for(const entry of database.schemaTable.entries){
		if(entry.tbl_name === tableName){
			schemaTableEntry = entry;
			break;
		}
	}
	
	if(schemaTableEntry === null){
		console.log(`Could not find schemaTableEntry with table name: ${tableName}`);
		return null;
	}
	
	await database.readPageWithRootPageNumber(schemaTableEntry.rootpage).then((page) => {
		if(columnName == "count(*)"){
			console.log(page.numOfCells);
		}else{
			let columnIdx = schemaTableEntry.columnNames.indexOf(columnName);
			let columnValues = [];
			for(const row of page.cells){
				columnValues.push(row.values[columnIdx]);
			}
			for(const colVal of columnValues) console.log(colVal);
		} 

	})

}

export async function handleQuery(query, database){
	let queryArray = query.toLowerCase().split(' ');
	let command = queryArray[0];

	switch(command){
		case ".dbinfo":
			handleDBInfoCommand(database);
			break;
		case ".tables":
			handleTablesCommand(database);
			break;
		case "select":
			await handleSelectCommand(queryArray.slice(1), database);
			break;
		default:
			console.log(`Unknown command: ${command}`);
			break;
	}

}
