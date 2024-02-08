import QueryParser from "./QueryParser.js";

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
	let selectColumnNames = queryObj.selectColumns;
    let tableName = queryObj.fromTableName;

    let tableColumnNames = database.readTableColumnNames(tableName);
    let tableValues = await database.readTableValues(tableName);

    if(selectColumnNames[0] === 'count(*)'){
        console.log(tableValues.length);
        return;
    }

    let selectColumnIdxs = [];
    let whereColumnIdx = tableColumnNames.indexOf(queryObj.whereColumn);
    let whereColumnValue = queryObj.whereValue;
    for(const columnName of selectColumnNames){
        selectColumnIdxs.push(tableColumnNames.indexOf(columnName));
    }

    let filteredValues = [];
    for(const row of tableValues){
        if(row[whereColumnIdx] !== whereColumnValue) continue;
        let filteredRow = [];
        for(const columnIdx of selectColumnIdxs){
            filteredRow.push(row[columnIdx]);
        }
        filteredValues.push(filteredRow);
    }

    for(const rowValues of filteredValues){
        console.log(rowValues.join('|'));
    }


	// await database.readPageWithPageNumber(schemaTableEntry.rootpage).then((page) => {
	// 	if(columnNames[0] == "count(*)"){
	// 		console.log(page.numOfCells);
	// 	} else{
    //         let columnIdxs = [];
    //         let whereColumnIdx = schemaTableEntry.columnNames.indexOf(queryObj.whereColumn);
    //         for(const columnName of columnNames){
    //             columnIdxs.push(schemaTableEntry.columnNames.indexOf(columnName));
    //         }
    //         let columnValues = getColumnValues(columnIdxs, page, whereColumnIdx, queryObj.whereValue);
    //         for(const rowValues of columnValues){
    //             console.log(rowValues.join('|'));
    //         }
    //     }
	// })
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
