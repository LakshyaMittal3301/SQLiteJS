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
	let columnNames = queryObj.selectColumns;
	let tableName = queryObj.fromTableName;
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
		if(columnNames[0] == "count(*)"){
			console.log(page.numOfCells);
		}else if(!queryObj.hasWhere){
            let columnIdxs = [];
            for(const columnName of columnNames){
                columnIdxs.push(schemaTableEntry.columnNames.indexOf(columnName));
            }
            let columnValues = getColumnValues(columnIdxs, page);
            for(const rowValues of columnValues){
                console.log(rowValues.join('|'));
            }
        } else if(queryObj.hasWhere){
            let columnIdxs = [];
            let whereColumnIdx = schemaTableEntry.columnNames.indexOf(queryObj.whereColumn);
            for(const columnName of columnNames){
                columnIdxs.push(schemaTableEntry.columnNames.indexOf(columnName));
            }
            let columnValues = getColumnValues(columnIdxs, page, whereColumnIdx, queryObj.whereValue);
            for(const rowValues of columnValues){
                console.log(rowValues.join('|'));
            }
        } else{
            console.log(`Problem Problem`);
        }
        
	})
}

function getColumnValues(columnIdxs, page, whereColumnIdx = null, whereColumnValue){
    
    let columnValues = [];
    for(const row of page.cells){
        if(whereColumnIdx !== null && row.values[whereColumnIdx] !== whereColumnValue) continue;

        let rowValues = [];
        for(const columnIdx of columnIdxs){
            rowValues.push(row.values[columnIdx]);
        }
        columnValues.push(rowValues);
    }
    return columnValues;
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
