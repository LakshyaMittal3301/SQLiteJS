import { open } from "fs/promises";
import BTreePage from "./BTreePage.js";
import { rootCertificates } from "tls";

const databaseFilePath = process.argv[2];
const command = process.argv[3];

class SchemaTableEntry{
	type;
	name;
	tbl_name;
	rootpage;
	sql;

	constructor(valueArray){
		this.type = valueArray[0];
		this.name = valueArray[1];
		this.tbl_name = valueArray[2];
		this.rootpage = valueArray[3];
		this.sql = valueArray[4];
	}
};

class SchemaTable{
	entries;

	constructor(cellArray){
		this.entries = [];
		for(const cell of cellArray){
			this.entries.push(new SchemaTableEntry(cell.values));
		}
	}
};

if (command === ".dbinfo") {

	const databaseFileHandler = await open(databaseFilePath, "r");

	let buffer = Buffer.alloc(100);

	await databaseFileHandler.read({
		length: 100,
		position: 0,
		buffer,
	});

	// You can use print statements as follows for debugging, they'll be visible when running tests.
	console.log("Logs from your program will appear here!");

	// Uncomment this to pass the first stage
	const pageSize = buffer.readUInt16BE(16); // page size is 2 bytes starting at offset 16
	console.log(`database page size: ${pageSize}`);

	buffer = Buffer.alloc(pageSize);

	await databaseFileHandler.read({
		length: pageSize,
		position: 0,
		buffer
	});

	let firstPage = new BTreePage(buffer, true);
	firstPage.readBTreePage();

	console.log(`number of tables: ${firstPage.numOfCells}`);

} 
else if(command === ".tables"){
	const databaseFileHandler = await open(databaseFilePath, "r");

	let buffer = Buffer.alloc(100);

	await databaseFileHandler.read({
		length: 100,
		position: 0,
		buffer,
	});

	const pageSize = buffer.readUInt16BE(16);

	buffer = Buffer.alloc(pageSize);

	await databaseFileHandler.read({
		length: pageSize,
		position: 0,
		buffer
	});

	let firstPage = new BTreePage(buffer, true);
	firstPage.readBTreePage();

	console.log(`number of tables: ${firstPage.numOfCells}`);
	let tableNames = [];

	for(const row of firstPage.cells){
		const columns = row.values;
		tableNames.push(columns[2]);
	}
	console.log(tableNames.join(' '));

}
else if(command.startsWith('SELECT')){
	const databaseFileHandler = await open(databaseFilePath, "r");

	let buffer = Buffer.alloc(100);

	await databaseFileHandler.read({
		length: 100,
		position: 0,
		buffer,
	});

	const pageSize = buffer.readUInt16BE(16);

	buffer = Buffer.alloc(pageSize);

	await databaseFileHandler.read({
		length: pageSize,
		position: 0,
		buffer
	});

	let firstPage = new BTreePage(buffer, true);
	firstPage.readBTreePage();

	let commandArray = command.split(' ');
	let tableName = commandArray[commandArray.length - 1];

	let schemaTable = new SchemaTable(firstPage.cells);

	let tableRootpage = null;

	for(const entry of schemaTable.entries){
		if(entry.tbl_name === tableName){
			tableRootpage = entry.rootpage;
			break;
		}
	}

	if(!tableRootpage){
		console.log('Table not found');
		
	}
	else{

		buffer = Buffer.alloc(pageSize);
		
		await databaseFileHandler.read({
			length: pageSize,
			position: (tableRootpage - 1) * (pageSize),
			buffer
		});
		
		let tablePage = new BTreePage(buffer, false);
		tablePage.readBTreePage();
		
		console.log(tablePage.numOfCells);
	}

}
else {
	console.log(`Unknown command ${command}`);
}
