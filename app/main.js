import { open } from "fs/promises";
import BTreePage from "./BTreePage.js";
import { table } from "console";

const databaseFilePath = process.argv[2];
const command = process.argv[3];

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
else {
	throw `Unknown command ${command}`;
}
