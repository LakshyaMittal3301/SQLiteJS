import { open } from "fs/promises";

const databaseFilePath = process.argv[2];
const command = process.argv[3];

class BTreeCell{
	payloadBytes;
	rowid;
	initialPayload;

	constructor(payloadBytes, rowid, initialPayload){
		this.payloadBytes = payloadBytes;
		this.rowid = rowid;
		this.initialPayload = initialPayload;
	}

	toString(){
		return `Bytes: ${this.payloadBytes}, rowid: ${this.rowid}, Payload: ${this.initialPayload}`
	}
};

class BTreePage{
	// Page Header
	isInterior;
	isLeaf;
	isIndex;
	isTable;
	freeblockStart;
	numOfCells;
	startOfCellArea;
	fragmentedFreeBytes;

	// Cell Pointer Array
	cellPointerArray;

	// List of cells
	cells;

	buffer;
	cursor;
	constructor(buffer){
		this.buffer = buffer;
		this.cursor = 0;

		this.isInterior = false;
		this.isLeaf = false;
		this.isIndex = false;
		this.isTable = false;

		this.cellPointerArray = [];
		
		this.cells = [];
	}

	readBTreePage(){
		this.readBTreePageHeader();
		this.readCellPointerArray();
		this.cursor = this.startOfCellArea;
		for(let cellNum = 0; cellNum < this.numOfCells; cellNum++){
			if(this.isTable && this.isLeaf) this.cells.push(this.readTableLeafCell(this.cellPointerArray[cellNum]));
		}

	}

	readBTreePageHeader(){
		switch(this.buffer[this.cursor]){
			case 0x02:
				this.isInterior = true;
				this.isIndex = true;
				break;
			case 0x05:
				this.isInterior = true;
				this. isTable = true;
				break;
			case 0x0a:
				this.isLeaf = true;
				this.isIndex = true;
				break;
			case 0x0d:
				this.isLeaf = true;
				this.isTable = true;
				break;
			default:
				console.log(`Error in reading oage type: ${this.buffer[this.cursor]}`);
		}
		this.cursor += 1;
	
		this.freeblockStart = this.readBuffer2Bytes();
	
		this.numOfCells = this.readBuffer2Bytes();
	
		this.startOfCellArea = this.readBuffer2Bytes();
		if(this.startOfCellArea == 0) this.startOfCellArea = 65536;

		this.fragmentedFreeBytes = this.readBufferByte();

		if(this.isInterior) this.rightMostPointer = this.readBuffer4Bytes();

	}

	readCellPointerArray(){
		for(let i = 0; i < this.numOfCells; i++){
			this.cellPointerArray.push(this.readBuffer2Bytes());
		}
	}

	readTableLeafCell(offset){
		this.cursor = offset;
		let payloadBytes = this.readVarInt();
		let rowid = this.readVarInt();
		let payload = this.readBufferCustomBytes(payloadBytes);
		return new BTreeCell(payloadBytes, rowid, payload);
	}

	readBufferByte(){
		let byte = this.buffer.readUIntBE(this.cursor, 1);
		this.cursor += 1;
		return byte;
	}

	readBuffer2Bytes(){
		let bytes = this.buffer.readUInt16BE(this.cursor);
		this.cursor += 2;
		return bytes;
	}

	readBuffer4Bytes(){
		let bytes = this.buffer.readUInt32BE(this.cursor);
		this.cursor += 4;
		return bytes;
	}
	
	readBufferCustomBytes(numOfBytes){
		let bytes = this.buffer.subarray(this.cursor, this.cursor + numOfBytes);
		this.cursor += numOfBytes;
		return bytes;
	}

	readVarInt() {
		let result = 0;
		let shift = 0;
		let byte;
	
		do {
			byte = this.readBufferByte();;
			result |= (byte & 0x7f) << shift;
			shift += 7;
		} while (byte & 0x80);
	
		// Convert from ZigZag encoding (signed) to two's complement (unsigned)
		if (result & 1) {
			// Negative number
			result = ~(result >>> 1);
		} else {
			// Positive number
			result >>>= 1;
		}
		
		return result;
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

	let offset = 100;

	// You can use print statements as follows for debugging, they'll be visible when running tests.
	console.log("Logs from your program will appear here!");

	// Uncomment this to pass the first stage
	const pageSize = buffer.readUInt16BE(16); // page size is 2 bytes starting at offset 16
	console.log(`database page size: ${pageSize}`);

	buffer = Buffer.alloc(pageSize);

	await databaseFileHandler.read({
		length: pageSize,
		position: offset,
		buffer
	});

	let firstPage = new BTreePage(buffer);
	firstPage.readBTreePage();

	console.log(`number of tables: ${firstPage.numOfCells}`)

} 
else {
	throw `Unknown command ${command}`;
}
