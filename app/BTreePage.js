class BTreeCell{
    payloadBytes;
    rowid;
    payload;

    headerBytes;
    serialTypes;
    values;

    buffer;
    cursor;

    constructor(payloadBytes, rowid, payloadRecordFormat){
        this.payloadBytes = payloadBytes;
        this.rowid = rowid;
        this.buffer = payloadRecordFormat;
        this.cursor = 0;

        this.serialTypes = [];
        this.values = [];

        let headerSizeObj = this.readVarInt();
        this.headerBytes = headerSizeObj.result;
        let bytestoRead = this.headerBytes - headerSizeObj.bytesRead;
        while(bytestoRead){
            let serialTypeObj = this.readVarInt();
            this.serialTypes.push(serialTypeObj.result);
            bytestoRead -= serialTypeObj.bytesRead;
        }

        for(const serialType of this.serialTypes){
            let value;
            if(serialType == 0) value = 1;
            else if(serialType == 1){
                value = this.readBufferByte();
            }
            else if(serialType >= 12 && serialType % 2 == 0){
                let valueLength = (serialType - 12) / 12;
                value = this.readBufferCustomBytes(valueLength).toString();
            }else if(serialType >= 13 && serialType % 2 != 0){
                let valueLength = (serialType - 13) / 2;
                value = this.readBufferCustomBytes(valueLength).toString();
            }
            else{
                value = `Not Implemented, serialType is : ${serialType}`;
            }
            this.values.push(value);
        }
    }

    readBufferByte(){
		let byte = this.buffer[this.cursor];
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
	
    // This method returns buffer, unlike other read methods which return an INT;
	readBufferCustomBytes(numOfBytes){
		let bytes = this.buffer.subarray(this.cursor, this.cursor + numOfBytes);
		this.cursor += numOfBytes;
		return bytes;
	}
    
    readVarInt() {
        let result = 0;
        let shift = 0;
        let byte;
        let bytesRead = 0;
    
        do {
            byte = this.readBufferByte();;
            bytesRead++;
            result |= (byte & 0x7f) << shift;
            shift += 7;
        } while (byte & 0x80);
        
        return {result, bytesRead};
    }

    toString(){
        return `PayloadBytes: ${this.payloadBytes}, rowId: ${this.rowid}, Values: ${this.values}`;
    }

};

export default class BTreePage{
	// Is it the first page
    isFirstPage;

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
	constructor(buffer, isFirstPage){
		this.isFirstPage = isFirstPage;

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
        if(this.isFirstPage) this.cursor += 100;
		this.readBTreePageHeader();
		this.readCellPointerArray();
		for(let cellNum = 0; cellNum < this.numOfCells; cellNum++){
			if(this.isTable && this.isLeaf) this.cells.push(this.readTableLeafCell(this.cellPointerArray[cellNum]));
		}

	}

	readBTreePageHeader(){
		switch(this.readBufferByte()){
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
				console.log(`Error in reading page type: ${this.buffer[this.cursor]}`);
		}

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

	readTableLeafCell(startOfCell){
		this.cursor = startOfCell;
		let payloadBytes = this.readVarInt();
		let rowid = this.readVarInt();
		let payloadRecordFormat = this.readBufferCustomBytes(payloadBytes);
		return new BTreeCell(payloadBytes, rowid, payloadRecordFormat);
	}

	readBufferByte(){
		let byte = this.buffer[this.cursor];
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
	
    // This method returns buffer, unlike other read methods which return an INT;
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
		
		return result;
	}

};