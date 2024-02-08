import BufferReader from "./BufferReader.js";

export default class BTreePage extends BufferReader{
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
    rightMostPointer;

	// Cell Pointer Array
	cellPointerArray;

	// List of cells
	cells;

	constructor(buffer, isFirstPage = false){
        super(buffer);
		this.isFirstPage = isFirstPage;

		this.isInterior = false;
		this.isLeaf = false;
		this.isIndex = false;
		this.isTable = false;

		this.cellPointerArray = [];
		
		this.cells = [];

        this.readBTreePage();
	}

	readBTreePage(){
        if(this.isFirstPage) super.moveCursorBy(100);
		this.readBTreePageHeader();
		this.readCellPointerArray();
		for(let cellNum = 0; cellNum < this.numOfCells; cellNum++){
			if(this.isTable && this.isLeaf) this.cells.push(this.readTableLeafCell(this.cellPointerArray[cellNum]));
            else if(this.isTable && this.isInterior) this.cells.push(this.readTableInteriorCell(this.cellPointerArray[cellNum]));
		}

	}

	readBTreePageHeader(){
		let pageType = super.readBufferByte();
        switch(pageType){
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
				console.log(`Error in reading page type: ${pageType}`);
		}

		this.freeblockStart = super.readBuffer2Bytes();
	
		this.numOfCells = super.readBuffer2Bytes();
	
		this.startOfCellArea = super.readBuffer2Bytes();
		if(this.startOfCellArea == 0) this.startOfCellArea = 65536;

		this.fragmentedFreeBytes = super.readBufferByte();

		if(this.isInterior) this.rightMostPointer = super.readBuffer4Bytes();

	}

	readCellPointerArray(){
		for(let i = 0; i < this.numOfCells; i++){
			this.cellPointerArray.push(super.readBuffer2Bytes());
		}
	}

	readTableLeafCell(startOfCell){
		this.cursor = startOfCell;
		let payloadBytes = super.readVarInt().result;
		let rowid = super.readVarInt().result;
		let payloadRecordFormat = super.readBufferCustomBytes(payloadBytes);
		return new BTreeTableLeafCell(payloadBytes, rowid, payloadRecordFormat);
	}

    readTableInteriorCell(startOfCell){
        super.setCursorTo(startOfCell);
        let leftChildPointer = super.readBuffer4Bytes();
        let rowid = super.readVarInt().result;
        return new BTreeTableInteriorCell(leftChildPointer, rowid);
    }

};

class BTreeTableLeafCell extends BufferReader{
    payloadBytes;
    rowid;
    payload;

    headerBytes;
    serialTypes;
    values;

    constructor(payloadBytes, rowid, payloadRecordFormat){
        super(payloadRecordFormat);
        this.payloadBytes = payloadBytes;
        this.rowid = rowid;

        this.serialTypes = [];
        this.values = [];

        let headerSizeObj = super.readVarInt();
        this.headerBytes = headerSizeObj.result;
        let bytestoRead = this.headerBytes - headerSizeObj.bytesRead;
        while(bytestoRead){
            let serialTypeObj = super.readVarInt();
            this.serialTypes.push(serialTypeObj.result);
            bytestoRead -= serialTypeObj.bytesRead;
        }

        let i = 0;
        for(const serialType of this.serialTypes){
            let value;
            if(serialType === 0){
                if(i == 0) {
                    value = this.rowid;
                }
                else {
                    value = null;
                }
            } 
            else if(serialType == 1){
                value = super.readBufferByte();
            }
            else if(serialType >= 12 && serialType % 2 == 0){
                let valueLength = (serialType - 12) / 12;
                value = super.readBufferCustomBytes(valueLength).toString();
            }else if(serialType >= 13 && serialType % 2 != 0){
                let valueLength = (serialType - 13) / 2;
                value = super.readBufferCustomBytes(valueLength).toString();
            }
            else{
                value = `Not Implemented, serialType is : ${serialType}`;
            }
            this.values.push(value);
            i++;
        }
    }

    toString(){
        return `PayloadBytes: ${this.payloadBytes}, rowId: ${this.rowid}, Values: ${this.values}`;
    }
};

class BTreeTableInteriorCell{
    leftChildPointer;
    rowid;

    constructor(leftChildPointer, rowid){
        this.leftChildPointer = leftChildPointer;
        this.rowid = rowid;    
    }
}