export default class BufferReader{
	buffer;
	cursor;

    constructor(buffer){
        this.buffer = buffer;
        this.cursor = 0;
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

    readBuffer3Bytes(){
        let bytes = this.readBuffer2Bytes();
        bytes = (bytes << 8) | this.readBufferByte();
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
        let shift = 7;
        let byte;
        let bytesRead = 0;
    
        do {
            byte = this.readBufferByte();;
            bytesRead++;
            result <<= shift;
            result |= (byte & 0x7f);
        } while (byte & 0x80);
        if(bytesRead > 9) throw new Error(`Reading Var Int failed`);
        return {result, bytesRead};
    }

    setCursorTo(position){
        this.cursor = position;
    }

    moveCursorBy(numOfBytes){
        this.cursor += numOfBytes;
    }
};