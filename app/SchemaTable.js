class SchemaTableEntry{
	type;
	name;
	tbl_name;
	rootpage;
	sql;
    columnNames;

	constructor(valueArray){
		this.type = valueArray[0];
		this.name = valueArray[1];
		this.tbl_name = valueArray[2];
		this.rootpage = valueArray[3];
		this.sql = valueArray[4];
		const matches = this.sql.match(/\(([^)]+)\)/g)[0].slice(1, -1).split(',');
        this.columnNames = matches.map(str => str.trim().split(' ')[0]);
    }

	toString(){
		let str = "Schema Table Entry:\n";
		for(const key in this){
			str += `\t\tKey: ${key}, Value: ${this[key]}\n`;
		}
		return str;

	}
};

export default class SchemaTable{
	entries;

	constructor(cellArray){
		this.entries = [];
		for(const cell of cellArray){
			this.entries.push(new SchemaTableEntry(cell.values));
		}
	}

	toString(){
		let str = "Schema Table:\n";
		for(const entry of this.entries){
			str += `\t${entry}\n`;
		}
		return str;
	}
};
