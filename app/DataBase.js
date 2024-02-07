import { open } from "fs/promises";
import BTreePage from "./BTreePage.js";
import SchemaTable from "./SchemaTable.js";

export default class DataBase{

	databaseFilePath;
	pageSize;
	firstPage;
    schemaTable;

	constructor(databaseFilePath){
		this.databaseFilePath = databaseFilePath;
		this.pageSize = -1;
	}

	async initializeDB(){
		let databaseFileHandler;
		let buffer;
		try{
			await this.readPageSize();
			databaseFileHandler = await open(this.databaseFilePath, "r");
	
			buffer = Buffer.alloc(this.pageSize);
	
			await databaseFileHandler.read({
				length: this.pageSize,
				position: 0,
				buffer
			});

			this.firstPage = new BTreePage(buffer, true);
            this.schemaTable = new SchemaTable(this.firstPage.cells);
			
		} catch (err){
			console.log(`Error Initializing DB: ${err}`);
		} finally{
			await databaseFileHandler?.close();
		}
	}
	
	async readPageSize(){
		let databaseFileHandler;
		let buffer;
		try{
			databaseFileHandler = await open(this.databaseFilePath, "r");
			
			buffer = Buffer.alloc(100);
			
			await databaseFileHandler.read({
				length: 100,
				position: 0,
				buffer,
			});
			
			this.pageSize = buffer.readUInt16BE(16);

		} catch(err){
			console.log(`Error in reading page size: ${err}`);
			throw err;
		} finally {
			await databaseFileHandler?.close();
		}
	}
	
    async readPageWithRootPageNumber(rootPageNumber){
        let databaseFileHandler;
		let buffer;
		try{
			databaseFileHandler = await open(this.databaseFilePath, "r");
			
			buffer = Buffer.alloc(this.pageSize);
			
			await databaseFileHandler.read({
				length: this.pageSize,
				position: (rootPageNumber - 1) * this.pageSize,
				buffer,
			});

            return new BTreePage(buffer);

		} catch(err){
			console.log(`Error in reading page with rootnum: ${rootPageNumber}, Error: ${err}`);
			throw err;
		} finally {
			await databaseFileHandler?.close();
		}
	}
    
};

