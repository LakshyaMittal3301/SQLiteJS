import { open } from "fs/promises";
import BTreePage from "./BTreePage.js";
import SchemaTable from "./SchemaTable.js";

export default class DataBase{

	databaseFilePath;
	pageSize;
	firstPage;
    schemaTable;
    tableValues;
    tableColumnNames;

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
            console.log(`page size: ${this.pageSize}`);
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
	
    async readPageWithPageNumber(pageNumber){
        let databaseFileHandler;
		let buffer;
		try{
			databaseFileHandler = await open(this.databaseFilePath, "r");
			
			buffer = Buffer.alloc(this.pageSize);
			
			await databaseFileHandler.read({
				length: this.pageSize,
				position: (pageNumber - 1) * this.pageSize,
				buffer,
			});

            return new BTreePage(buffer);

		} catch(err){
			console.log(`Error in reading page with rootnum: ${pageNumber}, Error: ${err}`);
			throw err;
		} finally {
			await databaseFileHandler?.close();
		}
	}
    
    readTableColumnNames(tableName){
        try{
            let schemaTableEntry = null;
            for(const entry of this.schemaTable.entries){
                if(entry.tbl_name === tableName){
                    schemaTableEntry = entry;
                    break;
                }
            }

            if(schemaTableEntry === null){
                throw new Error(`Cannot find Schema Table Entry for the table name: ${tableName}`);
            }

            this.tableColumnNames = schemaTableEntry.columnNames;
            return this.tableColumnNames;
        }
        catch (err){
            console.log(`Error raeding table column names: ${err}`);
        }
    }

    async readTableValues(tableName){
        this.tableValues = [];

        try{
            let schemaTableEntry = null;
            for(const entry of this.schemaTable.entries){
                if(entry.tbl_name === tableName){
                    schemaTableEntry = entry;
                    break;
                }
            }

            if(schemaTableEntry === null){
                throw new Error(`Cannot find Schema Table Entry for the table name: ${tableName}`);
            }

            await this.dfs(schemaTableEntry.rootpage);
            return this.tableValues;
        }
        catch (err){
            console.log(`Error raeding table values: ${err}`);
        }
    }

    async dfs(pageNum){
        const page = await this.readPageWithPageNumber(pageNum);
        if(page.isLeaf){
            for(const leafCell of page.cells){
                this.tableValues.push(leafCell.values);
            }
            return;
        } 

        for(const interiorCell of page.cells){
            await this.dfs(interiorCell.leftChildPointer);
        }
        await this.dfs(page.rightMostPointer);
    }
};

