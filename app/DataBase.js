import { open } from "fs/promises";
import BTreePage from "./BTreePage.js";
import SchemaTable from "./SchemaTable.js";

export default class DataBase{

	databaseFilePath;
	pageSize;
	firstPage;
    schemaTable;
    
    tableName;
    tableColumnNames;
    tableValues;

    // To get from the index table
    rowIds;

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
    
    async getData(queryObj){
        this.tableName = queryObj.fromTableName;
        this.readTableColumnNames();

        let selectColumnNames = queryObj.selectColumns;

        if(selectColumnNames[0] === 'count(*)'){
            await this.readTableAllValues();
            return this.tableValues.length;
        }

        if(!queryObj.hasWhere){
            await this.readTableAllValues();
            return this.getFilteredData(selectColumnNames);
        }

        let whereColumn = queryObj.whereColumn;
        let whereValue = queryObj.whereValue;

        if(this.indexTableExists(whereColumn)){
            await this.readTableValuesWithIndex(whereColumn, whereValue);
            return this.getFilteredData(selectColumnNames);
        }
        
        await this.readTableAllValues();
        return this.getFilteredData(selectColumnNames, whereColumn, whereValue);

    }

    getFilteredData(selectColumnNames, whereColumn = null, whereValue = null){
        let whereColumnIdx;
        let selectColumnIdxs = [];
        for(const selectColumnName of selectColumnNames){
            selectColumnIdxs.push(this.tableColumnNames.indexOf(selectColumnName));
        }

        if(whereColumn !== null){
            whereColumnIdx = this.tableColumnNames.indexOf(whereColumn);
        }

        let filteredData = [];

        for(const row of this.tableValues){
            if(whereColumn !== null && row[whereColumnIdx] !== whereValue) continue;
            let filteredRow = [];
            for(const idx of selectColumnIdxs){
                filteredRow.push(row[idx]);
            }
            filteredData.push(filteredRow);
        }
        return filteredData;
    }

    readTableColumnNames(){
        try{
            let schemaTableEntry = this.getSchemaEntryFromName(this.tableName);
            this.tableColumnNames = schemaTableEntry.columnNames;
        }
        catch (err){
            console.log(`Error raeding table column names: ${err}`);
        }
    }
    
    indexTableExists(indexColumn){
        const indexTableName = `idx_${this.tableName}_${indexColumn}`;
        try{
            let schemaTableEntry = this.getSchemaEntryFromName(indexTableName);
            return schemaTableEntry;
        } catch(err){
            return false;
        }
    }

    async readTableValuesWithIndex(indexColumn, columnValue){
        this.rowIds = [];
        this.tableValues = [];
        try{
            let schemaTableEntryForIndex = this.indexTableExists(indexColumn);
            if(!schemaTableEntryForIndex) {
                throw new Error(`Cannot find Index table: ${this.tableName} on column: ${indexColumn}`);
            }

            await this.dfsGetRowIds(schemaTableEntryForIndex.rootpage, columnValue);
            let schemaTableEntry = this.getSchemaEntryFromName(this.tableName);
            for(const rowId of this.rowIds){
                await this.dfsGetValuesFromRowId(schemaTableEntry.rootpage, rowId);
            }
            
        } catch (err){
            console.log(`Error reading the index table: ${err}`);
        }
    }

    async dfsGetRowIds(pageNum, columnValue){
        const page = await this.readPageWithPageNumber(pageNum);
        if(page.isLeaf){
            for(const leafCell of page.cells){
                let row = leafCell.values;
                if(!row[0] || row[0] < columnValue) continue;
                else if(row[0] === columnValue){
                    this.rowIds.push(row[1]);
                }
                else break;
            }
            return;
        } 
        
        let endFound = false;
        
        for(const interiorCell of page.cells){
            let row = interiorCell.values;
            if(!row[0] || row[0] < columnValue) continue;
            else if(row[0] === columnValue){
                await this.dfsGetRowIds(interiorCell.leftChildPointer, columnValue);
                this.rowIds.push(row[1]);
            } 
            else{
                await this.dfsGetRowIds(interiorCell.leftChildPointer, columnValue);
                endFound = true;
                break;
            }
        }
        if(!endFound) await this.dfsGetRowIds(page.rightMostPointer, columnValue);
    }

    async dfsGetValuesFromRowId(pageNum, rowId){
        const page = await this.readPageWithPageNumber(pageNum);
        if(page.isLeaf){
            for(const leafCell of page.cells){
                if(leafCell.rowid === rowId){
                    let row = leafCell.values;
                if(this.tableColumnNames[0] === 'id') row[0] = leafCell.rowid;
                this.tableValues.push(row);
                    break;
                }
            }
            return;
        }
        
        let endFound = false;

        for(const interiorCell of page.cells){
            if(interiorCell.rowid < rowId) continue;
            else {
                await this.dfsGetValuesFromRowId(interiorCell.leftChildPointer, rowId);
                endFound = true;
                break;
            }
        }

        if(!endFound) await this.dfsGetValuesFromRowId(page.rightMostPointer, rowId);

    }

    async readTableAllValues(){
        this.tableValues = [];
        try{
            let schemaTableEntry = this.getSchemaEntryFromName(this.tableName);
            await this.dfs(schemaTableEntry.rootpage);
        }
        catch (err){
            console.log(`Error raeding table values: ${err}`);
        }
    }

    async dfs(pageNum){
        const page = await this.readPageWithPageNumber(pageNum);
        if(page.isLeaf){
            for(const leafCell of page.cells){
                let row = leafCell.values;
                if(this.tableColumnNames[0] === 'id') row[0] = leafCell.rowid;
                this.tableValues.push(row);
            }
            return;
        } 

        for(const interiorCell of page.cells){
            await this.dfs(interiorCell.leftChildPointer);
        }
        await this.dfs(page.rightMostPointer);
    }

    getSchemaEntryFromName(tableName){
        let schemaTableEntry = null;
        for(const entry of this.schemaTable.entries){
            if(entry.name === tableName){
                schemaTableEntry = entry;
                break;
            }
        }

        if(schemaTableEntry === null){
            throw new Error(`Cannot find Schema Table Entry for the table name: ${tableName}`);
        }

        return schemaTableEntry;
    }
};

