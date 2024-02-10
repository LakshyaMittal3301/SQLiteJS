export default class QueryParser {
    command;
    fromTableName;
    selectColumns;
    hasWhere;
    whereColumn;
    whereValue;

    constructor(query){
        let queryArray = query.split(' ');
        
        this.selectColumns = [];
        this.hasWhere = false;

        let fromSeen = false;
        let whereSeen = false;
        let equalSeen = false;
        for(let i = 0; i < queryArray.length; i++){
            if(queryArray[i].toLowerCase() == 'from'){
                fromSeen = true;
                continue;
            }
            if(queryArray[i].toLowerCase() == 'where'){
                whereSeen = true;
                this.hasWhere = true;
                continue;
            }
            if(queryArray[i] == '='){
                equalSeen = true;
                continue;
            }
            if(i == 0){
                this.command = queryArray[i].toLowerCase();
                continue;
            }
            if(!fromSeen){
                if(queryArray[i].endsWith(',')) this.selectColumns.push(queryArray[i].slice(0, -1).toLowerCase());
                else this.selectColumns.push(queryArray[i].toLowerCase());
                continue;
            }
            if(fromSeen && !whereSeen){
                this.fromTableName = queryArray[i].toLowerCase();
                continue;
            }
            if(fromSeen && whereSeen && !equalSeen){
                this.whereColumn = queryArray[i].toLowerCase();
                continue;    
            }
            if(fromSeen && whereSeen && equalSeen){
                if(queryArray[i].endsWith("'")){
                    this.whereValue = queryArray[i].slice(1, -1);
                }else{
                    this.whereValue = queryArray[i++].slice(1);
                    while(!queryArray[i].endsWith("'")) this.whereValue += " " + queryArray[i++];
                    this.whereValue += " " + queryArray[i].slice(0, -1);
                }
                continue;
            }
            console.log(`Error parsing SQL query: ${queryArray[i]} is encountered`);
        }
    }

};