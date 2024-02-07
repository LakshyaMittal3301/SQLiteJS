import DataBase from "./DataBase.js";
import { handleQuery } from "./commands.js";

const databaseFilePath = process.argv[2];
const query = process.argv[3];

let database = new DataBase(databaseFilePath);

database.initializeDB().then(() => {
	handleQuery(query, database);
});