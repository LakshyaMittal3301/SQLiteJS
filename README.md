# To run an SQL query

1. Ensure you have `node (16)` installed locally
2. Run `./your_sqlite3.sh` to run your program, which is implemented in
   `app/main.js`.

## Sample Query
```
./your_sqlite3.sh companies.db "SELECT id, name FROM companies WHERE country = 'eritrea'"
```

# Sample Databases
There are 3 databases:
1. `sample.db`:
   - This is a very small database.
   - It contains two table: `apples and oranges`.
2. `superheroes.db`:
   - This is a small version of the test database used in the table-scan stage.
   - It contains one table: `superheroes`.
   - It is ~1MB in size.
3. `companies.db`:
   - It contains one table: `companies`, and one index: `idx_companies_country`
   - It is ~1GB in size.

