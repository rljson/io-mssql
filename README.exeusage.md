<!--
@license
Copyright (c) 2026 Rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# io-mssql.exe — Usage Guide

`io-mssql.exe` is a self-contained Windows executable for managing SQL Server
databases via the Rljson format. No Node.js installation is required.

The exe is built from `src/cli.ts` via `pnpm build:exe` and written to
`dist/io-mssql.exe`.

---

## Connection options

All connection flags can be replaced by environment variables.

| Flag              | Env var           | Default        | Description                 |
| ----------------- | ----------------- | -------------- | --------------------------- |
| `--server <host>` | `MSSQL_SERVER`    | `localhost`    | SQL Server hostname or IP   |
| `--database <db>` | `MSSQL_DATABASE`  | —              | Target database name        |
| `--user <user>`   | `MSSQL_USER`      | `sa`           | Login username              |
| `--password <pw>` | `MSSQL_PASSWORD`  | `Password123!` | Login password              |
| `--port <port>`   | `MSSQL_PORT`      | `1431`         | Server port                 |
| `--schema <name>` | `MSSQL_SCHEMA`    | `main`         | Schema name                 |
| `--encrypt`       | —                 | `false`        | Enable TLS encryption       |
| `-h`, `--help`    | —                 | —              | Print help and exit         |

Output is always JSON on **stdout**. Errors go to **stderr**; exit code `1` on
error.

---

## Commands

### `catalog <file>` — Import a catalog JSON file

Creates the database (named `db_<filename>` unless `--database` is set),
creates the schema, creates all tables defined in the catalog, and inserts
all rows.

```bat
io-mssql.exe --server myserver --user sa --password Secret1! catalog data\minimal-catalog.json
```

With an explicit target database and schema:

```bat
io-mssql.exe --server myserver --user sa --password Secret1! ^
  --database my_db --schema main ^
  catalog data\minimal-catalog.json
```

Using environment variables (useful in CI pipelines):

```bat
set MSSQL_SERVER=myserver
set MSSQL_USER=sa
set MSSQL_PASSWORD=Secret1!
set MSSQL_DATABASE=my_db

io-mssql.exe catalog data\minimal-catalog.json
```

**Catalog JSON structure:**

```json
{
  "tableCfgs": {
    "_type": "components",
    "_data": [
      {
        "key": "products",
        "columns": [
          { "key": "_hash", "type": "string", "titleLong": "Hash", "titleShort": "Hash" },
          { "key": "name",  "type": "string", "titleLong": "Name", "titleShort": "Name" }
        ]
      }
    ]
  },
  "products": {
    "_type": "components",
    "_data": [
      { "_hash": "abc123", "name": "Widget" }
    ]
  }
}
```

---

### `dump` — Dump all tables as Rljson JSON

Reads every table in the database and prints a complete Rljson document.

```bat
io-mssql.exe --server myserver --database my_db --user sa --password Secret1! dump
```

Pipe to a file:

```bat
io-mssql.exe --server myserver --database my_db --user sa --password Secret1! dump > backup.json
```

---

### `dump-table <table>` — Dump a single table

```bat
io-mssql.exe --server myserver --database my_db --user sa --password Secret1! dump-table products
```

---

### `table-exists <tableKey>` — Check whether a table exists

Prints `true` or `false`.

```bat
io-mssql.exe --server myserver --database my_db --user sa --password Secret1! table-exists products
```

Useful in scripts:

```bat
for /f %%i in ('io-mssql.exe --server myserver --database my_db --user sa --password Secret1! table-exists products') do set EXISTS=%%i
if "%EXISTS%"=="true" echo Table is present
```

---

### `create-table <tableCfgJson>` — Create or extend a table

The argument is a JSON table-config object passed directly on the command line.

```bat
io-mssql.exe --server myserver --database my_db --user sa --password Secret1! ^
  create-table "{\"key\":\"orders\",\"columns\":[{\"key\":\"_hash\",\"type\":\"string\",\"titleLong\":\"Hash\",\"titleShort\":\"H\"},{\"key\":\"amount\",\"type\":\"number\",\"titleLong\":\"Amount\",\"titleShort\":\"Amt\"}]}"
```

If the table already exists, new columns are added (existing ones are kept).

---

### `raw-table-cfgs` — List all table config records

Returns the internal `tableCfgs` table content as JSON, which describes all
registered tables and their columns.

```bat
io-mssql.exe --server myserver --database my_db --user sa --password Secret1! raw-table-cfgs
```

---

### `write <file | ->` — Insert Rljson data from a file or stdin

**From a file:**

```bat
io-mssql.exe --server myserver --database my_db --user sa --password Secret1! write data\rows.json
```

**From stdin** (use `-` as the argument):

```bat
echo {"products":{"_type":"components","_data":[{"_hash":"x1","name":"Gadget"}]}} | io-mssql.exe --server myserver --database my_db --user sa --password Secret1! write -
```

**Piped from another command:**

```bat
cat generated-data.json | io-mssql.exe --server myserver --database my_db --user sa --password Secret1! write -
```

**Write JSON structure:**

```json
{
  "products": {
    "_type": "components",
    "_data": [
      { "_hash": "abc123", "name": "Widget" },
      { "_hash": "def456", "name": "Gadget" }
    ]
  }
}
```

---

### `read-rows <table> <whereJson>` — Query rows with a filter

The second argument is a JSON where-clause object.

Read all rows (empty where):

```bat
io-mssql.exe --server myserver --database my_db --user sa --password Secret1! read-rows products "{}"
```

Filter by a column value:

```bat
io-mssql.exe --server myserver --database my_db --user sa --password Secret1! read-rows products "{\"name\":\"Widget\"}"
```

---

### `row-count <table>` — Print the row count

```bat
io-mssql.exe --server myserver --database my_db --user sa --password Secret1! row-count products
```

---

### `content-type <table>` — Print the content type of a table

```bat
io-mssql.exe --server myserver --database my_db --user sa --password Secret1! content-type products
```

---

## Typical workflows

### Seed a fresh database from a catalog file

```bat
set MSSQL_SERVER=db.company.internal
set MSSQL_USER=sa
set MSSQL_PASSWORD=MySecret!

io-mssql.exe catalog seed\full-catalog.json
```

### Export and re-import between environments

```bat
rem Export from production
io-mssql.exe --server prod-db --database prod --user sa --password ProdPw! dump > export.json

rem Import into staging (write only inserts rows; tables must already exist)
io-mssql.exe --server stage-db --database stage --user sa --password StagePw! write export.json
```

### Use in a PowerShell pipeline

```powershell
$MSSQL_SERVER = "myserver"
$MSSQL_USER   = "sa"
$MSSQL_PASSWORD = "Secret1!"
$MSSQL_DATABASE = "my_db"

$count = .\dist\io-mssql.exe row-count products | ConvertFrom-Json
Write-Host "Products: $count"
```

### Check table existence before writing

```bat
for /f %%i in ('io-mssql.exe --server myserver --database my_db --user sa --password Secret1! table-exists products') do set EXISTS=%%i
if "%EXISTS%"=="false" (
  io-mssql.exe --server myserver --database my_db --user sa --password Secret1! ^
    create-table "{\"key\":\"products\",\"columns\":[{\"key\":\"_hash\",\"type\":\"string\",\"titleLong\":\"Hash\",\"titleShort\":\"H\"},{\"key\":\"name\",\"type\":\"string\",\"titleLong\":\"Name\",\"titleShort\":\"N\"}]}"
)
io-mssql.exe --server myserver --database my_db --user sa --password Secret1! write data\products.json
```

---

## Help

```bat
io-mssql.exe --help
io-mssql.exe -h
```
