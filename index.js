const express = require("express");
const odbc = require("odbc");
const { Client } = require("pg");
const app = express();
const PORT = 3600;

const bodyParser = require("body-parser");
const helmet = require("helmet");
const compression = require("compression");
const cors = require("cors");

app.use(cors());
app.use(helmet());
app.use(compression());
app.use(bodyParser.json());

// Microsoft Access Database Path
// const ACCESS_DB_PATH = "C:\\Users\\Admin\\Documents\\Cookie Orders.accdb";

// // Access Connection String
// const accessConnectionString = `DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=${ACCESS_DB_PATH};`;

// PostgreSQL Connection Config
// const pgClient = new Client({
//     user: "postgres",
//     host: "34.46.166.186",
//     database: "testing3",
//     password: "Acumen#123",
//     port: 5432, // Default PostgreSQL port
// });

// Connect to PostgreSQL
// async function connectPostgres() {
//     try {
//         await pgClient.connect();
//         console.log("✅ Connected to PostgreSQL");
//     } catch (error) {
//         console.error("❌ PostgreSQL Connection Error:", error);
//         process.exit(1);
//     }
// }


async function connectPostgres(pgConfig) {
    const pgClient = new Client(pgConfig);
    try {
        await pgClient.connect();
        console.log("✅ Connected to PostgreSQL");
        return pgClient;
    } catch (error) {
        console.error("❌ PostgreSQL Connection Error:", error);
        return null;
    }
}

// Connect to Microsoft Access
async function connectAccess(accessDbPath) {
    console.log("🔗 Connecting to Access Database...");
    try {
        // const ACCESS_DB_PATH = "C:\\Users\\Admin\\Documents\\Cookie Orders.accdb";
        const accessConnectionString = `DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=${accessDbPath};`;
        const accessDb = await odbc.connect(accessConnectionString);
        console.log("✅ Connected to Access Database");

        const tablesResult = await accessDb.tables(null, null, null, "TABLE");
        const tables = tablesResult.map(row => row.TABLE_NAME);
        console.log("📂 Tables Found:", tables);

        for (const table of tables) {
            await migrateTable(accessDb, table);
        }

        await accessDb.close();
        console.log("🔌 Disconnected from Access Database");
        return true
    } catch (error) {
        console.error("❌ Access Connection Error:", error);
        return false
    }
}

// Fetch Data from Access Table
async function fetchDataFromAccess(accessDb, tableName) {
    console.log(`📥 Fetching data from ${tableName}...`);
    try {
        const result = await accessDb.query(`SELECT * FROM ${tableName}`);
        console.log(`📊 Found ${result.length} records in ${tableName}`);
        return result;
    } catch (error) {
        console.error(`❌ Error fetching data from ${tableName}:`, error);
        return [];
    }
}

// Ensure Table Exists in PostgreSQL
async function ensurePostgresTable(pgClient, tableName, sampleRecord) {
    const columns = Object.keys(sampleRecord)
        .map(col => `"${col.replace(/\s/g, "_")}" TEXT`)
        .join(", ");

    const primaryKey = `"${Object.keys(sampleRecord)[0].replace(/\s/g, "_")}"`; // Assume first column is PK

    const createTableQuery = `
        CREATE TABLE IF NOT EXISTS "${tableName}" (${columns}, PRIMARY KEY (${primaryKey}));
    `;

    try {
        await pgClient.query(createTableQuery);
        console.log(`✅ Table "${tableName}" ensured in PostgreSQL`);
    } catch (error) {
        console.error(`❌ Error creating table ${tableName}:`, error);
    }
}

// Insert Data into PostgreSQL (Handles Duplicates)
async function insertDataIntoPostgres(pgClient, tableName, records) {
    if (records.length === 0) return;

    const columns = Object.keys(records[0]).map(col => `"${col.replace(/\s/g, "_")}"`).join(", ");
    const primaryKey = `"${Object.keys(records[0])[0].replace(/\s/g, "_")}"`; // Assume first column is PK

    try {
        for (const record of records) {
            const values = Object.values(record)
                .map(value => (value ? `'${value.toString().replace(/'/g, "''")}'` : "NULL"))
                .join(", ");

            const insertQuery = `
                INSERT INTO "${tableName}" (${columns}) VALUES (${values})
                ON CONFLICT (${primaryKey}) DO UPDATE 
                SET ${columns.split(", ").map(col => `${col} = EXCLUDED.${col}`).join(", ")}
            `;

            await pgClient.query(insertQuery);
        }

        console.log(`✅ Successfully inserted/updated ${records.length} records into ${tableName}`);
    } catch (error) {
        console.error(`❌ Error inserting data into ${tableName}:`, error);
    }
}

// Migrate Table from Access to PostgreSQL
async function migrateTable(accessDb, pgClient, tableName) {
    const records = await fetchDataFromAccess(accessDb, tableName);
    if (records.length > 0) {
        await ensurePostgresTable(pgClient, tableName, records[0]);
        await insertDataIntoPostgres(pgClient, tableName, records);
    }
}

// API Endpoint to Trigger Migration
app.post("/migrate", async (req, res) => {
    try {
        const { accessDbPath, user, host, database, password, port = 5432 } = req.body;
        if (!accessDbPath || !pgConfig) {
            return res.status(400).send("❌ Missing required parameters: accessDbPath and pgConfig");
        }
        const data = {
            user, host, database, password, port
        }
        console.log("📢 Received migration request...");
        const pgClient = await connectPostgres(data);
        if (!pgClient) return res.status(500).send("❌ Failed to connect to PostgreSQL");

        const accessDb = await connectAccess(accessDbPath);
        if (!accessDb) return res.status(500).send("❌ Failed to connect to Access Database");
        console.log("✅ Data Migration Completed!");
        return res.status(200).json();

    } catch (error) {
        console.log(error);
        return res.status(500).json({ message: "Something went wrong", error });
    }

});

// Start Server
app.listen(PORT, () => {
    console.log(`🚀 Server running on http://localhost:${PORT}`);
});
