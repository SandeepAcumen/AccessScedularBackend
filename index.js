const express = require("express");
const odbc = require("odbc");
const { Client } = require("pg");
const cron = require("node-cron");
const bodyParser = require("body-parser");
const helmet = require("helmet");
const compression = require("compression");
const cors = require("cors");
const path = require("path");

const app = express();
const PORT = process.env.APP_PORT || 3000;

// Middleware
app.use(cors());
app.use(helmet());
app.use(compression());
app.use(bodyParser.json());

// Serve Frontend
app.use(express.static(path.join(__dirname, "../microsoft-access-migrate/build")));

app.get("*", (req, res) => {
    res.sendFile(path.join(__dirname, "../microsoft-access-migrate/build", "index.html"));
});

// PostgreSQL Connection
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

// Microsoft Access Connection
async function connectAccess(accessDbPath) {
    try {
        const accessConnectionString = `DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=${accessDbPath};`;
        const accessDb = await odbc.connect(accessConnectionString);
        console.log("✅ Connected to Access Database");
        return accessDb;
    } catch (error) {
        console.error("❌ Access Connection Error:", error);
        return null;
    }
}

// Fetch Data from Access Table
async function fetchDataFromAccess(accessDb, tableName) {
    try {
        const result = await accessDb.query(`SELECT * FROM ${tableName}`);
        console.log(`📊 Found ${result.length} records in ${tableName}`);
        return result;
    } catch (error) {
        console.error(`❌ Error fetching data from ${tableName}:`, error);
        return [];
    }
}

// Normalize column names (replace spaces and special characters)
function normalizeColumnName(column) {
    return column.replace(/\s+/g, "_").replace(/[^a-zA-Z0-9_]/g, "");
}

// Ensure Table Exists in PostgreSQL
async function ensurePostgresTable(pgClient, tableName, sampleRecord) {
    const columns = Object.keys(sampleRecord).map(col => `"${normalizeColumnName(col)}" TEXT`).join(", ");
    const primaryKey = `"${normalizeColumnName(Object.keys(sampleRecord)[0])}"`;
    const createTableQuery = `CREATE TABLE IF NOT EXISTS "${tableName}" (${columns}, PRIMARY KEY (${primaryKey}));`;

    try {
        await pgClient.query(createTableQuery);
        console.log(`✅ Table "${tableName}" ensured in PostgreSQL`);
    } catch (error) {
        console.error(`❌ Error creating table ${tableName}:`, error);
    }
}

// Delete records from PostgreSQL if they don't exist in Access
async function deleteMissingRecords(pgClient, tableName, accessRecords) {
    if (accessRecords.length === 0) return;

    const primaryKey = `"${normalizeColumnName(Object.keys(accessRecords[0])[0])}"`;
    const accessIds = accessRecords.map(record => `'${record[Object.keys(record)[0]].toString().replace(/'/g, "''")}'`).join(", ");

    try {
        const deleteQuery = `DELETE FROM "${tableName}" WHERE ${primaryKey} NOT IN (${accessIds});`;
        await pgClient.query(deleteQuery);
        console.log(`🗑 Deleted records from ${tableName} that are not in Access`);
    } catch (error) {
        console.error(`❌ Error deleting records from ${tableName}:`, error);
    }
}

// Insert or Update Data into PostgreSQL
async function insertDataIntoPostgres(pgClient, tableName, records) {
    if (records.length === 0) return;
    const columns = Object.keys(records[0]).map(col => `"${normalizeColumnName(col)}"`).join(", ");
    const primaryKey = `"${normalizeColumnName(Object.keys(records[0])[0])}"`;

    try {
        for (const record of records) {
            const values = Object.values(record).map(value => value ? `'${value.toString().replace(/'/g, "''")}'` : "NULL").join(", ");
            const insertQuery = `INSERT INTO "${tableName}" (${columns}) VALUES (${values})
                ON CONFLICT (${primaryKey}) DO UPDATE 
                SET ${columns.split(", ").map(col => `${col} = EXCLUDED.${col}`).join(", ")}`;
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
        await deleteMissingRecords(pgClient, tableName, records);  // DELETE MISSING RECORDS FIRST
        await insertDataIntoPostgres(pgClient, tableName, records);
    }
}

// API Endpoint to Trigger Migration Manually
app.post("/api/migrate", async (req, res) => {
    try {
        const { accessDbPath, user, host, database, password, port = 5432 } = req.body;
        const pgConfig = { user, host, database, password, port };

        if (!accessDbPath || !pgConfig) {
            return res.status(400).json({ message: "❌ Missing required parameters." });
        }

        console.log("📢 Received migration request...");
        const pgClient = await connectPostgres(pgConfig);
        if (!pgClient) return res.status(500).json({ message: "❌ Failed to connect to PostgreSQL" });

        const accessDb = await connectAccess(accessDbPath);
        if (!accessDb) return res.status(500).json({ message: "❌ Failed to connect to Access Database" });

        const tablesResult = await accessDb.tables(null, null, null, "TABLE");
        const tables = tablesResult.map(row => row.TABLE_NAME);

        for (const table of tables) {
            await migrateTable(accessDb, pgClient, table);
        }

        await accessDb.close();
        await pgClient.end();

        if (!schedulerRunning) {
            schedulerRunning = true;
            scheduleMigration(accessDbPath, pgConfig);
        }

        return res.status(200).json({ message: "✅ Data Migration Completed!" });
    } catch (error) {
        console.error("❌ Migration Error:", error);
        return res.status(500).json({ message: "Something went wrong", error });
    }
});

// Scheduler
let schedulerRunning = false;
async function scheduleMigration(accessDbPath, pgConfig) {
    cron.schedule("*/5 * * * *", async () => {
        console.log("⏳ Running scheduled migration...");

        const pgClient = await connectPostgres(pgConfig);
        if (!pgClient) return;

        const accessDb = await connectAccess(accessDbPath);
        if (!accessDb) {
            await pgClient.end();
            return;
        }

        try {
            const tablesResult = await accessDb.tables(null, null, null, "TABLE");
            const tables = tablesResult.map(row => row.TABLE_NAME);

            for (const table of tables) {
                await migrateTable(accessDb, pgClient, table);
            }
        } catch (error) {
            console.error("❌ Error during scheduled migration:", error);
        } finally {
            await accessDb.close();
            await pgClient.end();
        }
    });

    console.log("🔄 Migration Scheduler Started: Running every 5 minutes...");
}

app.listen(PORT, () => {
    console.log(`🚀 Server running on http://localhost:${PORT}`);
});
