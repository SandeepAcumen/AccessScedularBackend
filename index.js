const express = require("express");
const odbc = require("odbc");
const { Client } = require("pg");
const cron = require("node-cron");
const bodyParser = require("body-parser");
const helmet = require("helmet");
const compression = require("compression");
const cors = require("cors");
const path = require("path");
const moment = require("moment");
const { Server } = require("socket.io");

const app = express();
const PORT = process.env.APP_PORT || 3600;
let schedulerRunning = false;
let cronJob = null;

app.set("port", PORT);
const server = require("http").createServer(app);
const io = new Server(server, {
    path: "/socket.io",
    cors: { origin: "*" },
});

// Middleware
app.use(cors());
app.use(helmet());
app.use(compression());
app.use(bodyParser.json());

let users = {};

// Track previous record counts to detect updates
let previousCounts = {};

// Socket.io connection handling
io.on("connection", (socket) => {
    console.log(`⚡: ${socket.id} user connected!`);

    socket.on("connected", function (userId) {
        console.log("User connected:", userId);
        users[userId] = socket.id;
    });

    socket.on("disconnect", function () {
        console.log("User disconnected");
    });
});

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
        io.emit("updated-status", "Connected to PostgreSQL");
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
        io.emit("updated-status", "Connected to Access Database");
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
        console.log(`Access DB-📊  Found ${result.length} records in ${tableName}`);
        io.emit("updated-status", `Access DB -📊 Found ${result.length} records in ${tableName}`);
        return result;
    } catch (error) {
        console.error(`❌ Error fetching data from ${tableName}:`, error);
        return [];
    }
}

// Normalize column names
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
        console.log(`✅ Table ${tableName} ensured in PostgreSQL`);
        io.emit("updated-status", `✅ Table ${tableName} ensured in PostgreSQL`);
    } catch (error) {
        console.error(`❌ Error creating table ${tableName}:`, error);
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
        io.emit("updated-status", `✅ Successfully inserted/updated ${records.length} records into ${tableName}`);
        console.log(`✅ Successfully inserted/updated ${records.length} records into ${tableName}`);
    } catch (error) {
        console.error(`❌ Error inserting data into ${tableName}:`, error);
    }
}

// Migrate Table from Access to PostgreSQL
async function migrateTable(accessDb, pgClient, tableName) {
    const records = await fetchDataFromAccess(accessDb, tableName);

    if (previousCounts[tableName] === records.length) {
        console.log(`ℹ️ No changes detected in ${tableName}. Skipping migration.`);
        return;
    }

    previousCounts[tableName] = records.length;

    if (records.length > 0) {
        await ensurePostgresTable(pgClient, tableName, records[0]);
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
            io.emit("updated-status", `⏳ Running scheduled migration at ${moment().format("YYYY-MM-DD HH:mm:ss")}`);
        }

        return res.status(200).json({ message: "✅ Data Migration Completed!" });
    } catch (error) {
        console.error("❌ Migration Error:", error);
        return res.status(500).json({ message: "Something went wrong", error });
    }
});

// Scheduler
async function scheduleMigration(accessDbPath, pgConfig) {
    cronJob = cron.schedule("*/1 * * * *", async () => {
        console.log(`⏳ Running scheduled migration at ${moment().format("YYYY-MM-DD HH:mm:ss")}`);
        io.emit("updated-status", `⏳ Running scheduled migration at ${moment().format("YYYY-MM-DD HH:mm:ss")}`);

        const pgClient = await connectPostgres(pgConfig);
        if (!pgClient) return;

        const accessDb = await connectAccess(accessDbPath);
        if (!accessDb) {
            await pgClient.end();
            return;
        }

        const tablesResult = await accessDb.tables(null, null, null, "TABLE");
        const tables = tablesResult.map(row => row.TABLE_NAME);

        for (const table of tables) {
            await migrateTable(accessDb, pgClient, table);
        }

        await accessDb.close();
        await pgClient.end();
    });
    console.log("🔄 Migration Scheduler Started...");
}

app.post('/api/stop-scheduler', async (req, res) => {
    try {
        if (cronJob) {
            cronJob.stop();
            cronJob = null;
            schedulerRunning = false;
            previousCounts = {};
            return res.status(200).json({ message: "✅ Sheduler stopped!" });
        } else {
            return res.status(200).json({ message: "✅No Sheduler is running!" });
        }
    } catch (error) {
        console.error("❌ Scheduler Error:", error);
        return res.status(500).json({ message: "Something went wrong", error });
    }
})

server.listen(PORT, () => {
    console.log(`🚀 Server running on http://localhost:${PORT}`);
});
