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
let previousStates = {}; // Track previous record states

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
    if (tableName.startsWith("~")) {
        console.log(`ℹ️ Skipping temporary table: ${tableName}`);
        return []; // Skip processing
    }

    try {
        const result = await accessDb.query(`SELECT * FROM ${tableName}`);
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
    } catch (error) {
        console.error(`❌ Error creating table ${tableName}:`, error);
    }
}

// Sync Data Between Access and PostgreSQL
async function syncDataWithPostgres(pgClient, tableName, currentRecords) {
    if (currentRecords.length === 0) return;

    const columns = Object.keys(currentRecords[0]).map(col => `"${normalizeColumnName(col)}"`).join(", ");
    const primaryKey = `"${normalizeColumnName(Object.keys(currentRecords[0])[0])}"`;

    const changeLog = { inserts: 0, updates: 0, deletions: 0 }; // Track changes

    try {
        const previousRecords = previousStates[tableName] || [];
        const previousKeys = new Map(previousRecords.map(rec => [rec[Object.keys(currentRecords[0])[0]], rec]));

        // Determine deletions
        const currentKeys = new Set(currentRecords.map(rec => rec[Object.keys(currentRecords[0])[0]]));
        const recordsToDelete = previousRecords.filter(rec => !currentKeys.has(rec[Object.keys(currentRecords[0])[0]]));
        changeLog.deletions = recordsToDelete.length;

        for (const record of recordsToDelete) {
            const deleteQuery = `DELETE FROM "${tableName}" WHERE ${primaryKey} = '${record[Object.keys(currentRecords[0])[0]]}'`;
            await pgClient.query(deleteQuery);
        }

        // Process inserts and updates
        for (const record of currentRecords) {
            const values = Object.values(record).map(value => value ? `'${value.toString().replace(/'/g, "''")}'` : "NULL").join(", ");
            const upsertQuery = `INSERT INTO "${tableName}" (${columns}) VALUES (${values})
                ON CONFLICT (${primaryKey}) DO UPDATE 
                SET ${columns.split(", ").map(col => `${col} = EXCLUDED.${col}`).join(", ")}`;

            if (previousKeys.has(record[Object.keys(record)[0]])) {
                // Check if any values have changed
                const previousRecord = previousKeys.get(record[Object.keys(record)[0]]);
                let hasChanged = false;
                for (const key in record) {
                    if (record[key] !== previousRecord[key]) {
                        hasChanged = true;
                        break; // Exit loop if any change is found
                    }
                }

                if (hasChanged) {
                    changeLog.updates += 1;
                    await pgClient.query(upsertQuery);
                }
            } else {
                changeLog.inserts += 1;
                await pgClient.query(upsertQuery);
            }
        }

        previousStates[tableName] = currentRecords; // Update state for next run

        // Emit changes
        if (changeLog.inserts > 0 || changeLog.updates > 0 || changeLog.deletions > 0) {
            io.emit(
                "updated-status",
                `📊 Table: ${tableName} — Inserts: ${changeLog.inserts}, Updates: ${changeLog.updates}, Deletions: ${changeLog.deletions}`
            );
        }
    } catch (error) {
        console.error(`❌ Error syncing data with ${tableName}:`, error);
    }
}

// Migrate Table from Access to PostgreSQL
async function migrateTable(accessDb, pgClient, tableName) {
    const currentRecords = await fetchDataFromAccess(accessDb, tableName);

    const previousRecords = previousStates[tableName] || [];
    const hasChanges =
        currentRecords.length !== previousRecords.length ||
        JSON.stringify(currentRecords) !== JSON.stringify(previousRecords);

    if (hasChanges) {
        console.log(`📊 Found ${currentRecords.length} records in ${tableName}`);
        await ensurePostgresTable(pgClient, tableName, currentRecords[0]);
        await syncDataWithPostgres(pgClient, tableName, currentRecords);
        console.log(`✅ Synchronized ${currentRecords.length} records with ${tableName}`);
    } else {
        console.log(`ℹ️ No changes detected in ${tableName}`);
    }

    previousStates[tableName] = currentRecords;
}

// API Endpoint to Trigger Migration Manually
app.post("/api/migrate", async (req, res) => {
    try {
        const { accessDbPath, user, host, database, password, port = 5432 } = req.body;
        const pgConfig = { user, host, database, password, port };

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
async function scheduleMigration(accessDbPath, pgConfig) {
    cronJob = cron.schedule("*/1 * * * *", async () => {
        const timestamp = moment().format("YYYY-MM-DD HH:mm:ss");
        console.log(`⏳ Running scheduled migration at ${timestamp}`);
        io.emit("updated-status", `⏳ Running scheduled migration at ${timestamp}`);

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

        console.log(`✅ Scheduled Synchronization completed at ${timestamp}`);
        io.emit("updated-status", `✅ Scheduled Synchronization completed at ${timestamp}`);
    });

    console.log("🔄 Migration scheduler started...");
}

// API Endpoint to Stop the Scheduler
app.post("/api/stop-scheduler", async (req, res) => {
    try {
        if (cronJob) {
            cronJob.stop();
            cronJob = null;
            schedulerRunning = false;
            previousStates = {}; // Clear previous states
            console.log("✅ Scheduler stopped!");
            return res.status(200).json({ message: "✅ Scheduler stopped!" });
        } else {
            console.log("✅ No scheduler is running!");
            return res.status(200).json({ message: "✅ No scheduler is running!" });
        }
    } catch (error) {
        console.error("❌ Error stopping scheduler:", error);
        return res.status(500).json({ message: "Something went wrong", error });
    }
});

// Start Server
server.listen(PORT, () => {
    console.log(`🚀 Server running on http://localhost:${PORT}`);
});