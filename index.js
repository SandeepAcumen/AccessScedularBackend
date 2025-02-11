require("dotenv").config();
const express = require("express");
const bodyParser = require("body-parser");
const helmet = require("helmet");
const compression = require("compression");
const cors = require("cors");
const { Client } = require("pg");
const odbc = require("odbc");
const fs = require("fs");
const path = require("path");

const app = express();
const port = process.env.APP_PORT || 3600;

const loadMDBReader = async () => {
    const { default: MDBReader } = await import("mdb-reader");
    return MDBReader;
};

app.use(cors());
app.use(helmet());
app.use(compression());
app.use(bodyParser.json());

const connectAccess = async (accessDbPath) => {
    try {
        if (process.env.DSN_NAME) {
            return await odbc.connect(`DSN=${process.env.DSN_NAME}`);
        } else {
            const dbPath = path.resolve(accessDbPath);
            if (!fs.existsSync(dbPath)) throw new Error("Access file not found");

            // Dynamically import MDBReader
            const MDBReader = await loadMDBReader();
            return new MDBReader(dbPath);
        }
    } catch (err) {
        console.error("❌ Access Connection Error:", err);
        return null;
    }
};

// Get All Tables from Access
const getAccessTables = async (accessConn) => {
    try {
        if (process.env.DSN_NAME) {
            const result = await accessConn.query("SELECT name FROM MSysObjects WHERE type=1 AND name NOT LIKE 'MSys%';");
            return result.map(row => row.name);
        } else {
            return accessConn.listTables();
        }
    } catch (err) {
        console.error("❌ Error fetching tables:", err);
        return [];
    }
};

// Convert Access Data Types to PostgreSQL
const mapDataType = (type) => {
    if (typeof type === "number") return "INTEGER";
    if (typeof type === "string") return "TEXT";
    if (type instanceof Date) return "TIMESTAMP";
    return "TEXT";
};

// Create Table in PostgreSQL
const createTable = async (pgClient, tableName, columns) => {
    try {
        const columnDefs = columns.map(({ name, type }) => `"${name}" ${mapDataType(type)}`).join(", ");
        const sql = `CREATE TABLE IF NOT EXISTS "${tableName}" (${columnDefs}, PRIMARY KEY("${columns[0].name}"));`;
        await pgClient.query(sql);
        console.log(`✅ Table ensured: ${tableName}`);
    } catch (err) {
        // console.error(`❌ Error creating table ${tableName}:", err);
        console.log(err);

    }
};

// Migrate a Single Table
const migrateTable = async (pgClient, tableName, accessConn) => {
    try {
        console.log(`🚀 Syncing table: ${tableName}`);
        const data = process.env.DSN_NAME ? await accessConn.query(`SELECT * FROM ${tableName} ORDER BY 1 ASC`) : accessConn.getTable(tableName);
        if (!data.length) {
            console.log(`⚠️ Warning: ${tableName} is empty.Skipping...`);
            return;
        }
        const columns = Object.keys(data[0]).map(name => ({ name, type: data[0][name] }));
        await createTable(pgClient, tableName, columns);
    } catch (err) {
        console.error(`❌ Error migrating ${tableName}: `, err);
    }
};

app.post("/api/migrate", async (req, res) => {
    const { host, database, user, password, port, accessDbPath } = req.body;

    if (!host || !database || !user || !password || !port || !accessDbPath) {
        return res.status(400).json({ error: "Missing PostgreSQL credentials in request body" });
    }

    const pgClient = new Client({ host, database, user, password, port });

    try {
        await pgClient.connect();
        console.log("✅ Connected to PostgreSQL");
    } catch (err) {
        console.error("❌ PostgreSQL Connection Error:", err);
        return res.status(500).json({ error: "PostgreSQL connection failed" });
    }

    const accessConn = await connectAccess(accessDbPath);
    if (!accessConn) return res.status(500).json({ error: "Access connection failed" });

    const tables = await getAccessTables(accessConn);
    for (const table of tables) await migrateTable(pgClient, table, accessConn);

    res.json({ message: "🎉 Migration Completed Successfully!" });
});

app.listen(port, () => {
    console.log(`Listening on: http://localhost:${port}`);
});
