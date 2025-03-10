const { Client } = require("pg");
const path = require("path");
const { Database } = require("sqlite3"); // Use better-sqlite3 or mssql if needed

// Configure PostgreSQL Connection
const pgClient = new Client({
    user: "postgres",
    host: "34.46.166.186",
    database: "friday",
    password: "Acumen#123",
    port: 5432,
});

// Connect to PostgreSQL
pgClient.connect().catch(err => console.error("PostgreSQL connection error:", err));

// Path to MS Access Database
const accessDbPath = path.join(__dirname, "C:\\Users\\Admin\\Documents\\Cookie Orders.accdb");

// Function to Sync Access to PostgreSQL
async function syncAccessToPostgres() {
    return new Promise((resolve, reject) => {
        const accessDb = new Database(accessDbPath, err => {
            if (err) {
                console.error("Access DB connection error:", err);
                return reject(err);
            }
        });

        // Query to get new/updated records from Access
        accessDb.all("SELECT * FROM your_table WHERE last_modified >= datetime('now', '-1 minute')", [], async (err, rows) => {
            if (err) {
                console.error("Error fetching data from Access:", err);
                return reject(err);
            }

            for (let row of rows) {
                try {
                    // Insert or Update in PostgreSQL
                    await pgClient.query(
                        "INSERT INTO your_pg_table (id, name, updated_at) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, updated_at = EXCLUDED.updated_at",
                        [row.id, row.name, row.last_modified]
                    );
                } catch (pgErr) {
                    console.error("PostgreSQL update error:", pgErr);
                }
            }

            resolve();
        });
    });
}

module.exports = { syncAccessToPostgres };
