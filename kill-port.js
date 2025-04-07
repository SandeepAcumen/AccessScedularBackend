const find = require("find-process");
const { exec } = require("child_process");

const port = process.env.PORT || 3601;

find("port", port).then((list) => {
    if (list.length > 0) {
        console.log(`🔍 Found process on port ${port}. Killing...`);
        exec(process.platform === "win32" ? `taskkill /PID ${list[0].pid} /F` : `kill -9 ${list[0].pid}`, (err) => {
            if (err) {
                console.error(`❌ Error killing process:`, err);
            } else {
                console.log(`✅ Process on port ${port} killed.`);
            }
            startServer();
        });
    } else {
        console.log(`⚠️ No process found on port ${port}, starting server...`);
        startServer();
    }
});

function startServer() {
    const serverProcess = exec("node index.js");
    serverProcess.stdout.pipe(process.stdout);
    serverProcess.stderr.pipe(process.stderr);
}
