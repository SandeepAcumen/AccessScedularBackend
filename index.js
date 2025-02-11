require("dotenv").config();
const express = require("express");
const bodyParser = require("body-parser");
const helmet = require("helmet");
const compression = require("compression");
const cors = require("cors");
const app = express();
const port = process.env.APP_PORT || 3600;



app.use(cors());
app.use(helmet());
app.use(compression());
app.use(bodyParser.json());



app.get("/api", (req, res) => {
    // return response.success(res, {
    //     statusCode: 200,
    //     message: "Translation server working!",
    // });
});

app.get("*", function (req, res) {
    // return response.error(res, {
    //     statusCode: 404,
    //     message: "Invalid URL!",
    // });
    res.status(200).json({ message: "Server working" })
});


app.listen(port, () => {
    console.log(`Listening on: http://localhost:${port}`);
    // listenForMessages();
});