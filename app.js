"use strict";
const express = require("express");
const cors = require("cors");
const app = express();
const http = require("http").createServer(app);
const soRoute = require("./routing/so.route");
const companyRoute = require("./routing/company.route");
const userRoute = require("./routing/user.route");

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const db = require("./models/index");
db.sequelize.sync({ alter: true });

app.get("/", async(req, res) => {
    res.json({
        message: "Company created successfully test.",
    });
});

app.use("/api/so", soRoute);
app.use("/master/company", companyRoute);
app.use("/master/user", userRoute);

var server = http.listen(8123, () => {
    //   console.log("Server is running at port 8000");
});