"use strict";
const express = require("express");
const cors = require("cors");
const app = express();
const http = require("http").createServer(app);
const soRoute = require("./routing/so.route");
const companyRoute = require("./routing/company.route");
const userRoute = require("./routing/user.route");
const barangRoute = require("./routing/barang.route");
const satuanRoute = require("./routing/satuan.route");
const customerRoute = require("./routing/customer.route");
const authRoute = require("./routing/auth.route");
const reportRoute = require("./routing/report.route");
const penjualanRoute = require("./routing/penjualan.route");
var path = require("path");
const dashboardRoute = require("./routing/dashboard.route");

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.resolve("./public")));

const db = require("./models/index");
db.sequelize.sync({ alter: true });

app.get("/", async (req, res) => {
  // test comments
  res.json({
    message: "Company created successfully test again.",
  });
});

app.use("/api/dashboard", dashboardRoute);
app.use("/api/so", soRoute);
app.use("/api/penjualan", penjualanRoute);
app.use("/api/report", reportRoute);
app.use("/api/barang", barangRoute);
app.use("/api/satuan", satuanRoute);
app.use("/api/customer", customerRoute);
app.use("/master/company", companyRoute);
app.use("/master/user", userRoute);
app.use("/auth", authRoute);

var server = http.listen(8123, () => {
  //   console.log("Server is running at port 8000");
});
