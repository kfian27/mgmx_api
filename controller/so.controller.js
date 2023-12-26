const db = require("../models");
const sequelize = db.sequelize;
exports.findAll = async (req, res) => {
  const data = await sequelize.query("SELECT * FROM mgartsojual", {
    raw: false,
  });
  res.json({
    message: "All data SO",
    data: data[0],
  });
};
