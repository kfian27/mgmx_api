const db = require("./models/index");
const sequelize = db.sequelize;

exports.countDataFromQuery = async (query = "") => {
    var count_data = await sequelize.query(
        query, {
            raw: false,
            plain: true
        })
    return parseFloat(count_data.total);
}

exports.getDataFromQuery = async (query = "") => {
    var data = await sequelize.query(
        query, {
            raw: false,
        })
    return data[0];
}