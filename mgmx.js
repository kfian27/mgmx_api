const db = require("./models/index");
const sequelize = db.sequelize;
// const { QueryTypes } = require('sequelize');

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
            type: sequelize.SELECT
        })
    return data[0];
}

exports.getDateDiff = async (start = "", end = "") => {
    let date1 = new Date(start);
    let date2 = new Date(end);
    
    let diff_time = date2.getTime() - date1.getTime();
    let diff_days = Math.round(diff_time / (1000 * 3600 * 24));
    
    return diff_days;
}

