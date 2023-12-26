const dbConfig = require("../config/db.config");
const Sequelize = require("sequelize");
const sequelize = new Sequelize(dbConfig.DB, dbConfig.USER, dbConfig.PASSWORD, {
    host: dbConfig.HOST,
    dialect: dbConfig.dialect,
    operatorAlias: false,
    pool: {
        max: dbConfig.pool.max,
        min: dbConfig.pool.min,
        acquire: dbConfig.pool.acquire,
        idle: dbConfig.pool.idle,
    },
    logging: console.log,
});
const db = {};
db.Sequelize = Sequelize; // untuk all fungsi Sequelize
db.sequelize = sequelize; // untuk koneksi db

// db.company = require("./company")(sequelize, Sequelize);

module.exports = db;