const dbConfig = require("../config/db.config")(0);
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

db.company = require("./company.models")(sequelize, Sequelize);
db.user = require("./user.models")(sequelize, Sequelize);
db.user_company = require("./user_company.models")(sequelize, Sequelize);

db.company.belongsToMany(db.user, { through: db.user_company });
db.user.belongsToMany(db.company, { through: db.user_company });

module.exports = db;
