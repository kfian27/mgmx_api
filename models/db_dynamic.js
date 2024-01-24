module.exports = (dbConfig) => {
  const Sequelize = require("sequelize");
  const sequelize = new Sequelize(
    dbConfig.database,
    dbConfig.username,
    atob(dbConfig.password),
    {
      host: dbConfig.host,
      dialect: "mysql",
      operatorAlias: false,
      port: dbConfig.port,
      pool: {
        max: 5,
        min: 0,
        acquire: 30000,
        idle: 10000,
      },
      logging: console.log,
    }
  );
  const db = {};
  db.Sequelize = Sequelize; // untuk all fungsi Sequelize
  db.sequelize = sequelize; // untuk koneksi db
  return db;
};
