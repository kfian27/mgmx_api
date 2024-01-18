const allDB = [
  {
    HOST: "localhost",
    USER: "root",
    PASSWORD: "",
    DB: "mgmx_master",
    dialect: "mysql",
    pool: {
      max: 5,
      min: 0,
      acquire: 30000,
      idle: 10000,
    },
  },
  {
    HOST: "localhost",
    USER: "root",
    PASSWORD: "",
    DB: "mgmx",
    dialect: "mysql",
    pool: {
      max: 5,
      min: 0,
      acquire: 30000,
      idle: 10000,
    },
  },
];

module.exports = (dbSelect) => {
  return allDB[dbSelect];
};
