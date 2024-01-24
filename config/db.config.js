const allDB = [
  {
    HOST: "mge-qc",
    USER: "root",
    PASSWORD: "mgemge",
    DB: "mgmx_web",
    port: 3307,
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
