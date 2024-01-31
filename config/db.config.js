const allDB = [
  {
    // HOST: "mge-qc",
    HOST: "192.168.18.150",
    USER: "root",
    PASSWORD: "mgemge",
    DB: "mgmx_web",
    PORT: 3307,
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
