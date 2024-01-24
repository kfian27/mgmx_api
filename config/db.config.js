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
    DB: "mgmx_data_v3",
    dialect: "mysql",
    pool: {
      max: 5,
      min: 0,
      acquire: 30000,
      idle: 10000,
    },
  },

  {
    // koneksi QC
  // 18.150 // lokal
  // 30.219 // vpn
    HOST: "192.168.18.150",
    PORT: "3307",
    USER: "root",
    PASSWORD: "mgemge",
    DB: "mgmx_data_v3",
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
// module.exports = {
//   HOST: "localhost",
//   USER: "root",
//   PASSWORD: "",
//   DB: "mgmxweb",
//   dialect: "mysql",
//   pool: {
//     max: 5,
//     min: 0,
//     acquire: 30000,
//     idle: 10000,
//   },
// };
