module.exports = (sequelize, Sequelize) => {
  const company = sequelize.define(
    "company",
    {
      id: {
        type: Sequelize.INTEGER,
        autoIncrement: true,
        primaryKey: true,
      },
      kode: {
        type: Sequelize.STRING(50),
      },
      nama: {
        type: Sequelize.STRING,
      },
      status: {
        type: Sequelize.INTEGER,
      },
      jenis: {
        type: Sequelize.INTEGER,
      },
      nama_perusahaan: {
        type: Sequelize.STRING,
      },
      no_ktp: {
        type: Sequelize.STRING,
      },
      alamat: {
        type: Sequelize.TEXT,
      },
      telp: {
        type: Sequelize.STRING,
      },
      handphone: {
        type: Sequelize.STRING,
      },
      email: {
        type: Sequelize.STRING,
      },
      username: {
        type: Sequelize.STRING,
      },
      password: {
        type: Sequelize.STRING,
      },
      host: {
        type: Sequelize.STRING,
      },
      port: {
        type: Sequelize.STRING,
      },
      database: {
        type: Sequelize.STRING,
      },
      deviceID: {
        type: Sequelize.STRING,
      },
      otp: {
        type: Sequelize.STRING(7),
      },
      logo: {
        type: Sequelize.STRING,
      },
    },
    {
      tableName: "company",
      createdAt: "created_at",
      updatedAt: "updated_at",
      deletedAt: "delete_at",
      paranoid: true,
      timestamps: true,
    }
  );
  return company;
};
