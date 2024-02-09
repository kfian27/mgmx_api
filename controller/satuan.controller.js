// comment By fian
// const db = require("../models");
// const sequelize = db.sequelize;
const fun = require("../mgmx");

exports.findAll = async (req, res) => {
  // const db = require("../models/db_dynamic")(req.datacompany);
  // const sequelize = db.sequelize;
  const sequelize = await fun.connection(req.datacompany);

  let sql =
    "SELECT IdMStn, KdMStn, NmMStn, Aktif FROM mginmstn WHERE Hapus = 0";
  let sortBy = req.body.sort_by || "TglCreate";
  let sortType = req.body.sort_type || "desc";
  let search = req.body.search || "";
  sortBy = sortBy.split(",");
  sortType = sortType.split(",");
  let sortArr = [];
  for (let index = 0; index < sortBy.length; index++) {
    let element = [sortBy[index], sortType[index]];
    sortArr.push(element);
  }
  let limit = req.body.limit || 12;
  let page = req.body.page || 1;
  let offset = (page - 1) * limit;
  let where = {};
  // let Op = db.Sequelize.Op;
  let qsearch = "";
  let qsort = "";
  let qpaginate = "";

  if (search != "") {
    // let testDate = db.Sequelize.literal("DATE_FORMAT(created_at,'%d-%m-%Y')");
    qsearch = ` AND ( 
        KdMStn LIKE '%${search}%' OR 
        NmMStn LIKE '%${search}%')`;
  }

  qsort = ` ORDER BY `;
  // if (sortArr.length > 0) {
  //   for (let index = 0; index < sortArr.length; index++) {
  //     const element = array[index];

  //   }
  //   qsort += sortArr;
  // } else {
  //   qsort += `${sortBy} ${sortType}`
  // }
  qsort += `${sortBy} ${sortType}`;
  qpaginate = ` LIMIT ${limit} OFFSET ${offset}`;

  let querysql = sql + qsearch + qsort + qpaginate;

  const data = await sequelize.query(querysql, {
    raw: false,
  });

  let qcount = "SELECT COUNT(IdMStn) as total FROM (" + sql + qsearch + ") tbl";
  const count_data = await sequelize.query(qcount, {
    raw: false,
    plain: true,
  });

  let total_data = count_data.total;
  var total_page = Math.ceil(total_data / limit);
  var current_page = parseInt(page);
  var prev_page = 0;
  var next_page = 0;
  if (current_page > 1) {
    prev_page = current_page - 1;
  }

  if (current_page < total_page) {
    next_page = current_page + 1;
  }

  res.json({
    message: "Success",
    CountData: total_data,
    TotalPage: total_page,
    CurrentPage: current_page,
    PrevPage: prev_page,
    NextPage: next_page,
    data: data[0],
  });
};

exports.create = async (req, res) => {
  // const db = require("../models/db_dynamic")(req.datacompany);
  // const sequelize = db.sequelize;
  const sequelize = await fun.connection(req.datacompany);

  let qcount = "SELECT IdMStn as ID FROM mginmstn ORDER BY ID DESC LIMIT 1";
  const count_data = await sequelize.query(qcount, {
    raw: false,
    plain: true,
  });

  let last_id = count_data.ID || 0;
  let id = last_id + 1;

  req.body["id"] = id;
  let kode = req.body.kode || "";
  let nama = req.body.nama || "";
  let is_aktif = req.body.is_aktif || 0;

  let userid = 0;

  let sql = `INSERT INTO mginmstn (idmstn, kdmstn, nmmstn, idmusercreate, tglcreate, idmuserupdate, tglupdate, aktif, hapus)
  VALUES(${id},'${kode}', '${nama}', '${userid}', NOW(), '${userid}', NOW(), ${is_aktif}, 0)`;
  const data = await sequelize
    .query(sql, {
      raw: false,
    })
    .then((datanya) => {
      res.json({
        message: "Master Satuan berhasil dibuat.",
        data: req.body,
      });
    })
    .catch((err) => {
      res.status(500).json({
        message: err.message || "Some error occurred while updating the data.",
        data: null,
      });
    });
};

exports.update = async (req, res) => {
  // const db = require("../models/db_dynamic")(req.datacompany);
  // const sequelize = db.sequelize;
  const sequelize = await fun.connection(req.datacompany);

  const id = req.params.id;
  req.body["id"] = parseInt(id);

  let kode = req.body.kode || "";
  let nama = req.body.nama || "";
  let is_aktif = req.body.is_aktif || 0;

  let userid = 0;

  let sql = `UPDATE mginmstn SET
  kdmstn = '${kode}',
  nmmstn = '${nama}',
  idmuserupdate = '${userid}',
  tglupdate = NOW(),
  aktif = ${is_aktif}
  WHERE idmstn = ${id}`;
  const data = await sequelize
    .query(sql, {
      raw: false,
    })
    .then((datanya) => {
      res.json({
        message: "Master Satuan berhasil diupdate.",
        data: req.body,
      });
    })
    .catch((err) => {
      res.status(500).json({
        message: err.message || "Some error occurred while updating the data.",
        data: null,
      });
    });
};

// DELETE: Menghapus data sesuai id yang dikirimkan
exports.delete = async (req, res) => {
  // const db = require("../models/db_dynamic")(req.datacompany);
  // const sequelize = db.sequelize;
  const sequelize = await fun.connection(req.datacompany);

  const id = req.params.id;
  req.body["id"] = parseInt(id);
  let userid = 0;
  let sql = `UPDATE mginmstn SET Hapus = 1, idmuserupdate = ${userid}, tglupdate = NOW() WHERE idmstn = ${id}`;
  const data = await sequelize
    .query(sql, {
      raw: false,
    })
    .then((datanya) => {
      res.json({
        message: "Master Satuan berhasil dihapus.",
        data: req.body,
      });
    })
    .catch((err) => {
      res.status(500).json({
        message: err.message || "Some error occurred while deleting the data.",
        data: null,
      });
    });
};
