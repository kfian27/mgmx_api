// const db = require("../models");
// const sequelize = db.sequelize;

const fun = require("../mgmx");

// get list
exports.findAll = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  let sql = "SELECT IdMBrg, DATE_FORMAT(TglCreate,'%d-%m-%Y') as Created, TglUpdate, NmMBrg, Barcode, KdMBrg, Reserved_dec1 as HppBarang, Reserved_dec2 as HargaJual, Keterangan, KdMStn, Aktif, Gambar FROM mginmbrg WHERE Hapus = 0";
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
  let qsearch = '';
  let qsort = '';
  let qpaginate = '';
  if (search != "") {
    // let testDate = db.Sequelize.literal("DATE_FORMAT(created_at,'%d-%m-%Y')");
    qsearch = ` AND ( 
    NmMBrg LIKE '%${search}%' OR 
    Barcode LIKE '%${search}%' OR 
    KdMBrg LIKE '%${search}%' OR 
    Reserved_dec1 LIKE '%${search}%' OR 
    Reserved_dec2 LIKE '%${search}%' OR 
    Keterangan LIKE '%${search}%')`;
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
  qsort += `${sortBy} ${sortType}`
  qpaginate = ` LIMIT ${limit} OFFSET ${offset}`;

  let querysql = sql + qsearch + qsort + qpaginate;

  const data = await sequelize.query(querysql, {
    raw: false,    
  });

  let qcount = "SELECT COUNT(IdMBrg) as total FROM ("+sql+qsearch+") tbl";
  const count_data = await sequelize.query(qcount, {
    raw: false,
    plain: true
  });

  let total_data = count_data.total;
  var total_page = Math.ceil(total_data / limit);
  var current_page = parseInt(page);
  var prev_page = 0;
  var next_page = 0;
  if (current_page > 1) {
    prev_page = current_page - 1 
  }

  if (current_page < total_page) {
    next_page = current_page + 1
  }

  res.json({
    message: "Success",
    CountData: total_data,
    TotalPage: total_page,
    CurrentPage: current_page,
    PrevPage : prev_page,
    NextPage : next_page,
    data: data[0],
  });
};

// Create: Create data sesuai id yang dikirimkan
exports.create = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  let qcount = "SELECT COUNT(IdMBrg) as total FROM mginmbrg";
  const count_data = await sequelize.query(qcount, {
    raw: false,
    plain: true
  });

  let id = count_data.total + 1;

  let kode = req.body.kode || "";
  let nama = req.body.nama || "";
  let barcode = req.body.barcode || "";
  let satuan = req.body.satuan || "";
  let hpp = req.body.hpp || 0;
  let harga_jual = req.body.harga_jual || 0;
  let keterangan = req.body.keterangan || "";
  let gambar = req.body.gambar || "";
  let is_aktif = req.body.is_aktif || "";

  let userid = 0;

  let sql = `INSERT INTO mginmbrg (idmcabangmbrg, idmbrg, kdmbrg, nmmbrg, barcode, reserved_dec1, reserved_dec2, idmusercreate, tglcreate, idmuserupdate, tglupdate, kdmstn, keterangan, gambar, aktif, hapus)
  VALUES(0,'${id}','${kode}', '${nama}', '${barcode}', ${hpp}, ${harga_jual}, '${userid}', NOW(), '${userid}', NOW(), '${satuan}', '${keterangan}', '${gambar}', ${is_aktif}, 0)`
  const data = await sequelize.query(sql, {
    raw: false,    
  }).then(datanya => {
      res.json({
        message: "Master Barang berhasil dibuat.",
        data: req.body,
      });
  }).catch((err) => {
    res.status(500).json({
        message: err.message || "Some error occurred while updating the data.",
        data: null,
    });
  });
}

// UPDATE: Update data sesuai id yang dikirimkan
exports.update = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  const id = req.params.id;

  let kode = req.body.kode || "";
  let nama = req.body.nama || "";
  let barcode = req.body.barcode || "";
  let satuan = req.body.satuan || "";
  let hpp = req.body.hpp || 0;
  let harga_jual = req.body.harga_jual || 0;
  let keterangan = req.body.keterangan || "";
  let is_aktif = req.body.is_aktif || 0;
  let gambar = req.body.gambar || "";
  let userid = 0;


  let sql = `UPDATE mginmbrg SET kdmbrg = '${kode}', 
  nmmbrg = '${nama}', 
  barcode = '${barcode}', 
  reserved_dec1 = '${hpp}', 
  reserved_dec2 = '${harga_jual}', 
  idmuserupdate = '${userid}', 
  tglupdate = NOW(), 
  kdmstn = '${satuan}', 
  keterangan = '${keterangan}',
  aktif = '${is_aktif}',
  gambar = '${gambar}'
  WHERE idmbrg = ${id}`;
  const data = await sequelize.query(sql, {
    raw: false,    
  }).then(datanya => {
      res.json({
        message: "Master Barang berhasil diupdate.",
        data: req.body,
      });
  }).catch((err) => {
    res.status(500).json({
        message: err.message || "Some error occurred while updating the company.",
        data: null,
    });
  });
}

// DELETE: Menghapus data sesuai id yang dikirimkan
exports.delete = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  const id = req.params.id;
  let userid = 0;
  let sql = `UPDATE mginmbrg SET Hapus = 1, idmuserupdate = ${userid}, tglupdate = NOW() WHERE IdMBrg = ${id}`
  const data = await sequelize.query(sql, {
    raw: false,    
  }).then(datanya => {
      res.json({
        message: "Master Barang berhasil dihapus.",
        data: req.body,
      });
  }).catch((err) => {
    res.status(500).json({
        message: err.message || "Some error occurred while updating the company.",
        data: null,
    });
  });
};
