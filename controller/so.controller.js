const db = require("../models");
const sequelize = db.sequelize;
// exports.findAll = async (req, res) => {
//   const data = await sequelize.query("SELECT * FROM mgartsojual", {
//     raw: false,
//   });
//   res.json({
//     message: "All data SO",
//     data: data[0],
//   });
// };

exports.findAll = async (req, res) => {
  let sql = `Select so.IdTSOJual, so.IdMCabang, so.BuktiTSOJual, so.Bruto, so.Netto, 
  DATE_FORMAT(so.TglTsoJual,'%d-%m-%Y') as TglTsoJual, so.void as Status, cust.NmMcust, sales.NmMsales, so.Keterangan,
  CASE
  WHEN (SELECT COUNT(b.IdTSOJual) FROM MgArTSOJual b WHERE b.IdTSOJual = so.IdTSOJual AND so.Void = 1) > 0 THEN 1
  ELSE 0
  END as CanEdit
  from MgArTSOJual so join mgarmcust cust on so.IdMCust = cust.IdMCust join mgarmsales sales on sales.IdMSales = so.IdMSales where so.StatusSO=0`;
  let sortBy = req.body.sort_by || "so.IdTSOJual ";
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
  let Op = db.Sequelize.Op;
  let qsearch = '';
  let qsort = '';
  let qpaginate = '';

  if (search != "") {
    qsearch = ` and 
    (so.BuktiTSOJual like '%${search}%' or 
    so.tgltsojual like '%${search}%' or 
    cust.NmMCust like '%${search}%' or 
    so.bruto like '%${search}%')`;
  }

  qsort = ` ORDER BY `;
  qsort += `${sortBy} ${sortType}`
  qpaginate = ` LIMIT ${limit} OFFSET ${offset}`;

  let querysql = sql + qsearch + qsort + qpaginate;

  const data = await sequelize.query(querysql, {
    raw: false,    
  });

  let qcount = "SELECT COUNT(IdTSOJual) as total FROM ("+sql+qsearch+") tbl";
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


exports.create = async (req, res) => {
  let qcount = "SELECT idtsojual as ID FROM mgartsojual ORDER BY ID DESC LIMIT 1";
  const count_data = await sequelize.query(qcount, {
    raw: false,
    plain: true
  });

  let last_id = count_data.ID || 0;
  let id = last_id + 1;

  req.body['id'] = id;
  let kode = req.body.kode || "";
  let nama = req.body.nama || "";
  let is_aktif = req.body.is_aktif || 0;
  

  let userid = 0;
}
