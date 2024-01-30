// const db = require("../models");
// const sequelize = db.sequelize;
// exports.findAll = async (req, res) => {
//   const data = await sequelize.query("SELECT * FROM mgartsojual", {
//     raw: false,
//   });
//   res.json({
//     message: "All data SO",
//     data: data[0],
//   });
// };
const fun = require("../mgmx");
const { sequelize } = require("../models");

let now = `NOW()`;
let today = new Date().toJSON();


exports.findAll = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

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
  // let Op = db.Sequelize.Op;
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

exports.realSO = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  const id = req.params.id;
  let sql = `update mgartsojual set statusrealisasi=1, void=0 where idtsojual=${id}`;
  const data = await fun.execDataFromQuery(sequelize, sql);
    if (data.code == 500) {
      res.json({
        message: data.message
      })
    }
  res.json({
    message: 'Realisasi berhasil'
  })
}

exports.deleteSO = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  const id = req.params.id;
  let sql = `update mgartsojual set void=2, statusrealisasi=0, hapus=1 where idtsojual=${id}`;
  const data = await fun.execDataFromQuery(sequelize, sql);
  
  if (data.code == 500) {
    res.json({
      message: data.message
    })
  }
  res.json({
    message: 'Success deleted'
  })
}

exports.viewSO = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  const id = req.params.id;
  let sql = `SELECT j.idtsojual, j.buktitsojual, j.tgltsojual ,j.bruto, j.discp, j.discv, j.ppnp, j.ppnv, j.netto, j.jmlbayartunai, j.countedit, c.idmcust, c.nmmcust, c.alamat, c.telp1 FROM mgartsojual j LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust where j.idtsojual=${id}`;
  console.log('sql-man', sql)
  var so = await fun.getDataFromQuery(sequelize, sql);
  var arr_data = {};
  if (so.length > 0) {
    so = so[0];
    let sql1 = `SELECT jd.idtsojuald, jd.idmbrg, jd.qtytotal, jd.hrgstn, jd.discv, jd.subtotal, b.kdmbrg, b.nmmbrg FROM mgartsojuald jd LEFT OUTER JOIN mginmbrg b ON jd.idmbrg = b.idmbrg WHERE jd.idtsojual = ${id}`;
    const brg = await fun.getDataFromQuery(sequelize, sql1);
    var arr_brg = brg.map((item, index) => {
      return {
        "id": item.idtsojuald,
        "barcode" : item.kdmbrg,
        "nama": item.nmmbrg,
        "jumlah" : parseFloat(item.qtytotal),
        "harga": parseFloat(item.hrgstn),
        "diskon" : parseFloat(item.discv),
        "total": parseFloat(item.subtotal)
      }
    })
    var data = {
      "bukti": so.buktitsojual,
      "tanggal": so.tgltsojual,
      "bruto" : parseFloat(so.bruto),
      "discp" : parseFloat(so.discp),
      "discv": parseFloat(so.discv),
      "ppnp": parseFloat(so.ppnp),
      "ppnv": parseFloat(so.ppnv),
      "nettoraw" : parseFloat(so.netto),
      "netto" : parseFloat(so.netto),
      "bayar" : parseFloat(so.jmlbayartunai),
      "sisa" : parseFloat(so.jmlbayartunai) - parseFloat(so.netto),
      "edit" : so.countedit,
      "idcust" : so.idmcust,
      "nmmcust" : so.nmmcust,
      "alamat" : so.alamat,
      "telp" : so.telp1,
      "item": arr_brg
    }

    arr_data = data;
  }

  res.json({
    message: "Success",
    data: arr_data
  })
  
}

async function buktitransaksi() {
  // const today = today;

  const newdate = new Date();

  let bulan = newdate.getMonth() + 1;
  let year = newdate.getFullYear;
  let buktif = "SO/" + year + "/" + bulan + "/";
  let sql = `SELECT REPLACE(BuktiTSOJual,'${buktif}','') as data FROM MgArTSOJual WHERE BuktiTSOJual LIKE '%${buktif}%' ORDER BY BuktiTSOJual DESC LIMIT 1`;
  var bso = await fun.pickDataFromQuery(
    sequelize,
    sql
  )
  var num = 1;
  if (bso != "") {
    num = parseFloat(bso) + 1;
  }

  return num;
  
}

async function lastidtjual(sequelize) {
  let last = await fun.countDataFromQuery(
    sequelize,
    `SELECT idtsojual as total FROM mgartsojual ORDER BY total DESC LIMIT 1`
  );
  let last_id = last || 0;
  let id = last_id + 1;
  return id;
  
}

async function lastidtjuald(sequelize) {
  let last = await fun.countDataFromQuery(
    sequelize,
    `SELECT idtsojuald as total FROM mgartsojuald ORDER BY total DESC LIMIT 1`
  );
  let last_id = last || 0;
  let id = last_id + 1;
  return id;
}

async function getidmbrg(sequelize, barcode) {
  var data = await fun.pickDataFromQuery(
    sequelize,
    `SELECT idmbrg as data FROM mginmbrg where kdmbrg = '${barcode}' or barcode = '${barcode}' where Hapus = 0 ORDER BY ID DESC LIMIT 1`
  )
  return data;
}

exports.createSO = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  var paramnya = req.body;
  console.log('paramnya', req.body.idcust)
  let id = await lastidtjual(sequelize);

  // req.body['id'] = id;

  // let bukti = await buktitransaksi(sequelize);
  let bukti = 0;

  let counter = 0;

  var idmcabang = 0;
  let idtjual = 0;
  let buktitjual = bukti;
  let tgltjual = today;
  let jenistjual = 0;
  let idmcabangmcust = 0;
  let idmcabangmsales = 0;
  let idmcust = paramnya.idcust || 0;
  let idmsales = paramnya.idsales || 0;
  // let bruto = paramnya.bruto.replaceAll(',', '') || 0;
  // let discp = paramnya.globaldiskonp.replaceAll('%', '') || 0;
  // let discv = paramnya.globaldiskonv.replaceAll(',', '') || 0;
  // let ppnp = paramnya.ppnp.replaceAll('%', '') || 0;
  // let ppnv = paramnya.ppnv.replaceAll(',', '') || 0;
  let bruto = parseFloat(paramnya.bruto || 0);
  let discp = parseFloat(paramnya.globaldiskonp || 0);
  let discv = parseFloat(paramnya.globaldiskonv || 0);
  let ppnp = parseFloat(paramnya.ppnp || 0);
  let ppnv = parseFloat(paramnya.ppnv || 0);

  let pphp = 0;
  let pphv = 0;

  // let netto = paramnya.netto.replaceAll(',', '') || 0;
  let netto = parseFloat(paramnya.netto || 0);
  let hapus = 0;
  let _void = 1;
  let countedit = 0;
  let jmlbayartunai = paramnya.bayar || 0;
  let keterangan = "";
  // let itembrg = paramnya.itembrg;
  // console.log('itembrg', itembrg)
  // let jml_item = itembrg.length || 0;
  let statusrealisasi = 0;

  // for (let i = 0; i < jml_item; i++) {
  //   let idtjuald = await lastidtjuald(sequelize); // fungsi
  //   let idtjual = id;
  //   // let idmbrg = await getidmbrg(paramnya.itembrg[i].barcode); // fungsi
  //   let idmbrg = 0; // fungsi
  //   let qty1 = parseFloat(param.itembrg[i].jumlah);
  //   let qtytotal = parseFloat(param.itembrg[i].jumlah);

  //   // let hrgstn = param.harga[i].replaceAll(',', '');
  //   // let subtotal = param.total[i].replaceAll(',', '');
  //   // let discp = param.discp[i].replaceAll(',', '');
  //   // let discv = param.discv[i].replaceAll(',', '');
  //   let hrgstn = parseFloat(param.itembrg[i].harga);
  //   let subtotal = parseFloat(param.itembrg[i].total);
  //   let discp = parseFloat(param.itembrg[i].discp);
  //   let discv = parseFloat(param.itembrg[i].discv);

  //   // let sql = `insert into mgartsojuald (idmcabang, idtsojuald, idtsojual, idmbrg, qty1, hrgstn, subtotal, qtytotal, discp, discv) values('${idmcabang}', '${idtjuald}', '${idtjual}', '${idmbrg}', '${qty1}', '${hrgstn}', '${subtotal}', '${qtytotal}', '${discp}', '${discv}')`;
  //   let sql = `insert into mgartsojuald (idmcabang, idtsojuald, idtsojual, idmbrg, qty1, hrgstn, subtotal, qtytotal, discp, discv) 
  //   values('${idmcabang}', '${idtjuald}', '${idtjual}', '${idmbrg}', '${qty1}', '${hrgstn}', '${subtotal}', '${qtytotal}', '${discp}', '${discv}')`;
  //   // const data = await fun.execDataFromQuery(sequelize, sql);
  //   // if (data.code == 500) {
  //   //   res.json({
  //   //     message: data.message
  //   //   })
  //   // }
  //   console.log('man-mgartsojuald', sql)
    
  // }
  // let query = `insert into mgartsojual (idmcabang, idtsojual, buktitsojual,statusrealisasi, tgltsojual, jenistjual, idmcabangmcust, idmcust,idmcabangmsales,idmsales, bruto, discp, discv, ppnp, ppnv, netto, hapus, void, countedit, countprint, tglcreate,tglupdate,keterangan,idmkas,jmlbayarkredit,tgljtpiut,idtkontrak,jenistso,statusso,keteranganum,idmcabangmproject,idmproject,idmusercreate,idmuserupdate,buktitporeferensi,idmcabangmcustkirim,idmcustkirim,tgltermsojt,roundingvalue,jmlbayartunai,jenisekspedisi) 
  // values('${idmcabang}', '${idtjual}', '${buktitjual}','${statusrealisasi}', ${now}, '${jenistjual}', '${idmcabangmcust}', '${idmcust}','${idmcabangmsales}','${idmsales}', '${bruto}', '${discp}', '${discv}', '${ppnp}', '${ppnv}', '${netto}', '${hapus}', '${_void}', '${countedit}', '${countedit}', ${now},${now}, '${keterangan}','0','0','0','0','0','0','0','0','0','0','0','0','0','-1','0','0','0','0')`;
  // console.log('man-mgartsojual', query)

  // let query = `insert into mgartsojual (idmcabang, idtsojual, buktitsojual,statusrealisasi, tgltsojual, jenistjual, idmcabangmcust, idmcust,idmcabangmsales,idmsales, bruto, discp, discv, ppnp, ppnv, netto, hapus, void, countedit, countprint, tglcreate,tglupdate,keterangan,idmkas,jmlbayarkredit,tgljtpiut,idtkontrak,jenistso,statusso,keteranganum,idmcabangmproject,idmproject,idmusercreate,idmuserupdate,buktitporeferensi,idmcabangmcustkirim,idmcustkirim,tgltermsojt,roundingvalue,jmlbayartunai,jenisekspedisi) values('${idmcabang}', '${idtjual}', '${buktitjual}','${statusrealisasi}', '${tgltjual}', '${jenistjual}', '${idmcabangmcust}', '${idmcust}','${idmcabangmsales}','${idmsales}', '${bruto}', '${discp}', '${discv}', '${ppnp}', '${ppnv}', '${netto}', '${hapus}', '${_void}', '${countedit}', '${countedit}', '${tglcreate}','${tglcreate}', '${keterangan}','0','0','0','0','0','0','0','0','0','0','0','0','0','-1','0','0','0','0')`;
  // const data2 = await fun.execDataFromQuery(sequelize, query);
  // if (data2.code == 500) {
  //   res.json({
  //     message: data.message
  //   })
  // }


}
