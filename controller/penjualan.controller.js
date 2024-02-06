
const fun = require("../mgmx");
const { sequelize } = require("../models");

let now = `NOW()`;
let today = new Date().toJSON();


exports.findAll = async (req, res) => {
    const sequelize = await fun.connection(req.datacompany);
    
    let sql = `SELECT j.IdMCabang, j.IdTJual, j.BuktiTJual, j.TglTJual, j.IdMCust, c.NmMCust, j.IdMSales, s.NmMSales, j.Bruto, j.Netto, j.void, j.Keterangan 
    FROM mgartjual j
    LEFT OUTER JOIN mgarmcust c ON j.IdMCust = c.IdMCust
    left outer join mgarmsales s ON j.IdMSales = s.IdMSales`;

    let sortBy = req.body.sort_by || "j.idtjual";
    let sortType = req.body.sort_type || "desc";
    let search = req.body.search || "";
    sortBy = sortBy.split(",");
    sortType = sortType.split(",");
    let sortArr = [];
    for (let index = 0; index < sortBy.length; index++) {
        let element = [sortBy[index], sortType[index]];
        sortArr.push(element);
    }
    let limit = req.body.limit || 10;
    let page = req.body.page || 1;
    let offset = (page - 1) * limit;
    let where = {};
    // let Op = db.Sequelize.Op;
    let qsearch = '';
    let qsort = '';
    let qpaginate = '';

    if (search != "") {
        qsearch = ` where j.BuktiTJual like '%${search}%' or 
        j.TglTJual like '%${search}%' or 
        c.NmMCust like '%${search}%' or 
        j.bruto like '%${search}%'`;
    }

    qsort = ` ORDER BY `;
    qsort += `${sortBy} ${sortType}`
    qpaginate = ` LIMIT ${limit} OFFSET ${offset}`;

    let querysql = sql + qsearch + qsort + qpaginate;

    const data = await fun.getDataFromQuery(sequelize, querysql);

    let qcount = "SELECT COUNT(IdTJual) as total FROM (" + sql + qsearch + ") tbl";
    const count_data = await fun.countDataFromQuery(sequelize, qcount);

    let total_data = count_data;
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
        data: data,
    });
};

exports.real = async (req, res) => {
    const sequelize = await fun.connection(req.datacompany);

    const id = req.params.id;
    let sql = `update mgartjual set statusrealisasi=1, void=0  where idtjual=${id}`;
    const data = await fun.execDataFromQuery(sequelize, sql);
    if (data.code == 500) {
        res.json({
            message: data.message
        })
    }
    res.json({
        message: 'Realisasi berhasil',
    })
}

exports.delete = async (req, res) => {
    const sequelize = await fun.connection(req.datacompany);

    const id = req.params.id;
    let sql = `update mgartjual set void=2, statusrealisasi=0, hapus=1 where idtjual=${id}`;
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

exports.view = async (req, res) => {
    const sequelize = await fun.connection(req.datacompany);

    const id = req.params.id;
    let sql = `SELECT j.idtjual, j.buktitjual, j.tgltjual ,j.bruto, j.discp, j.discv, j.ppnp, j.ppnv, j.netto, j.jmlbayartunai, j.countedit, c.idmcust, c.nmmcust, c.alamat, c.telp1 FROM mgartjual j LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust where j.idtjual=${id}`;
    console.log('sql-man', sql)
    var so = await fun.getDataFromQuery(sequelize, sql);
    var arr_data = {};
    if (so.length > 0) {
        so = so[0];
        let sql1 = `SELECT jd.idtjuald, jd.idmbrg, jd.qtytotal, jd.hrgstn, jd.discp, jd.discv, jd.subtotal, b.kdmbrg, b.nmmbrg FROM mgartjuald jd LEFT OUTER JOIN mginmbrg b ON jd.idmbrg = b.idmbrg WHERE jd.idtjual = ${id}`;
        const brg = await fun.getDataFromQuery(sequelize, sql1);
        var arr_brg = brg.map((item, index) => {
        return {
            "id": item.idtjuald,
            "barcode" : item.kdmbrg,
            "nama": item.nmmbrg,
            "jumlah" : parseFloat(item.qtytotal),
            "harga": parseFloat(item.hrgstn),
            "diskon" : parseFloat(item.discv),
            "diskonp" : parseFloat(item.discp),
            "total": parseFloat(item.subtotal)
        }
        })
        var data = {
            "bukti": so.buktitjual,
            "tanggal": so.tgltjual,
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

async function buktitransaksi(sequelize) {
  // const today = today;

    const newdate = new Date();

    let bulan = ("0" + (newdate.getMonth() + 1)).slice(-2);
    let tahun = newdate.getFullYear().toString().slice(2);
    let buktif = "JL/" + tahun + "/" + bulan + "/";
    let sql = `SELECT REPLACE(buktitjual,'${buktif}','') as data FROM mgartjual WHERE buktitjual LIKE '%${buktif}%' ORDER BY buktitjual DESC LIMIT 1`;
    var bso = await fun.pickDataFromQuery(
        sequelize,
        sql
    )
    var num = 1;
    if (bso != null) {
        num = parseFloat(bso) + 1;
    }

    var digits = 8;
    var numlength = String(num).length;
    var sisa = digits - numlength;

    var number = "";
    for (let i = 0; i < sisa; i++) {
        number += "0";
    }
    number += num;
    let kode = buktif + number;

    return kode;
}

async function lastidtjual(sequelize) {
    let last = await fun.countDataFromQuery(
        sequelize,
        `SELECT idtjual as total FROM mgartjual ORDER BY total DESC LIMIT 1`
    );
    let last_id = last || 0;
    let id = last_id + 1;
    return id;
}

async function lastidtjuald(sequelize) {
  let last = await fun.countDataFromQuery(
    sequelize,
    `SELECT idtjuald as total FROM mgartjuald ORDER BY total DESC LIMIT 1`
  );
  let last_id = last || 0;
  let id = last_id + 1;
  return id;
}

async function getcountedit(sequelize, id) {
  let last = await fun.pickDataFromQuery(
    sequelize,
    `SELECT countedit as data FROM mgartjual where idtjual = ${id}`
  );
  let last_edit = last || 0;
  let count = parseFloat(last_edit) + 1;
  return count;
}

async function getidmbrg(sequelize, barcode) {
  var data = await fun.pickDataFromQuery(
    sequelize,
    `SELECT idmbrg as data FROM mginmbrg where (kdmbrg = '${barcode}' or barcode = '${barcode}') and Hapus = 0 ORDER BY data DESC LIMIT 1`
  )
  if (data == null) {
    return 0;
  }
  return data;
}

exports.create = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  var param = req.body;
  let bukti = await buktitransaksi(sequelize);
  let id = await lastidtjual(sequelize);
  param['ID'] = id;
  param['bukti'] = bukti;

  let counter = 0;

  var idmcabang = 0;
  let idtjual = id;
  let buktitjual = bukti;
  let tgltjual = today;
  let jenistjual = 0;
  let idmcabangmcust = 0;
  let idmcust = param.idcust || 0;

  
  let bruto = parseFloat(param.bruto || 0);
  let discp = parseFloat(param.globaldiskonp || 0);
  let discv = parseFloat(param.globaldiskonv || 0);
  let ppnp = parseFloat(param.ppnp || 0);
  let ppnv = parseFloat(param.ppnv || 0);

  let pphp = 0;
  let pphv = 0;

  let netto = parseFloat(param.netto || 0);
  let hapus = 0;
  let _void = 1;
  let countedit = 0;
  let jmlbayartunai = param.bayar || 0;
  let keterangan = "";
  let itembrg = param.itembrg;
  
  let jml_item = itembrg.length || 0;
  let statusrealisasi = 0;

  for (let i = 0; i < jml_item; i++) {
    let idtjuald = await lastidtjuald(sequelize); // fungsi
    let idmbrg = await getidmbrg(sequelize,param.itembrg[i].barcode); // fungsi
    
    let qty1 = parseFloat(param.itembrg[i].jumlah);
    let qtytotal = parseFloat(param.itembrg[i].jumlah);

    let hrgstn = parseFloat(param.itembrg[i].harga);
    let subtotal = parseFloat(param.itembrg[i].total);
    let discp = parseFloat(param.itembrg[i].discp);
    let discv = parseFloat(param.itembrg[i].discv);

    // let sql = `insert into mgartsojuald (idmcabang, idtsojuald, idtsojual, idmbrg, qty1, hrgstn, subtotal, qtytotal, discp, discv) values('${idmcabang}', '${idtjuald}', '${idtjual}', '${idmbrg}', '${qty1}', '${hrgstn}', '${subtotal}', '${qtytotal}', '${discp}', '${discv}')`;
    let sql = `insert into mgartjuald (idmcabang, idtjuald, idtjual, idmbrg, qty1, hrgstn, subtotal, qtytotal, discp, discv) 
    values('${idmcabang}', '${idtjuald}', '${idtjual}', '${idmbrg}', '${qty1}', '${hrgstn}', '${subtotal}', '${qtytotal}', '${discp}', '${discv}')`;
    const data = await fun.execDataFromQuery(sequelize, sql);
    if (data.code == 500) {
      res.json({
        message: data.message
      })
    }
    console.log('man-mgartsojuald', sql)
    
  }
  let query = `insert into mgartjual (idmcabang, idtjual, buktitjual,statusrealisasi, tgltjual, jenistjual, idmcabangmcust, idmcust, bruto, discp, discv, ppnp, ppnv, pphp, pphv, netto, hapus, void, countedit, countprint, tglcreate,tglupdate,keterangan,jmlbayartunai,idmusercreate,idmuserupdate) 
  values('${idmcabang}', '${idtjual}', '${buktitjual}','${statusrealisasi}', ${now}, '${jenistjual}', '${idmcabangmcust}', '${idmcust}', '${bruto}', '${discp}', '${discv}', '${ppnp}', '${ppnv}', '${pphp}', '${pphv}', '${netto}', '${hapus}', '${_void}', '${countedit}', '${countedit}', ${now},${now}, '${keterangan}',${jmlbayartunai},'0','0')`;
  console.log('man-mgartsojual', query)

  // let query = `insert into mgartsojual (idmcabang, idtsojual, buktitsojual,statusrealisasi, tgltsojual, jenistjual, idmcabangmcust, idmcust,idmcabangmsales,idmsales, bruto, discp, discv, ppnp, ppnv, netto, hapus, void, countedit, countprint, tglcreate,tglupdate,keterangan,idmkas,jmlbayarkredit,tgljtpiut,idtkontrak,jenistso,statusso,keteranganum,idmcabangmproject,idmproject,idmusercreate,idmuserupdate,buktitporeferensi,idmcabangmcustkirim,idmcustkirim,tgltermsojt,roundingvalue,jmlbayartunai,jenisekspedisi) values('${idmcabang}', '${idtjual}', '${buktitjual}','${statusrealisasi}', '${tgltjual}', '${jenistjual}', '${idmcabangmcust}', '${idmcust}','${idmcabangmsales}','${idmsales}', '${bruto}', '${discp}', '${discv}', '${ppnp}', '${ppnv}', '${netto}', '${hapus}', '${_void}', '${countedit}', '${countedit}', '${tglcreate}','${tglcreate}', '${keterangan}','0','0','0','0','0','0','0','0','0','0','0','0','0','-1','0','0','0','0')`;
  const data2 = await fun.execDataFromQuery(sequelize, query);
  if (data2.code == 500) {
    res.json({
      message: data2.message
    })
  }

  res.json({
    message: 'success',
    data: param
  })
}

exports.update = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  var param = req.body;
  // let bukti = await buktitransaksi(sequelize);
  let id = req.params.id;
  param['id'] = id;


  var idmcabang = 0;
  let idtjual = id;
  
  let idmcabangmcust = 0;
  
  let idmcust = param.idcust || 0;

  let bruto = parseFloat(param.bruto || 0);
  let discp = parseFloat(param.globaldiskonp || 0);
  let discv = parseFloat(param.globaldiskonv || 0);
  let ppnp = parseFloat(param.ppnp || 0);
  let ppnv = parseFloat(param.ppnv || 0);

  let pphp = 0;
  let pphv = 0;

  // let netto = param.netto.replaceAll(',', '') || 0;
  let netto = parseFloat(param.netto || 0);
  let hapus = 0;
  let _void = 1;
  let countedit = await getcountedit(sequelize, id);
  let jmlbayartunai = param.bayar || 0;
  let keterangan = "";
  let itembrg = param.itembrg;
  
  let jml_item = itembrg.length || 0;
  let statusrealisasi = 0;

  const del_data = await fun.execDataFromQuery(
    sequelize,
    `delete from mgartjuald where idtjual=${id}`
  );
  
  if (del_data.code == 500) {
    res.json({
      message: del_data.message
    })
  }
  
  for (let i = 0; i < jml_item; i++) {
    let idtjuald = await lastidtjuald(sequelize); // fungsi
    let idmbrg = await getidmbrg(sequelize,param.itembrg[i].barcode); // fungsi
    
    let qty1 = parseFloat(param.itembrg[i].jumlah);
    let qtytotal = parseFloat(param.itembrg[i].jumlah);

    let hrgstn = parseFloat(param.itembrg[i].harga);
    let subtotal = parseFloat(param.itembrg[i].total);
    let discp = parseFloat(param.itembrg[i].discp);
    let discv = parseFloat(param.itembrg[i].discv);

    let sql = `insert into mgartjuald (idmcabang, idtjuald, idtjual, idmbrg, qty1, hrgstn, subtotal, qtytotal, discp, discv) 
    values('${idmcabang}', '${idtjuald}', '${idtjual}', '${idmbrg}', '${qty1}', '${hrgstn}', '${subtotal}', '${qtytotal}', '${discp}', '${discv}')`;
    const data = await fun.execDataFromQuery(sequelize, sql);
    if (data.code == 500) {
      res.json({
        message: data.message
      })
    }
    console.log('man-mgartsojuald', sql)
    
  }
  
  let query = `update mgartjual set idmcabang = '${idmcabang}', tgltjual=${now}, jenistjual=0, idmcabangmcust = '${idmcabangmcust}', idmcust = '${idmcust}', bruto='${bruto}',discp='${discp}',discv='${discv}',ppnp='${ppnp}',ppnv='${ppnv}',netto='${netto}', jmlbayartunai=${jmlbayartunai},countedit=${countedit},tglupdate=${now},keterangan='${keterangan}' where idtjual = ${idtjual}`;
  console.log('man-mgartsojual', query)

  // let query = `insert into mgartsojual (idmcabang, idtsojual, buktitsojual,statusrealisasi, tgltsojual, jenistjual, idmcabangmcust, idmcust,idmcabangmsales,idmsales, bruto, discp, discv, ppnp, ppnv, netto, hapus, void, countedit, countprint, tglcreate,tglupdate,keterangan,idmkas,jmlbayarkredit,tgljtpiut,idtkontrak,jenistso,statusso,keteranganum,idmcabangmproject,idmproject,idmusercreate,idmuserupdate,buktitporeferensi,idmcabangmcustkirim,idmcustkirim,tgltermsojt,roundingvalue,jmlbayartunai,jenisekspedisi) values('${idmcabang}', '${idtjual}', '${buktitjual}','${statusrealisasi}', '${tgltjual}', '${jenistjual}', '${idmcabangmcust}', '${idmcust}','${idmcabangmsales}','${idmsales}', '${bruto}', '${discp}', '${discv}', '${ppnp}', '${ppnv}', '${netto}', '${hapus}', '${_void}', '${countedit}', '${countedit}', '${tglcreate}','${tglcreate}', '${keterangan}','0','0','0','0','0','0','0','0','0','0','0','0','0','-1','0','0','0','0')`;
  const data2 = await fun.execDataFromQuery(sequelize, query);
  if (data2.code == 500) {
    res.json({
      message: data2.message
    })
  }

  res.json({
    message: 'success',
    data: param
  })
}
