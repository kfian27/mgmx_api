// const db = require("../models");
// const sequelize = db.sequelize;
const qstock = require("../class/query_report/stock");

const fun = require("../mgmx");

let today = new Date().toJSON().slice(0, 10);

exports.tesRahman = async (req, res) => {
  let q = await qstock.queryPosisiStock();
  res.json({
    message: "Success",
    data: q,
  });
};

exports.getListCabang = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);
  let sql = `select IdMCabang as ID, NmMCabang as nama from mgsymcabang where aktif=1 and hapus=0`;
  const data = await fun.getDataFromQuery(sequelize, sql);

  res.json({
    message: "Success",
    data: data,
  });
};

exports.getListCustomer = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  let sql = `select IdMCust as ID, NmMCust as nama, Alamat as alamat from mgarmcust where aktif=1 and hapus=0`;
  const data = await fun.getDataFromQuery(sequelize, sql);

  res.json({
    message: "Success",
    data: data,
  });
};

exports.getListSupplier = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  let sql = `select idmsup as ID, nmmsup as nama from mgapmsup where aktif=1 and hapus=0`;
  const data = await fun.getDataFromQuery(sequelize, sql);

  res.json({
    message: "Success",
    data: data,
  });
};

exports.getListGudang = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  let sql = `select idmgd as ID, nmmgd as nama from mgsymgd where aktif=1 and hapus=0`;
  const data = await fun.getDataFromQuery(sequelize, sql);

  res.json({
    message: "Success",
    data: data,
  });
};
exports.getListBarang = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  let sql = `SELECT b.idmbrg as ID, REPLACE(b.nmmbrg,'"','') as nama FROM mginlkartustock k LEFT OUTER JOIN mginmbrg b ON k.idmbrg = b.idmbrg GROUP BY b.idmbrg`;
  const data = await fun.getDataFromQuery(sequelize, sql);

  res.json({
    message: "Success",
    data: data,
  });
};

exports.getListBank = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  let sql = `select idmbank as ID, nmmbank as nama from mgkbmbank where aktif=1 and hapus=0`;
  const data = await fun.getDataFromQuery(sequelize, sql);

  res.json({
    message: "Success",
    data: data,
  });
};

exports.getListKas = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  let sql = `select idmkas as ID, nmmkas as nama from mgkbmkas where aktif=1 and hapus=0`;
  const data = await fun.getDataFromQuery(sequelize, sql);

  res.json({
    message: "Success",
    data: data,
  });
};

exports.getListSales = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  let sql = `select idmsales as ID, nmmsales as nama from mgarmsales where aktif=1 and hapus=0`;
  const data = await fun.getDataFromQuery(sequelize, sql);

  res.json({
    message: "Success",
    data: data,
  });
};

exports.penjualan = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  let start = req.body.start || "2008-01-17";
  let end = req.body.end || "2024-02-17";
  let jenis = req.body.jenis || 1;

  let cabang = req.body.cabang;
  let qcabang = "";
  if (cabang && cabang != "") {
    qcabang = "and j.idmcabang=" + cabang;
  }

  let customer = req.body.customer;
  let qcustomer = "";
  if (customer && customer != "") {
    qcustomer = "and j.idmcust=" + customer;
  }

  let barang = req.body.barang;
  let qbarang = "";
  if (barang && barang != "") {
    qbarang = "and jd.idmbrg=" + barang;
  }

  let group = req.body.group;

  const count_penjualan = await fun.countDataFromQuery(
    sequelize,
    `SELECT COUNT(j.idtjual) as total FROM mgartjual j LEFT OUTER JOIN mgartjuald jd ON jd.idtjual = j.idtjual WHERE j.hapus=0 ${qcabang} ${qcustomer} ${qbarang} and j.tgltjual between '${start}%' and '${end}%'`
  );
  const count_produk = await fun.countDataFromQuery(
    sequelize,
    `SELECT SUM(jd.qtytotal) as total FROM mgartjuald jd LEFT OUTER JOIN mgartjual j ON jd.IdTJual = j.IdTJual WHERE j.tgltjual between '${start}%' and '${end}%' ${qcabang} ${qbarang} ${qcustomer}`
  );
  const count_pendapatan = await fun.countDataFromQuery(
    sequelize,
    `SELECT SUM(j.netto) as total FROM mgartjual j WHERE j.tgltjual between '${start}%' and '${end}%' and j.hapus=0 ${qcabang} ${qcustomer} ${qbarang}`
  );
  const count_hpp = await fun.countDataFromQuery(
    sequelize,
    `SELECT COALESCE((SELECT (jd.QtyTotal * b.reserved_dec1) AS hpp FROM mgartjuald jd LEFT OUTER JOIN mgartjual j ON jd.IdTJual=j.IdTJual LEFT OUTER JOIN mginmbrg b ON jd.IdMBrg = b.idmbrg WHERE j.tgltjual between '${start}%' and '${end}%' ${qbarang} ${qcustomer} ${qbarang} order by jd.IdTJualD desc limit 1),0) as total`
  );

  var count = {
    penjualan: count_penjualan,
    produk_terjual: parseFloat(count_produk),
    pendapatan: parseFloat(count_pendapatan),
    profit: parseFloat(count_pendapatan) - parseFloat(count_hpp),
  };

  if (jenis == 1) {
    async function barang_list(list) {
      let sql2 = `SELECT jd.idtjuald, jd.idmbrg, jd.qtytotal, jd.hrgstn, jd.discv, jd.subtotal, b.kdmbrg, b.nmmbrg FROM mgartjuald jd LEFT OUTER JOIN mginmbrg b ON jd.idmbrg = b.idmbrg WHERE jd.idtjual = ${list.idtjual}`;
      const brg = await sequelize.query(sql2, {
        raw: false,
      });

      var arr_brg = brg[0].map((brg, index_dua) => {
        return {
          id: brg.idtjuald,
          barcode: brg.kdmbrg,
          nama: brg.nmmbrg,
          jumlah: brg.qtytotal,
          harga: parseFloat(brg.hrgstn), // format
          diskon: parseFloat(brg.discv), // format
          total: parseFloat(brg.subtotal), // format
        };
      });

      return {
        id: list.idtjual,
        tanggal: list.tgltjual, // date_format(date_create(list.tgltjual), "m - d - Y"),
        transaksi: list.buktitjual,
        customer: list.nmmcust,
        subtotal: parseFloat(list.bruto), // "Rp & nbsp; ".number_format(list.bruto, 2),
        diskon: parseFloat(list.discv), // "Rp & nbsp; ".number_format(list.discv, 2),
        pajak: parseFloat(list.ppnv), // "Rp & nbsp; ".number_format(list.ppnv, 2),
        grandtotal: parseFloat(list.netto), // "Rp & nbsp; ".number_format(list.netto, 2),
        bayar: parseFloat(list.bayar), // "Rp & nbsp; ".number_format(list.bayar, 2),
        sisa: parseFloat(list.sisa), // number_format(list.sisa),
        sisabayar: parseFloat(list.sisa), // "Rp ".number_format(abs(list.sisa), 2),
        listitem: arr_brg,
      };
    }

    if (group == "cabang") {
      let sql = `SELECT fin.idmcust, fin.nmmcust, fin.idmcabang, fin.nmmcabang, SUM(fin.tagihan) AS 'tagihan', SUM(fin.bayar) AS 'bayar', SUM(fin.sisa) AS 'sisa' FROM (SELECT ca.idmcabang, ca.nmmcabang, s.nmmsales, j.tgltjual AS 'Tanggal', j.buktitjual AS 'Nota', c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qcabang} ${qbarang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC) fin group by fin.idmcabang`;
      const filter = await sequelize.query(sql, {
        raw: false,
      });

      var arr_data = await Promise.all(
        filter[0].map(async (fil, index) => {
          let sql1 = `SELECT ca.nmmcabang,j. idtjual, j.tgltjual, j.buktitjual, c.nmmcust, s.nmmsales, j.bruto, j.discv, j.ppnv, j.netto, SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qbarang} ${qcabang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC`;
          const list = await sequelize.query(sql1, {
            raw: false,
          });

          var arr_list = await Promise.all(
            list[0].map(async (list, index_satu) => {
              return barang_list(list);
            })
          );

          return {
            nama: "Cabang : " + fil.nmmcabang,
            netto: parseFloat(fil.tagihan), // "Rp&nbsp;"+ number_format(fil.tagihan, 2),
            sisa: parseFloat(Math.abs(fil.sisa)), // "Rp&nbsp;"+ number_format(abs(fil.sisa), 2),
            bayar: parseFloat(fil.bayar), // "Rp&nbsp;"+ number_format(fil.bayar, 2),
            list: arr_list,
          };
        })
      );
    } else if (group == "customer") {
      let sql = `SELECT fin.idmcust, fin.nmmcust, fin.idmcabang, fin.nmmcabang, SUM(fin.tagihan) AS 'tagihan', SUM(fin.bayar) AS 'bayar', SUM(fin.sisa) AS 'sisa' FROM (SELECT ca.idmcabang, ca.nmmcabang, s.nmmsales, j.tgltjual AS 'Tanggal', j.buktitjual AS 'Nota', c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qcabang} ${qbarang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC) fin GROUP BY fin.idmcust`;
      const filter = await sequelize.query(sql, {
        raw: false,
      });

      var arr_data = await Promise.all(
        filter[0].map(async (fil, index) => {
          let idmcust = fil.idmcust;
          let sql1 = `SELECT ca.nmmcabang,j. idtjual, j.tgltjual, j.buktitjual, c.idmcust, c.nmmcust, s.nmmsales, j.bruto, j.discv, j.ppnv, j.netto, SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qbarang} ${qcabang} ${qcustomer} and c.idmcust=${idmcust} GROUP BY j.idtjual ORDER BY j.tgltjual ASC`;
          const list = await sequelize.query(sql1, {
            raw: false,
          });

          var arr_list = await Promise.all(
            list[0].map(async (list, index_satu) => {
              var list_item = await barang_list(list);
              list_item.sales = list.nmmsales;
              return list_item;
            })
          );

          return {
            nama: "Customer : " + fil.nmmcust,
            netto: parseFloat(fil.tagihan), // "Rp&nbsp;"+ number_format(fil.tagihan, 2),
            sisa: parseFloat(Math.abs(fil.sisa)), // "Rp&nbsp;"+ number_format(abs(fil.sisa), 2),
            bayar: parseFloat(fil.bayar), // "Rp&nbsp;"+ number_format(fil.bayar, 2),
            list: arr_list,
          };
        })
      );
    } else if (group == "sales") {
      let sql = `SELECT fin.idmsales, fin.nmmsales, fin.idmcust, fin.nmmcust, fin.idmcabang, fin.nmmcabang, SUM(fin.tagihan) AS 'tagihan', SUM(fin.bayar) AS 'bayar', SUM(fin.sisa) AS 'sisa' FROM (SELECT ca.idmcabang, ca.nmmcabang, s.idmsales, s.nmmsales, j.tgltjual AS 'Tanggal', j.buktitjual AS 'Nota', c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE s.aktif=1 and s.hapus=0 and j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qcabang} ${qbarang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC) fin GROUP BY fin.idmsales`;
      const filter = await sequelize.query(sql, {
        raw: false,
      });

      var arr_data = await Promise.all(
        filter[0].map(async (fil, index) => {
          let sql1 = `SELECT ca.idmcabang, ca.nmmcabang, s.idmsales, s.nmmsales, j.idtjual, j.tgltjual AS 'Tanggal', j.buktitjual AS 'Nota', c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE s.aktif=1 and s.hapus=0 and j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qcabang} ${qbarang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC`;
          const list = await sequelize.query(sql1, {
            raw: false,
          });

          var arr_list = await Promise.all(
            list[0].map(async (list, index_satu) => {
              var list_item = await barang_list(list);
              list_item.sales = list.nmmsales;
              return list_item;
            })
          );

          return {
            nama: "Sales : " + fil.nmmsales,
            netto: parseFloat(fil.tagihan), // "Rp&nbsp;"+ number_format(fil.tagihan, 2),
            sisa: parseFloat(Math.abs(fil.sisa)), // "Rp&nbsp;"+ number_format(abs(fil.sisa), 2),
            bayar: parseFloat(fil.bayar), // "Rp&nbsp;"+ number_format(fil.bayar, 2),
            list: arr_list,
          };
        })
      );
    } else if (group == "barang") {
      let sql = `SELECT fin.idmbrg, fin.nmmbrg, fin.idmcust, fin.nmmcust, fin.idmcabang, fin.nmmcabang, SUM(fin.tagihan) AS 'tagihan', SUM(fin.bayar) AS 'bayar', SUM(fin.sisa) AS 'sisa' FROM (SELECT b.idmbrg, b.nmmbrg, ca.idmcabang, ca.nmmcabang, s.nmmsales, j.tgltjual AS 'Tanggal', j.buktitjual AS 'Nota', c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'bayar',IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang LEFT OUTER JOIN mgartjuald jd ON j.idtjual = jd.IdTJual LEFT OUTER JOIN mginmbrg b ON jd.idmbrg = b.idmbrg WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qcabang} ${qbarang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC) fin GROUP BY fin.idmbrg`;
      const filter = await sequelize.query(sql, {
        raw: false,
      });

      var arr_data = await Promise.all(
        filter[0].map(async (fil, index) => {
          let sql1 = `SELECT j.idtjual, j.tgltjual, j.buktitjual, j.bruto, j.discv, j.ppnv, j.netto, b.idmbrg, b.nmmbrg, ca.idmcabang, ca.nmmcabang, s.nmmsales, j.tgltjual AS 'Tanggal', j.buktitjual AS 'Nota', c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'bayar',IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang LEFT OUTER JOIN mgartjuald jd ON j.idtjual = jd.IdTJual LEFT OUTER JOIN mginmbrg b ON jd.idmbrg = b.idmbrg WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qcabang} ${qbarang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC`;
          const list = await sequelize.query(sql1, {
            raw: false,
          });

          var arr_list = await Promise.all(
            list[0].map(async (list, index_satu) => {
              var list_item = await barang_list(list);
              list_item.sales = list.nmmsales;
              return list_item;
            })
          );

          return {
            nama: "Barang : " + fil.nmmbrg,
            netto: parseFloat(fil.tagihan), // "Rp&nbsp;"+ number_format(fil.tagihan, 2),
            sisa: parseFloat(Math.abs(fil.sisa)), // "Rp&nbsp;"+ number_format(abs(fil.sisa), 2),
            bayar: parseFloat(fil.bayar), // "Rp&nbsp;"+ number_format(fil.bayar, 2),
            list: arr_list,
          };
        })
      );
    } else {
      var count = {};
      var arr_data = [];
    }
  } else if (jenis == 2) {
    async function barang_list(list) {
      let sql2 = `SELECT b.kdmbrg, b.nmmbrg, g.nmmgd, jd.qtytotal, s.nmmstn, jd.hrgstn, jd.discv, jd.ppnv, (jd.qtytotal * jd.hrgstn) AS dpp, (jd.qtytotal * jd.hrgstn - jd.discv + jd.ppnv) AS subtotal FROM mgartjuald jd LEFT OUTER JOIN mginmbrg b ON b.idmbrg = jd.idmbrg LEFT OUTER JOIN mginmstn s ON b.IdMStn1 = s.idmstn LEFT OUTER JOIN mgsymgd g ON g.idmgd = jd.idmgd WHERE idtjual =  ${list.idtjual}`;
      const brg = await sequelize.query(sql2, {
        raw: false,
      });

      var arr_brg = brg[0].map((brg, index_dua) => {
        return {
          kode: brg.kdmbrg,
          nama: brg.nmmbrg,
          gudang: brg.nmmgd,
          qty: brg.qtytotal,
          satuan: brg.nmmstn,
          hargasat: parseFloat(brg.hrgstn),
          diskon: parseFloat(brg.discv),
          pajak: parseFloat(brg.ppnv),
          dpp: parseFloat(brg.dpp),
          subtotal: parseFloat(brg.subtotal),
        };
      });

      return {
        id: list.idtjual,
        tanggal: list.tgltjual,
        transaksi: list.buktitjual,
        customer: list.nmmcust,
        subtotal: parseFloat(list.bruto),
        diskon: parseFloat(list.discv),
        pajak: parseFloat(list.ppnv),
        grandtotal: parseFloat(list.netto),
        sisa: parseFloat(list.sisa),
        item: arr_brg,
      };
    }

    if (group == "cabang") {
      let sql = `select nmmcabang from mgsymcabang where aktif = 1 and hapus = 0`;
      const filter = await sequelize.query(sql, {
        raw: false,
      });

      var arr_data = await Promise.all(
        filter[0].map(async (fil, index) => {
          let sql1 = `SELECT j.idtjual, j.tgltjual, j.buktitjual, c.nmmcust, j.bruto, j.discv, j.ppnv, j.netto, SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qbarang} ${qcabang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC`;
          const list = await sequelize.query(sql1, {
            raw: false,
          });

          var arr_list = await Promise.all(
            list[0].map(async (list, index_satu) => {
              return barang_list(list);
            })
          );

          return {
            nama: fil.nmmcabang,
            list: arr_list,
          };
        })
      );
    } else if (group == "customer") {
      let sql = `SELECT fin.idmcust, fin.nmmcust, fin.idmcabang, fin.nmmcabang, SUM(fin.tagihan) AS 'Tagihan', SUM(fin.bayar) AS 'Bayar', SUM(fin.sisa) AS 'Sisa' FROM (SELECT ca.idmcabang, ca.nmmcabang, s.nmmsales, j.tgltjual AS 'Tanggal', j.buktitjual AS 'Nota', c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'Tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'Bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'Sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qcabang} ${qbarang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC) fin GROUP BY fin.idmcust`;
      const filter = await sequelize.query(sql, {
        raw: false,
      });

      var arr_data = await Promise.all(
        filter[0].map(async (fil, index) => {
          let idmcust = fil.idmcust;
          let sql1 = `SELECT ca.nmmcabang,j. idtjual, j.tgltjual, j.buktitjual, c.idmcust, c.nmmcust, s.nmmsales, j.bruto, j.discv, j.ppnv, j.netto, SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'Bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'Sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qbarang} ${qcabang} ${qcustomer} and c.idmcust=${idmcust} GROUP BY j.idtjual ORDER BY j.tgltjual ASC`;
          const list = await sequelize.query(sql1, {
            raw: false,
          });

          var arr_list = await Promise.all(
            list[0].map(async (list, index_satu) => {
              return barang_list(list);
            })
          );

          return {
            nama: fil.nmmcust,
            list: arr_list,
          };
        })
      );
    } else if (group == "sales") {
      let sql = `SELECT fin.idmsales, fin.nmmsales, fin.idmcust, fin.nmmcust, fin.idmcabang, fin.nmmcabang, SUM(fin.tagihan) AS 'Tagihan', SUM(fin.bayar) AS 'Bayar', SUM(fin.sisa) AS 'Sisa' FROM (SELECT ca.idmcabang, ca.nmmcabang, s.idmsales, s.nmmsales, j.tgltjual AS 'Tanggal', j.buktitjual AS 'Nota', c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'Tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'Bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'Sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE s.aktif=1 and s.hapus=0 and j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qcabang} ${qbarang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC) fin GROUP BY fin.idmsales`;
      const filter = await sequelize.query(sql, {
        raw: false,
      });

      var arr_data = await Promise.all(
        filter[0].map(async (fil, index) => {
          let sql1 = `SELECT j.idtjual, j.bruto, j.discv, j.ppnv, j.netto, ca.idmcabang, ca.nmmcabang, s.idmsales, s.nmmsales, j.tgltjual, j.buktitjual, c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'Tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'Bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'Sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE s.aktif=1 and s.hapus=0 and j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qcabang} ${qbarang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC`;
          const list = await sequelize.query(sql1, {
            raw: false,
          });

          var arr_list = await Promise.all(
            list[0].map(async (list, index_satu) => {
              return barang_list(list);
            })
          );

          return {
            nama: fil.nmmsales,
            list: arr_list,
          };
        })
      );
    } else if (group == "barang") {
      let sql = `SELECT fin.idmbrg, fin.nmmbrg, fin.idmcust, fin.nmmcust, fin.idmcabang, fin.nmmcabang, SUM(fin.tagihan) AS 'Tagihan', SUM(fin.bayar) AS 'bayar', SUM(fin.sisa) AS 'Sisa' FROM (SELECT b.idmbrg, b.nmmbrg, ca.idmcabang, ca.nmmcabang, s.nmmsales, j.tgltjual AS 'Tanggal', j.buktitjual AS 'nota', c.idmcust, c.nmmcust, j.bruto AS 'subtotal', j.discv AS 'diskon', j.ppnv AS 'pajak', j.netto AS 'tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'Bayar',IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang LEFT OUTER JOIN mgartjuald jd ON j.idtjual = jd.IdTJual LEFT OUTER JOIN mginmbrg b ON jd.idmbrg = b.idmbrg WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qcabang} ${qbarang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC) fin GROUP BY fin.idmbrg`;
      const filter = await sequelize.query(sql, {
        raw: false,
      });

      var arr_data = await Promise.all(
        filter[0].map(async (fil, index) => {
          let idmbrg = fil.idmbrg;
          let sql1 = `SELECT j.idtjual, j.tgltjual, j.buktitjual, j.bruto, j.discv, j.ppnv, j.netto, b.idmbrg, b.nmmbrg, ca.idmcabang, ca.nmmcabang, s.nmmsales, j.tgltjual AS 'tanggal', j.buktitjual AS 'nota', c.idmcust, c.nmmcust, j.bruto AS 'subtotal', j.discv AS 'diskon', j.ppnv AS 'pajak', j.netto AS 'tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'bayar',IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang LEFT OUTER JOIN mgartjuald jd ON j.idtjual = jd.IdTJual LEFT OUTER JOIN mginmbrg b ON jd.idmbrg = b.idmbrg WHERE j.hapus=0 AND b.idmbrg = ${idmbrg} and j.tgltjual BETWEEN '${start}' AND '${end}' ${qcabang} ${qbarang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC`;
          const list = await sequelize.query(sql1, {
            raw: false,
          });

          var arr_list = await Promise.all(
            list[0].map(async (list, index_satu) => {
              let list_item = await barang_list(list);
              list_item.bayar = parseFloat(list.bayar);
              return list_item;
            })
          );

          return {
            nama: fil.nmmbrg,
            list: arr_list,
          };
        })
      );
    } else {
      var count = {};
      var arr_data = [];
    }
  }
  res.json({
    message: "Success",
    countData: count,
    data: arr_data,
  });
};

exports.pembelian = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  let start = req.body.start || "2008-01-17";
  let end = req.body.end || "2024-02-17";
  let jenis = req.body.jenis || 1;

  let cabang = req.body.cabang || "";
  let qcabang = "";
  let qcabang_count = "";
  if (cabang != "") {
    qcabang = "and b.idmcabang=" + cabang;
    qcabang_count = "and j.idmcabang=" + cabang;
  }

  let supplier = req.body.supplier || "";
  let qsupplier = "";
  let qsupplier_count = "";
  if (supplier != "") {
    qsupplier = "and b.idmsup=" + supplier;
    qsupplier_count = "and j.idmsup=" + supplier;
  }

  let barang = req.body.barang || "";
  let qbarang = "";
  let qbarang_count = "";
  if (barang != "") {
    qbarang = "and bb.idmbrg=" + barang;
    qbarang_count = "and jd.idmbrg=" + barang;
  }

  let group = req.body.group;

  const count_pembelian = await fun.countDataFromQuery(
    sequelize,
    `SELECT COUNT(j.idtbeli) as total FROM mgaptbeli j LEFT OUTER JOIN mgaptbelid jd ON jd.idtbeli = j.idtbeli WHERE j.hapus=0 ${qcabang_count} ${qsupplier_count} ${qbarang_count} and j.tgltbeli between '${start}%' and '${end}%'`
  );
  const count_produk = await fun.countDataFromQuery(
    sequelize,
    `SELECT count(jd.idmbrg) as total FROM mgaptbelid jd LEFT OUTER JOIN mgaptbeli j ON jd.Idtbeli = j.Idtbeli WHERE j.tgltbeli between '${start}%' and '${end}%' ${qcabang_count} ${qbarang_count} ${qsupplier_count}`
  );
  const count_pengeluaran = await fun.countDataFromQuery(
    sequelize,
    `SELECT COALESCE((SELECT SUM(j.netto) as total FROM mgaptbeli j left outer join mgaptbelid jd on jd.idtbeli = j.idtbeli WHERE tgltbeli between '${start}%' and '${end}%' ${qcabang_count} ${qbarang_count} ${qsupplier_count} order by j.idtbeli desc limit 1),0) as total`
  );

  var count = {
    pembelian: count_pembelian,
    produk_dibeli: parseFloat(count_produk),
    pengeluaran: parseFloat(count_pengeluaran),
  };

  var jenis_str = "";
  if (jenis == 1) {
    jenis_str = "summary";
    async function barang_list(list) {
      let sql2 = `SELECT jd.idtbelid, jd.idmbrg, jd.qtytotal, jd.hrgstn, jd.discv, jd.subtotal, b.kdmbrg, b.nmmbrg FROM mgaptbelid jd LEFT OUTER JOIN mginmbrg b ON jd.idmbrg = b.idmbrg WHERE jd.idtbeli = ${list.idtbeli}`;
      const brg = await sequelize.query(sql2, {
        raw: false,
      });

      var arr_brg = brg[0].map((brg, index_dua) => {
        return {
          id: brg.idtbelid,
          barcode: brg.kdmbrg,
          nama: brg.nmmbrg,
          jumlah: parseFloat(brg.qtytotal),
          harga: parseFloat(brg.hrgstn), // format
          diskon: parseFloat(brg.discv), // format
          total: parseFloat(brg.subtotal), // format
        };
      });

      return {
        id: list.idtbeli,
        tanggal: list.tgltbeli, // date_format(date_create(list.tgltjual), "m - d - Y"),
        transaksi: list.buktitbeli,
        supplier: list.nmmsup,
        subtotal: parseFloat(list.bruto), // "Rp & nbsp; ".number_format(list.bruto, 2),
        diskon: parseFloat(list.discv), // "Rp & nbsp; ".number_format(list.discv, 2),
        pajak: parseFloat(list.ppnv), // "Rp & nbsp; ".number_format(list.ppnv, 2),
        grandtotal: parseFloat(list.netto), // "Rp & nbsp; ".number_format(list.netto, 2),
        bayar: parseFloat(list.bayar), // "Rp & nbsp; ".number_format(list.bayar, 2),
        sisa: parseFloat(list.sisa), // number_format(list.sisa),
        listitem: arr_brg,
      };
    }

    if (group == "cabang") {
      let sql = `SELECT fin.nmmsup, fin.idmcabang, fin.nmmcabang, SUM(fin.bruto) AS 'subtotal', SUM(fin.ppnv) AS 'pajak', SUM(fin.discv) AS 'diskon',SUM(fin.netto) AS 'tagihan', SUM(fin.bayar) AS 'bayar', SUM(fin.sisa) AS 'sisa' FROM (SELECT b.idmcabang, c.NmMCabang, b.tgltbeli, b.buktitbeli, bb.idmbrg, bb.nmmbrg, b.idmsup, s.nmmsup, b.bruto, b.netto, b.discv, b.ppnv, b.JmlBayarTunai + IF(SUM(bd.jmlbayar)>0,SUM(bd.jmlbayar),0) AS 'bayar', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)<=0,0,b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)) AS 'sisa', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)>0,'Belum Lunas','Lunas') AS 'status' FROM mgaptbeli b LEFT OUTER JOIN mgaptbelid db ON b.idtbeli = db.idtbeli LEFT OUTER JOIN mginmbrg bb ON db.idmbrg = bb.idmbrg LEFT OUTER JOIN mgsymcabang c ON b.IdMCabang = c.idmcabang LEFT OUTER JOIN mgapmsup s ON b.IdMSup = s.idmsup LEFT OUTER JOIN mgaptbhutd bd ON b.IdTBeli = bd.idtrans WHERE b.hapus=0 AND b.tgltbeli BETWEEN '${start}' AND '${end}' ${qcabang} ${qsupplier} ${qbarang} GROUP BY b.idtbeli) fin GROUP BY fin.idmcabang`;
      const filter = await sequelize.query(sql, {
        raw: false,
      });

      var arr_data = await Promise.all(
        filter[0].map(async (fil, index) => {
          let idmcabang = fil.idmcabang;
          let sql1 = `SELECT c.NmMCabang, bb.nmmbrg, b.tgltbeli, b.idtbeli, b.buktitbeli, s.nmmsup, b.bruto, b.netto, b.discv, b.ppnv, b.JmlBayarTunai + IF(SUM(bd.jmlbayar)>0,SUM(bd.jmlbayar),0) AS 'bayar', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)<=0,0,b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)) AS 'sisa', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)>0,'Belum Lunas','Lunas') AS 'status' FROM mgaptbeli b LEFT OUTER JOIN mgaptbelid db ON b.idtbeli = db.idtbeli LEFT OUTER JOIN mginmbrg bb ON db.idmbrg = bb.idmbrg LEFT OUTER JOIN mgsymcabang c ON b.IdMCabang = c.idmcabang LEFT OUTER JOIN mgapmsup s ON b.IdMSup = s.idmsup LEFT OUTER JOIN mgaptbhutd bd ON b.IdTBeli = bd.idtrans WHERE b.hapus=0 AND b.tgltbeli BETWEEN '${start}' AND '${end}' and b.idmcabang = ${idmcabang} ${qcabang} ${qsupplier} ${qbarang} GROUP BY b.idtbeli`;
          const list = await sequelize.query(sql1, {
            raw: false,
          });

          var arr_list = await Promise.all(
            list[0].map(async (list, index_satu) => {
              return barang_list(list);
            })
          );

          return {
            nama: fil.nmmcabang,
            bruto: parseFloat(fil.subtotal),
            diskon: parseFloat(fil.diskon),
            pajak: parseFloat(fil.pajak),
            netto: parseFloat(fil.tagihan),
            tunai: parseFloat(fil.bayar), // "Rp&nbsp;"+ number_format(fil.bayar, 2),
            sisa: parseFloat(Math.abs(fil.sisa)), // "Rp&nbsp;"+ number_format(abs(fil.sisa), 2),
            list: arr_list,
          };
        })
      );
    } else if (group == "supplier") {
      let sql = `SELECT fin.idmsup, fin.nmmsup, fin.idmcabang, fin.nmmcabang, SUM(fin.bruto) AS 'subtotal', SUM(fin.ppnv) AS 'pajak', SUM(fin.discv) AS 'diskon',SUM(fin.netto) AS 'tagihan', SUM(fin.bayar) AS 'bayar', SUM(fin.sisa) AS 'sisa' FROM (SELECT b.idmcabang, c.NmMCabang, b.tgltbeli, b.buktitbeli, bb.idmbrg, bb.nmmbrg, b.idmsup, s.nmmsup, b.bruto, b.netto, b.discv, b.ppnv, b.JmlBayarTunai + IF(SUM(bd.jmlbayar)>0,SUM(bd.jmlbayar),0) AS 'bayar', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)<=0,0,b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)) AS 'sisa', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)>0,'Belum Lunas','Lunas') AS 'status' FROM mgaptbeli b LEFT OUTER JOIN mgaptbelid db ON b.idtbeli = db.idtbeli LEFT OUTER JOIN mginmbrg bb ON db.idmbrg = bb.idmbrg LEFT OUTER JOIN mgsymcabang c ON b.IdMCabang = c.idmcabang LEFT OUTER JOIN mgapmsup s ON b.IdMSup = s.idmsup LEFT OUTER JOIN mgaptbhutd bd ON b.IdTBeli = bd.idtrans WHERE b.hapus=0 AND b.tgltbeli BETWEEN '${start}' AND '${end}' ${qcabang} ${qsupplier} ${qbarang} GROUP BY b.idtbeli) fin GROUP BY fin.idmsup`;
      const filter = await sequelize.query(sql, {
        raw: false,
      });

      var arr_data = await Promise.all(
        filter[0].map(async (fil, index) => {
          let idmsup = fil.idmsup;
          let sql1 = `SELECT c.NmMCabang, bb.nmmbrg, b.tgltbeli, b.idtbeli, b.buktitbeli, s.nmmsup, b.bruto, b.netto, b.discv, b.ppnv, b.JmlBayarTunai + IF(SUM(bd.jmlbayar)>0,SUM(bd.jmlbayar),0) AS 'bayar', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)<=0,0,b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)) AS 'sisa', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)>0,'Belum Lunas','Lunas') AS 'status' FROM mgaptbeli b LEFT OUTER JOIN mgaptbelid db ON b.idtbeli = db.idtbeli LEFT OUTER JOIN mginmbrg bb ON db.idmbrg = bb.idmbrg LEFT OUTER JOIN mgsymcabang c ON b.IdMCabang = c.idmcabang LEFT OUTER JOIN mgapmsup s ON b.IdMSup = s.idmsup LEFT OUTER JOIN mgaptbhutd bd ON b.IdTBeli = bd.idtrans WHERE b.hapus=0 AND b.tgltbeli BETWEEN '${start}' AND '${end}' and b.idmsup = ${idmsup} ${qcabang} ${qsupplier} ${qbarang} GROUP BY b.idtbeli`;
          const list = await sequelize.query(sql1, {
            raw: false,
          });

          var arr_list = await Promise.all(
            list[0].map(async (list, index_satu) => {
              return barang_list(list);
            })
          );

          return {
            nama: fil.nmmsup,
            bruto: parseFloat(fil.subtotal),
            diskon: parseFloat(fil.diskon),
            pajak: parseFloat(fil.pajak),
            netto: parseFloat(fil.tagihan),
            tunai: parseFloat(fil.bayar), // "Rp&nbsp;"+ number_format(fil.bayar, 2),
            sisa: parseFloat(Math.abs(fil.sisa)), // "Rp&nbsp;"+ number_format(abs(fil.sisa), 2),
            list: arr_list,
          };
        })
      );
    } else if (group == "barang") {
      let sql = `SELECT fin.idmsup, fin.nmmsup, fin.idmbrg, fin.nmmbrg, fin.idmcabang, fin.nmmcabang, SUM(fin.bruto) AS 'subtotal', SUM(fin.ppnv) AS 'pajak', SUM(fin.discv) AS 'diskon',SUM(fin.netto) AS 'tagihan', SUM(fin.bayar) AS 'bayar', SUM(fin.sisa) AS 'sisa' FROM (SELECT b.idmcabang, c.NmMCabang, b.tgltbeli, b.buktitbeli, bb.idmbrg, bb.nmmbrg, b.idmsup, s.nmmsup, b.bruto, b.netto, b.discv, b.ppnv, b.JmlBayarTunai + IF(SUM(bd.jmlbayar)>0,SUM(bd.jmlbayar),0) AS 'bayar', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)<=0,0,b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)) AS 'sisa', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)>0,'Belum Lunas','Lunas') AS 'status' FROM mgaptbeli b LEFT OUTER JOIN mgaptbelid db ON b.idtbeli = db.idtbeli LEFT OUTER JOIN mginmbrg bb ON db.idmbrg = bb.idmbrg LEFT OUTER JOIN mgsymcabang c ON b.IdMCabang = c.idmcabang LEFT OUTER JOIN mgapmsup s ON b.IdMSup = s.idmsup LEFT OUTER JOIN mgaptbhutd bd ON b.IdTBeli = bd.idtrans WHERE b.hapus=0 AND b.tgltbeli BETWEEN '${start}' AND '${end}' ${qcabang} ${qsupplier} ${qbarang} GROUP BY b.idtbeli) fin GROUP BY fin.idmbrg`;
      const filter = await sequelize.query(sql, {
        raw: false,
      });

      var arr_data = await Promise.all(
        filter[0].map(async (fil, index) => {
          let idmbrg = fil.idmbrg;
          let sql1 = `SELECT c.NmMCabang, bb.nmmbrg, b.tgltbeli, b.idtbeli, b.buktitbeli, s.nmmsup, b.bruto, b.netto, b.discv, b.ppnv, b.JmlBayarTunai + IF(SUM(bd.jmlbayar)>0,SUM(bd.jmlbayar),0) AS 'bayar', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)<=0,0,b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)) AS 'sisa', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)>0,'Belum Lunas','Lunas') AS 'status' FROM mgaptbeli b LEFT OUTER JOIN mgaptbelid db ON b.idtbeli = db.idtbeli LEFT OUTER JOIN mginmbrg bb ON db.idmbrg = bb.idmbrg LEFT OUTER JOIN mgsymcabang c ON b.IdMCabang = c.idmcabang LEFT OUTER JOIN mgapmsup s ON b.IdMSup = s.idmsup LEFT OUTER JOIN mgaptbhutd bd ON b.IdTBeli = bd.idtrans WHERE b.hapus=0 AND b.tgltbeli BETWEEN '${start}' AND '${end}' and bb.idmbrg = ${idmbrg} ${qcabang} ${qsupplier} ${qbarang} GROUP BY b.idtbeli`;
          const list = await sequelize.query(sql1, {
            raw: false,
          });

          var arr_list = await Promise.all(
            list[0].map(async (list, index_satu) => {
              var list_item = await barang_list(list);
              list_item.sales = list.nmmsales;
              return list_item;
            })
          );

          return {
            nama: fil.nmmbrg,
            bruto: parseFloat(fil.subtotal),
            diskon: parseFloat(fil.diskon),
            pajak: parseFloat(fil.pajak),
            netto: parseFloat(fil.tagihan),
            tunai: parseFloat(fil.bayar), // "Rp&nbsp;"+ number_format(fil.bayar, 2),
            sisa: parseFloat(Math.abs(fil.sisa)), // "Rp&nbsp;"+ number_format(abs(fil.sisa), 2),
            list: arr_list,
          };
        })
      );
    } else {
      var count = {};
      var arr_data = [];
    }
  } else if (jenis == 2) {
    jenis_str = "detail";
    async function barang_list(list) {
      let sql2 = `SELECT bb.kdmbrg, bb.nmmbrg, g.nmmgd, bd.qtytotal, bd.hrgstn, s.nmmstn, bd.discv, bd.ppnv, (bd.qtytotal * bd.hrgstn) AS dpp, (bd.qtytotal * bd.hrgstn - bd.discv + bd.ppnv) AS subtotal FROM mgaptbelid bd LEFT OUTER JOIN mgsymgd g ON bd.idmgd = g.idmgd LEFT OUTER JOIN mginmbrg bb ON bd.idmbrg = bb.idmbrg LEFT OUTER JOIN mginmstn s ON bb.idmstn1 = s.idmstn LEFT OUTER JOIN mgaptbeli b ON bd.idtbeli = b.idtbeli WHERE b.hapus=0 AND b.tgltbeli BETWEEN '${start}%' AND '${end}%' ${qcabang} ${qsupplier} ${qbarang} AND b.idtbeli = ${list.idtbeli}`;
      // let sql2 = `SELECT bb.kdmbrg, bb.nmmbrg, g.nmmgd, bd.qtytotal, bd.hrgstn, s.nmmstn, bd.discv, bd.ppnv, (bd.qtytotal * bd.hrgstn) AS dpp, (bd.qtytotal * bd.hrgstn - bd.discv + bd.ppnv) AS subtotal FROM mgaptbelid bd LEFT OUTER JOIN mgsymgd g ON bd.idmgd = g.idmgd LEFT OUTER JOIN mginmbrg bb ON bd.idmbrg = bb.idmbrg LEFT OUTER JOIN mginmstn s ON bb.idmstn1 = s.idmstn LEFT OUTER JOIN mgaptbeli b ON bd.idtbeli = b.idtbeli WHERE b.hapus=0 AND b.tgltbeli BETWEEN '${start}%' AND '${end}%' AND b.idtbeli = $l->idtbeli`;
      const brg = await sequelize.query(sql2, {
        raw: false,
      });

      var arr_brg = brg[0].map((brg, index_dua) => {
        return {
          kode: brg.kdmbrg,
          nama: brg.nmmbrg,
          gudang: brg.nmmgd,
          qty: brg.qtytotal,
          satuan: brg.nmmstn,
          hargasat: parseFloat(brg.hrgstn),
          diskon: parseFloat(brg.discv),
          pajak: parseFloat(brg.ppnv),
          dpp: parseFloat(brg.dpp),
          subtotal: parseFloat(brg.subtotal),
        };
      });

      return {
        id: list.idtbeli,
        tanggal: list.tgltbeli,
        transaksi: list.buktitbeli,
        supplier: list.nmmsup,
        subtotal: parseFloat(list.bruto),
        diskon: parseFloat(list.discv),
        pajak: parseFloat(list.ppnv),
        grandtotal: parseFloat(list.netto),
        bayar: parseFloat(list.jmlbayartunai),
        kredit: parseFloat(list.jmlbayarkredit),
        sisa:
          parseFloat(list.jmlbayarkredit) +
          parseFloat(list.jmlbayartunai) -
          parseFloat(list.netto),
        item: arr_brg,
      };
    }

    if (group == "cabang") {
      let sql = `select nmmcabang from mgsymcabang where aktif = 1 and hapus = 0`;
      const filter = await sequelize.query(sql, {
        raw: false,
      });

      var arr_data = await Promise.all(
        filter[0].map(async (fil, index) => {
          let sql1 = `SELECT b.idtbeli, b.tgltbeli, b.buktitbeli, s.nmmsup, b.bruto, b.discv, b.ppnv, b.netto, b.jmlbayartunai, b.jmlbayarkredit, (b.netto-b.jmlbayartunai-b.jmlbayarkredit) AS STATUS FROM mgaptbeli b LEFT OUTER JOIN mgapmsup s ON b.idmsup = s.idmsup WHERE b.hapus = 0 AND b.tgltbeli BETWEEN '${start}%' AND '${end}%' ${qbarang} ${qcabang} ${qsupplier}`;
          const list = await sequelize.query(sql1, {
            raw: false,
          });

          var arr_list = await Promise.all(
            list[0].map(async (list, index_satu) => {
              return barang_list(list);
            })
          );

          return {
            nama: fil.nmmcabang,
            list: arr_list,
          };
        })
      );
    } else if (group == "supplier") {
      let sql = `SELECT s.idmsup, s.nmmsup FROM mgapmsup s LEFT OUTER JOIN mgaptbeli b ON s.idmsup = b.idmsup LEFT OUTER JOIN mgaptbelid bd ON b.idtbeli = bd.idtbeli WHERE s.Aktif = 1 AND s.hapus = 0 AND b.tgltbeli BETWEEN '${start}%' AND '${end}%' ${qbarang} ${qsupplier} ${qcabang} GROUP BY s.nmmsup HAVING COUNT(b.idtbeli)>0`;
      const filter = await sequelize.query(sql, {
        raw: false,
      });

      var arr_data = await Promise.all(
        filter[0].map(async (fil, index) => {
          let idmsup = fil.idmsup;
          let sql1 = `SELECT b.idtbeli, b.tgltbeli, b.buktitbeli, s.nmmsup, b.bruto, b.discv, b.ppnv, b.netto, b.jmlbayartunai, b.jmlbayarkredit, (b.netto-b.jmlbayartunai-b.jmlbayarkredit) AS STATUS FROM mgaptbeli b LEFT OUTER JOIN mgapmsup s ON b.idmsup = s.idmsup WHERE b.hapus=0 AND b.idmsup = ${idmsup} AND b.tgltbeli BETWEEN '${start}%' AND '${end}%' ${qcabang} ${qbarang} ${qsupplier}`;
          const list = await sequelize.query(sql1, {
            raw: false,
          });

          var arr_list = await Promise.all(
            list[0].map(async (list, index_satu) => {
              return barang_list(list);
            })
          );

          return {
            nama: fil.nmmsup,
            list: arr_list,
          };
        })
      );
    } else if (group == "barang") {
      let sql = `SELECT b.idmbrg,b.nmmbrg FROM mginmbrg b LEFT OUTER JOIN mgaptbelid bd ON b.IdMBrg = bd.idmbrg LEFT OUTER JOIN mgaptbeli bb ON bd.idtbeli = bb.idtbeli WHERE b.aktif = 1 AND b.hapus=0 AND bb.tgltbeli BETWEEN '${start}%' AND '${end}%' ${qcabang} ${qsupplier} ${qbarang} GROUP BY b.Idmbrg HAVING COUNT(bb.IdTbeli)>0`;
      const filter = await sequelize.query(sql, {
        raw: false,
      });

      var arr_data = await Promise.all(
        filter[0].map(async (fil, index) => {
          let idmbrg = fil.idmbrg;
          let sql1 = `SELECT b.idtbeli, b.tgltbeli, b.buktitbeli, s.nmmsup, b.bruto, b.discv, b.ppnv, b.netto, b.jmlbayartunai, b.jmlbayarkredit, (b.netto-b.jmlbayarkredit-b.jmlbayartunai) AS STATUS FROM mgaptbeli b LEFT OUTER JOIN mgapmsup s ON s.idmsup = b.idmsup LEFT OUTER JOIN mgaptbelid bd ON b.idtbeli = bd.idtbeli WHERE b.hapus = 0 AND bd.idmbrg =${idmbrg} AND b.tgltbeli BETWEEN '${start}%' AND '${end}%' ${qcabang} ${qsupplier} ${qbarang} GROUP BY b.idtbeli`;
          const list = await sequelize.query(sql1, {
            raw: false,
          });

          var arr_list = await Promise.all(
            list[0].map(async (list, index_satu) => {
              return barang_list(list);
            })
          );

          return {
            nama: fil.nmmbrg,
            list: arr_list,
          };
        })
      );
    } else {
      var count = {};
      var arr_data = [];
    }
  }
  res.json({
    message: "Success " + jenis_str,
    countData: count,
    data: arr_data,
  });
};

exports.stock = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);
  const companyid = req.datacompany.id;
  console.log("tesman", companyid);

  let jenis = req.body.jenis || 1;
  // posisi stock
  if (jenis == 1) {
    let date = req.body.tanggal || today;
    let qsql = await qstock.queryPosisiStock(companyid,date);
    const data = await fun.getDataFromQuery(sequelize, qsql);

    var arr_list = [];
    var listgudang = [];
    var arr_data = await Promise.all(data.map(async (fil, index) => {
        var arr_listitem = [];

        var list = {
            "id": fil.IdMBrg,
            "kode": fil.KdMBrg,
            "nama": fil.NmMBrg,
            "qty": parseFloat(fil.PosQty),
            "satuan": fil.KdMStn1
        };
          
        if (!listgudang.includes(fil.NmMGd)) {
          listgudang.push(fil.NmMGd);

          arr_list.push({
            "id_cabang": fil.IdMCabang,
            "cabang": fil.NmMCabang,
            "id_gudang": fil.IdMGd,
            "gudang": fil.NmMGd,
            "list": [list],
          });
        } else {
          let idx = listgudang.indexOf(fil.NmMGd);
          arr_list[idx].list.push(list);
        }
      })
    );

    res.json({
      message: "Success",
      data: arr_list,
    });
  }

  // kartu stock
  else if (jenis == 2) {
    let start = req.body.start || today;
    let end = req.body.end || today;

    let cabang = req.body.cabang || "";
    let gudang = req.body.gudang || "";
    let barang = req.body.barang || "";

    // console.log("logbrg", qbarang);
    // console.log("logstart", start);
    // console.log("logend", end);

    let qsql = await qstock.queryKartuStock(companyid,start,end,cabang,gudang,barang);
    const data = await fun.getDataFromQuery(sequelize, qsql);

    var arr_list = [];
    var listgudang = [];
    var listbarang = [];
    var arr_listitem = [];
    var arr_data = await Promise.all(data.map(async (fil, index) => {
      var list = {
        "tanggal": fil.TglTrans,
        "keterangan": fil.Keterangan,
        "satuan": fil.KdMStn,
        "debet": parseFloat(fil.Debit),
        "kredit": parseFloat(fil.Kredit),
        "saldo": parseFloat(fil.Saldo),
      };
      //
        
      var barang = {
        "kode": fil.KdMBrg,
        "nama": fil.NmMBrg,
        "listitem": [list]
      }

      var gudang = {
        "cabang": fil.NmMCabang,
        "gudang": fil.NmMGd,
        "list": [barang],
      }
    
      // gudang terbaru (gudang => barang => item)
      if (!listgudang.includes(fil.NmMGd)) {
        listgudang.push(fil.NmMGd);
        listbarang.push(fil.KdMBrg);
        //

        arr_list.push(gudang);
      }
      // gudang yang sudah ada
      else {
        let idx = listgudang.indexOf(fil.NmMGd);
        // barang terbaru di gudang yang sudah ada (barang => item)
        if (!listbarang.includes(fil.KdMBrg)) { 
          listbarang.push(fil.KdMBrg);
          arr_list[idx].list.push(barang);
        }
        // barang yang sudah ada (item)
        else {
          let idx2 = listbarang.indexOf(fil.KdMBrg);
          arr_list[idx].list[idx2].listitem.push(list);
        }
      }
    }));

    res.json({
      message: "Success kartu",
      data: arr_list,
    });
  }
};

exports.kas = async (req, res) => {
    const qkas = require("../class/query_report/kas");
    const sequelize = await fun.connection(req.datacompany);
    const companyid = req.datacompany.id;

    let jenis = req.body.jenis || 1;
    // posisi kas
    if (jenis == 1) {        
        let date = req.body.tanggal || today;
        let q = await qkas.queryPosisiKas(companyid,date);
        const data = await fun.getDataFromQuery(sequelize, q);

        var listitem = [];
        var listcabang = [];
        var grandtotal = 0;
        var arr_data = await Promise.all(data.map(async (fil, index) => {
            grandtotal += parseFloat(fil.PosKas)
            var data_kas = {
              kode : fil.KdMKas,
              nama : fil.NmMKas,
              qty : fil.PosKas
            };
            if (!listcabang.includes(fil.NmMCabang)) {              
              listcabang.push(fil.NmMCabang);
              listitem.push({
                cabang: fil.NmMCabang,
                list: [data_kas],
              });
            } else {
              let cek = listcabang.indexOf(fil.NmMCabang);
              listitem[cek].list.push(data_kas);
            }
          })
        );

        var count = {
          grandtotal : grandtotal
        }

        res.json({
            message: "Success",
            countData: count,
            data: listitem
        })
    }

    // kartu kas
    else if (jenis == 2) {
      let start = req.body.start || today;
      let end = req.body.end || today;
      let mkas = req.body.mkas || "";

      let q = await qkas.queryKartuKas(companyid,start,end,mkas);
      const data = await fun.getDataFromQuery(sequelize, q);

      var arr_list = [];
      var listcabang = [];
      var listkas = [];
      var arr_data = await Promise.all(data.map(async (fil, index) => {
        var list = {
          "tanggal": fil.TglTrans,
          "keterangan": fil.Keterangan,
          "debet": parseFloat(fil.Debit),
          "kredit": parseFloat(fil.Kredit),
          "saldo": parseFloat(fil.Saldo),
        };
          
        var kas = {
          "kode": fil.KdMKas,
          "nama": fil.NmMKas,
          "listitem": [list]
        }

        var cabang = {
          "cabang": fil.NmMCabang,
          "list": [kas],
        }
      
        // cabang terbaru (cabang => kas => item)
        if (!listcabang.includes(fil.NmMCabang)) {
          listcabang.push(fil.NmMCabang);
          listkas.push(fil.KdMKas);

          arr_list.push(cabang);
        }
        // cabang yang sudah ada
        else {
          let idx = listcabang.indexOf(fil.NmMCabang);
          // kas terbaru di cabang yang sudah ada (kas => item)
          if (!listkas.includes(fil.KdMKas)) { 
            listkas.push(fil.KdMKas);
            arr_list[idx].list.push(cabang);
          }
          // kas yang sudah ada (item)
          else {
            let idx2 = listkas.indexOf(fil.KdMKas);
            arr_list[idx].list[idx2].listitem.push(list);
          }
        }
      }));

      res.json({
        message: "Success kartu",
        data: arr_list,
      });
    }
};

exports.bank = async (req, res) => {
    const qbank = require("../class/query_report/bank");
    const sequelize = await fun.connection(req.datacompany);

    let jenis = req.body.jenis || 1;
    // posisi bank
    if (jenis == 1) {        
        let date = req.body.tanggal || today;
        let q = await qbank.queryPosisiBank(date);
        const data = await fun.getDataFromQuery(sequelize, q);

        var listitem = [];
        var listcabang = [];
        var grandtotal = 0;
        var arr_data = await Promise.all(data.map(async (fil, index) => {
            var data_bank = {
              bank : fil.NMMBANK,
              kode : fil.KdMRek,
              nama : fil.NmMRek,
              qty : fil.PosRek
            };
            if (!listcabang.includes(fil.NmMCabang)) {              
              listcabang.push(fil.NmMCabang);
              listitem.push({
                cabang: fil.NmMCabang,
                list: [data_bank],
              });
            } else {
              let cek = listcabang.indexOf(fil.NmMCabang);
              listitem[cek].list.push(data_bank);
            }
          })
        );

        res.json({
            message: "Success",
            data: listitem
        })
    }

    // kartu bank
    else if (jenis == 2) {
      let start = req.body.start || today;
      let end = req.body.end || today;
      let mbank = req.body.mbank || "";

      let q = await qbank.queryKartuBank(start,end,mbank);
      const data = await fun.getDataFromQuery(sequelize, q);

      var arr_list = [];
      var listcabang = [];
      var listbank = [];
      var arr_data = await Promise.all(data.map(async (fil, index) => {
        var list = {
          "tanggal": fil.TglTrans,
          "keterangan": fil.Keterangan,
          "debet": parseFloat(fil.Debit),
          "kredit": parseFloat(fil.Kredit),
          "saldo": parseFloat(fil.Saldo),
        };
          
        var bank = {
          "bank": fil.NMMBANK,
          "kode": fil.KdMRek,
          "nama": fil.NmMRek,
          "listitem": [list]
        }

        var cabang = {
          "cabang": fil.NmMCabang,
          "list": [bank],
        }
      
        // cabang terbaru (cabang => bank => item)
        if (!listcabang.includes(fil.NmMCabang)) {
          listcabang.push(fil.NmMCabang);
          listbank.push(fil.KdMRek);

          arr_list.push(cabang);
        }
        // cabang yang sudah ada
        else {
          let idx = listcabang.indexOf(fil.NmMCabang);
          // bank terbaru di cabang yang sudah ada (bank => item)
          if (!listbank.includes(fil.KdMRek)) { 
            listbank.push(fil.KdMRek);
            arr_list[idx].list.push(cabang);
          }
          // bank yang sudah ada (item)
          else {
            let idx2 = listbank.indexOf(fil.KdMRek);
            // arr_list[idx].list[idx2].listitem.push(list);
          }
        }
      }));

      res.json({
        message: "Success kartu",
        data: arr_list,
      });
    }
};

exports.hutang = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);
  const companyid = req.datacompany.id;
  const qhutang = require("../class/query_report/hutang");

  let jenis = req.body.jenis || 1;

  // posisi hutang
  if (jenis == 1) {
    let date = req.body.tanggal || today;
    let qsql = await qhutang.queryPosisiHutang(companyid,date);
    const data = await fun.getDataFromQuery(sequelize, qsql);

    var arr_list = [];
    var listcabang = [];
    var total = 0;
    var total_cabang = 0;
    var arr_data = await Promise.all(data.map(async (item, index) => {
      var list = {
        "kode": item.KdMSup,
        "nama": item.NmMSup,
        "qty": parseFloat(item.PosHut),
      };

      var poshut = parseFloat(item.PosHut);  
      if (!listcabang.includes(item.IdMCabang)) {
        listcabang.push(item.IdMCabang);

        total_cabang = 0;
        total_cabang += poshut;

        arr_list.push({
          "cabang": item.NmMCabang,
          "total": total_cabang,
          "list": [list],
        });
      } else {
        total_cabang += poshut;
        let idx = listcabang.indexOf(item.IdMCabang);
        arr_list[idx].total = total_cabang;
        arr_list[idx].list.push(list);
      }
      total += poshut;
    }));


    var count = {
      total: total,
    };

    res.json({
      message: "Success",
      countData: count,
      data: arr_list,
    });
  }

  // kartu hutang
  else if (jenis == 2) {
    let start = req.body.start || today;
    let end = req.body.end || today;

    let qsql = await qhutang.queryKartuHutang(companyid, start, end);
    const data = await fun.getDataFromQuery(sequelize, qsql);

    var arr_list = [];
    var listsupplier = [];

    var arr_data = await Promise.all(data.map(async (item, index) => {
      var list = {
        "tanggal": item.TglTrans,
        "bukti": item.BuktiTrans,
        "keterangan": item.Keterangan,
        "debit": parseFloat(item.Debit),
        "kredit": parseFloat(item.Kredit),
        "saldo": parseFloat(item.Saldo),
      };

      if (!listsupplier.includes(item.KdMSup)) {
        listsupplier.push(item.KdMSup);

        arr_list.push({
          "customer": item.NmMSup,
          "list": [list],
        });
      } else {
        let idx = listsupplier.indexOf(item.KdMSup);
        arr_list[idx].list.push(list);
      }
    }));

    res.json({
      message: "Success kartu",
      data: arr_list,
    });
  }
};

exports.piutang = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);
  const companyid = req.datacompany.id;
  const qpiutang = require("../class/query_report/piutang");

  let jenis = req.body.jenis || 1;

  // posisi piutang
  if (jenis == 1) {
    let date = req.body.tanggal || today;
    let qsql = await qpiutang.queryPosisiPiutang(companyid,date);
    const data = await fun.getDataFromQuery(sequelize, qsql);

    var arr_list = [];
    var listcabang = [];

    var total = 0;
    var arr_data = await Promise.all(data.map(async (item, index) => {

      var pospiut = parseFloat(item.PosPiut);
      total += pospiut;
      var list = {
        "kode": item.KdMCust,
        "nama": item.NmMCust,
        "qty": pospiut,
      };

      if (!listcabang.includes(item.KdMCabang)) {
        listcabang.push(item.KdMCabang);

        arr_list.push({
          "cabang": item.NmMCabang,
          "list": [list],
        });
      } else {
        let idx = listcabang.indexOf(item.KdMCabang);
        arr_list[idx].list.push(list);
      }
    }));

    var count = {
      total: total,
    };

    res.json({
      message: "Success",
      countData: count,
      data: arr_list,
    });
  }

  // kartu piutang
  else if (jenis == 2) {
    let start = req.body.start || today;
    let end = req.body.end || today;

    let qsql = await qpiutang.queryKartuPiutang(companyid, start, end);
    const data = await fun.getDataFromQuery(sequelize, qsql);

    var arr_list = [];
    var listcustomer = [];
    var saldo = 0;
    var arr_data = await Promise.all(data.map(async (item, index) => {
      saldo += (parseFloat(item.Debit) + parseFloat(item.Kredit));
      var list = {
        "tanggal": item.TglTrans,
        "bukti": item.BuktiTrans,
        "keterangan": item.Keterangan,
        "debit": parseFloat(item.Debit),
        "kredit": Math.abs(parseFloat(item.Kredit)),
        "saldo": saldo,
      };

      if (!listcustomer.includes(item.KdMCust)) {
        listcustomer.push(item.KdMCust);
        saldo = 0;
        saldo += (parseFloat(item.Debit) + parseFloat(item.Kredit));
        list.saldo = saldo;

        arr_list.push({
          "customer": item.NmMCust,
          "list": [list],
        });
      } else {
        let idx = listcustomer.indexOf(item.KdMCust);
        arr_list[idx].list.push(list);
      }

    }));

    res.json({
      message: "Success",
      data: arr_list,
    });
  }
};

exports.labarugi = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  let jenis = req.body.jenis || 1;
  let start = req.body.start || today;
  let end = req.body.end || today;

  // LABA RUGI PENJUALAN
  if (jenis == 1) {
    let cabang = req.body.cabang || "";
    let qcabang = "";
    if (cabang != "") {
      qcabang = "AND idmcabang=" + cabang;
    }

    let customer = req.body.customer || "";
    let qcustomer = "";
    if (customer != "") {
      qcustomer = "AND idmcust = " + customer;
    }

    let sales = req.body.sales || "";
    let qsales = "";
    if (sales != "") {
      qsales = "AND idmsales = " + sales;
    }

    let sql = `select mcabang.idmcabang, mcabang.nmmcabang, SUM(TRLPenjualan.nilaijual) AS nilaijual, SUM(TRLPenjualan.nilaihpp) AS nilaihpp, SUM(IF(idx = 5, 0, (nilaijual - nilaihpp))) AS labarugi, SUM(COALESCE((((nilaijual-nilaihpp)/nilaihpp)*100),0)) AS persenrl FROM (SELECT Idx, IdMCabang, TglTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales, nilaijual, nilaihpp, NilaiKomisi FROM (SELECT 3 AS Idx, IdMCabang, IdTrans, TglTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales, SUM(Qty * HrgStn) AS nilaijual, SUM(Qty * HPP) AS nilaihpp, SUM(Komisi) AS NilaiKomisi FROM (SELECT m.IdMCabang, m.IdTJual AS IdTrans, m.TglTJual AS TglTrans, m.BuktiTJual AS BuktiTrans, MCust.KdMCust, MCust.NmMCust, MSales.KdMSales, MSales.NmMSales, d.IdMBrg, d.QtyTotal AS Qty, COALESCE((d.HrgStn- IF(COALESCE(d.DiscV,0)=0, 0, d.DiscV) - ((d.HrgStn - IF(COALESCE(d.DiscV,0)=0, 0, d.DiscV)) * (m.discV/m.Bruto))),0) AS HrgStn, COALESCE(d.HPP, 0) AS HPP, 0 AS Komisi FROM MGARTJualD d LEFT OUTER JOIN MGARTJual m ON ((d.IdMCabang = m.IdMCabang) AND (d.IdTJual = m.IdTJual)) LEFT OUTER JOIN MGARMCust MCust ON (m.IdMCabangMCust = MCust.IdMCabang AND m.IdMCust = MCust.IdMCust) LEFT OUTER JOIN MGARMSales MSales ON (m.IdMCabangMSales = MSales.IdMCabang AND m.IdMSales = MSales.IdMSales) WHERE (m.Hapus = 0) AND (m.Void = 0) AND (MCust.Hapus = 0) AND (MSales.Hapus = 0) AND ((m.TglTJual >= '${start}') AND (m.TglTJual <= '${end}'))  AND d.IdMBrg <> 0 AND (MCust.KdMCust LIKE '%%') AND (MCust.NmMCust LIKE '%%') AND (MSales.KdMSales LIKE '%%') AND (MSales.NmMSales LIKE '%%') ) TablePenjualan GROUP BY IdMCabang, TglTrans, IdTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales UNION ALL SELECT 4 AS Idx, IdMCabang, IdTrans, TglTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales, SUM(Qty * HrgStn) AS nilaijual, SUM(Qty * HPP) AS nilaihpp, 0 AS nilaikomisi FROM ( SELECT m.IdMCabang, m.IdTRJual AS IdTrans, m.TglTRJual AS TglTrans, m.BuktiTRJual AS BuktiTrans, MCust.KdMCust, MCust.NmMCust, MSales.KdMSales, MSales.NmMSales, d.IdMBrg, d.QtyTotal AS Qty, - (d.HrgStn) AS HrgStn, COALESCE(-d.HPP, 0) AS HPP FROM MGARTRJualD d LEFT OUTER JOIN MGARTRJual m ON ((d.IdMCabang = m.IdMCabang) AND (d.IdTRJual = m.IdTRJual)) LEFT OUTER JOIN MGARTJual TJual ON (TJual.IdMCabang = m.IdMCabangTJual AND TJual.IdTJual = m.IdTJual) LEFT OUTER JOIN MGARMCust MCust ON (m.IdMCabangMCust = MCust.IdMCabang AND m.IdMCust = MCust.IdMCust) LEFT OUTER JOIN MGARMSales MSales ON (TJual.IdMCabangMSales = MSales.IdMCabang AND TJual.IdMSales = MSales.IdMSales) WHERE (m.IdTJual <> 0) AND (m.Hapus = 0) AND (m.Void = 0) AND ((TglTRJual >=  '${start}') AND (TglTRJual <= '${end}')) AND (MCust.Hapus = 0) AND (MCust.KdMCust LIKE '%%') AND (MCust.NmMCust LIKE '%%') AND (MSales.Hapus = 0) AND (MSales.KdMSales LIKE '%%') AND (MSales.NmMSales LIKE '%%')) TableReturPenjualan WHERE IdMBrg <> 0 GROUP BY IdMCabang, TglTrans, IdTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales UNION ALL SELECT 5 AS Idx, IdMCabang, IdTrans, TglTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales, SUM(Qty * HrgStn) AS nilaijual, SUM(HPP) AS nilaihpp, 0 AS nilaikomisi FROM (SELECT m.IdMCabang, m.IdTJual AS IdTrans, m.TglTJual AS TglTrans, CONCAT(m.BuktiTJual, ' (', ' Pembulatan ', ')') AS BuktiTrans, MCust.KdMCust, MCust.NmMCust, MSales.KdMSales, MSales.NmMSales, d.IdMBrg, 0 AS Qty, 0 AS HrgStn, ABS(COALESCE(LPembulatanKartuStock.HrgStn, 0)) AS HPP FROM MGARTJualD d LEFT OUTER JOIN MGARTJual m ON ((d.IdMCabang = m.IdMCabang) AND (d.IdTJual = m.IdTJual)) LEFT OUTER JOIN MGARMCust MCust ON (m.IdMCabangMCust = MCust.IdMCabang AND m.IdMCust = MCust.IdMCust) LEFT OUTER JOIN MGARMSales MSales ON (m.IdMCabangMSales = MSales.IdMCabang AND m.IdMSales = MSales.IdMSales) LEFT OUTER JOIN MGINLPembulatanKartuStock LPembulatanKartuStock ON (d.IdMCabang = LPembulatanKartuStock.IdMCabang AND d.IdTJual = LPembulatanKartuStock.IdTrans AND d.IdTJualD = LPembulatanKartuStock.IdTransD)  WHERE (m.Hapus = 0) AND (m.Void = 0) AND (MCust.Hapus = 0) AND (MSales.Hapus = 0) AND ((m.TglTJual >=  '${start}') AND (m.TglTJual <= '${end}'))  AND d.IdMBrg <> 0 AND SUBSTRING(LPembulatanKartuStock.Keterangan, 14, 3) = 'RJL' AND (MCust.KdMCust LIKE '%%') AND (MCust.NmMCust LIKE '%%') AND (MSales.KdMSales LIKE '%%') AND (MSales.NmMSales LIKE '%%')) TablePenjualan GROUP BY IdMCabang, TglTrans, IdTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales ) SubTRLPenjualan ) TRLPenjualan LEFT OUTER JOIN MGSYMCabang MCabang ON (TRLPenjualan.IdMCabang = MCabang.IdMCabang) WHERE (MCabang.Hapus = 0) ${qcabang} ${qsales} ${qcustomer} GROUP BY mcabang.idmcabang ORDER BY TglTrans`;
    const filter = await sequelize.query(sql, {
      raw: false,
    });

    var arr_data = await Promise.all(
      filter[0].map(async (fil, index) => {
        let sql1 = `select TRLPenjualan.TglTrans, SUM(TRLPenjualan.nilaijual) AS nilaijual, SUM(TRLPenjualan.nilaihpp) AS nilaihpp, SUM(IF(idx = 5, 0, (nilaijual - nilaihpp))) AS labarugi, SUM(COALESCE((((nilaijual-nilaihpp)/nilaihpp)*100),0)) AS persenrl FROM (SELECT Idx, IdMCabang, TglTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales, nilaijual, nilaihpp, NilaiKomisi FROM (SELECT 3 AS Idx, IdMCabang, IdTrans, TglTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales, SUM(Qty * HrgStn) AS nilaijual, SUM(Qty * HPP) AS nilaihpp, SUM(Komisi) AS NilaiKomisi FROM (SELECT m.IdMCabang, m.IdTJual AS IdTrans, m.TglTJual AS TglTrans, m.BuktiTJual AS BuktiTrans, MCust.KdMCust, MCust.NmMCust, MSales.KdMSales, MSales.NmMSales, d.IdMBrg, d.QtyTotal AS Qty, COALESCE((d.HrgStn- IF(COALESCE(d.DiscV,0)=0, 0, d.DiscV) - ((d.HrgStn - IF(COALESCE(d.DiscV,0)=0, 0, d.DiscV)) * (m.discV/m.Bruto))),0) AS HrgStn, COALESCE(d.HPP, 0) AS HPP, 0 AS Komisi FROM MGARTJualD d LEFT OUTER JOIN MGARTJual m ON ((d.IdMCabang = m.IdMCabang) AND (d.IdTJual = m.IdTJual)) LEFT OUTER JOIN MGARMCust MCust ON (m.IdMCabangMCust = MCust.IdMCabang AND m.IdMCust = MCust.IdMCust) LEFT OUTER JOIN MGARMSales MSales ON (m.IdMCabangMSales = MSales.IdMCabang AND m.IdMSales = MSales.IdMSales) WHERE (m.Hapus = 0) AND (m.Void = 0) AND (MCust.Hapus = 0) AND (MSales.Hapus = 0) AND ((m.TglTJual >= '${start}') AND (m.TglTJual <= '${end}'))  AND d.IdMBrg <> 0 AND (MCust.KdMCust LIKE '%%') AND (MCust.NmMCust LIKE '%%') AND (MSales.KdMSales LIKE '%%') AND (MSales.NmMSales LIKE '%%') ) TablePenjualan GROUP BY IdMCabang, TglTrans, IdTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales UNION ALL SELECT 4 AS Idx, IdMCabang, IdTrans, TglTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales, SUM(Qty * HrgStn) AS nilaijual, SUM(Qty * HPP) AS nilaihpp, 0 AS nilaikomisi FROM ( SELECT m.IdMCabang, m.IdTRJual AS IdTrans, m.TglTRJual AS TglTrans, m.BuktiTRJual AS BuktiTrans, MCust.KdMCust, MCust.NmMCust, MSales.KdMSales, MSales.NmMSales, d.IdMBrg, d.QtyTotal AS Qty, - (d.HrgStn) AS HrgStn, COALESCE(-d.HPP, 0) AS HPP FROM MGARTRJualD d LEFT OUTER JOIN MGARTRJual m ON ((d.IdMCabang = m.IdMCabang) AND (d.IdTRJual = m.IdTRJual)) LEFT OUTER JOIN MGARTJual TJual ON (TJual.IdMCabang = m.IdMCabangTJual AND TJual.IdTJual = m.IdTJual) LEFT OUTER JOIN MGARMCust MCust ON (m.IdMCabangMCust = MCust.IdMCabang AND m.IdMCust = MCust.IdMCust) LEFT OUTER JOIN MGARMSales MSales ON (TJual.IdMCabangMSales = MSales.IdMCabang AND TJual.IdMSales = MSales.IdMSales) WHERE (m.IdTJual <> 0) AND (m.Hapus = 0) AND (m.Void = 0) AND ((TglTRJual >=  '${start}') AND (TglTRJual <= '${end}')) AND (MCust.Hapus = 0) AND (MCust.KdMCust LIKE '%%') AND (MCust.NmMCust LIKE '%%') AND (MSales.Hapus = 0) AND (MSales.KdMSales LIKE '%%') AND (MSales.NmMSales LIKE '%%')) TableReturPenjualan WHERE IdMBrg <> 0 GROUP BY IdMCabang, TglTrans, IdTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales UNION ALL SELECT 5 AS Idx, IdMCabang, IdTrans, TglTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales, SUM(Qty * HrgStn) AS nilaijual, SUM(HPP) AS nilaihpp, 0 AS nilaikomisi FROM (SELECT m.IdMCabang, m.IdTJual AS IdTrans, m.TglTJual AS TglTrans, CONCAT(m.BuktiTJual, ' (', ' Pembulatan ', ')') AS BuktiTrans, MCust.KdMCust, MCust.NmMCust, MSales.KdMSales, MSales.NmMSales, d.IdMBrg, 0 AS Qty, 0 AS HrgStn, ABS(COALESCE(LPembulatanKartuStock.HrgStn, 0)) AS HPP FROM MGARTJualD d LEFT OUTER JOIN MGARTJual m ON ((d.IdMCabang = m.IdMCabang) AND (d.IdTJual = m.IdTJual)) LEFT OUTER JOIN MGARMCust MCust ON (m.IdMCabangMCust = MCust.IdMCabang AND m.IdMCust = MCust.IdMCust) LEFT OUTER JOIN MGARMSales MSales ON (m.IdMCabangMSales = MSales.IdMCabang AND m.IdMSales = MSales.IdMSales) LEFT OUTER JOIN MGINLPembulatanKartuStock LPembulatanKartuStock ON (d.IdMCabang = LPembulatanKartuStock.IdMCabang AND d.IdTJual = LPembulatanKartuStock.IdTrans AND d.IdTJualD = LPembulatanKartuStock.IdTransD)  WHERE (m.Hapus = 0) AND (m.Void = 0) AND (MCust.Hapus = 0) AND (MSales.Hapus = 0) AND ((m.TglTJual >=  '${start}') AND (m.TglTJual <= '${end}'))  AND d.IdMBrg <> 0 AND SUBSTRING(LPembulatanKartuStock.Keterangan, 14, 3) = 'RJL' AND (MCust.KdMCust LIKE '%%') AND (MCust.NmMCust LIKE '%%') AND (MSales.KdMSales LIKE '%%') AND (MSales.NmMSales LIKE '%%')) TablePenjualan GROUP BY IdMCabang, TglTrans, IdTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales ) SubTRLPenjualan ) TRLPenjualan LEFT OUTER JOIN MGSYMCabang MCabang ON (TRLPenjualan.IdMCabang = MCabang.IdMCabang) WHERE (MCabang.Hapus = 0 AND MCabang.IdMCabang = ${fil.idmcabang}) ${qcabang} ${qsales} ${qcustomer} GROUP BY TglTrans ORDER BY TglTrans`;
        const list = await sequelize.query(sql1, {
          raw: false,
        });

        var arr_list = await Promise.all(
          list[0].map(async (list, index_satu) => {
            console.log("list", list);
            let tgltrans = list.TglTrans.toISOString();
            let sql2 = `SELECT MCabang.KdMCabang, MCabang.NmMCabang, TRLPenjualan.*, IF(idx = 5, 0, (nilaijual - nilaihpp)) AS labarugi , COALESCE((((nilaijual-nilaihpp)/nilaihpp)*100),0) AS persenrl FROM (SELECT Idx, IdMCabang, TglTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales, nilaijual, nilaihpp, NilaiKomisi FROM (SELECT 3 AS Idx, IdMCabang, IdTrans, TglTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales, SUM(Qty * HrgStn) AS nilaijual, SUM(Qty * HPP) AS nilaihpp, SUM(Komisi) AS NilaiKomisi FROM (SELECT m.IdMCabang, m.IdTJual AS IdTrans, m.TglTJual AS TglTrans, m.BuktiTJual AS BuktiTrans, MCust.KdMCust, MCust.NmMCust, MSales.KdMSales, MSales.NmMSales, d.IdMBrg, d.QtyTotal AS Qty, COALESCE((d.HrgStn- IF(COALESCE(d.DiscV,0)=0, 0, d.DiscV) - ((d.HrgStn - IF(COALESCE(d.DiscV,0)=0, 0, d.DiscV)) * (m.discV/m.Bruto))),0) AS HrgStn, COALESCE(d.HPP, 0) AS HPP, 0 AS Komisi FROM MGARTJualD d LEFT OUTER JOIN MGARTJual m ON ((d.IdMCabang = m.IdMCabang) AND (d.IdTJual = m.IdTJual)) LEFT OUTER JOIN MGARMCust MCust ON (m.IdMCabangMCust = MCust.IdMCabang AND m.IdMCust = MCust.IdMCust) LEFT OUTER JOIN MGARMSales MSales ON (m.IdMCabangMSales = MSales.IdMCabang AND m.IdMSales = MSales.IdMSales) WHERE (m.Hapus = 0) AND (m.Void = 0) AND (MCust.Hapus = 0) AND (MSales.Hapus = 0) AND ((m.TglTJual >= '${start}') AND (m.TglTJual <= '${end}'))  AND d.IdMBrg <> 0 AND (MCust.KdMCust LIKE '%%') AND (MCust.NmMCust LIKE '%%') AND (MSales.KdMSales LIKE '%%') AND (MSales.NmMSales LIKE '%%') ) TablePenjualan GROUP BY IdMCabang, TglTrans, IdTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales UNION ALL SELECT 4 AS Idx, IdMCabang, IdTrans, TglTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales, SUM(Qty * HrgStn) AS nilaijual, SUM(Qty * HPP) AS nilaihpp, 0 AS nilaikomisi FROM ( SELECT m.IdMCabang, m.IdTRJual AS IdTrans, m.TglTRJual AS TglTrans, m.BuktiTRJual AS BuktiTrans, MCust.KdMCust, MCust.NmMCust, MSales.KdMSales, MSales.NmMSales, d.IdMBrg, d.QtyTotal AS Qty, - (d.HrgStn) AS HrgStn, COALESCE(-d.HPP, 0) AS HPP FROM MGARTRJualD d LEFT OUTER JOIN MGARTRJual m ON ((d.IdMCabang = m.IdMCabang) AND (d.IdTRJual = m.IdTRJual)) LEFT OUTER JOIN MGARTJual TJual ON (TJual.IdMCabang = m.IdMCabangTJual AND TJual.IdTJual = m.IdTJual) LEFT OUTER JOIN MGARMCust MCust ON (m.IdMCabangMCust = MCust.IdMCabang AND m.IdMCust = MCust.IdMCust) LEFT OUTER JOIN MGARMSales MSales ON (TJual.IdMCabangMSales = MSales.IdMCabang AND TJual.IdMSales = MSales.IdMSales) WHERE (m.IdTJual <> 0) AND (m.Hapus = 0) AND (m.Void = 0) AND ((TglTRJual >= '${start}') AND (TglTRJual <= '${end}')) AND (MCust.Hapus = 0) AND (MCust.KdMCust LIKE '%%') AND (MCust.NmMCust LIKE '%%') AND (MSales.Hapus = 0) AND (MSales.KdMSales LIKE '%%') AND (MSales.NmMSales LIKE '%%')) TableReturPenjualan WHERE IdMBrg <> 0 GROUP BY IdMCabang, TglTrans, IdTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales UNION ALL SELECT 5 AS Idx, IdMCabang, IdTrans, TglTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales, SUM(Qty * HrgStn) AS nilaijual, SUM(HPP) AS nilaihpp, 0 AS nilaikomisi FROM (SELECT m.IdMCabang, m.IdTJual AS IdTrans, m.TglTJual AS TglTrans, CONCAT(m.BuktiTJual, ' (', ' Pembulatan ', ')') AS BuktiTrans, MCust.KdMCust, MCust.NmMCust, MSales.KdMSales, MSales.NmMSales, d.IdMBrg, 0 AS Qty, 0 AS HrgStn, ABS(COALESCE(LPembulatanKartuStock.HrgStn, 0)) AS HPP FROM MGARTJualD d LEFT OUTER JOIN MGARTJual m ON ((d.IdMCabang = m.IdMCabang) AND (d.IdTJual = m.IdTJual)) LEFT OUTER JOIN MGARMCust MCust ON (m.IdMCabangMCust = MCust.IdMCabang AND m.IdMCust = MCust.IdMCust) LEFT OUTER JOIN MGARMSales MSales ON (m.IdMCabangMSales = MSales.IdMCabang AND m.IdMSales = MSales.IdMSales) LEFT OUTER JOIN MGINLPembulatanKartuStock LPembulatanKartuStock ON (d.IdMCabang = LPembulatanKartuStock.IdMCabang AND d.IdTJual = LPembulatanKartuStock.IdTrans AND d.IdTJualD = LPembulatanKartuStock.IdTransD)  WHERE (m.Hapus = 0) AND (m.Void = 0) AND (MCust.Hapus = 0) AND (MSales.Hapus = 0) AND ((m.TglTJual >= '${start}') AND (m.TglTJual <= '${end}'))  AND d.IdMBrg <> 0 AND SUBSTRING(LPembulatanKartuStock.Keterangan, 14, 3) = 'RJL' AND (MCust.KdMCust LIKE '%%') AND (MCust.NmMCust LIKE '%%') AND (MSales.KdMSales LIKE '%%') AND (MSales.NmMSales LIKE '%%')) TablePenjualan GROUP BY IdMCabang, TglTrans, IdTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales ) SubTRLPenjualan ) TRLPenjualan LEFT OUTER JOIN MGSYMCabang MCabang ON (TRLPenjualan.IdMCabang = MCabang.IdMCabang) WHERE (MCabang.Hapus = 0 AND MCabang.IdMCabang = ${fil.idmcabang}) ${qcabang} ${qsales} ${qcustomer} AND tgltrans = '${tgltrans}' ORDER BY TglTrans`;
            const item = await sequelize.query(sql2, {
              raw: false,
            });

            console.log("sql2", item);

            var arr_item = item[0].map((item, index_dua) => {
              return {
                bukti: item.BuktiTrans,
                customer: item.NmMCust,
                sales: item.NmMSales,
                jual: parseFloat(item.nilaijual),
                hpp: parseFloat(item.nilaihpp),
                labarugi: parseFloat(item.labarugi),
                persen: parseFloat(item.persenrl),
              };
            });

            return {
              tanggal: list.TglTrans,
              jual: parseFloat(list.nilaijual),
              hpp: parseFloat(list.nilaihpp),
              labarugi: parseFloat(list.labarugi),
              persen: parseFloat(list.persenrl),
              item: arr_item,
            };
          })
        );

        return {
          nama: fil.nmmcabang,
          jual: parseFloat(fil.nilaijual),
          hpp: parseFloat(fil.nilaihpp),
          labarugi: parseFloat(fil.labarugi),
          persen: parseFloat(fil.persenrl),
          list: arr_list,
        };
      })
    );

    res.json({
      message: "Success, laba rugi penjualan",
      data: arr_data,
    });
  }

  // PROGRESS LABA RUGI
  else if (jenis == 2) {
    let sql = `SELECT MCabang.KdMCabang, MCabang.nmmcabang, TblAll.idmcabang, Tgl, sum(NilaiJual) as nilaijual, sum(NilaiHPP) as nilaihpp, sum(NilaiBeli) as nilaibeli, sum(NilaiJual - NilaiHPP) AS rugilaba, sum(NilaiJual - NilaiBeli) AS rugilababeli FROM( SELECT IdMCabang, Tgl, SUM(NilaiJual) AS NilaiJual, SUM(NilaiHPP) AS NilaiHPP, SUM(NilaiBeli) AS NilaiBeli FROM(SELECT IdMCabang AS IdMCabang, Tgltjual AS Tgl, SUM(Qty * HrgStn) AS NilaiJual, SUM(Qty * HPP) AS NilaiHPP, SUM(Qty * HrgBeliAk) as NilaiBeli FROM(SELECT m.IdMCabang, m.Tgltjual, (d.HrgStn - COALESCE(d.DiscV, 0)) * (1 - (COALESCE(m.DiscV, 0)/m.Bruto)) AS HrgStn, COALESCE(d.HPP, 0) AS HPP, d.qtyTotal AS Qty, beli.HrgBeliAk FROM mgartjuald d LEFT OUTER JOIN mgartjual m ON (m.IdMCabang = d.IdMCabang AND m.Idtjual = d.Idtjual) LEFT OUTER JOIN (SELECT MAX(TglBeliAk) AS TglBeliAk, MAX(HrgBeliAk) AS HrgBeliAk, IdMCabang, IdMbrg FROM (SELECT m.TglTBeli AS TglBeliAk, m.TglCreate, m.IdMCabang, d.IdMBrg, COALESCE(d.HrgStn * ((100 - d.DiscP) / 100) * ((100 - m.DiscP) / 100) * ((100 + 0) / 100), 0) AS HrgBeliAk FROM MGAPTBeliD d LEFT OUTER JOIN MGAPTBeli m ON (d.IdMCabang = m.IdMCabang AND d.IdTBeli = m.IdTBeli) WHERE m.Hapus = 0 AND m.Void = 0 UNION ALL SELECT TglTSABrg AS TglBeliAk, TglCreate, IdMCabang, IdMBrg, COALESCE(HPP, 0) AS HrgBeliAk FROM MGINTSABrg) TabelBeli GROUP BY idmcabang, IdMBrg ORDER BY TglBeliAk DESC, IdMCabang, IdMBrg) beli ON (beli.IdMCabang = m.IdMCabang AND beli.IdMbrg = d.IdMbrg) WHERE m.Hapus = 0 AND m.Void = 0 AND m.Tgltjual >= '${start}' AND m.Tgltjual <='${end}') Jual GROUP BY IdMCabang, Tgltjual) TblTrans GROUP BY IdMCabang, Tgl) tblall LEFT OUTER JOIN MGSYMCabang MCabang ON (TblAll.IdMCabang = MCabang.IdMCabang) WHERE (MCabang.Hapus = 0) GROUP BY idmcabang ORDER BY KdMCabang, Tgl`;
    const filter = await sequelize.query(sql, {
      raw: false,
    });

    var arr_data = await Promise.all(
      filter[0].map(async (fil, index) => {
        let sql1 = `SELECT MCabang.KdMCabang, MCabang.NmMCabang, TblAll.IdMCabang, Tgl, NilaiJual, NilaiHPP, NilaiBeli,(NilaiJual - NilaiHPP) AS RugiLaba, (NilaiJual - NilaiBeli) AS RugiLabaBeli FROM( SELECT IdMCabang, Tgl, SUM(NilaiJual) AS NilaiJual, SUM(NilaiHPP) AS NilaiHPP, SUM(NilaiBeli) AS NilaiBeli FROM(SELECT IdMCabang AS IdMCabang, Tgltjual AS Tgl, SUM(Qty * HrgStn) AS NilaiJual, SUM(Qty * HPP) AS NilaiHPP, SUM(Qty * HrgBeliAk) as NilaiBeli FROM(SELECT m.IdMCabang, m.Tgltjual, (d.HrgStn - COALESCE(d.DiscV, 0)) * (1 - (COALESCE(m.DiscV, 0)/m.Bruto)) AS HrgStn, COALESCE(d.HPP, 0) AS HPP, d.qtyTotal AS Qty, beli.HrgBeliAk FROM mgartjuald d LEFT OUTER JOIN mgartjual m ON (m.IdMCabang = d.IdMCabang AND m.Idtjual = d.Idtjual) LEFT OUTER JOIN (SELECT MAX(TglBeliAk) AS TglBeliAk, MAX(HrgBeliAk) AS HrgBeliAk, IdMCabang, IdMbrg FROM (SELECT m.TglTBeli AS TglBeliAk, m.TglCreate, m.IdMCabang, d.IdMBrg, COALESCE(d.HrgStn * ((100 - d.DiscP) / 100) * ((100 - m.DiscP) / 100) * ((100 + 0) / 100), 0) AS HrgBeliAk FROM MGAPTBeliD d LEFT OUTER JOIN MGAPTBeli m ON (d.IdMCabang = m.IdMCabang AND d.IdTBeli = m.IdTBeli) WHERE m.Hapus = 0 AND m.Void = 0 UNION ALL SELECT TglTSABrg AS TglBeliAk, TglCreate, IdMCabang, IdMBrg, COALESCE(HPP, 0) AS HrgBeliAk FROM MGINTSABrg) TabelBeli GROUP BY idmcabang, IdMBrg ORDER BY TglBeliAk DESC, IdMCabang, IdMBrg) beli ON (beli.IdMCabang = m.IdMCabang AND beli.IdMbrg = d.IdMbrg) WHERE m.Hapus = 0 AND m.Void = 0 AND m.Tgltjual >= '${start}' AND m.Tgltjual <='${end}') Jual GROUP BY IdMCabang, Tgltjual) TblTrans GROUP BY IdMCabang, Tgl) tblall LEFT OUTER JOIN MGSYMCabang MCabang ON (TblAll.IdMCabang = MCabang.IdMCabang) WHERE (MCabang.Hapus = 0) and MCabang.idmcabang=${fil.idmcabang} ORDER BY KdMCabang, Tgl`;
        const list = await sequelize.query(sql1, {
          raw: false,
        });

        var arr_list = await Promise.all(
          list[0].map(async (list, index_satu) => {
            return {
              tanggal: list.Tgl,
              jual: parseFloat(list.NilaiJual),
              beli: parseFloat(list.NilaiBeli),
              hpp: parseFloat(list.NilaiHPP),
              labarugi: parseFloat(list.RugiLaba),
              labarugi_beli: parseFloat(list.RugiLabaBeli),
            };
          })
        );

        return {
          nama: fil.nmmcabang,
          jual: parseFloat(fil.nilaijual),
          beli: parseFloat(fil.nilaibeli),
          hpp: parseFloat(fil.nilaihpp),
          labarugi: parseFloat(fil.rugilaba),
          labarugi_beli: parseFloat(fil.rugilababeli),
          list: arr_list,
        };
      })
    );

    res.json({
      message: "Success, progress laba rugi penjualan",
      data: arr_data,
    });
  }

  // LABA RUGI
  else if (jenis == 3) {
    let date = req.body.tanggal || "2024-01-19";
    let sql = `SELECT idmcabang, nmmcabang FROM mgsymcabang where aktif=1 and hapus=0`;
    const filter = await sequelize.query(sql, {
      raw: false,
    });

    var total = 0;
    var arr_data = await Promise.all(
      filter[0].map(async (fil, index) => {
        var total = 0;
        var totalpenjualan = 0;
        var totalhpp = 0;
        var totallain = 0;

        let sql1 = `SELECT MCabang.KdMCabang, MCabang.NmMCabang, TableRL.* FROM (
                    SELECT 2 as Idx, 'Kotor' as Group2, 'Penjualan' as Group3, IdMCabang, 'Penjualan' AS Keterangan, sum(Jumlah) as Jumlah
                    FROM (
                        SELECT m.IdMCabang, IF(m.JenisTJual = 0, SUM(d.QtyTotal * (d.HrgStn - COALESCE(d.DiscV, 0)) * (1 - (COALESCE(m.DiscV, 0)/m.Bruto))), sum(QtyTotal * HrgStnSales)) AS Jumlah 
                        FROM MGARTJualD d 
                            LEFT OUTER JOIN MGARTJual m ON (d.IdMCabang = m.IdMCabang AND d.IdTJual = m.IdTJual) 
                        WHERE m.Hapus = 0 AND m.Void = 0 AND (TglTJual >= '${start}' AND TglTJual <= '${end}')
                        GROUP BY m.IdMCabang 
                        UNION ALL
                        SELECT m.IdMCabang, 0 AS Jumlah
                        FROM MGSYMCabang m
                    ) TablePenjualan
                    GROUP BY IdMCabang
                    
                    UNION ALL
                    
                    SELECT 3 as Idx, 'Kotor' as Group2, 'Penjualan' as Group3, IdMCabang, 'Retur Penjualan' AS Keterangan, sum(Jumlah) as Jumlah
                    FROM (
                        SELECT m.IdMCabang, - sum(QtyTotal * HrgStn) AS Jumlah
                        FROM MGARTRJualD d LEFT OUTER JOIN MGARTRJual m ON (d.IdMCabang = m.IdMCabang AND d.IdTRJual = m.IdTRJual) WHERE m.Hapus = 0 AND m.Void = 0 AND (TglTRJual >= '${start}' AND TglTRJual <= '${end}') GROUP BY m.IdMCabang
                        UNION ALL
                        SELECT m.IdMCabang, 0 AS Jumlah
                        FROM MGSYMCabang m
                    ) TableReturPenjualan
                    GROUP BY IdMCabang
                    
                    UNION ALL
                    
                    SELECT 4 as Idx, 'Kotor' as Group2, 'HPP' as Group3, IdMCabang, 'Harga Pokok Penjualan' AS Keterangan, - sum(Jumlah) as Jumlah
                    FROM (
                        SELECT df.IdMCabang, sum(df.QtyTotal * df.HPP) AS Jumlah 
                        FROM MGARTJualDF df LEFT OUTER JOIN MGARTJualD d ON (df.IdMCabang = d.IdMCabang AND df.IdTJualD = d.IdTJualD)
                                                LEFT OUTER JOIN MGARTJual m ON (d.IdMCabang = m.IdMCabang AND d.IdTJual = m.IdTJual)
                        WHERE m.Hapus = 0 AND m.Void = 0 AND (m.TglTJual >= '${start}' AND m.TglTJual <= '${end}') 
                        GROUP BY df.IdMCabang
                        UNION ALL
                        SELECT d.IdMCabang, SUM(d.QtyTotal * d.HrgStn) AS Jumlah
                        FROM mgaptbelid d
                            LEFT OUTER JOIN mgaptbeli m ON (m.IdMCabang = d.IdMCabang AND m.IdTBeli = d.IdTBeli)
                            LEFT OUTER JOIN mginmbrg brg ON (brg.IdMBrg = d.IdMBrg)
                        WHERE m.Hapus = 0 AND m.Void = 0 AND brg.Reserved_int1 = 2 AND (m.TglTBeli >= '${start}' AND m.TglTBeli <= '${end}') 
                        GROUP BY d.IdMCabang
                        UNION ALL
                        SELECT m.IdMCabang, - sum(QtyTotal * HPP) AS Jumlah
                        FROM MGARTRJualD d LEFT OUTER JOIN MGARTRJual m ON (d.IdMCabang = m.IdMCabang AND d.IdTRJual = m.IdTRJual) WHERE m.Hapus = 0 AND m.Void = 0 AND (TglTRJual >= '${start}' AND TglTRJual <= '${end}') GROUP BY m.IdMCabang
                        UNION ALL
                        SELECT m.IdMCabang, 0 AS Jumlah
                        FROM MGSYMCabang m
                    ) TableHPP
                    GROUP BY IdMCabang
                    
                    UNION ALL
                    SELECT 4.5 AS Idx, 'Kotor' AS Group2, 'HPP' AS Group3, IdMCabang, 'Barang Jasa' AS Keterangan, -SUM(Jumlah) AS Jumlah
                    FROM (
                        SELECT m.IdMCabang, SUM(QtyTotal * (HrgStn * (100 - d.DiscP)/100 * (100 - m.DiscP)/100 * (100 + m.PPNP)/100)) AS Jumlah
                        FROM MGAPTBeliD d
                            LEFT OUTER JOIN MGAPTBeli m ON (d.IdMCabang = m.IdMCabang AND d.IdTBeli = m.IdTBeli)
                            LEFT OUTER JOIN MGINMBrg MBrg ON (MBrg.IdMBrg = d.IdMBrg)
                            LEFT OUTER JOIN MGTRMTruck MTruck ON (MTruck.IdMTruck = d.IdMTruck)
                        WHERE m.Hapus = 0 AND m.Void = 0 AND (TglTBeli >= '${start}' AND TglTBeli <= '${end}')
                        AND MTruck.KdMTruck LIKE '%%'
                        AND MTruck.NoPol LIKE '%%'
                        AND MBrg.Reserved_int1 = 2
                        GROUP BY m.IdMCabang
                        UNION ALL
                        SELECT m.IdMCabang, 0 AS Jumlah
                        FROM MGSYMCabang m
                    ) TablePenjualan
                    GROUP BY IdMCabang
                    
                    UNION ALL
                    
                    SELECT 12 AS Idx, 'Operasional' AS Group2, 'Pendapatan & Biaya Lain-lain dari Kas' AS Group3, IdMCabang, 'Pendapatan dari Perkiraan' AS Keterangan, SUM(Jumlah) AS Jumlah
                    FROM (
                        SELECT m.IdMCabang, SUM(JMLBAYAR) AS Jumlah
                        FROM MGKBTTransferD d LEFT OUTER JOIN MGKBTTransfer m ON (m.IdMCabang = d.IdMCabang AND m.IdTTransfer = d.IdTTransfer) 
                                            LEFT OUTER JOIN MGGLMPrk p ON (p.IdMPrk = d.IdMRef AND d.JenisMRef = 'P' AND p.Periode = 0)
                        WHERE d.jenismref = 'P'
                        AND m.jenisttransfer = 'M'
                        AND p.jenismprkd IN (7, 12)
                        AND m.Hapus = 0 AND m.Void = 0 
                        AND m.TglTTransfer >= '${start}' AND m.TglTTransfer <= '${end}'
                        GROUP BY m.IdMCabang
                        UNION ALL
                        SELECT m.IdMCabang, 0 AS Jumlah
                        FROM MGSYMCabang m
                    ) TableBiayaKasKeluar
                    GROUP BY IdMCabang
                    
                    UNION ALL
                    
                    SELECT 13 AS Idx, 'Operasional' AS Group2, 'Pendapatan & Biaya Lain-lain dari Kas' AS Group3, IdMCabang, 'Biaya dari Perkiraan' AS Keterangan, SUM(Jumlah) AS Jumlah
                    FROM (
                        SELECT m.IdMCabang, -SUM(JMLBAYAR) AS Jumlah
                        FROM MGKBTTransferD d LEFT OUTER JOIN MGKBTTransfer m ON (m.IdMCabang = d.IdMCabang AND m.IdTTransfer = d.IdTTransfer) 
                                            LEFT OUTER JOIN MGGLMPrk p ON (p.IdMPrk = d.IdMRef AND d.JenisMRef = 'P' AND p.Periode = 0)
                        WHERE d.jenismref = 'P'
                        AND m.jenisttransfer = 'K'
                        AND p.jenismprkd IN (10, 11, 13)
                        AND m.Hapus = 0 AND m.Void = 0 
                        AND m.TglTTransfer >= '${start}' AND m.TglTTransfer <= '${end}'
                        GROUP BY m.IdMCabang
                        UNION ALL
                        SELECT m.IdMCabang, 0 AS Jumlah
                        FROM MGSYMCabang m
                    ) TableBiayaKasKeluar
                    GROUP BY IdMCabang
                    
                    ) TableRL LEFT OUTER JOIN MGSYMCabang MCabang ON (TableRL.IdMCabang = MCabang.IdMCabang)
                    WHERE MCabang.Hapus = 0
                    AND MCabang.Aktif = 1
                    AND MCabang.NmMCabang LIKE '%${fil.nmmcabang}%'
                    ORDER BY KdMCabang, Idx`;
        const laporan = await sequelize.query(sql1, {
          raw: false,
        });

        var arr_penjualan = [];
        var arr_hpp = [];
        var arr_lain = [];
        var arr_list = await Promise.all(
          laporan[0].map(async (list, index) => {
            total += parseFloat(list.Jumlah);
            if (list.Group3 == "Penjualan") {
              totalpenjualan += parseFloat(list.Jumlah);
              arr_penjualan.push({
                // "group": list.Group3,
                keterangan: list.Keterangan,
                jumlah: parseFloat(list.Jumlah),
              });
              // Object.assign(arr_penjualan, {
              //     "group": list.Group3,
              //     "keterangan": list.Keterangan,
              //     "jumlah": parseFloat(list.Jumlah)
              // });
            }
            if (list.Group3 == "HPP") {
              totalhpp += parseFloat(list.Jumlah);
              arr_hpp.push({
                // "group": list.Group3,
                keterangan: list.Keterangan,
                jumlah: parseFloat(list.Jumlah),
              });
              // Object.assign(arr_hpp, {
              //     "group": list.Group3,
              //     "keterangan": list.Keterangan,
              //     "jumlah": parseFloat(list.Jumlah)
              // });
            }
            if (list.Group3 == "Pendapatan & Biaya Lain-lain dari Kas") {
              totallain += parseFloat(list.Jumlah);
              arr_lain.push({
                // "group": list.Group3,
                keterangan: list.Keterangan,
                jumlah: parseFloat(list.Jumlah),
              });
              // Object.assign(arr_lain, {
              //     "group": list.Group3,
              //     "keterangan": list.Keterangan,
              //     "jumlah": parseFloat(list.Jumlah)
              // });
            }
          })
        );

        var penjualan = {
          total: totalpenjualan,
          data: arr_penjualan,
        };

        var hpp = {
          total: totalpenjualan,
          data: arr_hpp,
        };

        var lain = {
          total: totalpenjualan,
          data: arr_lain,
        };

        return {
          nama: fil.nmmcabang,
          labarugi: total,
          penjualan: penjualan,
          // "totalhpp" : totalhpp,
          hpp: hpp,
          // "totallain" : totallain,
          biayalain: lain,
          // "total" : total
        };
      })
    );

    res.json({
      message: "Success, report laba rugi",
      data: arr_data,
    });
  }
};
