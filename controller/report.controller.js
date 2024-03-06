// const db = require("../models");
// const sequelize = db.sequelize;
const qstock = require("../class/query_report/stock");

const fun = require("../mgmx");

let today = new Date().toJSON().slice(0, 10);

exports.tesRahman = async (req, res) => {
  let q = await qstock.queryPosisiStockWI();
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
  console.log("tesman");

  let jenis = req.body.jenis || 1;
  // posisi stock
  if (jenis == 1) {
    let date = req.body.tanggal || today;
    let qsql = await qstock.queryPosisiStockWI(date);
    const filter = await fun.getDataFromQuery(sequelize, qsql);

    var listitem = [];
    var listgudang = [];
    var arr_data = await Promise.all(
      filter.map(async (fil, index) => {
        // var item = fil;
        if (!listgudang.includes(fil.NmMGd)) {
          listgudang.push(fil.NmMGd);

          listitem.push({
            gudang: fil.NmMGd,
            list: [fil],
          });
        } else {
          let cek = listgudang.indexOf(fil.NmMGd);
          listitem[cek].list.push(fil);
        }

        // if (listgudang.length == 0 || (listgudang.length > 0 && fil.NmMGd != listgudang[0].gudang)){
        //     listgudang.push({
        //         "cabang": fil.NmMCabang,
        //         "gudang": fil.NmMGd,
        //         "list" : tesR
        //     })
        // }

        // listitem.push({
        //     "nmmbrg": '',
        //     "tgl": '',
        //     "bukti": 'dummy',
        //     "satuan": 'dummy',
        //     "qty": 0,
        // });
        // return {
        //     "cabang": fil.NmMCabang,
        //     "gudang": fil.NmMGd // "Rp&nbsp;"+ number_format(fil.tagihan, 2),
        //     // "list": arr_brg
        // }
        // let sql1 = `SELECT k.idmbrg, b.kdmbrg, b.nmmbrg, SUM(k.qtytotal) AS qty, s.nmmstn FROM mginlkartustock k LEFT OUTER JOIN mginmbrg b ON k.idmbrg = b.idmbrg LEFT OUTER JOIN mginmstn s ON b.idmstn1 = s.idmstn WHERE tgltrans <= '${date}' AND k.idmcabang = ${fil.idmcabang} AND idmgd=${fil.idmgd} GROUP BY k.idmbrg`;
        // const brg = await sequelize.query(sql1, {
        //     raw: false,
        // });

        // var arr_brg = await Promise.all(brg[0].map(async (brg, index_satu) => {
        //     let sql2 = `SELECT s.tgltrans, s.keterangan, ss.nmmstn, s.debet, s.kredit, SUM(s.saldo) as saldo FROM (SELECT tgltrans, idmbrg, idtrans, keterangan, IF(qtytotal >= 0, qtytotal, 0) AS debet, IF(qtytotal <= 0, qtytotal, 0) AS kredit, SUM(qtytotal) AS saldo FROM mginlkartustock WHERE STR_TO_DATE(tgltrans, '%Y-%m-%d') <= '${date}' AND idmbrg = ${brg.idmbrg} GROUP BY idtrans) s LEFT OUTER JOIN mginmbrg b ON b.idmbrg = s.idmbrg LEFT OUTER JOIN mginmstn ss ON ss.idmstn = b.idmstn1 GROUP BY s.keterangan ORDER BY s.tgltrans ASC`;
        //     const item = await sequelize.query(sql2, {
        //         raw: false,
        //     });

        //     var saldo = 0;
        //     var arr_item = item[0].map((item, index_dua) => {
        //         saldo += parseFloat(item.saldo);
        //         return {
        //             'tanggal': item.tgltrans,
        //             'keterangan': item.keterangan,
        //             'satuan': item.nmmstn,
        //             'debet': parseFloat(item.debet),
        //             'kredit': parseFloat(item.kredit),
        //             'saldo': parseFloat(item.saldo),
        //             // 'debet': item.debet,
        //             // 'kredit': item.kredit,
        //             // 'saldo': item.saldo,
        //         }
        //     })

        //     return {
        //         "id": brg.idmbrg,
        //         "kode": brg.kdmbrg,
        //         "nama": brg.nmmbrg,
        //         "qty": parseFloat(brg.qty),
        //         "satuan": brg.nmmstn,
        //         "listitem": arr_item,
        //     }
        // }))
      })
    );

    res.json({
      message: "Success",
      data: listitem,
    });
  }

  // kartu stock
  else if (jenis == 2) {
    let start = req.body.start || today;
    let end = req.body.end || today;

    let cabang = req.body.cabang || "";
    let qcabang = "";
    if (cabang != "") {
      qcabang = "AND c.idmcabang=" + cabang;
    }

    let gudang = req.body.gudang || "";
    let qgudang = "";
    if (gudang != "") {
      qgudang = "AND g.idmgd = " + gudang;
    }

    let barang = req.body.barang || "";
    let qbarang = "";
    if (barang != "") {
      qbarang = "AND b.idmbrg = " + barang;
    }
    console.log("logbrg", qbarang);
    console.log("logstart", start);
    console.log("logend", end);

    let sql = `SELECT c.idmcabang, c.nmmcabang, g.idmgd, g.nmmgd, b.idmbrg, b.kdmbrg, b.NmMBrg FROM mgsymcabang c LEFT OUTER JOIN mginlkartustock k ON c.idmcabang = k.idmcabang LEFT OUTER JOIN mgsymgd g ON k.idmgd = g.idmgd LEFT OUTER JOIN mginmbrg b ON k.idmbrg = b.idmbrg WHERE c.hapus = 0 AND k.tgltrans <= '${today}' ${qcabang} ${qgudang} GROUP BY c.nmmcabang`;
    const filter = await sequelize.query(sql, {
      raw: false,
    });

    var arr_data = await Promise.all(
      filter[0].map(async (fil, index) => {
        console.log("fil", fil);
        let sql1 = `select b.idmbrg, b.kdmbrg, b.nmmbrg from mginmbrg b left outer join mginlkartustock k on b.idmbrg = k.idmbrg where k.idmcabang = ${fil.idmcabang} and k.idmgd = ${fil.idmgd} ${qbarang} group by b.idmbrg`;
        const brg = await sequelize.query(sql1, {
          raw: false,
        });

        var arr_brg = await Promise.all(
          brg[0].map(async (brg, index_satu) => {
            console.log("brg", brg);

            let sql2 = `SELECT s.tgltrans, s.keterangan, ss.nmmstn, s.debet, s.kredit, SUM(s.saldo) as saldo FROM (SELECT '${start}' as tgltrans, idmbrg, idtrans, 'Saldo awal' AS keterangan, (0) AS debet, (0) AS kredit, SUM(qtytotal) AS saldo FROM mginlkartustock WHERE STR_TO_DATE(tgltrans, '%Y-%m-%d') < '${start}' AND idmbrg = ${brg.idmbrg} UNION ALL SELECT tgltrans, idmbrg, idtrans, keterangan, IF(qtytotal >= 0, qtytotal, 0) AS debet, IF(qtytotal <= 0, qtytotal, 0) AS kredit, SUM(qtytotal) AS saldo FROM mginlkartustock WHERE STR_TO_DATE(tgltrans, '%Y-%m-%d') between '${start}' AND '${end}' AND idmbrg = ${brg.idmbrg} GROUP BY idtrans) s LEFT OUTER JOIN mginmbrg b ON b.idmbrg = s.idmbrg LEFT OUTER JOIN mginmstn ss ON ss.idmstn = b.idmstn1 GROUP BY s.keterangan ORDER BY s.tgltrans ASC`;
            const item = await sequelize.query(sql2, {
              raw: false,
            });

            var saldo = 0;
            var arr_item = item[0].map((item, index_dua) => {
              saldo += parseFloat(item.saldo);
              return {
                tanggal: item.tgltrans,
                keterangan: item.keterangan,
                satuan: item.nmmstn,
                debet: parseFloat(item.debet),
                kredit: parseFloat(item.kredit),
                saldo: parseFloat(item.saldo),
                // 'debet': item.debet,
                // 'kredit': item.kredit,
                // 'saldo': item.saldo,
              };
            });

            return {
              // "id": brg.idmbrg,
              kode: brg.kdmbrg,
              nama: brg.nmmbrg,
              // "qty": parseFloat(brg.qty),
              // "satuan": brg.nmmstn,
              listitem: arr_item,
            };
          })
        );

        return {
          cabang: fil.nmmcabang,
          gudang: fil.nmmgd, // "Rp&nbsp;"+ number_format(fil.tagihan, 2),
          list: arr_brg,
        };
      })
    );

    res.json({
      message: "Success kartu",
      data: arr_data,
    });
  }
};

exports.kas = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  let jenis = req.body.jenis || 1;
  // posisi kas
  if (jenis == 1) {
    let date = req.body.tanggal || today;
    let sql = `SELECT idmcabang, nmmcabang FROM mgsymcabang where aktif=1 and hapus=0`;
    const filter = await sequelize.query(sql, {
      raw: false,
    });

    var arr_data = await Promise.all(
      filter[0].map(async (fil, index) => {
        let sql1 = `SELECT k.kdmkas, k.nmmkas, SUM(kas.jmlkas) AS total FROM mgkblkartukas kas LEFT OUTER JOIN mgkbmkas k ON kas.idmkas = k.idmkas WHERE k.aktif = 1 AND k.hapus = 0 AND STR_TO_DATE(kas.tgltrans, '%Y-%m-%d %H:%i:%s') <= '${date}' GROUP BY k.kdmkas`;
        const kas = await sequelize.query(sql1, {
          raw: false,
        });

        var arr_kas = await Promise.all(
          kas[0].map(async (item, index_satu) => {
            return {
              kode: item.kdmkas,
              nama: item.nmmkas,
              qty: parseFloat(item.total),
            };
          })
        );

        return {
          cabang: fil.nmmcabang,
          list: arr_kas,
        };
      })
    );

    var grandtotal = await fun.countDataFromQuery(
      sequelize,
      `SELECT SUM(jmlkas) AS total FROM mgkblkartukas`
    );

    var count = {
      grandtotal: grandtotal,
    };
    res.json({
      message: "Success",
      countData: count,
      data: arr_data,
    });
  }

  // kartu kas
  else if (jenis == 2) {
    let start = req.body.start || today;
    let end = req.body.end || today;

    let mkas = req.body.mkas || "";
    let qkas = "";
    if (mkas != "") {
      qkas = "AND k.idmkas=" + mkas;
    }

    let sql = `SELECT c.idmcabang, c.nmmcabang, g.idmgd, g.nmmgd, b.idmbrg, b.kdmbrg, b.NmMBrg FROM mgsymcabang c LEFT OUTER JOIN mginlkartustock k ON c.idmcabang = k.idmcabang LEFT OUTER JOIN mgsymgd g ON k.idmgd = g.idmgd LEFT OUTER JOIN mginmbrg b ON k.idmbrg = b.idmbrg WHERE c.hapus = 0 AND k.tgltrans <= '${today}' GROUP BY c.nmmcabang`;
    const filter = await sequelize.query(sql, {
      raw: false,
    });

    var arr_data = await Promise.all(
      filter[0].map(async (fil, index) => {
        let sql1 = `SELECT k.IdMKas, k.KdMKas, k.NmMKas FROM mgkblkartukas kas LEFT OUTER JOIN mgkbmkas k ON kas.idmkas = k.idmkas WHERE kas.idmcabang = ${fil.idmcabang} ${qkas} AND k.Aktif=1 AND k.Hapus=0 AND STR_TO_DATE(kas.TglTrans, '%Y-%m-%d') <= '${today}' GROUP BY k.idmkas`;
        const kas = await sequelize.query(sql1, {
          raw: false,
        });

        var arr_list = await Promise.all(
          kas[0].map(async (list, index_satu) => {
            let idmkas = list.IdMKas;
            let sql2 = `SELECT k.tgltrans, k.buktitrans, k.keterangan, k.debit, k.kredit, SUM(k.saldo) AS saldo FROM (SELECT kas.TglTrans, '-' AS buktitrans, 'Saldo Awal' AS keterangan, 0 AS debit, 0 AS kredit, SUM(kas.jmlkas) AS saldo FROM mgkblkartukas kas WHERE STR_TO_DATE(kas.TglTrans, '%Y-%m-%d') < '${start}' AND kas.idmkas=${idmkas} UNION ALL SELECT kas.TglTrans, kas.BuktiTrans, kas.Keterangan, IF(kas.jmlkas>=0,kas.jmlkas,0) AS debit, IF(kas.jmlkas<0,kas.jmlkas,0) AS kredit, SUM(kas.jmlkas) AS saldo FROM mgkblkartukas kas WHERE STR_TO_DATE(kas.TglTrans, '%Y-%m-%d') BETWEEN '${start}' AND '${end}' AND kas.idmkas=${idmkas} GROUP BY kas.BuktiTrans) k GROUP BY k.buktitrans ORDER BY k.tgltrans ASC`;
            const item = await sequelize.query(sql2, {
              raw: false,
            });

            var saldo = 0;
            var arr_item = item[0].map((item, index_dua) => {
              saldo += parseFloat(item.saldo);
              return {
                tanggal: item.tgltrans,
                keterangan: item.keterangan,
                debet: parseFloat(item.debit),
                kredit: parseFloat(item.kredit),
                saldo: parseFloat(item.saldo),
              };
            });

            return {
              kode: list.KdMKas,
              nama: list.NmMKas,
              listitem: arr_item,
            };
          })
        );

        return {
          cabang: fil.nmmcabang,
          list: arr_list,
        };
      })
    );

    res.json({
      message: "Success kartu",
      data: arr_data,
    });
  }
};

exports.bank = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  let jenis = req.body.jenis || 1;
  // posisi bank
  if (jenis == 1) {
    let date = req.body.tanggal || "2024-01-19";
    let sql = `SELECT idmcabang, nmmcabang FROM mgsymcabang where aktif=1 and hapus=0`;
    const filter = await sequelize.query(sql, {
      raw: false,
    });

    var arr_data = await Promise.all(
      filter[0].map(async (fil, index) => {
        let sql1 = `SELECT MCabang.KdMCabang, MCabang.NmMCabang, MCabang.Aktif,bank.nmmbank, MRek.kdmrek, MRek.nmmrek, MRek.Aktif, TablePosRek.PosRek FROM (SELECT TransAll.IdMCabang, IdMRek, SUM(JmlRek) AS PosRek FROM (SELECT k.TglTrans, k.IdMCabang, k.IdMRek, k.JmlRek FROM MGKBLKartuBank k UNION ALL SELECT '${date}' AS TglTrans, IdMCabang, IdMRek, 0 AS JmlRek FROM MGKBMRek) TransAll WHERE TglTrans < '${date}' GROUP BY TransAll.IdMCabang, IdMRek) TablePosRek LEFT OUTER JOIN MGSYMCabang MCabang ON (TablePosRek.IdMCabang = MCabang.IdMCabang) LEFT OUTER JOIN MGKBMRek MRek ON (TablePosRek.IdMCabang = MRek.IdMCabang AND TablePosRek.IdMRek = MRek.IdMRek) LEFT OUTER JOIN MGSYMUSerMRek MUserMRek ON (MUserMrek.IdMCabangMrek=Mrek.IdMCabang AND MUserMrek.IdMrek=Mrek.IdMrek) LEFT OUTER JOIN mgkbmbank bank ON mrek.IDMBANK=bank.IDMBANK WHERE MCabang.Hapus = 0 AND MCabang.Aktif = 1 AND MRek.Hapus = 0 AND MRek.Aktif = 1 ORDER BY MCabang.KdMCabang, MRek.NmMRek`;
        const bank = await sequelize.query(sql1, {
          raw: false,
        });

        var arr_item = await Promise.all(
          bank[0].map(async (item, index_satu) => {
            return {
              bank: item.nmmbank,
              kode: item.kdmrek,
              nama: item.nmmrek,
              qty: parseFloat(item.PosRek),
            };
          })
        );

        return {
          cabang: fil.nmmcabang,
          list: arr_item,
        };
      })
    );

    var grandtotal = await fun.countDataFromQuery(
      sequelize,
      `SELECT sum(TablePosRek.PosRek) as total FROM (SELECT TransAll.IdMCabang, IdMRek, Sum(JmlRek) as PosRek FROM (Select k.TglTrans, k.IdMCabang, k.IdMRek, k.JmlRek FROM MGKBLKartuBank k UNION ALL SELECT '${date}' as TglTrans, IdMCabang, IdMRek, 0 as JmlRek FROM MGKBMRek) TransAll WHERE TglTrans < '${date}' GROUP BY TransAll.IdMCabang, IdMRek) TablePosRek LEFT OUTER JOIN MGSYMCabang MCabang ON (TablePosRek.IdMCabang = MCabang.IdMCabang) LEFT OUTER JOIN MGKBMRek MRek ON (TablePosRek.IdMCabang = MRek.IdMCabang AND TablePosRek.IdMRek = MRek.IdMRek) LEFT OUTER JOIN MGSYMUSerMRek MUserMRek ON (MUserMrek.IdMCabangMrek=Mrek.IdMCabang AND MUserMrek.IdMrek=Mrek.IdMrek) WHERE MCabang.Hapus = 0 AND MCabang.Aktif = 1 AND MRek.Hapus = 0 AND MRek.Aktif = 1 AND PosRek <> 0 AND MUserMRek.IdMUser=1 ORDER BY MCabang.KdMCabang, MRek.NmMRek`
    );

    var count = {
      grandtotal: parseFloat(grandtotal),
    };

    res.json({
      message: "Success",
      countData: count,
      data: arr_data,
    });
  } else if (jenis == 2) {
    let start = req.body.start || today;
    let end = req.body.end || today;

    let bank = req.body.bank || "";
    let qbank = "";
    if (bank != "") {
      qbank = "AND bank.idmbank=" + bank;
    }

    let sql = `SELECT c.idmcabang, c.nmmcabang, g.idmgd, g.nmmgd, b.idmbrg, b.kdmbrg, b.NmMBrg FROM mgsymcabang c LEFT OUTER JOIN mginlkartustock k ON c.idmcabang = k.idmcabang LEFT OUTER JOIN mgsymgd g ON k.idmgd = g.idmgd LEFT OUTER JOIN mginmbrg b ON k.idmbrg = b.idmbrg WHERE c.hapus = 0 AND k.tgltrans >= '${start}' AND k.tgltrans <= '${end}' GROUP BY c.nmmcabang`;
    const filter = await sequelize.query(sql, {
      raw: false,
    });

    var arr_data = await Promise.all(
      filter[0].map(async (fil, index) => {
        let sql1 = `select mrek.idmrek, mrek.kdmrek, mrek.nmmrek, bank.idmbank, bank.nmmbank from mgkbmrek mrek left outer join mgkbmbank bank on mrek.idmbank = bank.idmbank where mrek.hapus=0 and mrek.aktif=1 and bank.aktif=1 and bank.hapus=0 ${qbank}`;
        const bank_data = await sequelize.query(sql1, {
          raw: false,
        });

        var arr_list = await Promise.all(
          bank_data[0].map(async (list, index_satu) => {
            let sql2 = `SELECT MCabang.KdMCabang, MCabang.NmMCabang, MRek.KdMRek, MRek.NmMRek, TableKartuRek.IdMRek, TableKartuRek.IdMCabang, Urut, BuktiTrans, NoRef, CAST(TglTrans as DATE) as TglTrans, TableKartuRek.Keterangan, Saldo, JmlRek, IF(Urut = 0, 0, IF(COALESCE(JmlRek,0) > 0,COALESCE(JmlRek,0), 0)) As Debit, IF(Urut = 0, 0, IF(COALESCE(JmlRek,0) >= 0, 0, COALESCE(JmlRek,0))) As Kredit, MCabang.IdMCabang, IdTrans, JenisTrans
                    FROM (
                    SELECT IdMCabang, IdMRek, 0 As Urut, 0 as JenisTrans, 0 as IdTrans, '-' As BuktiTrans, cast('${start} 00:00:00' as DateTime) As TglTrans, 0 As JmlRek, sum(JmlRek) As Saldo, 'Saldo Sebelumnya' As Keterangan
                        , '-' As NoRef FROM (
                    SELECT IdMCabang, IdMRek, 0 As JmlRek FROM MGKBMRek
                    UNION ALL
                    SELECT IdMCabang, IdMRek, JmlRek FROM MGKBLKartuBank where CAST(TglTrans as DATE) < CAST('${start} 00:00:00' AS DATE)
                    ) TableSaldoAwal
                    GROUP BY IdMCabang, IdMRek
                    UNION ALL
                    SELECT IdMCabang, IdMRek, 1 as Urut, JenisTrans, IdTrans, BuktiTrans, TglTrans, JmlRek, 0, Keterangan 
                        , NoRef
                    FROM MGKBLKartuBank
                    WHERE CAST(TglTrans as DATE) >= CAST('${start} 00:00:00' AS DATE) and CAST(TglTrans as DATE) <= CAST('${end} 00:00:00' AS DATE)
                    ) TableKartuRek LEFT OUTER JOIN MGSYMCabang MCabang ON (TableKartuRek.IdMCabang = MCabang.IdMCabang)
                                    LEFT OUTER JOIN MGKBMRek MRek ON (TableKartuRek.IdMCabang = MRek.IdMCabang AND TableKartuRek.IdMRek = MRek.IdMRek)
                        LEFT OUTER JOIN MGSYMUSerMRek MUserMRek ON (MUserMrek.IdMCabangMrek=Mrek.IdMCabang AND MUserMrek.IdMrek=Mrek.IdMrek)
                        left outer join mgkbmbank bank on mrek.idmbank = bank.idmbank
                    WHERE MCabang.Hapus = 0
                    AND MRek.Hapus = 0
                    AND MRek.IdMRek LIKE '%${list.idmrek}%'
                    ${qbank}
                    ORDER BY TableKartuRek.IdMCabang, TableKartuRek.IdMRek, Urut, TglTrans, JenisTrans, IdTrans`;
            const barang = await sequelize.query(sql2, {
              raw: false,
            });

            var saldo = 0;
            var arr_item = await Promise.all(
              barang[0].map(async (item, index_dua) => {
                var total = parseFloat(item.Debit) + parseFloat(item.Kredit);
                if (item.Saldo == 0) {
                  total = parseFloat(item.Debit) + parseFloat(item.Kredit);
                } else {
                  total = parseFloat(item.Saldo);
                }
                saldo += parseFloat(total);

                return {
                  tanggal: item.TglTrans,
                  keterangan: item.Keterangan,
                  debet: parseFloat(item.Debit),
                  kredit: parseFloat(Math.abs(item.Kredit)),
                  saldo: parseFloat(saldo),
                };
              })
            );

            return {
              bank: list.nmmbank,
              kode: list.kdmrek,
              nama: list.nmmrek,
              list: arr_item,
            };
          })
        );

        return {
          cabang: fil.nmmcabang,
          list: arr_list,
        };
      })
    );

    res.json({
      message: "Success",
      data: arr_data,
    });
  }
};

exports.hutang = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  let jenis = req.body.jenis || 1;

  // posisi hutang
  if (jenis == 1) {
    let date = req.body.tanggal || "2024-01-19";
    let sql = `SELECT idmcabang, nmmcabang FROM mgsymcabang where aktif=1 and hapus=0`;
    const filter = await sequelize.query(sql, {
      raw: false,
    });

    var arr_data = await Promise.all(
      filter[0].map(async (fil, index) => {
        let sql1 = `SELECT MSup.kdmsup, MSup.nmmsup, MSup.aktif, TablePosHut.poshut
                FROM (
                SELECT IdMSup, SUM(JmlHut) AS poshut FROM (
                SELECT TglTrans, IdMSup, JmlHut 
            FROM (
            SELECT IdMSup 
                    , 0 AS JenisTrans 
                    , IdTSAHut AS IdTrans 
                    , BuktiTSAHut AS BuktiTrans 
                    , CONCAT(DATE(TglTSAHut), ' ', TIME(TglUpdate)) AS TglTrans 
                    , JmlHut 
                    , 'Saldo Awal' AS Keterangan 
                FROM MGAPTSAHut 
                WHERE JmlHut <> 0 
                UNION ALL 
                SELECT IdMSup 
                    , 1 AS JenisTrans 
                    , IdTBeli AS IdTrans 
                    , IF(BuktiTBeli = '', BuktiTLPB, BuktiTBeli) AS BuktiTrans 
                    , CONCAT(DATE(TglTBeli), ' ', TIME(TglUpdate)) AS TglTrans 
                    , (Netto - JmlBayarTunai) AS JmlHut 
                    , CONCAT('Pembelian ', BuktiTBeli) AS Keterangan 
                FROM MGAPTBeli 
                WHERE Hapus = 0 
                AND Void = 0 
                AND (Netto - JmlBayarTunai) <> 0 
                AND HapusLPB = 0 AND VoidLPB = 0 
                AND BuktiTBeli <> ''
                UNION ALL 
                SELECT IdMSup 
                    , 1.1 AS JenisTrans 
                    , IdTBeliLain AS IdTrans 
                    , BuktiTBeliLain AS BuktiTrans 
                    , CONCAT(DATE(TglTBeliLain), ' ', TIME(TglUpdate)) AS TglTrans 
                    , (JmlBayarKredit) AS JmlHut 
                    , CONCAT('Biaya Lain-Lain ', BuktiTBeliLain) AS Keterangan 
                FROM MGAPTBeliLain 
                WHERE Hapus = 0 
                AND Void = 0 
                AND (JmlBayarKredit) <> 0 
                UNION ALL 
                SELECT IdMSup 
                    , 2 AS JenisTrans 
                    , IdTRBeli AS IdTrans 
                    , BuktiTRBeli AS BuktiTrans 
                    , CONCAT(DATE(TglTRBeli), ' ', TIME(TglUpdate)) AS TglTrans 
                    , - (Netto - JmlBayarTunai) AS JmlHut 
                    , CONCAT('Retur Pembelian ', BuktiTRBeli) AS Keterangan 
                FROM MGAPTRBeli 
                WHERE Hapus = 0 
                AND Void = 0 
                AND JenisTRBeli = 0 
                AND (Netto - JmlBayarTunai) <> 0 
                UNION ALL 
                SELECT IdMSup 
                    , 3 AS JenisTrans 
                    , IdTPBeli AS IdTrans 
                    , BuktiTPBeli AS BuktiTrans 
                    , CONCAT(DATE(TglTPBeli), ' ', TIME(TglUpdate)) AS TglTrans 
                    , - Total AS JmlHut 
                    , CONCAT('Potongan Pembelian ', BuktiTPBeli) AS Keterangan 
                FROM MGAPTPBeli 
                WHERE Hapus = 0 
                AND Void = 0 
                AND Total <> 0 
                UNION ALL 
                SELECT IdMSup 
                    , 4 AS JenisTrans 
                    , IdTBHut AS IdTrans 
                    , BuktiTBHut AS BuktiTrans 
                    , CONCAT(DATE(TglTBHut), ' ', TIME(TglUpdate)) AS TglTrans 
                    , - Total AS JmlHut 
                    , CONCAT('Pembayaran Hutang ', BuktiTBHut) AS Keterangan 
                FROM MGAPTBHut 
                WHERE Hapus = 0 
                AND Void = 0 
                AND Total <> 0 
                UNION ALL 
                SELECT hut.IdMSup
                    , 4.1 AS JenisTrans
                    , Hut.IdTBHut AS IdTrans
                    , BuktiTBHut AS BuktiTrans
                    , CONCAT(DATE(Hut.TglTBHut), ' ', TIME(Hut.TglUpdate)) AS tgltrans
                    , HutDB.JmlBayar AS JmlHut
                    , CONCAT('Titipan Giro ', Hut.BuktiTBHut, ' (', giro.kdmgiro, ')') AS Keterangan
                FROM MGAPTBHUTDB HutDB
                    LEFT OUTER JOIN MGAPTBHut Hut ON (hut.idmcabang = hutDB.idmcabang AND hut.idtbhut = hutdb.idtbhut)
                    LEFT OUTER JOIN mgapmsup sup ON (sup.idmsup = hut.idmsup)
                    LEFT OUTER JOIN MGKBMGiro giro ON (giro.idmcabang = HutDB.IdMCabangMRef AND giro.IdMGiro = HutDB.IdMRef)
                WHERE (Hut.Hapus = 0 AND Hut.Void = 0) AND HutDB.JenisMRef = 'G'
                UNION ALL 
                SELECT sup.idmsup
                    , 4.2 AS JenisTrans
                    , m.idtgirocair AS IdTrans
                    , m.BuktiTGiroCair AS BuktiTrans
                    , CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) AS TglTrans
                    , - HutDB.JmlBayar AS JmlHut
                    , CONCAT('Giro Cair ', m.Buktitgirocair, ' (', giro.kdmgiro, ')') AS Keterangan
                FROM MGKBTGiroCairD d
                    LEFT OUTER JOIN MGKBTGiroCair m ON (m.IdMCabang = d.IdMCabang AND d.idtgirocair = m.idtgirocair)
                    LEFT OUTER JOIN MGKBMGiro giro ON (giro.idmcabang = d.idmcabangmgiro AND giro.idmgiro = d.idmgiro)
                    LEFT OUTER JOIN MGAPMSup sup ON (sup.idmsup = giro.idmsup AND giro.jenismgiro = 'K')
                    LEFT OUTER JOIN MGAPTBHutDB HutDB ON (HutDB.IdMCabangMRef = giro.IdMCabang AND HutDB.IdMRef = giro.IdMGiro AND HutDB.JenisMRef = 'G')
                    LEFT OUTER JOIN MGAPTBHut Hut ON (Hut.IdMCabang = HutDB.IdMCabang AND hut.idtbhut = hutDB.idtbhut)
                WHERE (m.Hapus = 0 AND m.Void = 0) AND m.JenisTGiroCair = 'K' AND HUTDB.JenisMRef = 'G' AND HUT.Void = 0 AND HUT.Hapus = 0
                UNION ALL 
                SELECT Sup.idmsup 
                    , 4.3 AS JenisTrans 
                    , m.idtgirotolak AS IdTrans 
                    , m.buktitgirotolak AS BuktiTrans 
                    , CONCAT(DATE(m.tgltgirotolak), ' ', TIME(m.tglupdate)) AS TglTrans
                    , HutDB.jmlbayar AS JmlHut 
                    , CONCAT('Giro Tolak ', m.buktitgirotolak, ' (', giro.kdmgiro, ')') AS keterangan
                FROM MGKBTGiroTolakD d
                    LEFT OUTER JOIN MGKBTGiroTolak m ON (m.IdMCabang = d.IdMCabang AND m.IdTGiroTolak = d.IdTGiroTolak)
                    LEFT OUTER JOIN MGKBMGiro giro ON (giro.IdMCabang = d.IdMCabangMGiro AND giro.IdMGiro = d.IdMGiro)
                    LEFT OUTER JOIN MGAPMSup sup ON (sup.IdMSup = giro.IdMSup AND giro.JenisMGiro = 'K')
                    LEFT OUTER JOIN MGAPTBHutDB HutDB ON (HutDB.IdMCabangMRef = giro.IdMCabang AND HutDB.IdMRef = giro.IdMGiro AND HutDB.JenisMRef = 'G')
                    LEFT OUTER JOIN MGAPTBHut Hut ON (Hut.IdMCabang = HutDB.IdMCabang AND Hut.IdTBHut = HutDB.IdTBHut)
                WHERE (m.Hapus = 0 AND m.Void = 0)
                    AND m.JenisTGiroTolak = 'K' AND HutDB.JenisMRef = 'G' 
                    AND Hut.Void = 0 AND Hut.Hapus = 0
                UNION ALL 
                SELECT Sup.IdMSup AS IdMSup 
                    , 4.4 AS JenisTrans 
                    , m.IdTGiroGanti AS IdTrans 
                    , m.BuktiTGiroGanti AS BuktiTrans
                    , CONCAT(DATE(m.tgltgiroganti), ' ', TIME(m.tglupdate)) AS tgltrans
                    , -d.JmlBayar AS JmlHut
                    , CONCAT('Penggantian Giro ', m.buktitgiroganti, ' (', giro.kdmgiro, ')') AS Keterangan
                FROM MGKBTGiroGantiDG d
                    LEFT OUTER JOIN MGKBTGiroGanti m ON (m.IdMCabang = d.IdMCabang AND m.IDTGiroGanti = d.IDTGiroGanti)
                    LEFT OUTER JOIN MGKBMGiro giro ON (giro.IdMCabang = d.IdMCabangMGiro AND giro.IdMGiro = d.IdMGiro)
                    LEFT OUTER JOIN MGAPMSup Sup ON (Sup.IdMSup = giro.IdMSup AND Giro.JenisMGiro = 'K')
                    LEFT OUTER JOIN MGAPTBHutDB HutDB ON (HutDB.IdMCabangMRef = giro.IdMCabang AND HutDB.IdMRef = Giro.IdMGiro AND HutDB.JenisMREF = 'G')
                    LEFT OUTER JOIN MGAPTBHut Hut ON (Hut.IdMCabang = HutDB.IdMCabang AND Hut.IdTBHut = HutDB.IdTBHut)
                WHERE (m.Hapus = 0 AND m.Void = 0)
                    AND m.JenisTGiroGanti = 'K' AND HutDB.JenisMRef = 'G'
                    AND Hut.Void = 0 AND Hut.Hapus = 0
                UNION ALL 
                SELECT IdMSup 
                    , 5 AS JenisTrans 
                    , IdTKorHut AS IdTrans 
                    , BuktiTKorHut AS BuktiTrans 
                    , CONCAT(DATE(TglTKorHut), ' ', TIME(TglUpdate)) AS TglTrans 
                    , Total AS JmlHut 
                    , CONCAT('Koreksi Hutang ', BuktiTKorHut) AS Keterangan 
                FROM MGAPTKorHut 
                WHERE Hapus = 0 AND Void = 0 
                AND Total <> 0 
            
            ) Tbl
            
                UNION ALL
                SELECT '${date}' AS TglTrans, IdMSup, 0 AS PosHut FROM MGAPMSup
                ) TransAll
                WHERE TglTrans <= '${date}'
                GROUP BY IdMSup
            ) TablePosHut LEFT OUTER JOIN MGAPMSup MSup ON (TablePosHut.IdMSup = MSup.IdMSup)
            WHERE MSup.Hapus = 0
                AND MSup.Aktif = 1
            AND poshut > 0
            ORDER BY MSup.NmMSup`;
        const kas = await sequelize.query(sql1, {
          raw: false,
        });

        var total1 = 0;
        var arr_item = await Promise.all(
          kas[0].map(async (item, index_satu) => {
            var poshut = parseFloat(item.poshut);
            total1 += poshut;
            return {
              kode: item.kdmsup,
              nama: item.nmmsup,
              qty: parseFloat(item.poshut),
            };
          })
        );

        return {
          cabang: fil.nmmcabang,
          total: total1,
          list: arr_item,
        };
      })
    );

    const total = await fun.countDataFromQuery(
      sequelize,
      `SELECT MSup.KdMSup, MSup.NmMSup, MSup.Aktif
            , sum(TablePosHut.PosHut) as total
        FROM (
            SELECT IdMSup, SUM(JmlHut) AS PosHut FROM (
            SELECT TglTrans, IdMSup, JmlHut 
        FROM (
        SELECT IdMSup 
                , 0 AS JenisTrans 
                , IdTSAHut AS IdTrans 
                , BuktiTSAHut AS BuktiTrans 
                , CONCAT(DATE(TglTSAHut), ' ', TIME(TglUpdate)) AS TglTrans 
                , JmlHut 
                , 'Saldo Awal' AS Keterangan 
            FROM MGAPTSAHut 
            WHERE JmlHut <> 0 
            UNION ALL 
            SELECT IdMSup 
                , 1 AS JenisTrans 
                , IdTBeli AS IdTrans 
                , IF(BuktiTBeli = '', BuktiTLPB, BuktiTBeli) AS BuktiTrans 
                , CONCAT(DATE(TglTBeli), ' ', TIME(TglUpdate)) AS TglTrans 
                , (Netto - JmlBayarTunai) AS JmlHut 
                , CONCAT('Pembelian ', BuktiTBeli) AS Keterangan 
            FROM MGAPTBeli 
            WHERE Hapus = 0 
            AND Void = 0 
            AND (Netto - JmlBayarTunai) <> 0 
            AND HapusLPB = 0 AND VoidLPB = 0 
            AND BuktiTBeli <> ''
            UNION ALL 
            SELECT IdMSup 
                , 1.1 AS JenisTrans 
                , IdTBeliLain AS IdTrans 
                , BuktiTBeliLain AS BuktiTrans 
                , CONCAT(DATE(TglTBeliLain), ' ', TIME(TglUpdate)) AS TglTrans 
                , (JmlBayarKredit) AS JmlHut 
                , CONCAT('Biaya Lain-Lain ', BuktiTBeliLain) AS Keterangan 
            FROM MGAPTBeliLain 
            WHERE Hapus = 0 
            AND Void = 0 
            AND (JmlBayarKredit) <> 0 
            UNION ALL 
            SELECT IdMSup 
                , 2 AS JenisTrans 
                , IdTRBeli AS IdTrans 
                , BuktiTRBeli AS BuktiTrans 
                , CONCAT(DATE(TglTRBeli), ' ', TIME(TglUpdate)) AS TglTrans 
                , - (Netto - JmlBayarTunai) AS JmlHut 
                , CONCAT('Retur Pembelian ', BuktiTRBeli) AS Keterangan 
            FROM MGAPTRBeli 
            WHERE Hapus = 0 
            AND Void = 0 
            AND JenisTRBeli = 0 
            AND (Netto - JmlBayarTunai) <> 0 
            UNION ALL 
            SELECT IdMSup 
                , 3 AS JenisTrans 
                , IdTPBeli AS IdTrans 
                , BuktiTPBeli AS BuktiTrans 
                , CONCAT(DATE(TglTPBeli), ' ', TIME(TglUpdate)) AS TglTrans 
                , - Total AS JmlHut 
                , CONCAT('Potongan Pembelian ', BuktiTPBeli) AS Keterangan 
            FROM MGAPTPBeli 
            WHERE Hapus = 0 
            AND Void = 0 
            AND Total <> 0 
            UNION ALL 
            SELECT IdMSup 
                , 4 AS JenisTrans 
                , IdTBHut AS IdTrans 
                , BuktiTBHut AS BuktiTrans 
                , CONCAT(DATE(TglTBHut), ' ', TIME(TglUpdate)) AS TglTrans 
                , - Total AS JmlHut 
                , CONCAT('Pembayaran Hutang ', BuktiTBHut) AS Keterangan 
            FROM MGAPTBHut 
            WHERE Hapus = 0 
            AND Void = 0 
            AND Total <> 0 
            UNION ALL 
            SELECT hut.IdMSup
                , 4.1 AS JenisTrans
                , Hut.IdTBHut AS IdTrans
                , BuktiTBHut AS BuktiTrans
                , CONCAT(DATE(Hut.TglTBHut), ' ', TIME(Hut.TglUpdate)) AS tgltrans
                , HutDB.JmlBayar AS JmlHut
                , CONCAT('Titipan Giro ', Hut.BuktiTBHut, ' (', giro.kdmgiro, ')') AS Keterangan
            FROM MGAPTBHUTDB HutDB
                LEFT OUTER JOIN MGAPTBHut Hut ON (hut.idmcabang = hutDB.idmcabang AND hut.idtbhut = hutdb.idtbhut)
                LEFT OUTER JOIN mgapmsup sup ON (sup.idmsup = hut.idmsup)
                LEFT OUTER JOIN MGKBMGiro giro ON (giro.idmcabang = HutDB.IdMCabangMRef AND giro.IdMGiro = HutDB.IdMRef)
            WHERE (Hut.Hapus = 0 AND Hut.Void = 0) AND HutDB.JenisMRef = 'G'
            UNION ALL 
            SELECT sup.idmsup
                , 4.2 AS JenisTrans
                , m.idtgirocair AS IdTrans
                , m.BuktiTGiroCair AS BuktiTrans
                , CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) AS TglTrans
                , - HutDB.JmlBayar AS JmlHut
                , CONCAT('Giro Cair ', m.Buktitgirocair, ' (', giro.kdmgiro, ')') AS Keterangan
            FROM MGKBTGiroCairD d
                LEFT OUTER JOIN MGKBTGiroCair m ON (m.IdMCabang = d.IdMCabang AND d.idtgirocair = m.idtgirocair)
                LEFT OUTER JOIN MGKBMGiro giro ON (giro.idmcabang = d.idmcabangmgiro AND giro.idmgiro = d.idmgiro)
                LEFT OUTER JOIN MGAPMSup sup ON (sup.idmsup = giro.idmsup AND giro.jenismgiro = 'K')
                LEFT OUTER JOIN MGAPTBHutDB HutDB ON (HutDB.IdMCabangMRef = giro.IdMCabang AND HutDB.IdMRef = giro.IdMGiro AND HutDB.JenisMRef = 'G')
                LEFT OUTER JOIN MGAPTBHut Hut ON (Hut.IdMCabang = HutDB.IdMCabang AND hut.idtbhut = hutDB.idtbhut)
            WHERE (m.Hapus = 0 AND m.Void = 0) AND m.JenisTGiroCair = 'K' AND HUTDB.JenisMRef = 'G' AND HUT.Void = 0 AND HUT.Hapus = 0
            UNION ALL 
            SELECT Sup.idmsup 
                , 4.3 AS JenisTrans 
                , m.idtgirotolak AS IdTrans 
                , m.buktitgirotolak AS BuktiTrans 
                , CONCAT(DATE(m.tgltgirotolak), ' ', TIME(m.tglupdate)) AS TglTrans
                , HutDB.jmlbayar AS JmlHut 
                , CONCAT('Giro Tolak ', m.buktitgirotolak, ' (', giro.kdmgiro, ')') AS keterangan
            FROM MGKBTGiroTolakD d
                LEFT OUTER JOIN MGKBTGiroTolak m ON (m.IdMCabang = d.IdMCabang AND m.IdTGiroTolak = d.IdTGiroTolak)
                LEFT OUTER JOIN MGKBMGiro giro ON (giro.IdMCabang = d.IdMCabangMGiro AND giro.IdMGiro = d.IdMGiro)
                LEFT OUTER JOIN MGAPMSup sup ON (sup.IdMSup = giro.IdMSup AND giro.JenisMGiro = 'K')
                LEFT OUTER JOIN MGAPTBHutDB HutDB ON (HutDB.IdMCabangMRef = giro.IdMCabang AND HutDB.IdMRef = giro.IdMGiro AND HutDB.JenisMRef = 'G')
                LEFT OUTER JOIN MGAPTBHut Hut ON (Hut.IdMCabang = HutDB.IdMCabang AND Hut.IdTBHut = HutDB.IdTBHut)
            WHERE (m.Hapus = 0 AND m.Void = 0)
                AND m.JenisTGiroTolak = 'K' AND HutDB.JenisMRef = 'G' 
                AND Hut.Void = 0 AND Hut.Hapus = 0
            UNION ALL 
            SELECT Sup.IdMSup AS IdMSup 
                , 4.4 AS JenisTrans 
                , m.IdTGiroGanti AS IdTrans 
                , m.BuktiTGiroGanti AS BuktiTrans
                , CONCAT(DATE(m.tgltgiroganti), ' ', TIME(m.tglupdate)) AS tgltrans
                , -d.JmlBayar AS JmlHut
                , CONCAT('Penggantian Giro ', m.buktitgiroganti, ' (', giro.kdmgiro, ')') AS Keterangan
            FROM MGKBTGiroGantiDG d
                LEFT OUTER JOIN MGKBTGiroGanti m ON (m.IdMCabang = d.IdMCabang AND m.IDTGiroGanti = d.IDTGiroGanti)
                LEFT OUTER JOIN MGKBMGiro giro ON (giro.IdMCabang = d.IdMCabangMGiro AND giro.IdMGiro = d.IdMGiro)
                LEFT OUTER JOIN MGAPMSup Sup ON (Sup.IdMSup = giro.IdMSup AND Giro.JenisMGiro = 'K')
                LEFT OUTER JOIN MGAPTBHutDB HutDB ON (HutDB.IdMCabangMRef = giro.IdMCabang AND HutDB.IdMRef = Giro.IdMGiro AND HutDB.JenisMREF = 'G')
                LEFT OUTER JOIN MGAPTBHut Hut ON (Hut.IdMCabang = HutDB.IdMCabang AND Hut.IdTBHut = HutDB.IdTBHut)
            WHERE (m.Hapus = 0 AND m.Void = 0)
                AND m.JenisTGiroGanti = 'K' AND HutDB.JenisMRef = 'G'
                AND Hut.Void = 0 AND Hut.Hapus = 0
            UNION ALL 
            SELECT IdMSup 
                , 5 AS JenisTrans 
                , IdTKorHut AS IdTrans 
                , BuktiTKorHut AS BuktiTrans 
                , CONCAT(DATE(TglTKorHut), ' ', TIME(TglUpdate)) AS TglTrans 
                , Total AS JmlHut 
                , CONCAT('Koreksi Hutang ', BuktiTKorHut) AS Keterangan 
            FROM MGAPTKorHut 
            WHERE Hapus = 0 AND Void = 0 
            AND Total <> 0 
        
        ) Tbl
        
            UNION ALL
            SELECT '${date}' AS TglTrans, IdMSup, 0 AS PosHut FROM MGAPMSup
            ) TransAll
            WHERE TglTrans <= '${date}'
            GROUP BY IdMSup
        ) TablePosHut LEFT OUTER JOIN MGAPMSup MSup ON (TablePosHut.IdMSup = MSup.IdMSup)
        WHERE MSup.Hapus = 0
            AND MSup.Aktif = 1`
    );

    var count = {
      total: total,
    };

    res.json({
      message: "Success",
      countData: count,
      data: arr_data,
    });
  }

  // kartu hutang
  else if (jenis == 2) {
    let start = req.body.start || today;
    let end = req.body.end || today;

    let sql = `SELECT idmsup, nmmsup FROM mgapmsup where hapus=0 and aktif=1`;
    const filter = await sequelize.query(sql, {
      raw: false,
    });
    console.log("filnya", filter);

    var arr_data = await Promise.all(
      filter[0].map(async (fil, index) => {
        console.log("fil", fil);
        let sql1 = `SELECT MSup.KdMSup
            , MSup.NmMSup
            , TableKartuHut.IdMSup
            , Urut
            , JenisTrans
            , BuktiTrans
            , IdTrans
            , cast(TglTrans As DateTime) as TglTrans
            , TableKartuHut.Keterangan
            , Saldo
            , JmlHut
            , IF(Urut = 0, 0, IF(Coalesce(JmlHut, 0) > 0, Coalesce(JmlHut, 0), 0)) As Debit
            , IF(Urut = 0, 0, IF(Coalesce(JmlHut, 0) >= 0, 0, Coalesce(JmlHut, 0))) As Kredit
        FROM (
        SELECT IdMSup, 0 As Urut, 0 as JenisTrans, 0 as IdTrans, '-' As BuktiTrans, cast('${start}' as DateTime) As TglTrans, 0 As JmlHut, sum(JmlHut) As Saldo, 'Saldo Sebelumnya' As Keterangan FROM (
            SELECT IdMSup, 0 As JmlHut FROM MGAPMSup
            UNION ALL
            SELECT IdMSup, Sum(JmlHut) as JmlHut
            FROM (SELECT IdMSup 
                , 0 as JenisTrans 
                , IdTSAHut as IdTrans 
                , BuktiTSAHut as BuktiTrans 
                , concat(Date(TglTSAHut), ' ', Time(TglUpdate)) as TglTrans 
                , JmlHut 
                , 'Saldo Awal' as Keterangan 
            FROM MGAPTSAHut 
            WHERE JmlHut <> 0 
            UNION ALL 
            SELECT IdMSup 
                , 1 as JenisTrans 
                , IdTBeli as IdTrans 
                , IF(BuktiTBeli = '', BuktiTLPB, BuktiTBeli) as BuktiTrans 
                , concat(Date(TglTBeli), ' ', Time(TglUpdate)) as TglTrans 
                , (Netto - JmlBayarTunai) AS JmlHut 
                , concat('Pembelian ', BuktiTBeli) as Keterangan 
            FROM MGAPTBeli 
            WHERE Hapus = 0 
            AND Void = 0 
            AND (Netto - JmlBayarTunai) <> 0 
            AND HapusLPB = 0 AND VoidLPB = 0 
            AND BuktiTBeli <> ''
            UNION ALL 
            SELECT IdMSup 
                , 1.1 AS JenisTrans 
                , IdTBeliLain AS IdTrans 
                , BuktiTBeliLain AS BuktiTrans 
                , CONCAT(DATE(TglTBeliLain), ' ', TIME(TglUpdate)) AS TglTrans 
                , (JmlBayarKredit) AS JmlHut 
                , CONCAT('Biaya Lain-Lain ', BuktiTBeliLain) AS Keterangan 
            FROM MGAPTBeliLain 
            WHERE Hapus = 0 
            AND Void = 0 
            AND (JmlBayarKredit) <> 0 
            UNION ALL 
            SELECT IdMSup 
                , 2 as JenisTrans 
                , IdTRBeli as IdTrans 
                , BuktiTRBeli as BuktiTrans 
                , concat(Date(TglTRBeli), ' ', Time(TglUpdate)) as TglTrans 
                , - (Netto - JmlBayarTunai) AS JmlHut 
                , concat('Retur Pembelian ', BuktiTRBeli) as Keterangan 
            FROM MGAPTRBeli 
            WHERE Hapus = 0 
            AND Void = 0 
            AND JenisTRBeli = 0 
            AND (Netto - JmlBayarTunai) <> 0 
            UNION ALL 
            SELECT IdMSup 
                , 3 as JenisTrans 
                , IdTPBeli as IdTrans 
                , BuktiTPBeli as BuktiTrans 
                , concat(Date(TglTPBeli), ' ', Time(TglUpdate)) as TglTrans 
                , - Total AS JmlHut 
                , concat('Potongan Pembelian ', BuktiTPBeli) as Keterangan 
            FROM MGAPTPBeli 
            WHERE Hapus = 0 
            AND Void = 0 
            AND Total <> 0 
            UNION ALL 
            SELECT IdMSup 
                , 4 as JenisTrans 
                , IdTBHut as IdTrans 
                , BuktiTBHut as BuktiTrans 
                , concat(Date(TglTBHut), ' ', Time(TglUpdate)) as TglTrans 
                , - Total AS JmlHut 
                , concat('Pembayaran Hutang ', BuktiTBHut) as Keterangan 
            FROM MGAPTBHut 
            WHERE Hapus = 0 
            AND Void = 0 
            AND Total <> 0 
            UNION ALL 
            SELECT hut.IdMSup
                , 4.1 AS JenisTrans
                , Hut.IdTBHut AS IdTrans
                , BuktiTBHut AS BuktiTrans
                , CONCAT(DATE(Hut.TglTBHut), ' ', TIME(Hut.TglUpdate)) AS tgltrans
                , HutDB.JmlBayar AS JmlHut
                , CONCAT('Titipan Giro ', Hut.BuktiTBHut, ' (', giro.kdmgiro, ')') AS Keterangan
            FROM MGAPTBHUTDB HutDB
                LEFT OUTER JOIN MGAPTBHut Hut ON (hut.idmcabang = hutDB.idmcabang AND hut.idtbhut = hutdb.idtbhut)
                LEFT OUTER JOIN mgapmsup sup ON (sup.idmsup = hut.idmsup)
                LEFT OUTER JOIN MGKBMGiro giro ON (giro.idmcabang = HutDB.IdMCabangMRef AND giro.IdMGiro = HutDB.IdMRef)
            WHERE (Hut.Hapus = 0 AND Hut.Void = 0) AND HutDB.JenisMRef = 'G'
            UNION ALL 
            SELECT sup.idmsup
                , 4.2 AS JenisTrans
                , m.idtgirocair AS IdTrans
                , m.BuktiTGiroCair AS BuktiTrans
                , CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) AS TglTrans
                , - HutDB.JmlBayar AS JmlHut
                , CONCAT('Giro Cair ', m.Buktitgirocair, ' (', giro.kdmgiro, ')') AS Keterangan
            FROM MGKBTGiroCairD d
                LEFT OUTER JOIN MGKBTGiroCair m ON (m.IdMCabang = d.IdMCabang AND d.idtgirocair = m.idtgirocair)
                LEFT OUTER JOIN MGKBMGiro giro ON (giro.idmcabang = d.idmcabangmgiro AND giro.idmgiro = d.idmgiro)
                LEFT OUTER JOIN MGAPMSup sup ON (sup.idmsup = giro.idmsup AND giro.jenismgiro = 'K')
                LEFT OUTER JOIN MGAPTBHutDB HutDB ON (HutDB.IdMCabangMRef = giro.IdMCabang AND HutDB.IdMRef = giro.IdMGiro AND HutDB.JenisMRef = 'G')
                LEFT OUTER JOIN MGAPTBHut Hut ON (Hut.IdMCabang = HutDB.IdMCabang AND hut.idtbhut = hutDB.idtbhut)
            WHERE (m.Hapus = 0 AND m.Void = 0) AND m.JenisTGiroCair = 'K' AND HUTDB.JenisMRef = 'G' AND HUT.Void = 0 AND HUT.Hapus = 0
            UNION ALL 
            SELECT Sup.idmsup 
                , 4.3 AS JenisTrans 
                , m.idtgirotolak AS IdTrans 
                , m.buktitgirotolak AS BuktiTrans 
                , CONCAT(DATE(m.tgltgirotolak), ' ', TIME(m.tglupdate)) AS TglTrans
                , HutDB.jmlbayar AS JmlHut 
                , CONCAT('Giro Tolak ', m.buktitgirotolak, ' (', giro.kdmgiro, ')') AS keterangan
            FROM MGKBTGiroTolakD d
                LEFT OUTER JOIN MGKBTGiroTolak m ON (m.IdMCabang = d.IdMCabang AND m.IdTGiroTolak = d.IdTGiroTolak)
                LEFT OUTER JOIN MGKBMGiro giro ON (giro.IdMCabang = d.IdMCabangMGiro AND giro.IdMGiro = d.IdMGiro)
                LEFT OUTER JOIN MGAPMSup sup ON (sup.IdMSup = giro.IdMSup AND giro.JenisMGiro = 'K')
                LEFT OUTER JOIN MGAPTBHutDB HutDB ON (HutDB.IdMCabangMRef = giro.IdMCabang AND HutDB.IdMRef = giro.IdMGiro AND HutDB.JenisMRef = 'G')
                LEFT OUTER JOIN MGAPTBHut Hut ON (Hut.IdMCabang = HutDB.IdMCabang AND Hut.IdTBHut = HutDB.IdTBHut)
            WHERE (m.Hapus = 0 AND m.Void = 0)
                AND m.JenisTGiroTolak = 'K' AND HutDB.JenisMRef = 'G' 
                AND Hut.Void = 0 AND Hut.Hapus = 0
            UNION ALL 
            SELECT Sup.IdMSup AS IdMSup 
                , 4.4 AS JenisTrans 
                , m.IdTGiroGanti AS IdTrans 
                , m.BuktiTGiroGanti AS BuktiTrans
                , CONCAT(DATE(m.tgltgiroganti), ' ', TIME(m.tglupdate)) AS tgltrans
                , -d.JmlBayar AS JmlHut
                , CONCAT('Penggantian Giro ', m.buktitgiroganti, ' (', giro.kdmgiro, ')') AS Keterangan
            FROM MGKBTGiroGantiDG d
                LEFT OUTER JOIN MGKBTGiroGanti m ON (m.IdMCabang = d.IdMCabang AND m.IDTGiroGanti = d.IDTGiroGanti)
                LEFT OUTER JOIN MGKBMGiro giro ON (giro.IdMCabang = d.IdMCabangMGiro AND giro.IdMGiro = d.IdMGiro)
                LEFT OUTER JOIN MGAPMSup Sup ON (Sup.IdMSup = giro.IdMSup AND Giro.JenisMGiro = 'K')
                LEFT OUTER JOIN MGAPTBHutDB HutDB ON (HutDB.IdMCabangMRef = giro.IdMCabang AND HutDB.IdMRef = Giro.IdMGiro AND HutDB.JenisMREF = 'G')
                LEFT OUTER JOIN MGAPTBHut Hut ON (Hut.IdMCabang = HutDB.IdMCabang AND Hut.IdTBHut = HutDB.IdTBHut)
            WHERE (m.Hapus = 0 AND m.Void = 0)
                AND m.JenisTGiroGanti = 'K' AND HutDB.JenisMRef = 'G'
                AND Hut.Void = 0 AND Hut.Hapus = 0
            UNION ALL 
            SELECT IdMSup 
                , 5 as JenisTrans 
                , IdTKorHut as IdTrans 
                , BuktiTKorHut as BuktiTrans 
                , concat(Date(TglTKorHut), ' ', Time(TglUpdate)) as TglTrans 
                , Total AS JmlHut 
                , concat('Koreksi Hutang ', BuktiTKorHut) as Keterangan 
            FROM MGAPTKorHut 
            WHERE Hapus = 0 AND Void = 0 
            AND Total <> 0 
            )
            as LKartuHut WHERE TglTrans < '${start}' GROUP BY IdMSup
        ) TableSaldoAwal
        GROUP BY IdMSup
        UNION ALL
        SELECT IdMSup, 1 as Urut, JenisTrans, IdTrans, BuktiTrans, TglTrans, JmlHut, 0 As Saldo, Keterangan 
        FROM (SELECT IdMSup 
                , 0 as JenisTrans 
                , IdTSAHut as IdTrans 
                , BuktiTSAHut as BuktiTrans 
                , concat(Date(TglTSAHut), ' ', Time(TglUpdate)) as TglTrans 
                , JmlHut 
                , 'Saldo Awal' as Keterangan 
            FROM MGAPTSAHut 
            WHERE JmlHut <> 0 
            UNION ALL 
            SELECT IdMSup 
                , 1 as JenisTrans 
                , IdTBeli as IdTrans 
                , IF(BuktiTBeli = '', BuktiTLPB, BuktiTBeli) as BuktiTrans 
                , concat(Date(TglTBeli), ' ', Time(TglUpdate)) as TglTrans 
                , (Netto - JmlBayarTunai) AS JmlHut 
                , concat('Pembelian ', BuktiTBeli) as Keterangan 
            FROM MGAPTBeli 
            WHERE Hapus = 0 
            AND Void = 0 
            AND (Netto - JmlBayarTunai) <> 0 
            AND HapusLPB = 0 AND VoidLPB = 0 
            AND BuktiTBeli <> ''
            UNION ALL 
            SELECT IdMSup 
                , 1.1 AS JenisTrans 
                , IdTBeliLain AS IdTrans 
                , BuktiTBeliLain AS BuktiTrans 
                , CONCAT(DATE(TglTBeliLain), ' ', TIME(TglUpdate)) AS TglTrans 
                , (JmlBayarKredit) AS JmlHut 
                , CONCAT('Biaya Lain-Lain ', BuktiTBeliLain) AS Keterangan 
            FROM MGAPTBeliLain 
            WHERE Hapus = 0 
            AND Void = 0 
            AND (JmlBayarKredit) <> 0 
            UNION ALL 
            SELECT IdMSup 
                , 2 as JenisTrans 
                , IdTRBeli as IdTrans 
                , BuktiTRBeli as BuktiTrans 
                , concat(Date(TglTRBeli), ' ', Time(TglUpdate)) as TglTrans 
                , - (Netto - JmlBayarTunai) AS JmlHut 
                , concat('Retur Pembelian ', BuktiTRBeli) as Keterangan 
            FROM MGAPTRBeli 
            WHERE Hapus = 0 
            AND Void = 0 
            AND JenisTRBeli = 0 
            AND (Netto - JmlBayarTunai) <> 0 
            UNION ALL 
            SELECT IdMSup 
                , 3 as JenisTrans 
                , IdTPBeli as IdTrans 
                , BuktiTPBeli as BuktiTrans 
                , concat(Date(TglTPBeli), ' ', Time(TglUpdate)) as TglTrans 
                , - Total AS JmlHut 
                , concat('Potongan Pembelian ', BuktiTPBeli) as Keterangan 
            FROM MGAPTPBeli 
            WHERE Hapus = 0 
            AND Void = 0 
            AND Total <> 0 
            UNION ALL 
            SELECT IdMSup 
                , 4 as JenisTrans 
                , IdTBHut as IdTrans 
                , BuktiTBHut as BuktiTrans 
                , concat(Date(TglTBHut), ' ', Time(TglUpdate)) as TglTrans 
                , - Total AS JmlHut 
                , concat('Pembayaran Hutang ', BuktiTBHut) as Keterangan 
            FROM MGAPTBHut 
            WHERE Hapus = 0 
            AND Void = 0 
            AND Total <> 0 
            UNION ALL 
            SELECT hut.IdMSup
                , 4.1 AS JenisTrans
                , Hut.IdTBHut AS IdTrans
                , BuktiTBHut AS BuktiTrans
                , CONCAT(DATE(Hut.TglTBHut), ' ', TIME(Hut.TglUpdate)) AS tgltrans
                , HutDB.JmlBayar AS JmlHut
                , CONCAT('Titipan Giro ', Hut.BuktiTBHut, ' (', giro.kdmgiro, ')') AS Keterangan
            FROM MGAPTBHUTDB HutDB
                LEFT OUTER JOIN MGAPTBHut Hut ON (hut.idmcabang = hutDB.idmcabang AND hut.idtbhut = hutdb.idtbhut)
                LEFT OUTER JOIN mgapmsup sup ON (sup.idmsup = hut.idmsup)
                LEFT OUTER JOIN MGKBMGiro giro ON (giro.idmcabang = HutDB.IdMCabangMRef AND giro.IdMGiro = HutDB.IdMRef)
            WHERE (Hut.Hapus = 0 AND Hut.Void = 0) AND HutDB.JenisMRef = 'G'
            UNION ALL 
            SELECT sup.idmsup
                , 4.2 AS JenisTrans
                , m.idtgirocair AS IdTrans
                , m.BuktiTGiroCair AS BuktiTrans
                , CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) AS TglTrans
                , - HutDB.JmlBayar AS JmlHut
                , CONCAT('Giro Cair ', m.Buktitgirocair, ' (', giro.kdmgiro, ')') AS Keterangan
            FROM MGKBTGiroCairD d
                LEFT OUTER JOIN MGKBTGiroCair m ON (m.IdMCabang = d.IdMCabang AND d.idtgirocair = m.idtgirocair)
                LEFT OUTER JOIN MGKBMGiro giro ON (giro.idmcabang = d.idmcabangmgiro AND giro.idmgiro = d.idmgiro)
                LEFT OUTER JOIN MGAPMSup sup ON (sup.idmsup = giro.idmsup AND giro.jenismgiro = 'K')
                LEFT OUTER JOIN MGAPTBHutDB HutDB ON (HutDB.IdMCabangMRef = giro.IdMCabang AND HutDB.IdMRef = giro.IdMGiro AND HutDB.JenisMRef = 'G')
                LEFT OUTER JOIN MGAPTBHut Hut ON (Hut.IdMCabang = HutDB.IdMCabang AND hut.idtbhut = hutDB.idtbhut)
            WHERE (m.Hapus = 0 AND m.Void = 0) AND m.JenisTGiroCair = 'K' AND HUTDB.JenisMRef = 'G' AND HUT.Void = 0 AND HUT.Hapus = 0
            UNION ALL 
            SELECT Sup.idmsup 
                , 4.3 AS JenisTrans 
                , m.idtgirotolak AS IdTrans 
                , m.buktitgirotolak AS BuktiTrans 
                , CONCAT(DATE(m.tgltgirotolak), ' ', TIME(m.tglupdate)) AS TglTrans
                , HutDB.jmlbayar AS JmlHut 
                , CONCAT('Giro Tolak ', m.buktitgirotolak, ' (', giro.kdmgiro, ')') AS keterangan
            FROM MGKBTGiroTolakD d
                LEFT OUTER JOIN MGKBTGiroTolak m ON (m.IdMCabang = d.IdMCabang AND m.IdTGiroTolak = d.IdTGiroTolak)
                LEFT OUTER JOIN MGKBMGiro giro ON (giro.IdMCabang = d.IdMCabangMGiro AND giro.IdMGiro = d.IdMGiro)
                LEFT OUTER JOIN MGAPMSup sup ON (sup.IdMSup = giro.IdMSup AND giro.JenisMGiro = 'K')
                LEFT OUTER JOIN MGAPTBHutDB HutDB ON (HutDB.IdMCabangMRef = giro.IdMCabang AND HutDB.IdMRef = giro.IdMGiro AND HutDB.JenisMRef = 'G')
                LEFT OUTER JOIN MGAPTBHut Hut ON (Hut.IdMCabang = HutDB.IdMCabang AND Hut.IdTBHut = HutDB.IdTBHut)
            WHERE (m.Hapus = 0 AND m.Void = 0)
                AND m.JenisTGiroTolak = 'K' AND HutDB.JenisMRef = 'G' 
                AND Hut.Void = 0 AND Hut.Hapus = 0
            UNION ALL 
            SELECT Sup.IdMSup AS IdMSup 
                , 4.4 AS JenisTrans 
                , m.IdTGiroGanti AS IdTrans 
                , m.BuktiTGiroGanti AS BuktiTrans
                , CONCAT(DATE(m.tgltgiroganti), ' ', TIME(m.tglupdate)) AS tgltrans
                , -d.JmlBayar AS JmlHut
                , CONCAT('Penggantian Giro ', m.buktitgiroganti, ' (', giro.kdmgiro, ')') AS Keterangan
            FROM MGKBTGiroGantiDG d
                LEFT OUTER JOIN MGKBTGiroGanti m ON (m.IdMCabang = d.IdMCabang AND m.IDTGiroGanti = d.IDTGiroGanti)
                LEFT OUTER JOIN MGKBMGiro giro ON (giro.IdMCabang = d.IdMCabangMGiro AND giro.IdMGiro = d.IdMGiro)
                LEFT OUTER JOIN MGAPMSup Sup ON (Sup.IdMSup = giro.IdMSup AND Giro.JenisMGiro = 'K')
                LEFT OUTER JOIN MGAPTBHutDB HutDB ON (HutDB.IdMCabangMRef = giro.IdMCabang AND HutDB.IdMRef = Giro.IdMGiro AND HutDB.JenisMREF = 'G')
                LEFT OUTER JOIN MGAPTBHut Hut ON (Hut.IdMCabang = HutDB.IdMCabang AND Hut.IdTBHut = HutDB.IdTBHut)
            WHERE (m.Hapus = 0 AND m.Void = 0)
                AND m.JenisTGiroGanti = 'K' AND HutDB.JenisMRef = 'G'
                AND Hut.Void = 0 AND Hut.Hapus = 0
            UNION ALL 
            SELECT IdMSup 
                , 5 as JenisTrans 
                , IdTKorHut as IdTrans 
                , BuktiTKorHut as BuktiTrans 
                , concat(Date(TglTKorHut), ' ', Time(TglUpdate)) as TglTrans 
                , Total AS JmlHut 
                , concat('Koreksi Hutang ', BuktiTKorHut) as Keterangan 
            FROM MGAPTKorHut 
            WHERE Hapus = 0 AND Void = 0 
            AND Total <> 0 
        ) 
            as LKartuHut WHERE TglTrans >= '${start}' AND TglTrans <= '${end}'
        ) TableKartuHut LEFT OUTER JOIN MGAPMSup MSup ON (TableKartuHut.IdMSup = MSup.IdMSup)
        WHERE MSup.Hapus = 0
            AND MSup.KdMSup LIKE '%%'
            AND MSup.NmMSup LIKE '%%'
            AND MSup.idmsup = ${fil.idmsup}`;
        // ORDER BY , MSup.KdMSup, MSup.NmMSup, Urut, TglTrans, JenisTrans, IdTrans
        const cust = await sequelize.query(sql1, {
          raw: false,
        });

        var saldo = 0;
        var detail = await Promise.all(
          cust[0].map(async (nilai, index_satu) => {
            saldo += parseFloat(nilai.Kredit) + parseFloat(nilai.Debit);
            return {
              tanggal: nilai.TglTrans,
              bukti: nilai.BuktiTrans,
              keterangan: nilai.Keterangan,
              debit: parseFloat(nilai.Debit),
              kredit: parseFloat(nilai.Kredit),
              saldo: saldo,
            };
          })
        );

        return {
          customer: fil.nmmsup,
          list: detail,
        };
      })
    );

    res.json({
      message: "Success kartu",
      data: arr_data,
    });
  }
};

exports.piutang = async (req, res) => {
  const sequelize = await fun.connection(req.datacompany);

  let jenis = req.body.jenis || 1;

  // posisi piutang
  if (jenis == 1) {
    let date = req.body.tanggal || "2024-01-19";
    let sql = `SELECT idmcabang, nmmcabang FROM mgsymcabang where aktif=1 and hapus=0`;
    const filter = await sequelize.query(sql, {
      raw: false,
    });

    var total = 0;
    var arr_data = await Promise.all(
      filter[0].map(async (fil, index) => {
        let sql1 = `SELECT MCabang.idMCabang, MCabang.KdMCabang, MCabang.NmMCabang, MCabang.Aktif as AktifMCabang
            , Coalesce(MCust.KdMCust,'') as kdmcust, Coalesce(MCust.NmMCust,'') as nmmcust, coalesce(MCust.Aktif,1) as AktifMCust, coalesce(MCust.LimitPiut,0) as LimitPiut
            , TablePosPiut.pospiut, (MCust.LimitPiut - TablePosPiut.pospiut) As Selisih
        FROM (
            SELECT TransAll.IdMCabang, TransAll.IdMCabangMCust, IdMCust, Sum(JmlPiut) as pospiut
            FROM (SELECT tbl.TglTrans, tbl.IdMCabang, tbl.IdMCabangMCust, tbl.IdMCust, tbl.JmlPiut, Tbl.id_bcf
        FROM (
            SELECT IdMCabang as IdMCabangMCust 
                , IdMCust 
                , 0 as JenisTrans 
                , IdMCabang 
                , IdTSAPiut as IdTrans 
                , BuktiTSAPiut as BuktiTrans 
                , concat(Date(TglTSAPiut), ' ', Time(TglTSAPiut)) as TglTrans 
                , 0 as JenisInvoice 
                , JmlPiut 
                , concat('Saldo Awal ', BuktiTSAPiut) as Keterangan 
                , -1 as Id_bcf
            FROM MGARTSAPiut 
            WHERE JmlPiut <> 0 
            AND TglTSAPiut <= '${date}'
            UNION ALL 
            SELECT IdMCabangMCust 
                , IdMCust 
                , 1 as JenisTrans 
                , IdMCabang 
                , IdTJualPOS as IdTrans 
                , BuktiTJualPOS as BuktiTrans 
                , concat(Date(TglTJualPOS), ' ', Time(TglUpdate)) as TglTrans 
                , 0 as JenisInvoice 
                , (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) AS JmlPiut 
                , concat('Penjualan POS ', BuktiTJualPOS) as Keterangan 
                , -1 as Id_bcf
            FROM MGARTJualPOS 
            WHERE Hapus = 0 AND Void = 0 
            AND (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) <> 0 
            AND TglTJualPOS <= '${date}'
        
            UNION ALL 
            SELECT Jual.IdMCabangMCust 
                , Jual.IdMCust 
                , 2 as JenisTrans 
                , Jual.IdMCabang 
                , Jual.IdTJual as IdTrans 
                , Jual.BuktiTJual as BuktiTrans 
                , concat(Date(Jual.TglTJual), ' ', Time(Jual.TglUpdate)) as TglTrans 
                , Jual.JenisTJual as JenisInvoice 
                , (Jual.JmlBayarKredit) AS JmlPiut 
                , concat('Penjualan ', Jual.BuktiTJual) as Keterangan 
                , -1 as Id_bcf
            FROM MGARTJual Jual
            WHERE Jual.Hapus = 0 AND Jual.Void = 0 
            AND (Jual.JmlBayarKredit) <> 0 
            AND Coalesce(Jual.IdTRJual, 0) = 0
            AND Jual.TglTJual <= '${date}'
        
            UNION ALL 
            SELECT rj.IdMCabangMCust 
                , rj.IdMCust 
                , 3 as JenisTrans 
                , rj.IdMCabang 
                , rj.IdTRJual as IdTrans 
                , rj.BuktiTRJual as BuktiTrans 
                , concat(Date(rj.TglTRJual), ' ', Time(rj.TglUpdate)) as TglTrans 
                , rj.JenisInvoice as JenisInvoice 
                , - rj.JmlBayarKredit AS JmlPiut 
                , concat('Retur Penjualan ', rj.BuktiTRJual) as Keterangan 
                , -1 as Id_bcf
            FROM MGARTRJual rj
                LEFT OUTER JOIN MGARTJual jual ON (jual.IdMCabangTRJual = rj.IdMCabang AND jual.IdTRJual = rj.IdTRJual and jual.Void = 0 and jual.Hapus = 0)
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = rj.IdMCabangMCust AND MCust.IdMCust = rj.IdMCust)
            WHERE rj.Hapus = 0 AND rj.Void = 0 
            AND rj.JmlBayarKredit <> 0 
            AND rj.JenisRJual = 0
            AND jual.IdTJual IS NULL
            AND TglTRJual <= '${date}'
        
            UNION ALL 
            SELECT TJualLain.IdMCabangCust as IdMCabangMCust 
                , TJualLain.IdMCust as IdMCust 
                , 2.1 as JenisTrans 
                , TJualLain.IdMCabang 
                , TJualLain.IdTJualLain as IdTrans 
                , TJualLain.BuktiTJualLain as BuktiTrans 
                , concat(Date(TJualLain.TglTJualLain), ' ', Time(TJualLain.TglUpdate)) as TglTrans 
                , 0 as JenisInvoice 
                , TJualLain.Netto AS JmlPiut 
                , CONCAT('Penjualan ', ' ', IF(TJualLain.JenisEkspedisi = 1, 'FRANCO',
                    IF(TJualLain.JenisEkspedisi = 2, 'LOCO', '')), ' '
                    , IF(TJualLain.CountPrint = -1, '(Peti/Ekspedisi)', ''), '') as Keterangan
                , -1 as Id_bcf
            FROM MGARTJualLain TJualLain
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = TJualLain.IdMCabangCust AND MCust.IdMCust = TJualLain.IdMCust)
            WHERE TJualLain.Hapus = 0 AND TJualLain.Void = 0 
            AND TJualLain.Netto <> 0 
            AND TglTJualLain <= '${date}'
        
            UNION ALL 
            SELECT TAngkutan.IdMCabangMCust
                , TAngkutan.IdMCust
                , 2.2 AS JenisTrans
                , TAngkutan.IdMCabang
                , TAngkutan.IdTTAngkutan AS IdTrans
                , TAngkutan.BuktiTTAngkutan AS BuktiTrans
                , CONCAT(DATE(TAngkutan.TglTTAngkutan), ' ', TIME(TAngkutan.TglUpdate)) AS TglTrans
                , TAngkutan.JenisTTAngkutan AS JenisInvoice
                , (TAngkutan.JmlBayarKredit) AS JmlPiut
                , CONCAT('Titip Angkutan ', TAngkutan.BuktiTTAngkutan) AS Keterangan
                , -1 as Id_bcf
            FROM MGARTTAngkutan TAngkutan
            WHERE Hapus = 0 AND Void = 0
            AND (TAngkutan.JmlBayarKredit) <> 0
            AND TglTTAngkutan <= '${date}'
            UNION ALL 
            SELECT m.IdMCabangMCust 
                , m.IdMCust 
                , 4 as JenisTrans 
                , m.IdMCabang 
                , m.IdTBPiut as IdTrans 
                , m.BuktiTBPiut as BuktiTrans 
                , concat(Date(d.TglBayar), ' ', Time(m.TglUpdate)) as TglTrans 
                , m.JenisInvoice as JenisInvoice 
                , - (d.JmlBayar - COALESCE(UMJUalD.JmlBayar, 0)) AS JmlPiut 
                , concat('Pembayaran Piutang ', BuktiTBPiut, ' ' ,if(d.jenisMref = 'K',Kas.KdMKas, if(d.JenisMRef ='B',Rek.KdMRek, if(d.JenisMref ='G' ,Giro.KdMGiro,IF(d.JenisMRef = 'P',Prk.KdMPrk,''))))) as Keterangan  
                , m.Id_bcf
            FROM MGARTBPiutDB d 
                LEFT OUTER JOIN MGARTBPiut m ON (d.IdMCabang = m.IdMCabang AND d.IdTBPiut = m.IdTBPiut)
                LEFT OUTER JOIN MGARTUMJual UMJual ON(m.IdMCabang = UMJual.IdMCabangTBPiut AND m.IdTBPiut = UMJual.IdTBPiut AND UMJual.Hapus = 0 AND UMJual.Void = 0)
                LEFT OUTER JOIN MGARTUMJualD UMJualD ON(UMJual.IdMCabang = UMJualD.IdMCabang AND UMJual.IdTUMJual = UMJualD.IdTUMJual AND d.IdMCabangMRef = UMJualD.IdMCabangMRef AND d.IdMref = UMJualD.IdMRef AND d.TglBayar = UMJualD.TglBayar)
                LEFT OUTER JOIN MGKBMKas Kas ON(Kas.IdMCabang = d.IdMCabangMref AND Kas.IdMKas = d.IdMref and d.JenisMRef ='K')
                LEFT OUTER JOIN MGKBMRek Rek ON(Rek.IdMCabang = d.IdMCabangMref AND Rek.IdMRek = d.IdMref and d.JenisMRef ='B')
                LEFT OUTER JOIN MGKBMGiro Giro ON(Giro.IdMCabang = d.IdMCabangMref AND Giro.IdMGiro = d.IdMref and d.JenisMRef ='G')
                LEFT OUTER JOIN MGGLMPrk Prk ON( Prk.IdMPrk = d.IdMref and d.JenisMRef ='P' and Prk.Periode = 0)
            WHERE m.Hapus = 0 AND m.Void = 0
            AND m.Total <> 0 
            AND d.TglBayar <= '${date}'
        
            UNION ALL 
            SELECT m.IdMCabangMCust 
                , m.IdMCust 
                , 4 as JenisTrans 
                , m.IdMCabang 
                , m.IdTBPiut as IdTrans 
                , m.BuktiTBPiut as BuktiTrans 
                , concat(Date(m.TglTBPiut), ' ', Time(m.TglUpdate)) as TglTrans 
                , m.JenisInvoice as JenisInvoice 
                , - m.JmlUM AS JmlPiut 
                , concat('Pembayaran Piutang dgn Uang Muka ', BuktiTBPiut) as Keterangan  
                , m.Id_bcf
            FROM MGARTBPiut m
            WHERE m.Hapus = 0 AND m.Void = 0
            AND m.JmlUM <> 0 
            AND TglTBPiut <= '${date}'
        
            UNION ALL 
            SELECT MCust.IdMCabang
                , MCust.IdMCust AS IdMCust
                , 4.1 AS JenisTrans
                , TBPiut.IdMCabang
                , TBPiut.IdTBPiut AS IdTrans
                , TBPiut.BuktiTBPiut AS BuktiTrans
                , CONCAT(DATE(TBPiutB.TglBayar), ' ', TIME(TBPiut.TglUpdate)) AS TglTrans
                , TBPiut.JenisInvoice as JenisInvoice 
                , TBPiutB.JMLBayar AS JmlPiut
                , CONCAT('Titipan Giro ', TBPiut.BuktiTBPiut, ' (', MG.KdMGiro, ')') AS Keterangan
                , -1 as Id_bcf
            FROM MGARTBPiutDB TBPiutB
                LEFT OUTER JOIN MGARTBPiut TBPiut ON (TBPiut.IdMCabang = TBPiutB.IdMCabang AND TBPiut.IdTBPiut = TBPiutB.IdTBPiut)
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = TBPiut.IdMCabangMCust AND MCust.IdMCust = TBPiut.IdMCust)
                LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = TBPiutB.IdMCabangMREF AND MG.IdMGiro = TBPiutB.IdMREF)
            WHERE (TBPiut.HAPUS = 0 AND TBPiut.VOID = 0) AND TBPiutB.JenisMREF = 'G'
            AND TBPiutB.TglBayar <= '${date}'
        
            UNION ALL 
            SELECT MCust.IdMCabang AS IdMCabang
                , MCust.IdMCust AS IdMCust
                , 4.2 AS JenisTrans
                , m.IdMCabang
                , M.IdTGiroCair AS IdTrans 
                , M.BuktiTGiroCair AS BuktiTrans 
                , CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) AS TglTrans
                , BPiut.JenisInvoice as JenisInvoice 
                , -BPiutDB.JMLBayar AS JmlPiut 
                , CONCAT('Giro Cair ', m.BuktiTGiroCair, ' (', MG.KdmGiro, ')') AS Keterangan 
                , -1 as Id_bcf
            FROM MGKBTGiroCairD D
                LEFT OUTER JOIN MGKBTGiroCair M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroCair = D.IdTGiroCair)
                LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
                LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
                LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
            WHERE (M.HAPUS = 0 AND M.VOID = 0) AND M.JenisTGiroCair = 'M' AND BPiutDB.JenisMRef = 'G' AND BPiut.VOID = 0 AND BPiut.HAPUS = 0
            AND TglTGiroCair <= '${date}'
        
            UNION ALL 
            SELECT MCust.IdMCabang AS IdMCabang
                , MCust.IdMCust AS IdMCust
                , 4.3 AS JenisTrans
                , m.IdMCabang
                , M.IdTGiroTolak AS IdTrans
                , M.BuktiTGiroTolak AS BuktiTrans
                , CONCAT(DATE(m.TglTGiroTolak), ' ', TIME(m.tglupdate)) AS TglTrans
                , BPiut.JenisInvoice as JenisInvoice 
                , BPiutDB.JMLBayar AS JmlPiut
                , CONCAT('Giro Tolak ', m.BuktiTGiroTolak,  ' (', MG.KdMGiro, ')') AS Keterangan
                , -1 as Id_bcf
            FROM MGKBTGiroTolakD D
                LEFT OUTER JOIN MGKBTGiroTolak M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroTolak = D.IdTGiroTolak)
                LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
                LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
                LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
            WHERE (M.HAPUS = 0 AND M.VOID = 0) 
                AND M.JenisTGiroTolak = 'M' AND BPiutDB.JenisMRef = 'G' 
                AND BPiut.VOID = 0 AND BPiut.HAPUS = 0
            AND TglTGiroTolak <= '${date}'
        
            UNION ALL 
            SELECT MCust.IdMCabang AS IdMCabang 
                , MCust.IdMCust AS IdMCust 
                , 4.4 AS JenisTrans
                , m.IdMCabang
                , M.IDTGiroGanti AS IdTrans
                , M.BuktiTGiroGanti AS BuktiTrans
                , CONCAT(DATE(m.TglTGiroGanti), ' ', TIME(m.tglupdate)) AS TglTrans
                , BPiut.JenisInvoice as JenisInvoice 
                , -D.JMLBayar AS JmlPiut
                , CONCAT('Penggantian Giro ', m.BuktiTGiroGanti, ' (', MG.KdMGiro, ')') AS Keterangan
                , -1 as Id_bcf
            FROM MGKBTGiroGanti M
                LEFT OUTER JOIN MGKBTGiroGantiDG D ON (M.IdMCabang = D.IdMCabang AND M.IDTGiroGanti = D.IDTGiroGanti)
                LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
                LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
                LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
            WHERE (M.HAPUS = 0 AND M.VOID = 0)
                AND M.JenisTGiroGanti = 'M' AND BPiutDB.JenisMRef = 'G'
                AND BPiut.VOID = 0 AND BPiut.HAPUS = 0
            AND TglTGiroGanti >= '${date}' and TglTGiroGanti < '1899-12-30'
        
            UNION ALL 
            SELECT IdMCabangMCust 
                , IdMCust 
                , 5 as JenisTrans 
                , IdMCabang 
                , IdTKorPiut as IdTrans 
                , BuktiTKorPiut as BuktiTrans 
                , concat(Date(TglTKorPiut), ' ', Time(TglUpdate)) as TglTrans 
                , IdMJenisInvoice as JenisInvoice 
                , Total AS JmlPiut 
                , concat('Koreksi Piutang ', BuktiTKorPiut) as Keterangan 
                , -1 as Id_bcf
            FROM MGARTKorPiut 
            WHERE Hapus = 0 AND Void = 0
            AND Total <> 0 
            AND TglTKorPiut <= '${date}'
        
            UNION ALL 
        SELECT mj.IdMCabangMCust
                , mj.IdMCust
                , 6 AS JenisTrans
                , m.IdMCabang
                , m.IdTTagihan AS IdTrans
                , m.BuktiTTagihan AS BuktiTrans
                , CONCAT(DATE(TglTTagihan), ' ', TIME(m.TglUpdate)) AS TglTrans
                , 0 AS JenisInvoice
                , -d.jmlbayar AS JmlPiut
                , CONCAT('Bayar Tagihan No. Jual : ',mj.buktiTJual,IF(mg.KdMGiro<>'',CONCAT('(',mg.KdMGiro,')'),'')) AS Keterangan
                , -1 as Id_bcf
        FROM MGARTTagihanD d
                LEFT OUTER JOIN MGARTTagihan m ON (d.IdTTagihan=m.IdTTagihan)
                LEFT OUTER JOIN MGARTJual mj ON (d.IdTrans=mj.IdTJual)
            LEFT OUTER JOIN MGKBMGiro mg ON (d.IdMRef=mg.IdMGiro AND jenisMRef='G')
        WHERE m.Hapus =0 AND m.Void = 0
                AND d.jmlbayar <> 0
            AND TglTTagihan <= '${date}'
        UNION ALL 
        SELECT mj.IdMCabang
                , mj.IdMCust AS IdMCust
                , 6.1 AS JenisTrans
                , m.IdMCabang
                , m.IdTTagihan AS IdTrans
                , m.BuktiTTagihan AS BuktiTrans
                , CONCAT(DATE(m.TglTTagihan), ' ', TIME(m.TglUpdate)) AS TglTrans
            , 0 AS JenisInvoice
                , sum(d.JMLBayar) AS JmlPiut
                , CONCAT('Titipan Giro ', m.buktiTTagihan,' (',g.KdMGiro,')' ) AS Keterangan
                , -1 as Id_bcf
        FROM MGARTTagihanD D
                LEFT OUTER JOIN mgarttagihan m ON (d.IdTTagihan=m.IdTTagihan AND d.IdMCabang=m.IdMCabang)
                LEFT OUTER JOIN mgartjual mj ON (d.idMCabang=mj.IdMCabang AND d.IdTrans=mj.IdTJual)
                LEFT OUTER JOIN mgkbmgiro g ON (d.IdMRef=g.IdMGiro AND d.IdMCabang=g.IdMCabang)
        
        WHERE (m.HAPUS = 0 AND m.VOID = 0) AND d.JenisMREF = 'G'
            AND TglTTagihan <= '${date}'
        Group By Keterangan
        Union ALL
        SELECT MCust.IdMCabang AS IdMCabang
            , MCust.IdMCust AS IdMCust
            , 6.2 AS JenisTrans
            , m.IdMCabang
            , M.IdTGiroCair AS IdTrans
            , M.BuktiTGiroCair AS BuktiTrans
            , CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) AS TglTrans
            , 0 AS JenisInvoice
            , -sum(mtd.JMLBayar) AS JmlPiut
            , CONCAT('Giro Cair ', m.BuktiTGiroCair, ' (', MG.KdmGiro, ')') AS Keterangan
                , -1 as Id_bcf
        FROM MGKBTGiroCairD D
            LEFT OUTER JOIN MGKBTGiroCair M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroCair = D.IdTGiroCair)
            LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
            LEFT OUTER JOIN MGARTTagihand mtd ON (mtd.IdMCabang = MG.IdMCabang AND mtd.IdMRef = MG.IdMGiro AND mtd.JenisMREF ='G')
            LEFT OUTER JOIN mgarttagihan mt ON (mt.IdMCabang = mtd.IdMCabang AND mt.IdTTagihan = mtd.IdTTagihan)
        WHERE (M.HAPUS = 0 AND M.VOID = 0) AND M.JenisTGiroCair = 'M' AND mtd.JenisMRef = 'G' AND mt.VOID = 0 AND mt.HAPUS = 0
            AND TglTGiroCair <= '${date}'
        
        group by IdMCust
        Union ALL
        SELECT MCust.IdMCabang AS IdMCabang
            , MCust.IdMCust AS IdMCust
            , 6.3 AS JenisTrans
            , m.IdMCabang
            , M.IdTGiroTolak AS IdTrans
            , M.BuktiTGiroTolak AS BuktiTrans
            , CONCAT(DATE(m.TglTGiroTolak), ' ', TIME(m.tglupdate)) AS TglTrans
            , 0 AS JenisInvoice
            , sum(mtd.JMLBayar) AS JmlPiut
            , CONCAT('Giro Tolak ', m.BuktiTGiroTolak,  ' (', MG.KdMGiro, ')') AS Keterangan
                , -1 as Id_bcf
        FROM MGKBTGiroTolakD D
            LEFT OUTER JOIN MGKBTGiroTolak M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroTolak = D.IdTGiroTolak)
            LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
            LEFT OUTER JOIN MGARTTagihand mtd ON (mtd.IdMCabang = MG.IdMCabang AND mtd.IdMRef = MG.IdMGiro AND mtd.JenisMREF = 'G')
            LEFT OUTER JOIN MGARTTagihan mt ON (mt.IdMCabang = mtd.IdMCabang AND mt.IdTTagihan = mtd.IdTTagihan)
        WHERE (M.HAPUS =0 AND M.VOID = 00)
            AND M.JenisTGiroTolak = 'M' AND mtd.JenisMRef = 'G'
            AND mt.VOID = 0 AND mt.HAPUS = 0
            AND TglTGiroTolak <= '${date}'
        
        group by IdMCust
        union all
        SELECT MCust.IdMCabang AS IdMCabang
            , MCust.IdMCust AS IdMCust
            , 6.4 AS JenisTrans
            , m.IdMCabang
            , M.IDTGiroGanti AS IdTrans
            , M.BuktiTGiroGanti AS BuktiTrans
            , CONCAT(DATE(m.TglTGiroGanti), ' ', TIME(m.tglupdate)) AS TglTrans
            , 0 AS JenisInvoice
            , -D.JMLBayar AS JmlPiut
            , CONCAT('Penggantian Giro ', m.BuktiTGiroGanti, ' (', MG.KdMGiro, ')') AS Keterangan
                , -1 as Id_bcf
        FROM MGKBTGiroGanti M
            LEFT OUTER JOIN MGKBTGiroGantiDG D ON (M.IdMCabang = D.IdMCabang AND M.IDTGiroGanti = D.IDTGiroGanti)
            LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
            LEFT OUTER JOIN MGARTTAgihanD TTagihD ON (TTagihD.IdMCabang = MG.IdMCabang AND TTagihd.IdMRef = MG.IdMGiro AND TTagihD.JenisMREF = 'G')
            LEFT OUTER JOIN MGARTTAgihan TTagih ON (TTagih.IdMCabang = TTagihD.IdMCabang AND TTagih.IdTTagihan = TTagihD.IdTTagihan)
        WHERE (M.HAPUS = 0 AND M.VOID = 0)
            AND M.JenisTGiroGanti = 'M' AND TTagihD.JenisMRef = 'G'
            AND TTagih.VOID = 0 AND TTagih.HAPUS = 0
            AND TglTGiroGanti <= '${date}'
        
        
        ) Tbl
        
            LEFT OUTER JOIN MGARMJenisInvoice JI on (Tbl.JenisInvoice = JI.IdMJenisInvoice and Tbl.IdMCabang = JI.IdMCabang)
            UNION ALL
            SELECT '${date}' as TglTrans, IdMCabang, IdMCabang as IdMCabangMCust, IdMCust, 0 as JmlPiut, -1 as Id_bcf 
            FROM MGARMCust
            ) TransAll
            WHERE TglTrans <= '${date}'
            GROUP BY TransAll.IdMCabang, TransAll.IdMCabangMCust, IdMCust
        ) TablePosPiut LEFT OUTER JOIN MGSYMCabang MCabang ON (TablePosPiut.IdMCabang = MCabang.IdMCabang)
                        LEFT OUTER JOIN MGARMCust MCust ON (TablePosPiut.IdMCabangMCust = MCust.IdMCabang AND TablePosPiut.IdMCust = MCust.IdMCust)
        WHERE MCabang.Hapus = 0
            AND MCust.Hapus = 0
            AND MCust.idmcabang = ${fil.idmcabang}
        AND pospiut != 0
        ORDER BY MCabang.KdMCabang, MCust.NmMCust`;
        const kas = await sequelize.query(sql1, {
          raw: false,
        });

        var arr_item = await Promise.all(
          kas[0].map(async (item, index_satu) => {
            let pospiut = parseFloat(item.pospiut);
            total += pospiut;
            return {
              kode: item.kdmcust,
              nama: item.nmmcust,
              qty: pospiut,
            };
          })
        );

        return {
          cabang: fil.nmmcabang,
          list: arr_item,
        };
      })
    );

    // const total = await fun.countDataFromQuery(
    //     `SELECT MCabang.idMCabang, MCabang.KdMCabang, MCabang.NmMCabang, MCabang.Aktif as AktifMCabang
    //     , Coalesce(MCust.KdMCust,'') as KdMCust, Coalesce(MCust.NmMCust,'') as NmMCust, coalesce(MCust.Aktif,1) as AktifMCust, sum(coalesce(MCust.LimitPiut,0)) as LimitPiut
    //     , sum(TablePosPiut.PosPiut) as total, sum(MCust.LimitPiut - TablePosPiut.PosPiut) As Selisih
    // FROM (
    //     SELECT TransAll.IdMCabang, TransAll.IdMCabangMCust, IdMCust, Sum(JmlPiut) as PosPiut
    //     FROM (SELECT tbl.TglTrans, tbl.IdMCabang, tbl.IdMCabangMCust, tbl.IdMCust, tbl.JmlPiut, Tbl.id_bcf
    // FROM (
    //     SELECT IdMCabang as IdMCabangMCust
    //         , IdMCust
    //         , 0 as JenisTrans
    //         , IdMCabang
    //         , IdTSAPiut as IdTrans
    //         , BuktiTSAPiut as BuktiTrans
    //         , concat(Date(TglTSAPiut), ' ', Time(TglTSAPiut)) as TglTrans
    //         , 0 as JenisInvoice
    //         , JmlPiut
    //         , concat('Saldo Awal ', BuktiTSAPiut) as Keterangan
    //         , -1 as Id_bcf
    //     FROM MGARTSAPiut
    //     WHERE JmlPiut <> 0
    //     AND TglTSAPiut <= '${date}'
    //     UNION ALL
    //     SELECT IdMCabangMCust
    //         , IdMCust
    //         , 1 as JenisTrans
    //         , IdMCabang
    //         , IdTJualPOS as IdTrans
    //         , BuktiTJualPOS as BuktiTrans
    //         , concat(Date(TglTJualPOS), ' ', Time(TglUpdate)) as TglTrans
    //         , 0 as JenisInvoice
    //         , (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) AS JmlPiut
    //         , concat('Penjualan POS ', BuktiTJualPOS) as Keterangan
    //         , -1 as Id_bcf
    //     FROM MGARTJualPOS
    //     WHERE Hapus = 0 AND Void = 0
    //     AND (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) <> 0
    //     AND TglTJualPOS <= '${date}'

    //     UNION ALL
    //     SELECT Jual.IdMCabangMCust
    //         , Jual.IdMCust
    //         , 2 as JenisTrans
    //         , Jual.IdMCabang
    //         , Jual.IdTJual as IdTrans
    //         , Jual.BuktiTJual as BuktiTrans
    //         , concat(Date(Jual.TglTJual), ' ', Time(Jual.TglUpdate)) as TglTrans
    //         , Jual.JenisTJual as JenisInvoice
    //         , (Jual.JmlBayarKredit) AS JmlPiut
    //         , concat('Penjualan ', Jual.BuktiTJual) as Keterangan
    //         , Jual.Id_bcf
    //     FROM MGARTJual Jual
    //     WHERE Jual.Hapus = 0 AND Jual.Void = 0
    //     AND (Jual.JmlBayarKredit) <> 0
    //     AND Coalesce(Jual.IdTRJual, 0) = 0
    //     AND Jual.TglTJual <= '${date}'

    //     UNION ALL
    //     SELECT rj.IdMCabangMCust
    //         , rj.IdMCust
    //         , 3 as JenisTrans
    //         , rj.IdMCabang
    //         , rj.IdTRJual as IdTrans
    //         , rj.BuktiTRJual as BuktiTrans
    //         , concat(Date(rj.TglTRJual), ' ', Time(rj.TglUpdate)) as TglTrans
    //         , rj.JenisInvoice as JenisInvoice
    //         , - rj.JmlBayarKredit AS JmlPiut
    //         , concat('Retur Penjualan ', rj.BuktiTRJual) as Keterangan
    //         , -1 as Id_bcf
    //     FROM MGARTRJual rj
    //         LEFT OUTER JOIN MGARTJual jual ON (jual.IdMCabangTRJual = rj.IdMCabang AND jual.IdTRJual = rj.IdTRJual and jual.Void = 0 and jual.Hapus = 0)
    //         LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = rj.IdMCabangMCust AND MCust.IdMCust = rj.IdMCust)
    //     WHERE rj.Hapus = 0 AND rj.Void = 0
    //     AND rj.JmlBayarKredit <> 0
    //     AND rj.JenisRJual = 0
    //     AND jual.IdTJual IS NULL
    //     AND TglTRJual <= '${date}'

    //     UNION ALL
    //     SELECT TJualLain.IdMCabangCust as IdMCabangMCust
    //         , TJualLain.IdMCust as IdMCust
    //         , 2.1 as JenisTrans
    //         , TJualLain.IdMCabang
    //         , TJualLain.IdTJualLain as IdTrans
    //         , TJualLain.BuktiTJualLain as BuktiTrans
    //         , concat(Date(TJualLain.TglTJualLain), ' ', Time(TJualLain.TglUpdate)) as TglTrans
    //         , 0 as JenisInvoice
    //         , TJualLain.Netto AS JmlPiut
    //         , CONCAT('Penjualan ', ' ', IF(TJualLain.JenisEkspedisi = 1, 'FRANCO',
    //             IF(TJualLain.JenisEkspedisi = 2, 'LOCO', '')), ' '
    //             , IF(TJualLain.CountPrint = -1, '(Peti/Ekspedisi)', ''), '') as Keterangan
    //         , -1 as Id_bcf
    //     FROM MGARTJualLain TJualLain
    //         LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = TJualLain.IdMCabangCust AND MCust.IdMCust = TJualLain.IdMCust)
    //     WHERE TJualLain.Hapus = 0 AND TJualLain.Void = 0
    //     AND TJualLain.Netto <> 0
    //     AND TglTJualLain <= '${date}'

    //     UNION ALL
    //     SELECT TAngkutan.IdMCabangMCust
    //         , TAngkutan.IdMCust
    //         , 2.2 AS JenisTrans
    //         , TAngkutan.IdMCabang
    //         , TAngkutan.IdTTAngkutan AS IdTrans
    //         , TAngkutan.BuktiTTAngkutan AS BuktiTrans
    //         , CONCAT(DATE(TAngkutan.TglTTAngkutan), ' ', TIME(TAngkutan.TglUpdate)) AS TglTrans
    //         , TAngkutan.JenisTTAngkutan AS JenisInvoice
    //         , (TAngkutan.JmlBayarKredit) AS JmlPiut
    //         , CONCAT('Titip Angkutan ', TAngkutan.BuktiTTAngkutan) AS Keterangan
    //         , -1 as Id_bcf
    //     FROM MGARTTAngkutan TAngkutan
    //     WHERE Hapus = 0 AND Void = 0
    //     AND (TAngkutan.JmlBayarKredit) <> 0
    //     AND TglTTAngkutan <= '${date}'
    //     UNION ALL
    //     SELECT m.IdMCabangMCust
    //         , m.IdMCust
    //         , 4 as JenisTrans
    //         , m.IdMCabang
    //         , m.IdTBPiut as IdTrans
    //         , m.BuktiTBPiut as BuktiTrans
    //         , concat(Date(d.TglBayar), ' ', Time(m.TglUpdate)) as TglTrans
    //         , m.JenisInvoice as JenisInvoice
    //         , - (d.JmlBayar - COALESCE(UMJUalD.JmlBayar, 0)) AS JmlPiut
    //         , concat('Pembayaran Piutang ', BuktiTBPiut, ' ' ,if(d.jenisMref = 'K',Kas.KdMKas, if(d.JenisMRef ='B',Rek.KdMRek, if(d.JenisMref ='G' ,Giro.KdMGiro,IF(d.JenisMRef = 'P',Prk.KdMPrk,''))))) as Keterangan
    //         , m.Id_bcf
    //     FROM MGARTBPiutDB d
    //         LEFT OUTER JOIN MGARTBPiut m ON (d.IdMCabang = m.IdMCabang AND d.IdTBPiut = m.IdTBPiut)
    //         LEFT OUTER JOIN MGARTUMJual UMJual ON(m.IdMCabang = UMJual.IdMCabangTBPiut AND m.IdTBPiut = UMJual.IdTBPiut AND UMJual.Hapus = 0 AND UMJual.Void = 0)
    //         LEFT OUTER JOIN MGARTUMJualD UMJualD ON(UMJual.IdMCabang = UMJualD.IdMCabang AND UMJual.IdTUMJual = UMJualD.IdTUMJual AND d.IdMCabangMRef = UMJualD.IdMCabangMRef AND d.IdMref = UMJualD.IdMRef AND d.TglBayar = UMJualD.TglBayar)
    //         LEFT OUTER JOIN MGKBMKas Kas ON(Kas.IdMCabang = d.IdMCabangMref AND Kas.IdMKas = d.IdMref and d.JenisMRef ='K')
    //         LEFT OUTER JOIN MGKBMRek Rek ON(Rek.IdMCabang = d.IdMCabangMref AND Rek.IdMRek = d.IdMref and d.JenisMRef ='B')
    //         LEFT OUTER JOIN MGKBMGiro Giro ON(Giro.IdMCabang = d.IdMCabangMref AND Giro.IdMGiro = d.IdMref and d.JenisMRef ='G')
    //         LEFT OUTER JOIN MGGLMPrk Prk ON( Prk.IdMPrk = d.IdMref and d.JenisMRef ='P' and Prk.Periode = 0)
    //     WHERE m.Hapus = 0 AND m.Void = 0
    //     AND m.Total <> 0
    //     AND d.TglBayar <= '${date}'

    //     UNION ALL
    //     SELECT m.IdMCabangMCust
    //         , m.IdMCust
    //         , 4 as JenisTrans
    //         , m.IdMCabang
    //         , m.IdTBPiut as IdTrans
    //         , m.BuktiTBPiut as BuktiTrans
    //         , concat(Date(m.TglTBPiut), ' ', Time(m.TglUpdate)) as TglTrans
    //         , m.JenisInvoice as JenisInvoice
    //         , - m.JmlUM AS JmlPiut
    //         , concat('Pembayaran Piutang dgn Uang Muka ', BuktiTBPiut) as Keterangan
    //         , m.Id_bcf
    //     FROM MGARTBPiut m
    //     WHERE m.Hapus = 0 AND m.Void = 0
    //     AND m.JmlUM <> 0
    //     AND TglTBPiut <= '${date}'

    //     UNION ALL
    //     SELECT MCust.IdMCabang
    //         , MCust.IdMCust AS IdMCust
    //         , 4.1 AS JenisTrans
    //         , TBPiut.IdMCabang
    //         , TBPiut.IdTBPiut AS IdTrans
    //         , TBPiut.BuktiTBPiut AS BuktiTrans
    //         , CONCAT(DATE(TBPiutB.TglBayar), ' ', TIME(TBPiut.TglUpdate)) AS TglTrans
    //         , TBPiut.JenisInvoice as JenisInvoice
    //         , TBPiutB.JMLBayar AS JmlPiut
    //         , CONCAT('Titipan Giro ', TBPiut.BuktiTBPiut, ' (', MG.KdMGiro, ')') AS Keterangan
    //         , -1 as Id_bcf
    //     FROM MGARTBPiutDB TBPiutB
    //         LEFT OUTER JOIN MGARTBPiut TBPiut ON (TBPiut.IdMCabang = TBPiutB.IdMCabang AND TBPiut.IdTBPiut = TBPiutB.IdTBPiut)
    //         LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = TBPiut.IdMCabangMCust AND MCust.IdMCust = TBPiut.IdMCust)
    //         LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = TBPiutB.IdMCabangMREF AND MG.IdMGiro = TBPiutB.IdMREF)
    //     WHERE (TBPiut.HAPUS = 0 AND TBPiut.VOID = 0) AND TBPiutB.JenisMREF = 'G'
    //     AND TBPiutB.TglBayar <= '${date}'

    //     UNION ALL
    //     SELECT MCust.IdMCabang AS IdMCabang
    //         , MCust.IdMCust AS IdMCust
    //         , 4.2 AS JenisTrans
    //         , m.IdMCabang
    //         , M.IdTGiroCair AS IdTrans
    //         , M.BuktiTGiroCair AS BuktiTrans
    //         , CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) AS TglTrans
    //         , BPiut.JenisInvoice as JenisInvoice
    //         , -BPiutDB.JMLBayar AS JmlPiut
    //         , CONCAT('Giro Cair ', m.BuktiTGiroCair, ' (', MG.KdmGiro, ')') AS Keterangan
    //         , -1 as Id_bcf
    //     FROM MGKBTGiroCairD D
    //         LEFT OUTER JOIN MGKBTGiroCair M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroCair = D.IdTGiroCair)
    //         LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
    //         LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
    //         LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
    //         LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
    //     WHERE (M.HAPUS = 0 AND M.VOID = 0) AND M.JenisTGiroCair = 'M' AND BPiutDB.JenisMRef = 'G' AND BPiut.VOID = 0 AND BPiut.HAPUS = 0
    //     AND TglTGiroCair <= '${date}'

    //     UNION ALL
    //     SELECT MCust.IdMCabang AS IdMCabang
    //         , MCust.IdMCust AS IdMCust
    //         , 4.3 AS JenisTrans
    //         , m.IdMCabang
    //         , M.IdTGiroTolak AS IdTrans
    //         , M.BuktiTGiroTolak AS BuktiTrans
    //         , CONCAT(DATE(m.TglTGiroTolak), ' ', TIME(m.tglupdate)) AS TglTrans
    //         , BPiut.JenisInvoice as JenisInvoice
    //         , BPiutDB.JMLBayar AS JmlPiut
    //         , CONCAT('Giro Tolak ', m.BuktiTGiroTolak,  ' (', MG.KdMGiro, ')') AS Keterangan
    //         , -1 as Id_bcf
    //     FROM MGKBTGiroTolakD D
    //         LEFT OUTER JOIN MGKBTGiroTolak M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroTolak = D.IdTGiroTolak)
    //         LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
    //         LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
    //         LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
    //         LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
    //     WHERE (M.HAPUS = 0 AND M.VOID = 0)
    //         AND M.JenisTGiroTolak = 'M' AND BPiutDB.JenisMRef = 'G'
    //         AND BPiut.VOID = 0 AND BPiut.HAPUS = 0
    //     AND TglTGiroTolak <= '${date}'

    //     UNION ALL
    //     SELECT MCust.IdMCabang AS IdMCabang
    //         , MCust.IdMCust AS IdMCust
    //         , 4.4 AS JenisTrans
    //         , m.IdMCabang
    //         , M.IDTGiroGanti AS IdTrans
    //         , M.BuktiTGiroGanti AS BuktiTrans
    //         , CONCAT(DATE(m.TglTGiroGanti), ' ', TIME(m.tglupdate)) AS TglTrans
    //         , BPiut.JenisInvoice as JenisInvoice
    //         , -D.JMLBayar AS JmlPiut
    //         , CONCAT('Penggantian Giro ', m.BuktiTGiroGanti, ' (', MG.KdMGiro, ')') AS Keterangan
    //         , -1 as Id_bcf
    //     FROM MGKBTGiroGanti M
    //         LEFT OUTER JOIN MGKBTGiroGantiDG D ON (M.IdMCabang = D.IdMCabang AND M.IDTGiroGanti = D.IDTGiroGanti)
    //         LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
    //         LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
    //         LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
    //         LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
    //     WHERE (M.HAPUS = 0 AND M.VOID = 0)
    //         AND M.JenisTGiroGanti = 'M' AND BPiutDB.JenisMRef = 'G'
    //         AND BPiut.VOID = 0 AND BPiut.HAPUS = 0
    //     AND TglTGiroGanti >= '${date}' and TglTGiroGanti < '1899-12-30'

    //     UNION ALL
    //     SELECT IdMCabangMCust
    //         , IdMCust
    //         , 5 as JenisTrans
    //         , IdMCabang
    //         , IdTKorPiut as IdTrans
    //         , BuktiTKorPiut as BuktiTrans
    //         , concat(Date(TglTKorPiut), ' ', Time(TglUpdate)) as TglTrans
    //         , IdMJenisInvoice as JenisInvoice
    //         , Total AS JmlPiut
    //         , concat('Koreksi Piutang ', BuktiTKorPiut) as Keterangan
    //         , -1 as Id_bcf
    //     FROM MGARTKorPiut
    //     WHERE Hapus = 0 AND Void = 0
    //     AND Total <> 0
    //     AND TglTKorPiut <= '${date}'

    //     UNION ALL
    // SELECT mj.IdMCabangMCust
    //         , mj.IdMCust
    //         , 6 AS JenisTrans
    //         , m.IdMCabang
    //         , m.IdTTagihan AS IdTrans
    //         , m.BuktiTTagihan AS BuktiTrans
    //         , CONCAT(DATE(TglTTagihan), ' ', TIME(m.TglUpdate)) AS TglTrans
    //         , 0 AS JenisInvoice
    //         , -d.jmlbayar AS JmlPiut
    //         , CONCAT('Bayar Tagihan No. Jual : ',mj.buktiTJual,IF(mg.KdMGiro<>'',CONCAT('(',mg.KdMGiro,')'),'')) AS Keterangan
    //         , -1 as Id_bcf
    // FROM MGARTTagihanD d
    //         LEFT OUTER JOIN MGARTTagihan m ON (d.IdTTagihan=m.IdTTagihan)
    //         LEFT OUTER JOIN MGARTJual mj ON (d.IdTrans=mj.IdTJual)
    //     LEFT OUTER JOIN MGKBMGiro mg ON (d.IdMRef=mg.IdMGiro AND jenisMRef='G')
    // WHERE m.Hapus =0 AND m.Void = 0
    //         AND d.jmlbayar <> 0
    //     AND TglTTagihan <= '${date}'
    // UNION ALL
    // SELECT mj.IdMCabang
    //         , mj.IdMCust AS IdMCust
    //         , 6.1 AS JenisTrans
    //         , m.IdMCabang
    //         , m.IdTTagihan AS IdTrans
    //         , m.BuktiTTagihan AS BuktiTrans
    //         , CONCAT(DATE(m.TglTTagihan), ' ', TIME(m.TglUpdate)) AS TglTrans
    //     , 0 AS JenisInvoice
    //         , sum(d.JMLBayar) AS JmlPiut
    //         , CONCAT('Titipan Giro ', m.buktiTTagihan,' (',g.KdMGiro,')' ) AS Keterangan
    //         , -1 as Id_bcf
    // FROM MGARTTagihanD D
    //         LEFT OUTER JOIN mgarttagihan m ON (d.IdTTagihan=m.IdTTagihan AND d.IdMCabang=m.IdMCabang)
    //         LEFT OUTER JOIN mgartjual mj ON (d.idMCabang=mj.IdMCabang AND d.IdTrans=mj.IdTJual)
    //         LEFT OUTER JOIN mgkbmgiro g ON (d.IdMRef=g.IdMGiro AND d.IdMCabang=g.IdMCabang)

    // WHERE (m.HAPUS = 0 AND m.VOID = 0) AND d.JenisMREF = 'G'
    //     AND TglTTagihan <= '${date}'
    // Group By Keterangan
    // Union ALL
    // SELECT MCust.IdMCabang AS IdMCabang
    //     , MCust.IdMCust AS IdMCust
    //     , 6.2 AS JenisTrans
    //     , m.IdMCabang
    //     , M.IdTGiroCair AS IdTrans
    //     , M.BuktiTGiroCair AS BuktiTrans
    //     , CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) AS TglTrans
    //     , 0 AS JenisInvoice
    //     , -sum(mtd.JMLBayar) AS JmlPiut
    //     , CONCAT('Giro Cair ', m.BuktiTGiroCair, ' (', MG.KdmGiro, ')') AS Keterangan
    //         , -1 as Id_bcf
    // FROM MGKBTGiroCairD D
    //     LEFT OUTER JOIN MGKBTGiroCair M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroCair = D.IdTGiroCair)
    //     LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
    //     LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
    //     LEFT OUTER JOIN MGARTTagihand mtd ON (mtd.IdMCabang = MG.IdMCabang AND mtd.IdMRef = MG.IdMGiro AND mtd.JenisMREF ='G')
    //     LEFT OUTER JOIN mgarttagihan mt ON (mt.IdMCabang = mtd.IdMCabang AND mt.IdTTagihan = mtd.IdTTagihan)
    // WHERE (M.HAPUS = 0 AND M.VOID = 0) AND M.JenisTGiroCair = 'M' AND mtd.JenisMRef = 'G' AND mt.VOID = 0 AND mt.HAPUS = 0
    //     AND TglTGiroCair <= '${date}'

    // group by IdMCust
    // Union ALL
    // SELECT MCust.IdMCabang AS IdMCabang
    //     , MCust.IdMCust AS IdMCust
    //     , 6.3 AS JenisTrans
    //     , m.IdMCabang
    //     , M.IdTGiroTolak AS IdTrans
    //     , M.BuktiTGiroTolak AS BuktiTrans
    //     , CONCAT(DATE(m.TglTGiroTolak), ' ', TIME(m.tglupdate)) AS TglTrans
    //     , 0 AS JenisInvoice
    //     , sum(mtd.JMLBayar) AS JmlPiut
    //     , CONCAT('Giro Tolak ', m.BuktiTGiroTolak,  ' (', MG.KdMGiro, ')') AS Keterangan
    //         , -1 as Id_bcf
    // FROM MGKBTGiroTolakD D
    //     LEFT OUTER JOIN MGKBTGiroTolak M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroTolak = D.IdTGiroTolak)
    //     LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
    //     LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
    //     LEFT OUTER JOIN MGARTTagihand mtd ON (mtd.IdMCabang = MG.IdMCabang AND mtd.IdMRef = MG.IdMGiro AND mtd.JenisMREF = 'G')
    //     LEFT OUTER JOIN MGARTTagihan mt ON (mt.IdMCabang = mtd.IdMCabang AND mt.IdTTagihan = mtd.IdTTagihan)
    // WHERE (M.HAPUS =0 AND M.VOID = 00)
    //     AND M.JenisTGiroTolak = 'M' AND mtd.JenisMRef = 'G'
    //     AND mt.VOID = 0 AND mt.HAPUS = 0
    //     AND TglTGiroTolak <= '${date}'

    // group by IdMCust
    // union all
    // SELECT MCust.IdMCabang AS IdMCabang
    //     , MCust.IdMCust AS IdMCust
    //     , 6.4 AS JenisTrans
    //     , m.IdMCabang
    //     , M.IDTGiroGanti AS IdTrans
    //     , M.BuktiTGiroGanti AS BuktiTrans
    //     , CONCAT(DATE(m.TglTGiroGanti), ' ', TIME(m.tglupdate)) AS TglTrans
    //     , 0 AS JenisInvoice
    //     , -D.JMLBayar AS JmlPiut
    //     , CONCAT('Penggantian Giro ', m.BuktiTGiroGanti, ' (', MG.KdMGiro, ')') AS Keterangan
    //         , -1 as Id_bcf
    // FROM MGKBTGiroGanti M
    //     LEFT OUTER JOIN MGKBTGiroGantiDG D ON (M.IdMCabang = D.IdMCabang AND M.IDTGiroGanti = D.IDTGiroGanti)
    //     LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
    //     LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
    //     LEFT OUTER JOIN MGARTTAgihanD TTagihD ON (TTagihD.IdMCabang = MG.IdMCabang AND TTagihd.IdMRef = MG.IdMGiro AND TTagihD.JenisMREF = 'G')
    //     LEFT OUTER JOIN MGARTTAgihan TTagih ON (TTagih.IdMCabang = TTagihD.IdMCabang AND TTagih.IdTTagihan = TTagihD.IdTTagihan)
    // WHERE (M.HAPUS = 0 AND M.VOID = 0)
    //     AND M.JenisTGiroGanti = 'M' AND TTagihD.JenisMRef = 'G'
    //     AND TTagih.VOID = 0 AND TTagih.HAPUS = 0
    //     AND TglTGiroGanti <= '${date}'

    // ) Tbl

    //     LEFT OUTER JOIN MGARMJenisInvoice JI on (Tbl.JenisInvoice = JI.IdMJenisInvoice and Tbl.IdMCabang = JI.IdMCabang)
    //     UNION ALL
    //     SELECT '${date}' as TglTrans, IdMCabang, IdMCabang as IdMCabangMCust, IdMCust, 0 as JmlPiut, -1 as Id_bcf
    //     FROM MGARMCust
    //     ) TransAll
    //     WHERE TglTrans <= '${date}'
    //     GROUP BY TransAll.IdMCabang, TransAll.IdMCabangMCust, IdMCust
    // ) TablePosPiut LEFT OUTER JOIN MGSYMCabang MCabang ON (TablePosPiut.IdMCabang = MCabang.IdMCabang)
    //                 LEFT OUTER JOIN MGARMCust MCust ON (TablePosPiut.IdMCabangMCust = MCust.IdMCabang AND TablePosPiut.IdMCust = MCust.IdMCust)
    // WHERE MCabang.Hapus = 0
    //     AND MCust.Hapus = 0
    // ORDER BY MCabang.KdMCabang, MCust.NmMCust`
    // );

    var count = {
      total: total,
    };

    res.json({
      message: "Success",
      countData: count,
      data: arr_data,
    });
  }

  // kartu piutang
  else if (jenis == 2) {
    let start = req.body.start || today;
    let end = req.body.end || today;

    let sql = `SELECT idmcust, nmmcust FROM mgarmcust where aktif=1 and hapus=0`;
    const filter = await sequelize.query(sql, {
      raw: false,
    });

    var arr_data = await Promise.all(
      filter[0].map(async (fil, index) => {
        let sql1 = `SELECT MCabang.KdMCabang
            , MCabang.NmMCabang
            , Coalesce(MCust.KdMCust, '') as KdMCust
            , coalesce(MCust.NmMCust, '') as NmMCust
            , Bcf.no_bcf
            , TableKartuPiut.IdMCabangMCust As IdMCabangMCust
            , TableKartuPiut.IdMCust
            , TableKartuPiut.IdMCabangTrans
            , TableKartuPiut.IdTrans
            , TableKartuPiut.JenisTrans
            , Urut
            , BuktiTrans
            , cast(TglTrans as datetime) as TglTrans
            , TableKartuPiut.Keterangan, Saldo, JmlPiut
            , IF(Urut = 0, 0, IF(Coalesce(JmlPiut,0) > 0, Coalesce(JmlPiut,0), 0)) As Debit
            , IF(Urut = 0, 0, IF(Coalesce(JmlPiut,0) >= 0, 0, Coalesce(JmlPiut,0))) As Kredit
        FROM (
        SELECT IdMCabangTrans, IdMCabangMCust, IdMCust, 0 As Urut, 0 as JenisTrans, 0 as IdTrans, '-' As BuktiTrans, cast('${start}' as Date) As TglTrans, 0 As JmlPiut, sum(JmlPiut) As Saldo, 'Saldo Sebelumnya' As Keterangan
        , Id_bcf
        FROM (
            SELECT IdMCabang as IdMCabangTrans, IdMCabang as IdMCabangMCust, IdMCust, 0 As JmlPiut, -1 as id_bcf FROM MGARMCust
            UNION ALL
            SELECT LKartuPiut.IdMCabang as IdMCabangTrans, LKartuPiut.IdMCabangMCust, LKartuPiut.IdMCust, Sum(LKartuPiut.JmlPiut) as JmlPiut
            ,LKartuPiut.id_bcf
            FROM (
            SELECT IdMCabang as IdMCabangMCust 
                , IdMCust 
                , 0 as JenisTrans 
                , IdMCabang 
                , IdTSAPiut as IdTrans 
                , BuktiTSAPiut as BuktiTrans 
                , concat(Date(TglTSAPiut), ' ', Time(TglTSAPiut)) as TglTrans 
                , 0 as JenisInvoice 
                , JmlPiut 
                , concat('Saldo Awal ', BuktiTSAPiut) as Keterangan 
                , -1 as Id_bcf
            FROM MGARTSAPiut 
            WHERE JmlPiut <> 0 
            AND TglTSAPiut < '${start}'
            UNION ALL 
            SELECT IdMCabangMCust 
                , IdMCust 
                , 1 as JenisTrans 
                , IdMCabang 
                , IdTJualPOS as IdTrans 
                , BuktiTJualPOS as BuktiTrans 
                , concat(Date(TglTJualPOS), ' ', Time(TglUpdate)) as TglTrans 
                , 0 as JenisInvoice 
                , (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) AS JmlPiut 
                , concat('Penjualan POS ', BuktiTJualPOS) as Keterangan 
                , -1 as Id_bcf
            FROM MGARTJualPOS 
            WHERE Hapus = 0 AND Void = 0 
            AND (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) <> 0 
            AND TglTJualPOS < '${start}'
        
            UNION ALL 
            SELECT Jual.IdMCabangMCust 
                , Jual.IdMCust 
                , 2 as JenisTrans 
                , Jual.IdMCabang 
                , Jual.IdTJual as IdTrans 
                , Jual.BuktiTJual as BuktiTrans 
                , concat(Date(Jual.TglTJual), ' ', Time(Jual.TglUpdate)) as TglTrans 
                , Jual.JenisTJual as JenisInvoice 
                , (Jual.JmlBayarKredit) AS JmlPiut 
                , concat('Penjualan ', Jual.BuktiTJual) as Keterangan 
                , Jual.Id_bcf
            FROM MGARTJual Jual
            WHERE Jual.Hapus = 0 AND Jual.Void = 0 
            AND (Jual.JmlBayarKredit) <> 0 
            AND Coalesce(Jual.IdTRJual, 0) = 0
            AND Jual.TglTJual < '${start}'
        
            UNION ALL 
            SELECT rj.IdMCabangMCust 
                , rj.IdMCust 
                , 3 as JenisTrans 
                , rj.IdMCabang 
                , rj.IdTRJual as IdTrans 
                , rj.BuktiTRJual as BuktiTrans 
                , concat(Date(rj.TglTRJual), ' ', Time(rj.TglUpdate)) as TglTrans 
                , rj.JenisInvoice as JenisInvoice 
                , - rj.JmlBayarKredit AS JmlPiut 
                , concat('Retur Penjualan ', rj.BuktiTRJual) as Keterangan 
                , -1 as Id_bcf
            FROM MGARTRJual rj
                LEFT OUTER JOIN MGARTJual jual ON (jual.IdMCabangTRJual = rj.IdMCabang AND jual.IdTRJual = rj.IdTRJual and jual.Void = 0 and jual.Hapus = 0)
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = rj.IdMCabangMCust AND MCust.IdMCust = rj.IdMCust)
            WHERE rj.Hapus = 0 AND rj.Void = 0 
            AND rj.JmlBayarKredit <> 0 
            AND rj.JenisRJual = 0
            AND jual.IdTJual IS NULL
            AND TglTRJual < '${start}'
        
            UNION ALL 
            SELECT TJualLain.IdMCabangCust as IdMCabangMCust 
                , TJualLain.IdMCust as IdMCust 
                , 2.1 as JenisTrans 
                , TJualLain.IdMCabang 
                , TJualLain.IdTJualLain as IdTrans 
                , TJualLain.BuktiTJualLain as BuktiTrans 
                , concat(Date(TJualLain.TglTJualLain), ' ', Time(TJualLain.TglUpdate)) as TglTrans 
                , 0 as JenisInvoice 
                , TJualLain.Netto AS JmlPiut 
                , CONCAT('Penjualan ', ' ', IF(TJualLain.JenisEkspedisi = 1, 'FRANCO',
                    IF(TJualLain.JenisEkspedisi = 2, 'LOCO', '')), ' '
                    , IF(TJualLain.CountPrint = -1, '(Peti/Ekspedisi)', ''), '') as Keterangan
                , -1 as Id_bcf
            FROM MGARTJualLain TJualLain
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = TJualLain.IdMCabangCust AND MCust.IdMCust = TJualLain.IdMCust)
            WHERE TJualLain.Hapus = 0 AND TJualLain.Void = 0 
            AND TJualLain.Netto <> 0 
            AND TglTJualLain < '${start}'
        
            UNION ALL 
            SELECT TAngkutan.IdMCabangMCust
                , TAngkutan.IdMCust
                , 2.2 AS JenisTrans
                , TAngkutan.IdMCabang
                , TAngkutan.IdTTAngkutan AS IdTrans
                , TAngkutan.BuktiTTAngkutan AS BuktiTrans
                , CONCAT(DATE(TAngkutan.TglTTAngkutan), ' ', TIME(TAngkutan.TglUpdate)) AS TglTrans
                , TAngkutan.JenisTTAngkutan AS JenisInvoice
                , (TAngkutan.JmlBayarKredit) AS JmlPiut
                , CONCAT('Titip Angkutan ', TAngkutan.BuktiTTAngkutan) AS Keterangan
                , -1 as Id_bcf
            FROM MGARTTAngkutan TAngkutan
            WHERE Hapus = 0 AND Void = 0
            AND (TAngkutan.JmlBayarKredit) <> 0
            AND TglTTAngkutan < '${start}'
            UNION ALL 
            SELECT m.IdMCabangMCust 
                , m.IdMCust 
                , 4 as JenisTrans 
                , m.IdMCabang 
                , m.IdTBPiut as IdTrans 
                , m.BuktiTBPiut as BuktiTrans 
                , concat(Date(d.TglBayar), ' ', Time(m.TglUpdate)) as TglTrans 
                , m.JenisInvoice as JenisInvoice 
                , - (d.JmlBayar - COALESCE(UMJUalD.JmlBayar, 0)) AS JmlPiut 
                , concat('Pembayaran Piutang ', BuktiTBPiut, ' ' ,if(d.jenisMref = 'K',Kas.KdMKas, if(d.JenisMRef ='B',Rek.KdMRek, if(d.JenisMref ='G' ,Giro.KdMGiro,IF(d.JenisMRef = 'P',Prk.KdMPrk,''))))) as Keterangan  
                , m.Id_bcf
            FROM MGARTBPiutDB d 
                LEFT OUTER JOIN MGARTBPiut m ON (d.IdMCabang = m.IdMCabang AND d.IdTBPiut = m.IdTBPiut)
                LEFT OUTER JOIN MGARTUMJual UMJual ON(m.IdMCabang = UMJual.IdMCabangTBPiut AND m.IdTBPiut = UMJual.IdTBPiut AND UMJual.Hapus = 0 AND UMJual.Void = 0)
                LEFT OUTER JOIN MGARTUMJualD UMJualD ON(UMJual.IdMCabang = UMJualD.IdMCabang AND UMJual.IdTUMJual = UMJualD.IdTUMJual AND d.IdMCabangMRef = UMJualD.IdMCabangMRef AND d.IdMref = UMJualD.IdMRef AND d.TglBayar = UMJualD.TglBayar)
                LEFT OUTER JOIN MGKBMKas Kas ON(Kas.IdMCabang = d.IdMCabangMref AND Kas.IdMKas = d.IdMref and d.JenisMRef ='K')
                LEFT OUTER JOIN MGKBMRek Rek ON(Rek.IdMCabang = d.IdMCabangMref AND Rek.IdMRek = d.IdMref and d.JenisMRef ='B')
                LEFT OUTER JOIN MGKBMGiro Giro ON(Giro.IdMCabang = d.IdMCabangMref AND Giro.IdMGiro = d.IdMref and d.JenisMRef ='G')
                LEFT OUTER JOIN MGGLMPrk Prk ON( Prk.IdMPrk = d.IdMref and d.JenisMRef ='P' and Prk.Periode = 0)
            WHERE m.Hapus = 0 AND m.Void = 0
            AND m.Total <> 0 
            AND d.TglBayar < '${start}'
        
            UNION ALL 
            SELECT m.IdMCabangMCust 
                , m.IdMCust 
                , 4 as JenisTrans 
                , m.IdMCabang 
                , m.IdTBPiut as IdTrans 
                , m.BuktiTBPiut as BuktiTrans 
                , concat(Date(m.TglTBPiut), ' ', Time(m.TglUpdate)) as TglTrans 
                , m.JenisInvoice as JenisInvoice 
                , - m.JmlUM AS JmlPiut 
                , concat('Pembayaran Piutang dgn Uang Muka ', BuktiTBPiut) as Keterangan  
                , m.Id_bcf
            FROM MGARTBPiut m
            WHERE m.Hapus = 0 AND m.Void = 0
            AND m.JmlUM <> 0 
            AND TglTBPiut < '${start}'
        
            UNION ALL 
            SELECT MCust.IdMCabang
                , MCust.IdMCust AS IdMCust
                , 4.1 AS JenisTrans
                , TBPiut.IdMCabang
                , TBPiut.IdTBPiut AS IdTrans
                , TBPiut.BuktiTBPiut AS BuktiTrans
                , CONCAT(DATE(TBPiutB.TglBayar), ' ', TIME(TBPiut.TglUpdate)) AS TglTrans
                , TBPiut.JenisInvoice as JenisInvoice 
                , TBPiutB.JMLBayar AS JmlPiut
                , CONCAT('Titipan Giro ', TBPiut.BuktiTBPiut, ' (', MG.KdMGiro, ')') AS Keterangan
                , -1 as Id_bcf
            FROM MGARTBPiutDB TBPiutB
                LEFT OUTER JOIN MGARTBPiut TBPiut ON (TBPiut.IdMCabang = TBPiutB.IdMCabang AND TBPiut.IdTBPiut = TBPiutB.IdTBPiut)
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = TBPiut.IdMCabangMCust AND MCust.IdMCust = TBPiut.IdMCust)
                LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = TBPiutB.IdMCabangMREF AND MG.IdMGiro = TBPiutB.IdMREF)
            WHERE (TBPiut.HAPUS = 0 AND TBPiut.VOID = 0) AND TBPiutB.JenisMREF = 'G'
            AND TBPiutB.TglBayar < '${start}'
        
            UNION ALL 
            SELECT MCust.IdMCabang AS IdMCabang
                , MCust.IdMCust AS IdMCust
                , 4.2 AS JenisTrans
                , m.IdMCabang
                , M.IdTGiroCair AS IdTrans 
                , M.BuktiTGiroCair AS BuktiTrans 
                , CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) AS TglTrans
                , BPiut.JenisInvoice as JenisInvoice 
                , -BPiutDB.JMLBayar AS JmlPiut 
                , CONCAT('Giro Cair ', m.BuktiTGiroCair, ' (', MG.KdmGiro, ')') AS Keterangan 
                , -1 as Id_bcf
            FROM MGKBTGiroCairD D
                LEFT OUTER JOIN MGKBTGiroCair M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroCair = D.IdTGiroCair)
                LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
                LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
                LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
            WHERE (M.HAPUS = 0 AND M.VOID = 0) AND M.JenisTGiroCair = 'M' AND BPiutDB.JenisMRef = 'G' AND BPiut.VOID = 0 AND BPiut.HAPUS = 0
            AND TglTGiroCair < '${start}'
        
            UNION ALL 
            SELECT MCust.IdMCabang AS IdMCabang
                , MCust.IdMCust AS IdMCust
                , 4.3 AS JenisTrans
                , m.IdMCabang
                , M.IdTGiroTolak AS IdTrans
                , M.BuktiTGiroTolak AS BuktiTrans
                , CONCAT(DATE(m.TglTGiroTolak), ' ', TIME(m.tglupdate)) AS TglTrans
                , BPiut.JenisInvoice as JenisInvoice 
                , BPiutDB.JMLBayar AS JmlPiut
                , CONCAT('Giro Tolak ', m.BuktiTGiroTolak,  ' (', MG.KdMGiro, ')') AS Keterangan
                , -1 as Id_bcf
            FROM MGKBTGiroTolakD D
                LEFT OUTER JOIN MGKBTGiroTolak M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroTolak = D.IdTGiroTolak)
                LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
                LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
                LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
            WHERE (M.HAPUS = 0 AND M.VOID = 0) 
                AND M.JenisTGiroTolak = 'M' AND BPiutDB.JenisMRef = 'G' 
                AND BPiut.VOID = 0 AND BPiut.HAPUS = 0
            AND TglTGiroTolak < '${start}'
        
            UNION ALL 
            SELECT MCust.IdMCabang AS IdMCabang 
                , MCust.IdMCust AS IdMCust 
                , 4.4 AS JenisTrans
                , m.IdMCabang
                , M.IDTGiroGanti AS IdTrans
                , M.BuktiTGiroGanti AS BuktiTrans
                , CONCAT(DATE(m.TglTGiroGanti), ' ', TIME(m.tglupdate)) AS TglTrans
                , BPiut.JenisInvoice as JenisInvoice 
                , -D.JMLBayar AS JmlPiut
                , CONCAT('Penggantian Giro ', m.BuktiTGiroGanti, ' (', MG.KdMGiro, ')') AS Keterangan
                , -1 as Id_bcf
            FROM MGKBTGiroGanti M
                LEFT OUTER JOIN MGKBTGiroGantiDG D ON (M.IdMCabang = D.IdMCabang AND M.IDTGiroGanti = D.IDTGiroGanti)
                LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
                LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
                LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
            WHERE (M.HAPUS = 0 AND M.VOID = 0)
                AND M.JenisTGiroGanti = 'M' AND BPiutDB.JenisMRef = 'G'
                AND BPiut.VOID = 0 AND BPiut.HAPUS = 0
            AND TglTGiroGanti >= '${start}' and TglTGiroGanti < '1899-12-30 00:00:00'
        
            UNION ALL 
            SELECT IdMCabangMCust 
                , IdMCust 
                , 5 as JenisTrans 
                , IdMCabang 
                , IdTKorPiut as IdTrans 
                , BuktiTKorPiut as BuktiTrans 
                , concat(Date(TglTKorPiut), ' ', Time(TglUpdate)) as TglTrans 
                , IdMJenisInvoice as JenisInvoice 
                , Total AS JmlPiut 
                , concat('Koreksi Piutang ', BuktiTKorPiut) as Keterangan 
                , -1 as Id_bcf
            FROM MGARTKorPiut 
            WHERE Hapus = 0 AND Void = 0
            AND Total <> 0 
            AND TglTKorPiut < '${start}'
        
            UNION ALL 
        SELECT mj.IdMCabangMCust
                , mj.IdMCust
                , 6 AS JenisTrans
                , m.IdMCabang
                , m.IdTTagihan AS IdTrans
                , m.BuktiTTagihan AS BuktiTrans
                , CONCAT(DATE(TglTTagihan), ' ', TIME(m.TglUpdate)) AS TglTrans
                , 0 AS JenisInvoice
                , -d.jmlbayar AS JmlPiut
                , CONCAT('Bayar Tagihan No. Jual : ',mj.buktiTJual,IF(mg.KdMGiro<>'',CONCAT('(',mg.KdMGiro,')'),'')) AS Keterangan
                , -1 as Id_bcf
        FROM MGARTTagihanD d
                LEFT OUTER JOIN MGARTTagihan m ON (d.IdTTagihan=m.IdTTagihan)
                LEFT OUTER JOIN MGARTJual mj ON (d.IdTrans=mj.IdTJual)
            LEFT OUTER JOIN MGKBMGiro mg ON (d.IdMRef=mg.IdMGiro AND jenisMRef='G')
        WHERE m.Hapus =0 AND m.Void = 0
                AND d.jmlbayar <> 0
            AND TglTTagihan < '${start}'
        UNION ALL 
        SELECT mj.IdMCabang
                , mj.IdMCust AS IdMCust
                , 6.1 AS JenisTrans
                , m.IdMCabang
                , m.IdTTagihan AS IdTrans
                , m.BuktiTTagihan AS BuktiTrans
                , CONCAT(DATE(m.TglTTagihan), ' ', TIME(m.TglUpdate)) AS TglTrans
            , 0 AS JenisInvoice
                , sum(d.JMLBayar) AS JmlPiut
                , CONCAT('Titipan Giro ', m.buktiTTagihan,' (',g.KdMGiro,')' ) AS Keterangan
                , -1 as Id_bcf
        FROM MGARTTagihanD D
                LEFT OUTER JOIN mgarttagihan m ON (d.IdTTagihan=m.IdTTagihan AND d.IdMCabang=m.IdMCabang)
                LEFT OUTER JOIN mgartjual mj ON (d.idMCabang=mj.IdMCabang AND d.IdTrans=mj.IdTJual)
                LEFT OUTER JOIN mgkbmgiro g ON (d.IdMRef=g.IdMGiro AND d.IdMCabang=g.IdMCabang)
        
        WHERE (m.HAPUS = 0 AND m.VOID = 0) AND d.JenisMREF = 'G'
            AND TglTTagihan < '${start}'
        Group By Keterangan
        Union ALL
        SELECT MCust.IdMCabang AS IdMCabang
            , MCust.IdMCust AS IdMCust
            , 6.2 AS JenisTrans
            , m.IdMCabang
            , M.IdTGiroCair AS IdTrans
            , M.BuktiTGiroCair AS BuktiTrans
            , CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) AS TglTrans
            , 0 AS JenisInvoice
            , -sum(mtd.JMLBayar) AS JmlPiut
            , CONCAT('Giro Cair ', m.BuktiTGiroCair, ' (', MG.KdmGiro, ')') AS Keterangan
                , -1 as Id_bcf
        FROM MGKBTGiroCairD D
            LEFT OUTER JOIN MGKBTGiroCair M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroCair = D.IdTGiroCair)
            LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
            LEFT OUTER JOIN MGARTTagihand mtd ON (mtd.IdMCabang = MG.IdMCabang AND mtd.IdMRef = MG.IdMGiro AND mtd.JenisMREF ='G')
            LEFT OUTER JOIN mgarttagihan mt ON (mt.IdMCabang = mtd.IdMCabang AND mt.IdTTagihan = mtd.IdTTagihan)
        WHERE (M.HAPUS = 0 AND M.VOID = 0) AND M.JenisTGiroCair = 'M' AND mtd.JenisMRef = 'G' AND mt.VOID = 0 AND mt.HAPUS = 0
            AND TglTGiroCair < '${start}'
        
        group by IdMCust
        Union ALL
        SELECT MCust.IdMCabang AS IdMCabang
            , MCust.IdMCust AS IdMCust
            , 6.3 AS JenisTrans
            , m.IdMCabang
            , M.IdTGiroTolak AS IdTrans
            , M.BuktiTGiroTolak AS BuktiTrans
            , CONCAT(DATE(m.TglTGiroTolak), ' ', TIME(m.tglupdate)) AS TglTrans
            , 0 AS JenisInvoice
            , sum(mtd.JMLBayar) AS JmlPiut
            , CONCAT('Giro Tolak ', m.BuktiTGiroTolak,  ' (', MG.KdMGiro, ')') AS Keterangan
                , -1 as Id_bcf
        FROM MGKBTGiroTolakD D
            LEFT OUTER JOIN MGKBTGiroTolak M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroTolak = D.IdTGiroTolak)
            LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
            LEFT OUTER JOIN MGARTTagihand mtd ON (mtd.IdMCabang = MG.IdMCabang AND mtd.IdMRef = MG.IdMGiro AND mtd.JenisMREF = 'G')
            LEFT OUTER JOIN MGARTTagihan mt ON (mt.IdMCabang = mtd.IdMCabang AND mt.IdTTagihan = mtd.IdTTagihan)
        WHERE (M.HAPUS =0 AND M.VOID = 00)
            AND M.JenisTGiroTolak = 'M' AND mtd.JenisMRef = 'G'
            AND mt.VOID = 0 AND mt.HAPUS = 0
            AND TglTGiroTolak < '${start}'
        
        group by IdMCust
        union all
        SELECT MCust.IdMCabang AS IdMCabang
            , MCust.IdMCust AS IdMCust
            , 6.4 AS JenisTrans
            , m.IdMCabang
            , M.IDTGiroGanti AS IdTrans
            , M.BuktiTGiroGanti AS BuktiTrans
            , CONCAT(DATE(m.TglTGiroGanti), ' ', TIME(m.tglupdate)) AS TglTrans
            , 0 AS JenisInvoice
            , -D.JMLBayar AS JmlPiut
            , CONCAT('Penggantian Giro ', m.BuktiTGiroGanti, ' (', MG.KdMGiro, ')') AS Keterangan
                , -1 as Id_bcf
        FROM MGKBTGiroGanti M
            LEFT OUTER JOIN MGKBTGiroGantiDG D ON (M.IdMCabang = D.IdMCabang AND M.IDTGiroGanti = D.IDTGiroGanti)
            LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
            LEFT OUTER JOIN MGARTTAgihanD TTagihD ON (TTagihD.IdMCabang = MG.IdMCabang AND TTagihd.IdMRef = MG.IdMGiro AND TTagihD.JenisMREF = 'G')
            LEFT OUTER JOIN MGARTTAgihan TTagih ON (TTagih.IdMCabang = TTagihD.IdMCabang AND TTagih.IdTTagihan = TTagihD.IdTTagihan)
        WHERE (M.HAPUS = 0 AND M.VOID = 0)
            AND M.JenisTGiroGanti = 'M' AND TTagihD.JenisMRef = 'G'
            AND TTagih.VOID = 0 AND TTagih.HAPUS = 0
            AND TglTGiroGanti < '${start}'
        
        
            ) LKartuPiut
            left outer join mgarmjenisinvoice m on (lkartupiut.jenisinvoice = m.idmjenisinvoice)
            WHERE TglTrans < '${start}'
        GROUP BY LKartuPiut.IdMCabang, LKartuPiut.IdMCabangMCust, LKartuPiut.IdMCust
        ) TableSaldoAwal
        GROUP BY IdMCabangTrans, IdMCabangMCust, IdMCust
        UNION ALL
        SELECT LKartuPiut.IdMCabang as IdMCabangTrans, LKartuPiut.IdMCabangMCust, LKartuPiut.IdMCust, 1 as Urut, LKartuPiut.JenisTrans, LKartuPiut.IdTrans, LKartuPiut.BuktiTrans, LKartuPiut.TglTrans, LKartuPiut.JmlPiut, 0, LKartuPiut.Keterangan
            ,LKartuPiut.id_bcf
        FROM (
            SELECT IdMCabang as IdMCabangMCust 
                , IdMCust 
                , 0 as JenisTrans 
                , IdMCabang 
                , IdTSAPiut as IdTrans 
                , BuktiTSAPiut as BuktiTrans 
                , concat(Date(TglTSAPiut), ' ', Time(TglTSAPiut)) as TglTrans 
                , 0 as JenisInvoice 
                , JmlPiut 
                , concat('Saldo Awal ', BuktiTSAPiut) as Keterangan 
                , -1 as Id_bcf
            FROM MGARTSAPiut 
            WHERE JmlPiut <> 0 
            AND TglTSAPiut >= '${start}' and TglTSAPiut < '${end}'
            UNION ALL 
            SELECT IdMCabangMCust 
                , IdMCust 
                , 1 as JenisTrans 
                , IdMCabang 
                , IdTJualPOS as IdTrans 
                , BuktiTJualPOS as BuktiTrans 
                , concat(Date(TglTJualPOS), ' ', Time(TglUpdate)) as TglTrans 
                , 0 as JenisInvoice 
                , (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) AS JmlPiut 
                , concat('Penjualan POS ', BuktiTJualPOS) as Keterangan 
                , -1 as Id_bcf
            FROM MGARTJualPOS 
            WHERE Hapus = 0 AND Void = 0 
            AND (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) <> 0 
            AND TglTJualPOS >= '${start}' and TglTJualPOS < '${end}'
        
            UNION ALL 
            SELECT Jual.IdMCabangMCust 
                , Jual.IdMCust 
                , 2 as JenisTrans 
                , Jual.IdMCabang 
                , Jual.IdTJual as IdTrans 
                , Jual.BuktiTJual as BuktiTrans 
                , concat(Date(Jual.TglTJual), ' ', Time(Jual.TglUpdate)) as TglTrans 
                , Jual.JenisTJual as JenisInvoice 
                , (Jual.JmlBayarKredit) AS JmlPiut 
                , concat('Penjualan ', Jual.BuktiTJual) as Keterangan 
                , Jual.Id_bcf
            FROM MGARTJual Jual
            WHERE Jual.Hapus = 0 AND Jual.Void = 0 
            AND (Jual.JmlBayarKredit) <> 0 
            AND Coalesce(Jual.IdTRJual, 0) = 0
            AND Jual.TglTJual >= '${start}' and Jual.TglTJual < '${end}'
        
            UNION ALL 
            SELECT rj.IdMCabangMCust 
                , rj.IdMCust 
                , 3 as JenisTrans 
                , rj.IdMCabang 
                , rj.IdTRJual as IdTrans 
                , rj.BuktiTRJual as BuktiTrans 
                , concat(Date(rj.TglTRJual), ' ', Time(rj.TglUpdate)) as TglTrans 
                , rj.JenisInvoice as JenisInvoice 
                , - rj.JmlBayarKredit AS JmlPiut 
                , concat('Retur Penjualan ', rj.BuktiTRJual) as Keterangan 
                , -1 as Id_bcf
            FROM MGARTRJual rj
                LEFT OUTER JOIN MGARTJual jual ON (jual.IdMCabangTRJual = rj.IdMCabang AND jual.IdTRJual = rj.IdTRJual and jual.Void = 0 and jual.Hapus = 0)
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = rj.IdMCabangMCust AND MCust.IdMCust = rj.IdMCust)
            WHERE rj.Hapus = 0 AND rj.Void = 0 
            AND rj.JmlBayarKredit <> 0 
            AND rj.JenisRJual = 0
            AND jual.IdTJual IS NULL
            AND TglTRJual >= '${start}' and TglTRJual < '${end}'
        
            UNION ALL 
            SELECT TJualLain.IdMCabangCust as IdMCabangMCust 
                , TJualLain.IdMCust as IdMCust 
                , 2.1 as JenisTrans 
                , TJualLain.IdMCabang 
                , TJualLain.IdTJualLain as IdTrans 
                , TJualLain.BuktiTJualLain as BuktiTrans 
                , concat(Date(TJualLain.TglTJualLain), ' ', Time(TJualLain.TglUpdate)) as TglTrans 
                , 0 as JenisInvoice 
                , TJualLain.Netto AS JmlPiut 
                , CONCAT('Penjualan ', ' ', IF(TJualLain.JenisEkspedisi = 1, 'FRANCO',
                    IF(TJualLain.JenisEkspedisi = 2, 'LOCO', '')), ' '
                    , IF(TJualLain.CountPrint = -1, '(Peti/Ekspedisi)', ''), '') as Keterangan
                , -1 as Id_bcf
            FROM MGARTJualLain TJualLain
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = TJualLain.IdMCabangCust AND MCust.IdMCust = TJualLain.IdMCust)
            WHERE TJualLain.Hapus = 0 AND TJualLain.Void = 0 
            AND TJualLain.Netto <> 0 
            AND TglTJualLain >= '${start}' and TglTJualLain < '${end}'
        
            UNION ALL 
            SELECT TAngkutan.IdMCabangMCust
                , TAngkutan.IdMCust
                , 2.2 AS JenisTrans
                , TAngkutan.IdMCabang
                , TAngkutan.IdTTAngkutan AS IdTrans
                , TAngkutan.BuktiTTAngkutan AS BuktiTrans
                , CONCAT(DATE(TAngkutan.TglTTAngkutan), ' ', TIME(TAngkutan.TglUpdate)) AS TglTrans
                , TAngkutan.JenisTTAngkutan AS JenisInvoice
                , (TAngkutan.JmlBayarKredit) AS JmlPiut
                , CONCAT('Titip Angkutan ', TAngkutan.BuktiTTAngkutan) AS Keterangan
                , -1 as Id_bcf
            FROM MGARTTAngkutan TAngkutan
            WHERE Hapus = 0 AND Void = 0
            AND (TAngkutan.JmlBayarKredit) <> 0
            AND TglTTAngkutan >= '${start}' and TglTTAngkutan < '${end}'
            UNION ALL 
            SELECT m.IdMCabangMCust 
                , m.IdMCust 
                , 4 as JenisTrans 
                , m.IdMCabang 
                , m.IdTBPiut as IdTrans 
                , m.BuktiTBPiut as BuktiTrans 
                , concat(Date(d.TglBayar), ' ', Time(m.TglUpdate)) as TglTrans 
                , m.JenisInvoice as JenisInvoice 
                , - (d.JmlBayar - COALESCE(UMJUalD.JmlBayar, 0)) AS JmlPiut 
                , concat('Pembayaran Piutang ', BuktiTBPiut, ' ' ,if(d.jenisMref = 'K',Kas.KdMKas, if(d.JenisMRef ='B',Rek.KdMRek, if(d.JenisMref ='G' ,Giro.KdMGiro,IF(d.JenisMRef = 'P',Prk.KdMPrk,''))))) as Keterangan  
                , m.Id_bcf
            FROM MGARTBPiutDB d 
                LEFT OUTER JOIN MGARTBPiut m ON (d.IdMCabang = m.IdMCabang AND d.IdTBPiut = m.IdTBPiut)
                LEFT OUTER JOIN MGARTUMJual UMJual ON(m.IdMCabang = UMJual.IdMCabangTBPiut AND m.IdTBPiut = UMJual.IdTBPiut AND UMJual.Hapus = 0 AND UMJual.Void = 0)
                LEFT OUTER JOIN MGARTUMJualD UMJualD ON(UMJual.IdMCabang = UMJualD.IdMCabang AND UMJual.IdTUMJual = UMJualD.IdTUMJual AND d.IdMCabangMRef = UMJualD.IdMCabangMRef AND d.IdMref = UMJualD.IdMRef AND d.TglBayar = UMJualD.TglBayar)
                LEFT OUTER JOIN MGKBMKas Kas ON(Kas.IdMCabang = d.IdMCabangMref AND Kas.IdMKas = d.IdMref and d.JenisMRef ='K')
                LEFT OUTER JOIN MGKBMRek Rek ON(Rek.IdMCabang = d.IdMCabangMref AND Rek.IdMRek = d.IdMref and d.JenisMRef ='B')
                LEFT OUTER JOIN MGKBMGiro Giro ON(Giro.IdMCabang = d.IdMCabangMref AND Giro.IdMGiro = d.IdMref and d.JenisMRef ='G')
                LEFT OUTER JOIN MGGLMPrk Prk ON( Prk.IdMPrk = d.IdMref and d.JenisMRef ='P' and Prk.Periode = 0)
            WHERE m.Hapus = 0 AND m.Void = 0
            AND m.Total <> 0 
            AND d.TglBayar >= '${start}' and d.TglBayar < '${end}'
        
            UNION ALL 
            SELECT m.IdMCabangMCust 
                , m.IdMCust 
                , 4 as JenisTrans 
                , m.IdMCabang 
                , m.IdTBPiut as IdTrans 
                , m.BuktiTBPiut as BuktiTrans 
                , concat(Date(m.TglTBPiut), ' ', Time(m.TglUpdate)) as TglTrans 
                , m.JenisInvoice as JenisInvoice 
                , - m.JmlUM AS JmlPiut 
                , concat('Pembayaran Piutang dgn Uang Muka ', BuktiTBPiut) as Keterangan  
                , m.Id_bcf
            FROM MGARTBPiut m
            WHERE m.Hapus = 0 AND m.Void = 0
            AND m.JmlUM <> 0 
            AND TglTBPiut >= '${start}' and TglTBPiut < '${end}'
        
            UNION ALL 
            SELECT MCust.IdMCabang
                , MCust.IdMCust AS IdMCust
                , 4.1 AS JenisTrans
                , TBPiut.IdMCabang
                , TBPiut.IdTBPiut AS IdTrans
                , TBPiut.BuktiTBPiut AS BuktiTrans
                , CONCAT(DATE(TBPiutB.TglBayar), ' ', TIME(TBPiut.TglUpdate)) AS TglTrans
                , TBPiut.JenisInvoice as JenisInvoice 
                , TBPiutB.JMLBayar AS JmlPiut
                , CONCAT('Titipan Giro ', TBPiut.BuktiTBPiut, ' (', MG.KdMGiro, ')') AS Keterangan
                , -1 as Id_bcf
            FROM MGARTBPiutDB TBPiutB
                LEFT OUTER JOIN MGARTBPiut TBPiut ON (TBPiut.IdMCabang = TBPiutB.IdMCabang AND TBPiut.IdTBPiut = TBPiutB.IdTBPiut)
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = TBPiut.IdMCabangMCust AND MCust.IdMCust = TBPiut.IdMCust)
                LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = TBPiutB.IdMCabangMREF AND MG.IdMGiro = TBPiutB.IdMREF)
            WHERE (TBPiut.HAPUS = 0 AND TBPiut.VOID = 0) AND TBPiutB.JenisMREF = 'G'
            AND TBPiutB.TglBayar >= '${start}' and TBPiutB.TglBayar < '${end}'
        
            UNION ALL 
            SELECT MCust.IdMCabang AS IdMCabang
                , MCust.IdMCust AS IdMCust
                , 4.2 AS JenisTrans
                , m.IdMCabang
                , M.IdTGiroCair AS IdTrans 
                , M.BuktiTGiroCair AS BuktiTrans 
                , CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) AS TglTrans
                , BPiut.JenisInvoice as JenisInvoice 
                , -BPiutDB.JMLBayar AS JmlPiut 
                , CONCAT('Giro Cair ', m.BuktiTGiroCair, ' (', MG.KdmGiro, ')') AS Keterangan 
                , -1 as Id_bcf
            FROM MGKBTGiroCairD D
                LEFT OUTER JOIN MGKBTGiroCair M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroCair = D.IdTGiroCair)
                LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
                LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
                LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
            WHERE (M.HAPUS = 0 AND M.VOID = 0) AND M.JenisTGiroCair = 'M' AND BPiutDB.JenisMRef = 'G' AND BPiut.VOID = 0 AND BPiut.HAPUS = 0
            AND TglTGiroCair >= '${start}' and TglTGiroCair < '${end}'
        
            UNION ALL 
            SELECT MCust.IdMCabang AS IdMCabang
                , MCust.IdMCust AS IdMCust
                , 4.3 AS JenisTrans
                , m.IdMCabang
                , M.IdTGiroTolak AS IdTrans
                , M.BuktiTGiroTolak AS BuktiTrans
                , CONCAT(DATE(m.TglTGiroTolak), ' ', TIME(m.tglupdate)) AS TglTrans
                , BPiut.JenisInvoice as JenisInvoice 
                , BPiutDB.JMLBayar AS JmlPiut
                , CONCAT('Giro Tolak ', m.BuktiTGiroTolak,  ' (', MG.KdMGiro, ')') AS Keterangan
                , -1 as Id_bcf
            FROM MGKBTGiroTolakD D
                LEFT OUTER JOIN MGKBTGiroTolak M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroTolak = D.IdTGiroTolak)
                LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
                LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
                LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
            WHERE (M.HAPUS = 0 AND M.VOID = 0) 
                AND M.JenisTGiroTolak = 'M' AND BPiutDB.JenisMRef = 'G' 
                AND BPiut.VOID = 0 AND BPiut.HAPUS = 0
            AND TglTGiroTolak >= '${start}' and TglTGiroTolak < '${end}'
        
            UNION ALL 
            SELECT MCust.IdMCabang AS IdMCabang 
                , MCust.IdMCust AS IdMCust 
                , 4.4 AS JenisTrans
                , m.IdMCabang
                , M.IDTGiroGanti AS IdTrans
                , M.BuktiTGiroGanti AS BuktiTrans
                , CONCAT(DATE(m.TglTGiroGanti), ' ', TIME(m.tglupdate)) AS TglTrans
                , BPiut.JenisInvoice as JenisInvoice 
                , -D.JMLBayar AS JmlPiut
                , CONCAT('Penggantian Giro ', m.BuktiTGiroGanti, ' (', MG.KdMGiro, ')') AS Keterangan
                , -1 as Id_bcf
            FROM MGKBTGiroGanti M
                LEFT OUTER JOIN MGKBTGiroGantiDG D ON (M.IdMCabang = D.IdMCabang AND M.IDTGiroGanti = D.IDTGiroGanti)
                LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
                LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
                LEFT OUTER JOIN MGARTBPiutDB BPiutDB ON (BPiutDB.IdMCabangMRef = MG.IdMCabang AND BPiutDB.IdMRef = MG.IdMGiro AND BPiutDB.JenisMREF = 'G')
                LEFT OUTER JOIN MGARTBPiut BPiut ON (BPiut.IdMCabang = BPiutDB.IdMCabang AND BPiut.IdTBPiut = BPiutDB.IdTBPiut)
            WHERE (M.HAPUS = 0 AND M.VOID = 0)
                AND M.JenisTGiroGanti = 'M' AND BPiutDB.JenisMRef = 'G'
                AND BPiut.VOID = 0 AND BPiut.HAPUS = 0
            AND TglTGiroGanti >= '${start}' and TglTGiroGanti < '${end}'
        
            UNION ALL 
            SELECT IdMCabangMCust 
                , IdMCust 
                , 5 as JenisTrans 
                , IdMCabang 
                , IdTKorPiut as IdTrans 
                , BuktiTKorPiut as BuktiTrans 
                , concat(Date(TglTKorPiut), ' ', Time(TglUpdate)) as TglTrans 
                , IdMJenisInvoice as JenisInvoice 
                , Total AS JmlPiut 
                , concat('Koreksi Piutang ', BuktiTKorPiut) as Keterangan 
                , -1 as Id_bcf
            FROM MGARTKorPiut 
            WHERE Hapus = 0 AND Void = 0
            AND Total <> 0 
            AND TglTKorPiut >= '${start}' and TglTKorPiut < '${end}'
        
            UNION ALL 
        SELECT mj.IdMCabangMCust
                , mj.IdMCust
                , 6 AS JenisTrans
                , m.IdMCabang
                , m.IdTTagihan AS IdTrans
                , m.BuktiTTagihan AS BuktiTrans
                , CONCAT(DATE(TglTTagihan), ' ', TIME(m.TglUpdate)) AS TglTrans
                , 0 AS JenisInvoice
                , -d.jmlbayar AS JmlPiut
                , CONCAT('Bayar Tagihan No. Jual : ',mj.buktiTJual,IF(mg.KdMGiro<>'',CONCAT('(',mg.KdMGiro,')'),'')) AS Keterangan
                , -1 as Id_bcf
        FROM MGARTTagihanD d
                LEFT OUTER JOIN MGARTTagihan m ON (d.IdTTagihan=m.IdTTagihan)
                LEFT OUTER JOIN MGARTJual mj ON (d.IdTrans=mj.IdTJual)
            LEFT OUTER JOIN MGKBMGiro mg ON (d.IdMRef=mg.IdMGiro AND jenisMRef='G')
        WHERE m.Hapus =0 AND m.Void = 0
                AND d.jmlbayar <> 0
            AND TglTTagihan >= '${start}' and TglTTagihan < '${end}'
        UNION ALL 
        SELECT mj.IdMCabang
                , mj.IdMCust AS IdMCust
                , 6.1 AS JenisTrans
                , m.IdMCabang
                , m.IdTTagihan AS IdTrans
                , m.BuktiTTagihan AS BuktiTrans
                , CONCAT(DATE(m.TglTTagihan), ' ', TIME(m.TglUpdate)) AS TglTrans
            , 0 AS JenisInvoice
                , sum(d.JMLBayar) AS JmlPiut
                , CONCAT('Titipan Giro ', m.buktiTTagihan,' (',g.KdMGiro,')' ) AS Keterangan
                , -1 as Id_bcf
        FROM MGARTTagihanD D
                LEFT OUTER JOIN mgarttagihan m ON (d.IdTTagihan=m.IdTTagihan AND d.IdMCabang=m.IdMCabang)
                LEFT OUTER JOIN mgartjual mj ON (d.idMCabang=mj.IdMCabang AND d.IdTrans=mj.IdTJual)
                LEFT OUTER JOIN mgkbmgiro g ON (d.IdMRef=g.IdMGiro AND d.IdMCabang=g.IdMCabang)
        
        WHERE (m.HAPUS = 0 AND m.VOID = 0) AND d.JenisMREF = 'G'
            AND TglTTagihan >= '${start}' and TglTTagihan < '${end}'
        Group By Keterangan
        Union ALL
        SELECT MCust.IdMCabang AS IdMCabang
            , MCust.IdMCust AS IdMCust
            , 6.2 AS JenisTrans
            , m.IdMCabang
            , M.IdTGiroCair AS IdTrans
            , M.BuktiTGiroCair AS BuktiTrans
            , CONCAT(DATE(m.TglTGiroCair), ' ', TIME(m.TglUpdate)) AS TglTrans
            , 0 AS JenisInvoice
            , -sum(mtd.JMLBayar) AS JmlPiut
            , CONCAT('Giro Cair ', m.BuktiTGiroCair, ' (', MG.KdmGiro, ')') AS Keterangan
                , -1 as Id_bcf
        FROM MGKBTGiroCairD D
            LEFT OUTER JOIN MGKBTGiroCair M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroCair = D.IdTGiroCair)
            LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
            LEFT OUTER JOIN MGARTTagihand mtd ON (mtd.IdMCabang = MG.IdMCabang AND mtd.IdMRef = MG.IdMGiro AND mtd.JenisMREF ='G')
            LEFT OUTER JOIN mgarttagihan mt ON (mt.IdMCabang = mtd.IdMCabang AND mt.IdTTagihan = mtd.IdTTagihan)
        WHERE (M.HAPUS = 0 AND M.VOID = 0) AND M.JenisTGiroCair = 'M' AND mtd.JenisMRef = 'G' AND mt.VOID = 0 AND mt.HAPUS = 0
            AND TglTGiroCair >= '${start}' and TglTGiroCair < '${end}'
        
        group by IdMCust
        Union ALL
        SELECT MCust.IdMCabang AS IdMCabang
            , MCust.IdMCust AS IdMCust
            , 6.3 AS JenisTrans
            , m.IdMCabang
            , M.IdTGiroTolak AS IdTrans
            , M.BuktiTGiroTolak AS BuktiTrans
            , CONCAT(DATE(m.TglTGiroTolak), ' ', TIME(m.tglupdate)) AS TglTrans
            , 0 AS JenisInvoice
            , sum(mtd.JMLBayar) AS JmlPiut
            , CONCAT('Giro Tolak ', m.BuktiTGiroTolak,  ' (', MG.KdMGiro, ')') AS Keterangan
                , -1 as Id_bcf
        FROM MGKBTGiroTolakD D
            LEFT OUTER JOIN MGKBTGiroTolak M ON (M.IdMCabang = D.IdMCabang AND M.IdTGiroTolak = D.IdTGiroTolak)
            LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
            LEFT OUTER JOIN MGARTTagihand mtd ON (mtd.IdMCabang = MG.IdMCabang AND mtd.IdMRef = MG.IdMGiro AND mtd.JenisMREF = 'G')
            LEFT OUTER JOIN MGARTTagihan mt ON (mt.IdMCabang = mtd.IdMCabang AND mt.IdTTagihan = mtd.IdTTagihan)
        WHERE (M.HAPUS =0 AND M.VOID = 00)
            AND M.JenisTGiroTolak = 'M' AND mtd.JenisMRef = 'G'
            AND mt.VOID = 0 AND mt.HAPUS = 0
            AND TglTGiroTolak >= '${start}' and TglTGiroTolak < '${end}'
        
        group by IdMCust
        union all
        SELECT MCust.IdMCabang AS IdMCabang
            , MCust.IdMCust AS IdMCust
            , 6.4 AS JenisTrans
            , m.IdMCabang
            , M.IDTGiroGanti AS IdTrans
            , M.BuktiTGiroGanti AS BuktiTrans
            , CONCAT(DATE(m.TglTGiroGanti), ' ', TIME(m.tglupdate)) AS TglTrans
            , 0 AS JenisInvoice
            , -D.JMLBayar AS JmlPiut
            , CONCAT('Penggantian Giro ', m.BuktiTGiroGanti, ' (', MG.KdMGiro, ')') AS Keterangan
                , -1 as Id_bcf
        FROM MGKBTGiroGanti M
            LEFT OUTER JOIN MGKBTGiroGantiDG D ON (M.IdMCabang = D.IdMCabang AND M.IDTGiroGanti = D.IDTGiroGanti)
            LEFT OUTER JOIN MGKBMGiro MG ON (MG.IdMCabang = D.IdMCabangMGiro AND MG.IdMGiro = D.IdMGiro)
            LEFT OUTER JOIN MGARMCust MCust ON (MCust.IdMCabang = MG.IdMCabangMCust AND MCust.IdMCust = MG.IdMCust AND MG.JenisMGiro = 'M')
            LEFT OUTER JOIN MGARTTAgihanD TTagihD ON (TTagihD.IdMCabang = MG.IdMCabang AND TTagihd.IdMRef = MG.IdMGiro AND TTagihD.JenisMREF = 'G')
            LEFT OUTER JOIN MGARTTAgihan TTagih ON (TTagih.IdMCabang = TTagihD.IdMCabang AND TTagih.IdTTagihan = TTagihD.IdTTagihan)
        WHERE (M.HAPUS = 0 AND M.VOID = 0)
            AND M.JenisTGiroGanti = 'M' AND TTagihD.JenisMRef = 'G'
            AND TTagih.VOID = 0 AND TTagih.HAPUS = 0
            AND TglTGiroGanti >= '${start}' and TglTGiroGanti < '${end}'
        
        
        ) LKartuPiut 
            left outer join mgarmjenisinvoice m on (lkartupiut.jenisinvoice = m.idmjenisinvoice)
        where LKartuPiut.TglTrans >= '${start}' and LKartuPiut.TglTrans < '${end}'
        ) TableKartuPiut LEFT OUTER JOIN MGSYMCabang MCabang ON (TableKartuPiut.IdMCabangTrans = MCabang.IdMCabang)
                            LEFT OUTER JOIN MGARMCust MCust ON (TableKartuPiut.IdMCabangMCust = MCust.IdMCabang AND TableKartuPiut.IdMCust = MCust.IdMCust)
                            LEFT OUTER JOIN bookout_bcf bcf ON (TableKartuPiut.id_bcf = bcf.id)
        WHERE MCabang.Hapus = 0
            AND MCabang.Aktif = 1
            AND MCust.Hapus = 0
            AND MCust.Aktif = 1
            AND MCust.idmcust = ${fil.idmcust}
        ORDER BY MCabang.KdMCabang, MCabang.NmMCabang, TableKartuPiut.IdMCabangMCust
                , MCust.KdMCust, MCust.NmMCust, Urut, TglTrans, JenisTrans, IdTrans`;
        const cust = await sequelize.query(sql1, {
          raw: false,
        });

        var saldo = 0;
        var detail = await Promise.all(
          cust[0].map(async (nilai, index_satu) => {
            saldo += parseFloat(nilai.Kredit) + parseFloat(nilai.Debit);
            return {
              tanggal: nilai.TglTrans,
              bukti: nilai.BuktiTrans,
              keterangan: nilai.Keterangan,
              debit: parseFloat(nilai.Debit),
              kredit: parseFloat(nilai.Kredit),
              saldo: saldo,
            };
          })
        );

        return {
          customer: fil.nmmcust,
          detail: detail,
        };
      })
    );

    res.json({
      message: "Success",
      data: arr_data,
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
