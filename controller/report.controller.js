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
  const qpenjualan = require("../class/query_report/penjualan");
  const sequelize = await fun.connection(req.datacompany);
  const companyid = req.datacompany.id;

  let start = req.body.start || today;
  let end = req.body.end || today;

  let cabang = req.body.cabang || "";
  let customer = req.body.customer || "";
  let barang = req.body.barang || "";
  let group = req.body.group;

  let jenis = req.body.jenis || 1;

  // summary dan detail penjualan, dibedakan per group
  let q = await qpenjualan.queryDetail(companyid,start,end,cabang,customer,barang,group);
  const data = await fun.getDataFromQuery(sequelize, q);

  if(group == "cabang"){
    var arr_list = [];
    var listcabang = [];
    var listbrg = [];

    var penjualan = 0; var produk_terjual = 0; var pendapatan = 0; var profit = 0;

    var arr_data = await Promise.all(data.map(async (fil, index) => {

      produk_terjual += parseFloat(fil.QtyTotal); // hitung produk terjual

      var list = {
        "id": fil.IdMBrg,
        "kode": fil.KdMBrg,
        "barcode": fil.KdMBrg,
        "nama": fil.NmMBrg,
        "gudang": fil.NmMGd,
        "jumlah": fil.QtyTotal != '' ? parseFloat(fil.QtyTotal) : 0,
        "qty": fil.QtyTotal != '' ? parseFloat(fil.QtyTotal) : 0,
        "harga": fil.HrgStn != '' ? parseFloat(fil.HrgStn) : 0,
        "satuan": fil.NmMStn1 != '' ? fil.NmMStn1 : 0,
        "hargasat": fil.HrgStn != '' ? parseFloat(fil.HrgStn) : 0,
        "diskon": fil.DiscVDetail != '' ? parseFloat(fil.DiscVDetail) : 0,
        "total": fil.SubTotal != '' ? parseFloat(fil.SubTotal) : 0,
        "pajak": fil.PPNVEcer != '' ? parseFloat(fil.PPNVEcer) : 0,
        "dpp": fil.dpp != '' ? parseFloat(fil.dpp) : 0,
        "subtotal": fil.SubTotal != '' ? parseFloat(fil.SubTotal) : 0,
      };

      var sisa = parseFloat(fil.Netto) - parseFloat(fil.bayar);
        
      var data_per_nota = {
        "id": fil.IdTJualPOS,
        "tanggal": fil.TglTJualPOS,
        "transaksi": fil.BuktiTJualPOS,
        "customer": fil.NmMCust,
        "sales": fil.NmMSales,
        "subtotal": fil.Bruto != null ? parseFloat(fil.Bruto) : 0,
        "diskon": fil.DiscV != null ? parseFloat(fil.DiscV) : 0,
        "pajak": fil.PPNV != null ? parseFloat(fil.PPNV) : 0,
        "grandtotal": fil.Netto != null ? parseFloat(fil.Netto) : 0,
        "bayar": parseFloat(fil.bayar),
        "sisa": sisa,
        "sisabayar": sisa,
        "listitem": [list]
      }

      var cabang = {
        "nama": fil.NmMCabang,
        // "netto": 0,
        // "sisa": 0,
        // "bayar": 0,
        "list": [data_per_nota],
      }
    
      // cabang terbaru (cabang => nota => item)
      if (!listcabang.includes(fil.NmMCabang)) {
        penjualan += 1;
        pendapatan += parseFloat(fil.Netto);
        profit += parseFloat(fil.Netto);

        listcabang.push(fil.NmMCabang);
        listbrg = [];
        listbrg.push(fil.BuktiTJualPOS);

        arr_list.push(cabang);
      }
      else { // cabang yang sudah ada
        let idx = listcabang.indexOf(fil.NmMCabang);

        // nota terbaru di cabang yang sudah ada (nota => item)
        if (!listbrg.includes(fil.BuktiTJualPOS)) {
          penjualan += 1; 
          pendapatan += parseFloat(fil.Netto);
          profit += parseFloat(fil.Netto);

          listbrg.push(fil.BuktiTJualPOS);
          arr_list[idx].list.push(data_per_nota);
        }

        // nota yang sudah ada (item)
        else {
          let idx2 = listbrg.indexOf(fil.BuktiTJualPOS);
          arr_list[idx].list[idx2].listitem.push(list);
        }
      }
    }));

    count = {
      "penjualan" : penjualan,
      "produk_terjual" : produk_terjual,
      "pendapatan" : pendapatan,
      "profit" : profit
    }

    res.json({
      message: "Success",
      countData: count,
      data: arr_list,
    });
  }else if(group == "customer"){
    var arr_list = [];
    var listcabang = [];
    var listbrg = [];

    var penjualan = 0; var produk_terjual = 0; var pendapatan = 0; var profit = 0;

    var arr_data = await Promise.all(data.map(async (fil, index) => {

      produk_terjual += parseFloat(fil.QtyTotal); // hitung produk terjual

      var list = {
        "id": fil.IdMBrg,
        "kode": fil.KdMBrg,
        "barcode": fil.KdMBrg,
        "nama": fil.NmMBrg,
        "gudang": fil.NmMGd,
        "jumlah": fil.QtyTotal != '' ? parseFloat(fil.QtyTotal) : 0,
        "qty": fil.QtyTotal != '' ? parseFloat(fil.QtyTotal) : 0,
        "harga": fil.HrgStn != '' ? parseFloat(fil.HrgStn) : 0,
        "satuan": fil.NmMStn1 != '' ? fil.NmMStn1 : 0,
        "hargasat": fil.HrgStn != '' ? parseFloat(fil.HrgStn) : 0,
        "diskon": fil.DiscVDetail != '' ? parseFloat(fil.DiscVDetail) : 0,
        "total": fil.SubTotal != '' ? parseFloat(fil.SubTotal) : 0,
        "pajak": fil.PPNVEcer != '' ? parseFloat(fil.PPNVEcer) : 0,
        "dpp": fil.dpp != '' ? parseFloat(fil.dpp) : 0,
        "subtotal": fil.SubTotal != '' ? parseFloat(fil.SubTotal) : 0,
      };

      var sisa = parseFloat(fil.Netto) - parseFloat(fil.bayar);
        
      var data_per_nota = {
        "id": fil.IdTJualPOS,
        "tanggal": fil.TglTJualPOS,
        "transaksi": fil.BuktiTJualPOS,
        "customer": fil.NmMCust,
        "sales": fil.NmMSales,
        "subtotal": fil.Bruto != null ? parseFloat(fil.Bruto) : 0,
        "diskon": fil.DiscV != null ? parseFloat(fil.DiscV) : 0,
        "pajak": fil.PPNV != null ? parseFloat(fil.PPNV) : 0,
        "grandtotal": fil.Netto != null ? parseFloat(fil.Netto) : 0,
        "bayar": parseFloat(fil.bayar),
        "sisa": sisa,
        "sisabayar": sisa,
        "listitem": [list]
      }

      var cabang = {
        "nama": fil.NmMCust,
        "list": [data_per_nota],
      }
    
      // cabang terbaru (cabang => nota => item)
      if (!listcabang.includes(fil.IdMCust)) {
        penjualan += 1;
        pendapatan += parseFloat(fil.Netto);
        profit += parseFloat(fil.Netto);

        listcabang.push(fil.IdMCust);
        listbrg = [];
        listbrg.push(fil.IdTJualPOS);

        arr_list.push(cabang);
      }
      else { // cabang yang sudah ada
        let idx = listcabang.indexOf(fil.IdMCust);        

        // nota terbaru di cabang yang sudah ada (nota => item)
        if (!listbrg.includes(fil.IdTJualPOS)) {
          penjualan += 1; 
          pendapatan += parseFloat(fil.Netto);
          profit += parseFloat(fil.Netto);
          listbrg.push(fil.IdTJualPOS);
          arr_list[idx].list.push(data_per_nota);
        }

        // nota yang sudah ada (item)
        else {
          let idx2 = listbrg.indexOf(fil.IdTJualPOS);
          arr_list[idx].list[idx2].listitem.push(list);
        }
      }
    }));

    count = {
      "penjualan" : penjualan,
      "produk_terjual" : produk_terjual,
      "pendapatan" : pendapatan,
      "profit" : profit
    }

    res.json({
      message: "Success",
      countData: count,
      data: arr_list,
    });
  }else if(group == "sales"){
    var arr_list = [];
    var listcabang = [];
    var listbrg = [];

    var penjualan = 0; var produk_terjual = 0; var pendapatan = 0; var profit = 0;

    var arr_data = await Promise.all(data.map(async (fil, index) => {

      produk_terjual += parseFloat(fil.QtyTotal); // hitung produk terjual

      var list = {
        "id": fil.IdMBrg,
        "kode": fil.KdMBrg,
        "barcode": fil.KdMBrg,
        "nama": fil.NmMBrg,
        "gudang": fil.NmMGd,
        "jumlah": fil.QtyTotal != '' ? parseFloat(fil.QtyTotal) : 0,
        "qty": fil.QtyTotal != '' ? parseFloat(fil.QtyTotal) : 0,
        "harga": fil.HrgStn != '' ? parseFloat(fil.HrgStn) : 0,
        "satuan": fil.NmMStn1 != '' ? fil.NmMStn1 : 0,
        "hargasat": fil.HrgStn != '' ? parseFloat(fil.HrgStn) : 0,
        "diskon": fil.DiscVDetail != '' ? parseFloat(fil.DiscVDetail) : 0,
        "total": fil.SubTotal != '' ? parseFloat(fil.SubTotal) : 0,
        "pajak": fil.PPNVEcer != '' ? parseFloat(fil.PPNVEcer) : 0,
        "dpp": fil.dpp != '' ? parseFloat(fil.dpp) : 0,
        "subtotal": fil.SubTotal != '' ? parseFloat(fil.SubTotal) : 0,
      };

      var sisa = parseFloat(fil.Netto) - parseFloat(fil.bayar);
        
      var data_per_nota = {
        "id": fil.IdTJualPOS,
        "tanggal": fil.TglTJualPOS,
        "transaksi": fil.BuktiTJualPOS,
        "customer": fil.NmMCust,
        "sales": fil.NmMSales,
        "subtotal": fil.Bruto != null ? parseFloat(fil.Bruto) : 0,
        "diskon": fil.DiscV != null ? parseFloat(fil.DiscV) : 0,
        "pajak": fil.PPNV != null ? parseFloat(fil.PPNV) : 0,
        "grandtotal": fil.Netto != null ? parseFloat(fil.Netto) : 0,
        "bayar": parseFloat(fil.bayar),
        "sisa": sisa,
        "sisabayar": sisa,
        "listitem": [list]
      }

      var cabang = {
        "nama": fil.NmMSales,
        "list": [data_per_nota],
      }
    
      // cabang terbaru (cabang => nota => item)
      if (!listcabang.includes(fil.IdMSales)) {
        penjualan += 1;
        pendapatan += parseFloat(fil.Netto);
        profit += parseFloat(fil.Netto);

        listcabang.push(fil.IdMSales);
        listbrg = [];
        listbrg.push(fil.IdTJualPOS);

        arr_list.push(cabang);
      }
      else { // cabang yang sudah ada
        let idx = listcabang.indexOf(fil.IdMSales);        

        // nota terbaru di cabang yang sudah ada (nota => item)
        if (!listbrg.includes(fil.IdTJualPOS)) {
          penjualan += 1; 
          pendapatan += parseFloat(fil.Netto);
          profit += parseFloat(fil.Netto);
          listbrg.push(fil.IdTJualPOS);
          arr_list[idx].list.push(data_per_nota);
        }

        // nota yang sudah ada (item)
        else {
          let idx2 = listbrg.indexOf(fil.IdTJualPOS);
          arr_list[idx].list[idx2].listitem.push(list);
        }
      }
    }));

    count = {
      "penjualan" : penjualan,
      "produk_terjual" : produk_terjual,
      "pendapatan" : pendapatan,
      "profit" : profit
    }

    res.json({
      message: "Success",
      countData: count,
      data: arr_list,
    });
  }else{
    var arr_list = [];
    var listcabang = [];
    var listbrg = [];

    var penjualan = 0; var produk_terjual = 0; var pendapatan = 0; var profit = 0;

    var arr_data = await Promise.all(data.map(async (fil, index) => {

      produk_terjual += parseFloat(fil.QtyTotal); // hitung produk terjual

      var list = {
        "id": fil.IdMBrg,
        "kode": fil.KdMBrg,
        "barcode": fil.KdMBrg,
        "nama": fil.NmMBrg,
        "gudang": fil.NmMGd,
        "jumlah": fil.QtyTotal != '' ? parseFloat(fil.QtyTotal) : 0,
        "qty": fil.QtyTotal != '' ? parseFloat(fil.QtyTotal) : 0,
        "harga": fil.HrgStn != '' ? parseFloat(fil.HrgStn) : 0,
        "satuan": fil.NmMStn1 != '' ? fil.NmMStn1 : 0,
        "hargasat": fil.HrgStn != '' ? parseFloat(fil.HrgStn) : 0,
        "diskon": fil.DiscVDetail != '' ? parseFloat(fil.DiscVDetail) : 0,
        "total": fil.SubTotal != '' ? parseFloat(fil.SubTotal) : 0,
        "pajak": fil.PPNVEcer != '' ? parseFloat(fil.PPNVEcer) : 0,
        "dpp": fil.dpp != '' ? parseFloat(fil.dpp) : 0,
        "subtotal": fil.SubTotal != '' ? parseFloat(fil.SubTotal) : 0,
      };

      var sisa = parseFloat(fil.Netto) - parseFloat(fil.bayar);
        
      var data_per_nota = {
        "id": fil.IdTJualPOS,
        "tanggal": fil.TglTJualPOS,
        "transaksi": fil.BuktiTJualPOS,
        "customer": fil.NmMCust,
        "sales": fil.NmMSales,
        "subtotal": fil.Bruto != null ? parseFloat(fil.Bruto) : 0,
        "diskon": fil.DiscV != null ? parseFloat(fil.DiscV) : 0,
        "pajak": fil.PPNV != null ? parseFloat(fil.PPNV) : 0,
        "grandtotal": fil.Netto != null ? parseFloat(fil.Netto) : 0,
        "bayar": parseFloat(fil.bayar),
        "sisa": sisa,
        "sisabayar": sisa,
        "listitem": [list]
      }

      var cabang = {
        "nama": fil.NmMBrg,
        "list": [data_per_nota],
      }
    
      // cabang terbaru (cabang => nota => item)
      if (!listcabang.includes(fil.KdMBrg)) {
        penjualan += 1;
        pendapatan += parseFloat(fil.Netto);
        profit += parseFloat(fil.Netto);

        listcabang.push(fil.KdMBrg);
        listbrg = [];
        listbrg.push(fil.IdTJualPOS);

        arr_list.push(cabang);
      }
      else { // cabang yang sudah ada
        let idx = listcabang.indexOf(fil.KdMBrg);        

        // nota terbaru di cabang yang sudah ada (nota => item)
        if (!listbrg.includes(fil.IdTJualPOS)) {
          penjualan += 1; 
          pendapatan += parseFloat(fil.Netto);
          profit += parseFloat(fil.Netto);
          listbrg.push(fil.IdTJualPOS);
          arr_list[idx].list.push(data_per_nota);
        }

        // nota yang sudah ada (item)
        else {
          let idx2 = listbrg.indexOf(fil.IdTJualPOS);
          arr_list[idx].list[idx2].listitem.push(list);
        }
      }
    }));

    count = {
      "penjualan" : penjualan,
      "produk_terjual" : produk_terjual,
      "pendapatan" : pendapatan,
      "profit" : profit
    }

    res.json({
      message: "Success",
      countData: count,
      data: arr_list,
    });
  }
};

exports.pembelian = async (req, res) => {
  const qpembelian = require("../class/query_report/pembelian");
  const sequelize = await fun.connection(req.datacompany);
  const companyid = req.datacompany.id;

  let start = req.body.start || today;
  let end = req.body.end || today;

  let cabang = req.body.cabang || "";
  let supplier = req.body.supplier || "";
  let barang = req.body.barang || "";
  let group = req.body.group;

  let jenis = req.body.jenis || 1;

  // summary dan detail pembelian, dibedakan per group
  let q = await qpembelian.queryDetail(companyid,start,end,cabang,supplier,barang,group);
  const data = await fun.getDataFromQuery(sequelize, q);

  if(group == "cabang"){
    var arr_list = [];
    var listcabang = [];
    var listbrg = [];

    var pembelian = 0; var produk_dibeli = 0; var pengeluaran = 0;

    var arr_data = await Promise.all(data.map(async (fil, index) => {

      produk_dibeli += parseFloat(fil.QtyTotal); // hitung produk terjual

      var list = {
        "id": fil.IdMBrg,
        "kode": fil.KdMBrg,
        "barcode": fil.KdMBrg,
        "nama": fil.NmMBrg,
        "gudang": fil.NmMGd,
        "jumlah": parseFloat(fil.QtyTotal),
        "qty": fil.QtyTotal != '' ? parseFloat(fil.QtyTotal) : 0,
        "harga": fil.HrgStn != '' ? parseFloat(fil.HrgStn) : 0,
        "satuan": fil.NmMStn1,
        "hargasat": fil.HrgStn != '' ? parseFloat(fil.HrgStn) : 0,
        "diskon": fil.DiscVD != '' ? parseFloat(fil.DiscVD) : 0,
        "total": fil.SubTotal != '' ? parseFloat(fil.SubTotal) : 0,
        "pajak": 0,
        "dpp": fil.dpp != '' ? parseFloat(fil.dpp) : 0,
        "subtotal": fil.SubTotal != '' ? parseFloat(fil.SubTotal) : 0,
      };

      var bayar = 0;
      if(jenis == 1){
        bayar = parseFloat(fil.bayar)
      }else{
        bayar = parseFloat(fil.JmlBayarTunai)
      }

      var sisa = parseFloat(fil.Netto) - parseFloat(fil.bayar);

      var data_per_nota = {
        "id": fil.IdTBeli,
        "tanggal": fil.TglTBeli,
        "transaksi": fil.BuktiTBeli,
        "supplier": fil.NmMSup,
        "subtotal": fil.Bruto != null ? parseFloat(fil.Bruto) : 0,
        "diskon": fil.DiscV != null ? parseFloat(fil.DiscV) : 0,
        "pajak": fil.PPNV != null ? parseFloat(fil.PPNV) : 0,
        "grandtotal": fil.Netto != null ? parseFloat(fil.Netto) : 0,
        "bayar": bayar,
        "sisa": sisa,
        "kredit": parseFloat(fil.JmlBayarKredit),
        "listitem": [list]
      }

      var cabang = {
        "nama": fil.NmMCabang,
        // "bruto": 0,
        // "diskon": 0,
        // "pajak": 0,
        // "netto": 0,
        // "tunai": 0,
        // "sisa": 0,
        "list": [data_per_nota],
      }
    
      // cabang terbaru (cabang => nota => item)
      if (!listcabang.includes(fil.NmMCabang)) {
        pembelian += 1;
        pengeluaran += parseFloat(fil.Netto);

        listcabang.push(fil.NmMCabang);
        listbrg = [];
        listbrg.push(fil.IdTBeli);

        arr_list.push(cabang);
      }
      else { // cabang yang sudah ada
        let idx = listcabang.indexOf(fil.NmMCabang);

        // nota terbaru di cabang yang sudah ada (nota => item)
        if (!listbrg.includes(fil.IdTBeli)) {
          pembelian += 1; 
          pengeluaran += parseFloat(fil.Netto);

          listbrg.push(fil.IdTBeli);
          arr_list[idx].list.push(data_per_nota);
        }

        // nota yang sudah ada (item)
        else {
          let idx2 = listbrg.indexOf(fil.IdTBeli);
          arr_list[idx].list[idx2].listitem.push(list);
        }
      }
    }));

    count = {
      "pembelian" : pembelian,
      "produk_dibeli" : produk_dibeli,
      "pengeluaran" : pengeluaran,
    }

    res.json({
      message: "Success",
      countData: count,
      data: arr_list,
    });
  }else if(group == "supplier"){
    var arr_list = [];
    var listcabang = [];
    var listbrg = [];

    var pembelian = 0; var produk_dibeli = 0; var pengeluaran = 0;

    var arr_data = await Promise.all(data.map(async (fil, index) => {

      produk_dibeli += parseFloat(fil.QtyTotal); // hitung produk terjual

      var list = {
        "id": fil.IdMBrg,
        "kode": fil.KdMBrg,
        "barcode": fil.KdMBrg,
        "nama": fil.NmMBrg,
        "gudang": fil.NmMGd,
        "jumlah": parseFloat(fil.QtyTotal),
        "qty": fil.QtyTotal != '' ? parseFloat(fil.QtyTotal) : 0,
        "harga": fil.HrgStn != '' ? parseFloat(fil.HrgStn) : 0,
        "satuan": fil.NmMStn1,
        "hargasat": fil.HrgStn != '' ? parseFloat(fil.HrgStn) : 0,
        "diskon": fil.DiscVD != '' ? parseFloat(fil.DiscVD) : 0,
        "total": fil.SubTotal != '' ? parseFloat(fil.SubTotal) : 0,
        "pajak": 0,
        "dpp": fil.dpp != '' ? parseFloat(fil.dpp) : 0,
        "subtotal": fil.SubTotal != '' ? parseFloat(fil.SubTotal) : 0,
      };

      var bayar = 0;
      if(jenis == 1){
        bayar = parseFloat(fil.bayar)
      }else{
        bayar = parseFloat(fil.JmlBayarTunai)
      }

      var sisa = parseFloat(fil.Netto) - parseFloat(fil.bayar);

      var data_per_nota = {
        "id": fil.IdTBeli,
        "tanggal": fil.TglTBeli,
        "transaksi": fil.BuktiTBeli,
        "supplier": fil.NmMSup,
        "subtotal": fil.Bruto != null ? parseFloat(fil.Bruto) : 0,
        "diskon": fil.DiscV != null ? parseFloat(fil.DiscV) : 0,
        "pajak": fil.PPNV != null ? parseFloat(fil.PPNV) : 0,
        "grandtotal": fil.Netto != null ? parseFloat(fil.Netto) : 0,
        "bayar": bayar,
        "sisa": sisa,
        "kredit": parseFloat(fil.JmlBayarKredit),
        "listitem": [list]
      }

      var cabang = {
        "nama": fil.NmMSup,
        "list": [data_per_nota],
      }
    
      // cabang terbaru (cabang => nota => item)
      if (!listcabang.includes(fil.NmMSup)) {
        pembelian += 1;
        pengeluaran += parseFloat(fil.Netto);

        listcabang.push(fil.NmMSup);
        listbrg = [];
        listbrg.push(fil.IdTBeli);

        arr_list.push(cabang);
      }
      else { // cabang yang sudah ada
        let idx = listcabang.indexOf(fil.NmMSup);

        // nota terbaru di cabang yang sudah ada (nota => item)
        if (!listbrg.includes(fil.IdTBeli)) {
          pembelian += 1; 
          pengeluaran += parseFloat(fil.Netto);

          listbrg.push(fil.IdTBeli);
          arr_list[idx].list.push(data_per_nota);
        }

        // nota yang sudah ada (item)
        else {
          let idx2 = listbrg.indexOf(fil.IdTBeli);
          arr_list[idx].list[idx2].listitem.push(list);
        }
      }
    }));

    count = {
      "pembelian" : pembelian,
      "produk_dibeli" : produk_dibeli,
      "pengeluaran" : pengeluaran,
    }

    res.json({
      message: "Success",
      countData: count,
      data: arr_list,
    });
  }else{
    var arr_list = [];
    var listcabang = [];
    var listbrg = [];

    var pembelian = 0; var produk_dibeli = 0; var pengeluaran = 0;

    var arr_data = await Promise.all(data.map(async (fil, index) => {

      produk_dibeli += parseFloat(fil.QtyTotal); // hitung produk terjual

      var list = {
        "id": fil.IdMBrg,
        "kode": fil.KdMBrg,
        "barcode": fil.KdMBrg,
        "nama": fil.NmMBrg,
        "gudang": fil.NmMGd,
        "jumlah": parseFloat(fil.QtyTotal),
        "qty": fil.QtyTotal != '' ? parseFloat(fil.QtyTotal) : 0,
        "harga": fil.HrgStn != '' ? parseFloat(fil.HrgStn) : 0,
        "satuan": fil.NmMStn1,
        "hargasat": fil.HrgStn != '' ? parseFloat(fil.HrgStn) : 0,
        "diskon": fil.DiscVD != '' ? parseFloat(fil.DiscVD) : 0,
        "total": fil.SubTotal != '' ? parseFloat(fil.SubTotal) : 0,
        "pajak": 0,
        "dpp": fil.dpp != '' ? parseFloat(fil.dpp) : 0,
        "subtotal": fil.SubTotal != '' ? parseFloat(fil.SubTotal) : 0,
      };

      var bayar = 0;
      if(jenis == 1){
        bayar = parseFloat(fil.bayar)
      }else{
        bayar = parseFloat(fil.JmlBayarTunai)
      }

      var sisa = parseFloat(fil.Netto) - parseFloat(fil.bayar);

      var data_per_nota = {
        "id": fil.IdTBeli,
        "tanggal": fil.TglTBeli,
        "transaksi": fil.BuktiTBeli,
        "supplier": fil.NmMSup,
        "subtotal": fil.Bruto != null ? parseFloat(fil.Bruto) : 0,
        "diskon": fil.DiscV != null ? parseFloat(fil.DiscV) : 0,
        "pajak": fil.PPNV != null ? parseFloat(fil.PPNV) : 0,
        "grandtotal": fil.Netto != null ? parseFloat(fil.Netto) : 0,
        "bayar": bayar,
        "sisa": sisa,
        "kredit": parseFloat(fil.JmlBayarKredit),
        "listitem": [list]
      }

      var cabang = {
        "nama": fil.NmMSup,
        "list": [data_per_nota],
      }
    
      // cabang terbaru (cabang => nota => item)
      if (!listcabang.includes(fil.IdMBrg)) {
        pembelian += 1;
        pengeluaran += parseFloat(fil.Netto);

        listcabang.push(fil.IdMBrg);
        listbrg = [];
        listbrg.push(fil.IdTBeli);

        arr_list.push(cabang);
      }
      else { // cabang yang sudah ada
        let idx = listcabang.indexOf(fil.IdMBrg);

        // nota terbaru di cabang yang sudah ada (nota => item)
        if (!listbrg.includes(fil.IdTBeli)) {
          pembelian += 1; 
          pengeluaran += parseFloat(fil.Netto);

          listbrg.push(fil.IdTBeli);
          arr_list[idx].list.push(data_per_nota);
        }

        // nota yang sudah ada (item)
        else {
          let idx2 = listbrg.indexOf(fil.IdTBeli);
          arr_list[idx].list[idx2].listitem.push(list);
        }
      }
    }));

    count = {
      "pembelian" : pembelian,
      "produk_dibeli" : produk_dibeli,
      "pengeluaran" : pengeluaran,
    }

    res.json({
      message: "Success",
      countData: count,
      data: arr_list,
    });
  }
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
            arr_list[idx].list.push(kas);
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
    const companyid = req.datacompany.id;

    let jenis = req.body.jenis || 1;
    // posisi bank
    if (jenis == 1) {        
        let date = req.body.tanggal || today;
        let q = await qbank.queryPosisiBank(companyid, date);
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

      let q = await qbank.queryKartuBank(companyid,start,end,mbank);
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
        if (!listcabang.includes(fil.KdMCabang)) {
          listcabang.push(fil.KdMCabang);
          listbank.push(fil.KdMRek);

          arr_list.push(cabang);
        }
        // cabang yang sudah ada
        else {
          let idx = listcabang.indexOf(fil.KdMCabang);
          // bank terbaru di cabang yang sudah ada (bank => item)
          if (!listbank.includes(fil.KdMRek)) { 
            listbank.push(fil.KdMRek);
            arr_list[idx].list.push(bank);
          }
          // bank yang sudah ada (item)
          else {
            let idx2 = listbank.indexOf(fil.KdMRek);
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
