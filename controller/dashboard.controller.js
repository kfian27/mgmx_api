// const db = require("../models");
// const sequelize = db.sequelize;

const fun = require("../mgmx");
const query = require("../query");

let today = new Date().toJSON().slice(0, 10);


function resData(count = 0, list = []) {
    return {
        countData: count,
        data: list
    };
}

exports.getDataCustomer = async (req, res) => {
    const sequelize = await fun.connection(req.datacompany);
    let countData = await fun.countDataFromQuery(
        sequelize,
        `SELECT COUNT(*) as total from MgArMCust where hapus=0`
    );
    
    let sql = `select idmcust as ID, nmmcust as nama, alamat, kota, Telp1 as hp from mgarmcust where aktif=1 and hapus=0 ORDER BY nama ASC`;
    const data = await sequelize.query(sql, {
        raw: false,
    });

    res.json({
        message: "Success",
        countData: countData,
        data: data[0]
    });
}

exports.getDataSupplier = async (req, res) => {
    const sequelize = await fun.connection(req.datacompany);

    let countData = await fun.countDataFromQuery(
        sequelize,
        `SELECT COUNT(*) as total from mgapmsup where hapus=0`
    );
    
    let sql = `select idmsup as ID, nmmsup as nama, alamat, kota, Telp1 as hp from mgapmsup where aktif=1 and hapus=0 ORDER BY nama ASC`;
    const data = await sequelize.query(sql, {
        raw: false,
    });

    res.json({
        message: "Success",
        countData: countData,
        data: data[0]
    });
}

exports.getDataBarang = async (req, res) => {
    const sequelize = await fun.connection(req.datacompany);

    let countData = await fun.countDataFromQuery(
        sequelize,
        `SELECT COUNT(*) as total from mginmbrg where hapus=0`
    );
    
    let sql = `select idmbrg as ID, kdmbrg as kode, nmmbrg as nama from mginmbrg where aktif=1 and hapus=0 ORDER BY kode ASC`;
    const data = await sequelize.query(sql, {
        raw: false,
    });

    res.json({
        message: "Success",
        countData: countData,
        data: data[0]
    });
}



// jual < create (backdate)
exports.getWarningToday = async (req, res) => {
    const q = require("../class/query_dashboard/warning_today");
    const sequelize = await fun.connection(req.datacompany);
    const companyid = req.datacompany.id;

    // jual_lewatjt
    var count_juallewatjt = 0;
    let qsql_juallewatjt = await q.queryPenjualanJatuhTempo(companyid);
    const juallewatjt = await fun.getDataFromQuery(sequelize,qsql_juallewatjt);
    var arr_juallewatjt = await Promise.all(juallewatjt.map(async (list, index) => {
        count_juallewatjt++;
        return resList(list, 1);
    }))

    // jual_jthi
    var count_jualjthi = 0;
    let qsql_jualjthi = await q.queryPenjualanJatuhTempo(companyid, 1);
    const jualjthi = await fun.getDataFromQuery(sequelize,qsql_jualjthi);
    var arr_jualjthi = await Promise.all(jualjthi.map(async (list, index) => {
        count_jualjthi++;
        return resList(list, 1);
    }))

    // jual_jt7
    var count_jualjt7 = 0;
    let qsql_jualjt7 = await q.queryPenjualanJatuhTempo(companyid, 2);
    const jualjt7 = await fun.getDataFromQuery(sequelize,qsql_jualjt7);
    var arr_jualjt7 = await Promise.all(jualjt7.map(async (list, index) => {
        count_jualjt7++;
        return resList(list, 1);
    }))

    //jual_voideditbackdate
    var count_jualvoid = 0
    var count_jualedit = 0
    var count_jualbackdate = 0
    let qsql_jualvoideditbackdate = await q.queryPenjualanVoidEditBackdate(companyid);
    const jualvoideditbackdate = await fun.getDataFromQuery(sequelize,qsql_jualvoideditbackdate);
    var arr_jualvoideditbackdate = await Promise.all(jualvoideditbackdate.map(async (list, index) => {
        if (list.void == 'Ya') {
            count_jualvoid++;
        }
        if (list.countedit > 0) {
            count_jualedit++;
        }
        if (list.backdate == 'Ya') {
            count_jualbackdate++;
        }
        return resListTransaksi(list, 1);
    }))

    //  ======== BELI
    // beli_lewatjt
    var count_belilewatjt = 0;
    let qsql_belilewatjt = await q.queryPembelianJatuhTempo(companyid);
    const belilewatjt = await fun.getDataFromQuery(sequelize,qsql_belilewatjt);
    var arr_belilewatjt = await Promise.all(belilewatjt.map(async (list, index) => {
        count_belilewatjt++;
        return resList(list, 2);
    }))

    // beli_jthi
    var count_belijthi = 0;
    let qsql_belijthi = await q.queryPembelianJatuhTempo(companyid, 1);
    const belijthi = await fun.getDataFromQuery(sequelize,qsql_belijthi);
    var arr_belijthi = await Promise.all(belijthi.map(async (list, index) => {
        count_belijthi++;
        return resList(list, 2);
    }))

    // beli lewat 7 hari
    var count_belijt7 = 0;
    let qsql_belijt7 = await q.queryPembelianJatuhTempo(companyid, 2);
    const belijt7 = await fun.getDataFromQuery(sequelize,qsql_belijt7);
    var arr_belijt7 = await Promise.all(belijt7.map(async (list, index) => {
        count_belijt7++;
        return resList(list, 2);
    }))

    //beli voideditbackdate
    var count_belivoid = 0;
    var count_beliedit = 0;
    var count_belibackdate = 0;
    let qsql_belivoideditbackdate = await q.queryPembelianVoidEditBackdate(companyid);
    const belivoideditbackdate = await fun.getDataFromQuery(sequelize, qsql_belivoideditbackdate);
    var arr_belivoideditbackdate = await Promise.all(belivoideditbackdate.map(async (list, index) => {
        if (list.void == 'Ya') {
            count_belivoid++;
        }
        if (list.countedit > 0) {
            count_beliedit++;
        }
        if (list.backdate == 'Ya') {
            count_belibackdate++;
        }
        return resListTransaksi(list, 2);
    }))


    // ===== STOCK
    var count_minstock = 0;
    let qsql_minstock = await q.queryMinStock(companyid, today);
    const minstock = await fun.getDataFromQuery(sequelize,qsql_minstock);
    var arr_minstock = await Promise.all(minstock.map(async (list, index) => {
        count_minstock++;
        return {
            "kdmbrg" : list.KdMBrg,
            "nmmbrg" : list.NmMBrg,
            "stock" : parseFloat(list.PosQty || 0)
        }
    }))

    function resList(list, jenis = 0) {
        // jual
        if (jenis == 1) {
            return {
                "tanggal": list.TglTrans,
                "notransaksi": list.BuktiTrans,
                "nmmcust": list.NmMCust,
                "netto": parseFloat(list.JmlPiut),
                "tanggaljt": list.TglJTPiut,
                "jmlbayar": parseFloat(list.JmlBayar)
            }
        }
        // beli
        else if (jenis == 2) {
            return {
                "tanggal": list.TglTrans,
                "notransaksi": list.BuktiTrans,
                "nmmcust": list.NmMSup,
                "netto": parseFloat(list.JmlHut),
                "tanggaljt": list.TglJTHut,
                "jmlbayar": parseFloat(list.BayarHut)
            }
        } else {
            return {};
        }
    }

    function resListTransaksi(list, jenis = 0) {
        // jual
        if (jenis == 1) {
            return {
                "tanggal": list.tgltjual,
                "tglcreate": list.tglcreate,
                "notransaksi": list.buktitjual,
                "nmmcust": list.nmmcust,
                "netto": parseFloat(list.netto),
                "void": list.void,
                "edit": list.countedit,
                "backdate": list.backdate
            }
        }
        // beli
        else if (jenis == 2) {
            return {
                "tanggal": list.tgltbeli,
                "tglcreate": list.tglcreate,
                "notransaksi": list.buktitbeli,
                "nmmcust": list.nmmsup,
                "netto": parseFloat(list.netto),
                "void": list.void,
                "edit": list.countedit,
                "backdate": list.backdate
            }
        }
    }

    function resDataTransaksi(countVoid = 0, countEdit = 0, countBackdate=0, list = []) {
        return {
            void: countVoid,
            edit: countEdit,
            backdate: countBackdate,
            data: list
        };
    }
    
    var data = {
        jual_lewatjt: resData(count_juallewatjt, arr_juallewatjt),
        jual_jthi: resData(count_jualjthi, arr_jualjthi),
        jual_jt7: resData(count_jualjt7, arr_jualjt7),
        jual_voideditbackdate: resDataTransaksi(count_jualvoid, count_jualedit, count_jualbackdate, arr_jualvoideditbackdate),
        beli_lewatjt: resData(count_belilewatjt, arr_belilewatjt),
        beli_jthi: resData(count_belijthi, arr_belijthi),
        beli_jt7: resData(count_belijt7, arr_belijt7),
        beli_voideditbackdate: resDataTransaksi(count_belivoid, count_beliedit, count_belibackdate, arr_belivoideditbackdate),
        minstok: resData(count_minstock, arr_minstock),
    }

    res.json({
        message: "Success",
        data: data
    });
}

exports.getTransaksiAdjustKoreksi = async (req, res) => { 
    const q = require("../class/query_dashboard/adjust_koreksi");
    const sequelize = await fun.connection(req.datacompany);
    const companyid = req.datacompany.id;

    let start = req.query.start || today;
    let end = req.query.end || today;
    let date = today;

    // PENYESUAIAN STOCK
    var count_penstock = 0;
    var arr_buktipenstock = [];
    let qsql = await q.queryPenyesuaianStok(companyid,start, end);
    const penstock = await fun.getDataFromQuery(sequelize,qsql);
    var arr_penstock = await Promise.all(penstock.map(async (list, index) => {
        // count_penstock++;
        if (!arr_buktipenstock.includes(list.BuktiTPenyesuaianBrg)) {
            arr_buktipenstock.push(list.BuktiTPenyesuaianBrg);
        }
        return {
            "nmmbrg" : list.NmMBrg,
            "tgl" : list.TglTPenyesuaianBrg,
            "bukti" : list.BuktiTPenyesuaianBrg,
            "satuan" : list.KdMStn1 || list.KdMStn2 || list.KdMStn3 || list.KdMStn4 || list.KdMStn5 || '',
            "qty" : parseFloat(list.QtyTotal)
        }
    }))
    count_penstock = arr_buktipenstock.length;

    // KOREKSI HUTANG
    var count_korhut = 0;
    var arr_buktikorhut = [];
    let qsql_korhut = await q.queryKoreksiHutang(companyid, start, end);
    const korhut = await fun.getDataFromQuery(sequelize, qsql_korhut);
    var arr_korhut = await Promise.all(korhut.map(async (list, index) => {
        // count_korhut++;
        if (!arr_buktikorhut.includes(list.BuktiTKorHut)) {
            arr_buktikorhut.push(list.BuktiTKorHut);
        }
        return {
            "tgl" : list.TglTKorHut,
            "bukti" : list.BuktiTKorHut,
            "supplier" : list.NmMSup,
            "jumlah" : parseFloat(list.JmlKor)
        }
    }))
    count_korhut = arr_buktikorhut.length;


    // KOREKSI PIUTANG
    var count_korpiut = 0;
    var arr_buktikorpiut = [];
    let qsql_korpiut = await q.queryKoreksiPiutang(companyid, start, end);
    const korpiut = await fun.getDataFromQuery(sequelize, qsql_korpiut);
    
    var arr_korpiut = await Promise.all(korpiut.map(async (list, index) => {
        // count_korpiut ++;
        if (!arr_buktikorpiut.includes(list.BuktiTKorPiut)) {
            arr_buktikorpiut.push(list.BuktiTKorPiut);
        }
        return {
            "tgl" : list.TglTKorPiut,
            "bukti" : list.BuktiTKorPiut,
            "customer" : list.NmMCust,
            "jumlah" : parseFloat(list.JmlKor)
        }
    }))
    count_korpiut = arr_buktikorpiut.length;


    // TRANSAKSI JURNAL UMUM
    var count_jurnalumum = 0;
    var arr_buktijurnalumum = [];
    let qsql_jurnalumum = await q.queryJurnalMemo(companyid, start, end);
    const jurnalumum = await fun.getDataFromQuery(sequelize, qsql_jurnalumum);
    
    var arr_jurnalumum = await Promise.all(jurnalumum.map(async (list, index) => {
        // count_jurnalumum++;
        if (!arr_buktijurnalumum.includes(list.BuktiTJurnal)) {
            arr_buktijurnalumum.push(list.BuktiTJurnal);
        }
        return {
            "tgl" : list.TglTJurnal,
            "bukti" : list.BuktiTJurnal,
            "debit" : parseFloat(list.JmlD),
            "kredit": parseFloat(list.JmlK),
            "keterangan" : list.Keterangan
        }
    }))
    count_jurnalumum = arr_buktijurnalumum.length;


    var data = {
        penyesuaian_stok: resData(count_penstock, arr_penstock),
        koreksi_hutang: resData(count_korhut, arr_korhut),
        koreksi_piutang: resData(count_korpiut, arr_korpiut),
        transaksi_jurnalumum: resData(count_jurnalumum, arr_jurnalumum),
    }

    res.json({
        message: "Success",
        data: data
    });
}

exports.getNilaiBisnis = async (req, res) => { 
    const q = require("../class/query_dashboard/nilai_bisnis");
    const sequelize = await fun.connection(req.datacompany);
    const companyid = req.datacompany.id;

    let date = today;

    let qsql_nilaibarang = await q.queryBarang(companyid, today);
    let query_barang = await sequelize.query(
        qsql_nilaibarang, {
            raw: false,
            plain: true
    })
    let nilaibarang = parseFloat(query_barang.total);
    let stockbarang = parseFloat(query_barang.total_stock);

    let qsql_totalpiutang = await q.queryPiutang(companyid, today);
    // let q_totalpiutang = await query.dashboard_totalpiutang(date);
    let totalpiutang = await fun.countDataFromQuery(
        sequelize,
        qsql_totalpiutang
    );

    let qsql_totalhutang = await q.queryHutang(companyid, today);
    // let q_totalhutang = await query.dashboard_totalpiutang(date);
    let totalhutang = await fun.countDataFromQuery(
        sequelize,
        qsql_totalhutang
    );

    let qsql_totalkas = await q.queryKas(companyid, today);
    let totalkas = await fun.countDataFromQuery(
        sequelize,
        qsql_totalkas
    );

    let qsql_totalbank = await q.queryBank(companyid, today);
    let totalbank = await fun.countDataFromQuery(
        sequelize,
        qsql_totalbank
    );

    var data = {
        nilaibarang: nilaibarang,
        stockbarang: stockbarang,
        totalpiutang: totalpiutang,
        totalhutang: totalhutang,
        totalkas: totalkas,
        totalbank: totalbank
    }
    res.json({
        message: "Success",
        data: data
    });

    
}

exports.getBarangTerlaku = async (req, res) => {
    const q = require("../class/query_dashboard/barang_terlaku");
    const sequelize = await fun.connection(req.datacompany);
    const companyid = req.datacompany.id;

    let start = req.query.start || today;
    let end = req.query.end || today;
    let date = today;
    let qsql_barangterlaku = await q.queryBarangTerlaku(companyid, start, end);
    const data = await fun.getDataFromQuery(
        sequelize,
        qsql_barangterlaku
    );

    var arr_data = data.map((list, index) => {
        list.jumlah = parseFloat(list.jumlah);
        return list;
    })
    
    res.json({
        message: "Success",
        data: arr_data
    });
}

exports.getDataGrafik = async (req, res) => {
    const q = require("../class/query_dashboard/grafik");
    const sequelize = await fun.connection(req.datacompany);
    const companyid = req.datacompany.id;

    let start = req.query.start || today;
    let end = req.query.end || today;
    let jenis = req.query.jenis;

    

    var qsql = ``;

    if (jenis == 'jual') { 
        qsql = await q.queryPenjualan(companyid, start, end);
    } else if (jenis == 'beli') {
        qsql = await q.queryPembelian(companyid, start, end);
    } else if (jenis == 'rjual') {
        qsql = await q.queryReturJual(companyid, start, end);
    } else if (jenis == 'rbeli') {
        qsql = await q.queryReturBeli(companyid, start, end);
    } else if (jenis == 'kmasuk') {
        qsql = await q.queryKasMasuk(companyid, start, end);
    } else if (jenis == 'kkeluar') {
        qsql = await q.queryKasKeluar(companyid, start, end);
    } else if (jenis == 'bmasuk') {
        qsql = await q.queryBankMasuk(companyid, start, end);
    } else if (jenis == 'bkeluar') {
        qsql = await q.queryBankKeluar(companyid, start, end);
    } else if (jenis == 'hutang') {
        qsql = await q.queryHutang(companyid, start, end);
    } else if (jenis == 'piutang') {
        qsql = await q.queryPiutang(companyid, start, end);
    } else if (jenis == 'labarugi') {
        qsql = await q.queryLabaRugi(companyid, start, end);
    }
    const data2 = await fun.getDataFromQuery(
        sequelize,
        qsql
    );
        
    var total = 0;
    var raw_data = [];
    var arr_item = await Promise.all(data2.map( async (list, index) => { 
        var newdate = new Date(list.Tanggal);
        newdate = newdate.toISOString();
        newdate = newdate.slice(0, 10);

        var jumlah = parseFloat(list.jumlah || 0);
        total += jumlah;
        raw_data.push({
            "tanggal": newdate,
            "nilai" : jumlah
        })
    }))
    
    var arr_data = [];

    start = new Date(start);

    var diff = await fun.getDateDiff(start, end);
    if (qsql != ``) {
        for (var i = 0; i <= diff; i++) {
            var tgl = start;
            if (i > 0) {
                tgl.setDate(tgl.getDate() + 1)
                tgl.toLocaleDateString();    
            }
            
            let tgl2 = tgl.toISOString();
            tgl2 = tgl2.slice(0, 10);
            
            var nilai = 0;
            var result = raw_data.find(({ tanggal }) => tanggal == String(tgl2));
            // console.log('tgl2', tgl2);
            // console.log('result', result);
            if (result) {
                nilai = parseFloat(result.nilai)
            } 
    
            var newdate2 = new Date(tgl);
            arr_data.push({
                "tanggal": newdate2,
                "nilai" : nilai
            })
        }
    }

    res.json({
        message: "Success get data grafik " + jenis,
        total:total,
        data: arr_data
    })
}