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
    
    let sql = `select idmcust as ID, nmmcust as nama, alamat, kota, hp1 as hp from mgarmcust where aktif=1 and hapus=0`;
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
    
    let sql = `select idmsup as ID, nmmsup as nama, alamat, kota, hp1 as hp from mgapmsup where aktif=1 and hapus=0`;
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
    
    let sql = `select idmbrg as ID, kdmbrg as kode, nmmbrg as nama from mginmbrg where aktif=1 and hapus=0`;
    const data = await sequelize.query(sql, {
        raw: false,
    });

    res.json({
        message: "Success",
        countData: countData,
        data: data[0]
    });
}

exports.getWarningToday = async (req, res) => {
    const sequelize = await fun.connection(req.datacompany);

    // jual_lewatjt
    let count_juallewatjt = await fun.countDataFromQuery(
        sequelize,
        `SELECT COUNT(tbl.status) AS total FROM (SELECT j.tglcreate, j.TglTJual, j.buktitjual,c.nmmcust, j.netto, j.TglJTPiut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar,IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgartbpiut p ON pd.IdTBPiut=p.IdTBPiut LEFT OUTER JOIN mgarmcust c ON j.idmcust=c.idmcust WHERE j.TglJTPiut < CURDATE() GROUP BY j.idtjual) tbl WHERE tbl.status='Belum lunas'`
    );
    const juallewatjt = await fun.getDataFromQuery(
        sequelize,
        `SELECT * FROM (SELECT j.tglcreate, j.tgltjual, j.buktitjual,c.nmmcust, j.netto, j.tgljtpiut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar,IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgartbpiut p ON pd.IdTBPiut=p.IdTBPiut LEFT OUTER JOIN mgarmcust c ON j.idmcust=c.idmcust WHERE j.TglJTPiut < CURDATE() GROUP BY j.idtjual) tbl WHERE tbl.status='Belum lunas' order by tgltjual desc`
    );
    var arr_juallewatjt = await Promise.all(juallewatjt.map(async (list, index) => {
        return resList(list, 1);
    }))

    // jual_jthi
    let count_jualjthi = await fun.countDataFromQuery(
        sequelize,
        `SELECT COUNT(tbl.status) AS total FROM (SELECT j.tglcreate, j.TglTJual, j.buktitjual,c.nmmcust, j.netto, j.TglJTPiut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar, IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgartbpiut p ON pd.IdTBPiut=p.IdTBPiut LEFT OUTER JOIN mgarmcust c ON j.idmcust=c.idmcust WHERE j.TglJTPiut = CURDATE() GROUP BY j.idtjual) tbl WHERE tbl.status='Belum lunas'`
    );
    const jualjthi = await fun.getDataFromQuery(
        sequelize,
        `SELECT * FROM (SELECT j.tglcreate, j.tgltjual, j.buktitjual,c.nmmcust, j.netto, j.tgljtpiut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar, IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgartbpiut p ON pd.IdTBPiut=p.IdTBPiut LEFT OUTER JOIN mgarmcust c ON j.idmcust=c.idmcust WHERE j.TglJTPiut = CURDATE() GROUP BY j.idtjual) tbl WHERE tbl.status='Belum lunas'`
    );
    var arr_jualjthi = await Promise.all(jualjthi.map(async (list, index) => {
        return resList(list, 1);
    }))

    // jual_jt7
    let nw = 'DATE_ADD(CURDATE(), INTERVAL 6 DAY)';
    let count_jualjt7 = await fun.countDataFromQuery(
        sequelize,
        `SELECT COUNT(tbl.status) AS total FROM (SELECT j.tglcreate, j.TglTJual, j.buktitjual,c.nmmcust, j.netto, j.TglJTPiut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar, IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgartbpiut p ON pd.IdTBPiut=p.IdTBPiut LEFT OUTER JOIN mgarmcust c ON j.idmcust=c.idmcust WHERE j.TglJTPiut between CURDATE() and ${nw} GROUP BY j.idtjual) tbl WHERE tbl.status='Belum lunas'`
    );
    const jualjt7 = await fun.getDataFromQuery(
        sequelize,
        `SELECT * FROM (SELECT j.tglcreate, j.tgltjual, j.buktitjual,c.nmmcust, j.netto, j.tgljtpiut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar, IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgartbpiut p ON pd.IdTBPiut=p.IdTBPiut LEFT OUTER JOIN mgarmcust c ON j.idmcust=c.idmcust WHERE j.TglJTPiut between CURDATE() and ${nw} GROUP BY j.idtjual) tbl WHERE tbl.status='Belum lunas'`
    );
    var arr_jualjt7 = await Promise.all(jualjt7.map(async (list, index) => {
        return resList(list, 1);
    }))

    //jual_voideditbackdate
    let count_jualvoid = await fun.countDataFromQuery(
        sequelize,
        `SELECT COUNT(*) AS total FROM mgartjual WHERE void = 1 AND hapus = 0`
    );
    let count_jualedit = await fun.countDataFromQuery(
        sequelize,
        `SELECT COUNT(*) AS total FROM mgartjual WHERE countedit > 0 AND hapus = 0`
    );
    let count_jualbackdate = await fun.countDataFromQuery(
        sequelize,
        `SELECT COUNT(*) AS total FROM mgartjual WHERE LEFT(tglcreate,10) <> LEFT(tgltjual,10) AND hapus = 0`
    );
    const jualvoideditbackdate = await fun.getDataFromQuery(
        sequelize,
        `SELECT j.tgltjual, j.tglcreate, j.buktitjual, c.nmmcust, j.netto, IF(j.void=0,'Tidak','Ya') AS void, j.countedit, IF(LEFT(j.tglcreate,10) <> LEFT(j.tgltjual,10),'Ya','Tidak') AS backdate FROM mgartjual j LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust WHERE (LEFT(j.tglcreate,10) <> LEFT(j.tgltjual,10) AND j.hapus = 0) OR (j.countedit > 0 AND j.hapus = 0) OR (j.void = 1 AND j.hapus = 0)`
    );
    var arr_jualvoideditbackdate = await Promise.all(jualvoideditbackdate.map(async (list, index) => {
        return resListTransaksi(list, 1);
    }))

    //  ======== BELI
    // beli_lewatjt
    let count_belilewatjt = await fun.countDataFromQuery(
        sequelize,
        `SELECT COUNT(tbl.status) AS total FROM (SELECT j.tglcreate, j.TglTBeli, j.buktitbeli,c.nmmsup, j.netto, j.TglJThut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar,IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgaptbeli j LEFT OUTER JOIN mgaptbhutd pd ON j.idtbeli = pd.idtrans LEFT OUTER JOIN mgaptbhut p ON pd.IdTBhut=p.IdTBhut LEFT OUTER JOIN mgapmsup c ON j.idmsup=c.idmsup WHERE j.TglJThut < CURDATE() GROUP BY j.idtbeli) tbl WHERE tbl.status='Belum lunas'`
    );
    const belilewatjt = await fun.getDataFromQuery(
        sequelize,
        `SELECT * FROM (SELECT j.tglcreate, j.tgltbeli, j.buktitbeli,c.nmmsup, j.netto, j.tgljthut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar,IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgaptbeli j LEFT OUTER JOIN mgaptbhutd pd ON j.idtbeli = pd.idtrans LEFT OUTER JOIN mgaptbhut p ON pd.IdTBhut=p.IdTBhut LEFT OUTER JOIN mgapmsup c ON j.idmsup=c.idmsup WHERE j.TglJThut < CURDATE() GROUP BY j.idtbeli) tbl WHERE tbl.status='Belum lunas' ORDER BY tgltbeli DESC`
    );
    var arr_belilewatjt = await Promise.all(belilewatjt.map(async (list, index) => {
        return resList(list, 2);
    }))

    // beli_jthi
    let count_belijthi = await fun.countDataFromQuery(
        sequelize,
        `SELECT COUNT(tbl.status) AS total FROM (SELECT j.tglcreate, j.TglTBeli, j.buktitbeli,c.nmmsup, j.netto, j.TglJThut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar,IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgaptbeli j LEFT OUTER JOIN mgaptbhutd pd ON j.idtbeli = pd.idtrans LEFT OUTER JOIN mgaptbhut p ON pd.IdTBhut=p.IdTBhut LEFT OUTER JOIN mgapmsup c ON j.idmsup=c.idmsup WHERE j.TglJThut= CURDATE() GROUP BY j.idtbeli) tbl WHERE tbl.status='Belum lunas'`
    );
    const belijthi = await fun.getDataFromQuery(
        sequelize,
        `SELECT * FROM (SELECT j.tglcreate, j.tgltbeli, j.buktitbeli,c.nmmsup, j.netto, j.tgljthut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar,IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgaptbeli j LEFT OUTER JOIN mgaptbhutd pd ON j.idtbeli = pd.idtrans LEFT OUTER JOIN mgaptbhut p ON pd.IdTBhut=p.IdTBhut LEFT OUTER JOIN mgapmsup c ON j.idmsup=c.idmsup WHERE j.TglJThut = CURDATE() GROUP BY j.idtbeli) tbl WHERE tbl.status='Belum lunas' ORDER BY tgltbeli DESC`
    );
    var arr_belijthi = await Promise.all(belijthi.map(async (list, index) => {
        return resList(list, 2);
    }))

    // beli lewat 7 hari
    let count_belijt7 = await fun.countDataFromQuery(
        sequelize,
        `SELECT COUNT(tbl.status) AS total FROM (SELECT j.tglcreate, j.TglTBeli, j.buktitbeli,c.nmmsup, j.netto, j.TglJThut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar,IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgaptbeli j LEFT OUTER JOIN mgaptbhutd pd ON j.idtbeli = pd.idtrans LEFT OUTER JOIN mgaptbhut p ON pd.IdTBhut=p.IdTBhut LEFT OUTER JOIN mgapmsup c ON j.idmsup=c.idmsup WHERE j.TglJThut between CURDATE() and '${nw}' GROUP BY j.idtbeli) tbl WHERE tbl.status='Belum lunas'`
    );
    const belijt7 = await fun.getDataFromQuery(
        sequelize,
        `SELECT * FROM (SELECT j.tglcreate, j.tgltbeli, j.buktitbeli,c.nmmsup, j.netto, j.tgljthut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar,IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgaptbeli j LEFT OUTER JOIN mgaptbhutd pd ON j.idtbeli = pd.idtrans LEFT OUTER JOIN mgaptbhut p ON pd.IdTBhut=p.IdTBhut LEFT OUTER JOIN mgapmsup c ON j.idmsup=c.idmsup WHERE j.TglJThut between CURDATE() and '${nw}' GROUP BY j.idtbeli) tbl WHERE tbl.status='Belum lunas' ORDER BY tgltbeli DESC`
    );
    var arr_belijt7 = await Promise.all(belijt7.map(async (list, index) => {
        return resList(list, 2);
    }))

    //beli voideditbackdate
    let count_belivoid = await fun.countDataFromQuery(
        sequelize,
        `SELECT COUNT(*) AS total FROM mgaptbeli WHERE void = 1 AND hapus = 0`
    );
    let count_beliedit = await fun.countDataFromQuery(
        sequelize,
        `SELECT COUNT(*) AS total FROM mgaptbeli WHERE countedit > 0 AND hapus = 0`
    );
    let count_belibackdate = await fun.countDataFromQuery(
        sequelize,
        `SELECT COUNT(*) AS total FROM mgaptbeli WHERE LEFT(tglcreate,10) <> LEFT(tgltbeli,10) AND hapus = 0`
    );
    const belivoideditbackdate = await fun.getDataFromQuery(
        sequelize,
        `SELECT j.tgltbeli, j.tglcreate, j.buktitbeli, c.nmmsup, j.netto, IF(j.void=0,'Tidak','Ya') AS void, j.countedit, IF(LEFT(j.tglcreate,10) <> LEFT(j.tgltbeli,10),'Ya','Tidak') AS backdate FROM mgaptbeli j LEFT OUTER JOIN mgapmsup c ON j.idmsup = c.idmsup WHERE (LEFT(j.tglcreate,10) <> LEFT(j.tgltbeli,10) AND j.hapus = 0) OR (j.countedit > 0 AND j.hapus = 0) OR (j.void = 1 AND j.hapus = 0)`
    );
    var arr_belivoideditbackdate = await Promise.all(belivoideditbackdate.map(async (list, index) => {
        return resListTransaksi(list, 2);
    }))


    // ===== STOCK
    let count_minstock = await fun.countDataFromQuery(
        sequelize,
        `select count(*) as total from (select max(b.idmbrg), b.kdmbrg, b.nmmbrg, sum(ks.QtyTotal) as stock, b.QtyMinStockCabang, b.QtyMinStockGd from mginmbrg b left outer join mginlkartustock ks on b.idmbrg = ks.idmbrg WHERE b.Hapus=0 AND b.Aktif=1 group by b.idmbrg having SUM(ks.QtyTotal) < b.QtyMinStockCabang or SUM(ks.QtyTotal) < b.QtyMinStockGd or SUM(ks.QtyTotal) <=0 or isnull(SUM(ks.QtyTotal))) tbl`
    );
    const minstock = await fun.getDataFromQuery(
        sequelize,
        `SELECT MAX(b.idmbrg), b.kdmbrg, b.nmmbrg, SUM(ks.QtyTotal) AS stock, b.QtyMinStockCabang, b.QtyMinStockGd FROM mginmbrg b LEFT OUTER JOIN mginlkartustock ks ON b.idmbrg = ks.idmbrg WHERE b.Hapus=0 AND b.Aktif=1 GROUP BY b.idmbrg HAVING SUM(ks.QtyTotal) < b.QtyMinStockCabang OR SUM(ks.QtyTotal) < b.QtyMinStockGd OR SUM(ks.QtyTotal) <=0 OR ISNULL(SUM(ks.QtyTotal))`
    );
    var arr_minstock = await Promise.all(minstock.map(async (list, index) => {
        return {
            "kdmbrg" : list.kdmbrg,
            "nmmbrg" : list.nmmbrg,
            "stock" : parseFloat(list.stock || 0)
        }
    }))

    function resList(list, jenis = 0) {
        // jual
        if (jenis == 1) {
            return {
                "tanggal": list.tgltjual,
                "notransaksi": list.buktitjual,
                "nmmcust": list.nmmcust,
                "netto": parseFloat(list.netto),
                "tanggaljt": list.tgljtpiut,
                "jmlbayar": parseFloat(list.jmlbayar)
            }
        }
        // beli
        else if (jenis == 2) {
            return {
                "tanggal": list.tgltbeli,
                "notransaksi": list.buktitbeli,
                "nmmcust": list.nmmsup,
                "netto": parseFloat(list.netto),
                "tanggaljt": list.tgljthut,
                "jmlbayar": parseFloat(list.jmlbayar)
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
    const sequelize = await fun.connection(req.datacompany);

    let start = req.query.start || today;
    let end = req.query.end || today;
    let date = today;
    // PENYESUAIAN STOCK
    let count_penstock = await fun.countDataFromQuery(
        sequelize,
        `SELECT count(*) as total FROM mgintpenyesuaianbrg pb LEFT OUTER JOIN mgintpenyesuaianbrgd pbd ON pb.IdTPenyesuaianBrg = pbd.IdTPenyesuaianBrg LEFT OUTER JOIN mginmbrg b ON pbd.idmbrg = b.idmbrg LEFT OUTER JOIN mginmstn s ON b.IdMStn1 = s.idmstn WHERE pb.TglTPenyesuaianBrg between '${start}' and '${end}' ORDER BY pb.TglTPenyesuaianBrg DESC`
    );
    const penstock = await fun.getDataFromQuery(
        sequelize,
        `SELECT b.nmmbrg, pb.tgltpenyesuaianbrg, pb.buktitpenyesuaianbrg, pbd.qtytotal, s.nmmstn FROM mgintpenyesuaianbrg pb LEFT OUTER JOIN mgintpenyesuaianbrgd pbd ON pb.IdTPenyesuaianBrg = pbd.IdTPenyesuaianBrg LEFT OUTER JOIN mginmbrg b ON pbd.idmbrg = b.idmbrg LEFT OUTER JOIN mginmstn s ON b.IdMStn1 = s.idmstn WHERE pb.TglTPenyesuaianBrg between '${start}' and '${end}' and pb.Hapus = 0 ORDER BY pb.TglTPenyesuaianBrg DESC`
    );
    var arr_penstock = await Promise.all(penstock.map(async (list, index) => {
        return {
            "nmmbrg" : list.nmmbrg,
            "tgl" : list.tgltpenyesuaianbrg,
            "bukti" : list.buktitpenyesuaianbrg,
            "satuan" : list.nmmstn,
            "qty" : parseFloat(list.qtytotal)
        }
    }))
    // arr_penstock.push({
    //     "nmmbrg": '',
    //     "tgl": '',
    //     "bukti": 'dummy',
    //     "satuan": 'dummy',
    //     "qty": 0,
    // });

    // KOREKSI HUTANG
    let count_korhut = await fun.countDataFromQuery(
        sequelize,
        `SELECT count(*) as total FROM mgaptbhut h LEFT OUTER JOIN mgaptbhutd hd ON h.IdTBHut = hd.IdTBHut LEFT OUTER JOIN mgartjual j ON hd.idtrans = j.idtjual LEFT OUTER JOIN mgarmcust c ON j.IdMCust = c.IdMCust WHERE h.TglTBHut between '${start}' and '${end}' ORDER BY h.TglTBHut DESC`
    );
    const korhut = await fun.getDataFromQuery(
        sequelize,
        `SELECT h.tgltbhut, h.buktitbhut,c.nmmcust,hd.jmlbayar FROM mgaptbhut h LEFT OUTER JOIN mgaptbhutd hd ON h.IdTBHut = hd.IdTBHut LEFT OUTER JOIN mgartjual j ON hd.idtrans = j.idtjual LEFT OUTER JOIN mgarmcust c ON j.IdMCust = c.IdMCust WHERE h.TglTBHut between '${start}' and '${end}' ORDER BY h.TglTBHut DESC`
    );
    var arr_korhut = await Promise.all(korhut.map(async (list, index) => {
        return {
            "tgl" : list.tgltbhut,
            "bukti" : list.buktitbhut,
            "customer" : list.nmmcust,
            "jumlah" : parseFloat(list.jmlbayar)
        }
    }))
    // arr_korhut.push({
    //     "tgl": '',
    //     "bukti": 'dummy',
    //     "customer": 'dummy',
    //     "jumlah": 0,
    // });

    // KOREKSI PIUTANG
    let count_korpiut = await fun.countDataFromQuery(
        sequelize,
        `SELECT COUNT(*) AS total FROM mgartbpiut p LEFT OUTER JOIN mgartbpiutd pd ON p.IdTBPiut = pd.IdTBPiut LEFT OUTER JOIN mgaptbeli b ON pd.IdTrans = b.IdTBeli LEFT OUTER JOIN mgapmsup s ON b.IdMSup = s.IdMSup WHERE p.TglTBPiut between '${start}' and '${end}' ORDER BY p.TglTBPiut DESC`
    );
    const korpiut = await fun.getDataFromQuery(
        sequelize,
        `SELECT p.tgltbpiut, p.buktitbpiut, s.nmmsup, pd.jmlbayar FROM mgartbpiut p LEFT OUTER JOIN mgartbpiutd pd ON p.IdTBPiut = pd.IdTBPiut LEFT OUTER JOIN mgaptbeli b ON pd.IdTrans = b.IdTBeli LEFT OUTER JOIN mgapmsup s ON b.IdMSup = s.IdMSup WHERE p.TglTBPiut between '${start}' and '${end}' ORDER BY p.TglTBPiut DESC`
    );
    var arr_korpiut = await Promise.all(korpiut.map(async (list, index) => {
        return {
            "tgl" : list.tgltbpiut,
            "bukti" : list.buktitbpiut,
            "customer" : list.nmmsup,
            "jumlah" : parseFloat(list.jmlbayar)
        }
    }))
    // arr_korpiut.push({
    //     "tgl": '',
    //     "bukti": 'dummy',
    //     "customer": 'dummy',
    //     "jumlah": 0,
    // });

    // TRANSAKSI JURNAL UMUM
    let count_jurnalumum = await fun.countDataFromQuery(
        sequelize,
        `SELECT count(*) as total FROM mgglljurnalhrn jj where jj.tgltrans between '${start}' and '${end}' ORDER BY jj.TglTrans DESC`
    );
    const jurnalumum = await fun.getDataFromQuery(
        sequelize,
        `SELECT LEFT(jj.tgltrans,10) AS tgl, jj.buktitrans, jj.jmld, jj.jmlk, jj.keterangan FROM mgglljurnalhrn jj where jj.tgltrans between '${start}' and '${end}' ORDER BY jj.TglTrans DESC`
    );
    var arr_jurnalumum = await Promise.all(jurnalumum.map(async (list, index) => {
        return {
            "tgl" : list.tgl,
            "bukti" : list.buktitrans,
            "debit" : parseFloat(list.jmld),
            "kredit": parseFloat(list.jmlk),
            "keterangan" : list.keterangan
        }
    }))
    // arr_jurnalumum.push({
    //     "tgl": '',
    //     "bukti": 'dummy',
    //     "debit": 0,
    //     "kredit": 0,
    //     "keterangan": 'dummy'
    // });

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
    const sequelize = await fun.connection(req.datacompany);

    let date = today;
    let nilaibarang = await fun.countDataFromQuery(
        sequelize,
        `SELECT SUM(fin.nilai) AS total FROM (SELECT qtytotal, hrgstn, (hrgstn * qtytotal) AS nilai  FROM mginlkartustock) fin`
    );

    let stockbarang = await fun.countDataFromQuery(
        sequelize,
        `SELECT SUM(fin.qtytotal) AS total FROM (SELECT qtytotal, hrgstn, (hrgstn * qtytotal) AS nilai  FROM mginlkartustock) fin`
    );

    let q_totalpiutang = await query.dashboard_totalpiutang(date);
    let totalpiutang = await fun.countDataFromQuery(
        sequelize,
        q_totalpiutang
    );

    let q_totalhutang = await query.dashboard_totalpiutang(date);
    let totalhutang = await fun.countDataFromQuery(
        sequelize,
        q_totalhutang
    );

    let totalkas = await fun.countDataFromQuery(
        sequelize,
        `SELECT SUM(jmlkas) AS total FROM mgkblkartukas`
    );

    let totalbank = await fun.countDataFromQuery(
        sequelize,
        `SELECT sum(TablePosRek.PosRek) as total FROM (SELECT TransAll.IdMCabang, IdMRek, Sum(JmlRek) as PosRek FROM (Select k.TglTrans, k.IdMCabang, k.IdMRek, k.JmlRek FROM MGKBLKartuBank k UNION ALL SELECT '${date}' as TglTrans, IdMCabang, IdMRek, 0 as JmlRek FROM MGKBMRek) TransAll WHERE TglTrans < '${date}' GROUP BY TransAll.IdMCabang, IdMRek) TablePosRek LEFT OUTER JOIN MGSYMCabang MCabang ON (TablePosRek.IdMCabang = MCabang.IdMCabang) LEFT OUTER JOIN MGKBMRek MRek ON (TablePosRek.IdMCabang = MRek.IdMCabang AND TablePosRek.IdMRek = MRek.IdMRek) LEFT OUTER JOIN MGSYMUSerMRek MUserMRek ON (MUserMrek.IdMCabangMrek=Mrek.IdMCabang AND MUserMrek.IdMrek=Mrek.IdMrek) WHERE MCabang.Hapus = 0 AND MCabang.Aktif = 1 AND MRek.Hapus = 0 AND MRek.Aktif = 1 AND PosRek <> 0 AND MUserMRek.IdMUser=1 ORDER BY MCabang.KdMCabang, MRek.NmMRek`
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
    const sequelize = await fun.connection(req.datacompany);

    let start = req.query.start || today;
    let end = req.query.end || today;
    let date = today;
    const data = await fun.getDataFromQuery(
        sequelize,
        `SELECT b.nmmbrg as nama, SUM(jd.qtytotal) AS jumlah FROM mgartjuald jd LEFT OUTER JOIN mgartjual j ON j.idtjual = jd.idtjual LEFT OUTER JOIN mginmbrg b ON jd.idmbrg = b.idmbrg WHERE j.tgltjual BETWEEN '${start}' AND '${end}' GROUP BY b.idmbrg ORDER BY SUM(jd.qtytotal) DESC LIMIT 10`
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
    const sequelize = await fun.connection(req.datacompany);

    let start = req.query.start || today;
    let end = req.query.end || today;
    let jenis = req.query.jenis;

    start = new Date(start);

    var diff = await fun.getDateDiff(start, end);

    var arr_data = [];
    var total = 0;
    for (var i = 0; i <= diff; i++) {
        var tgl = start;
        if (i > 0) {
            tgl.setDate(tgl.getDate() + 1)
            tgl.toLocaleDateString();    
        }
        
        let tgl2 = tgl.toISOString();
        tgl2 = tgl2.slice(0, 10);
        var sql = '';
        if (jenis == 'jual') { 
            sql = await query.dashboard_grafikjual(tgl2)
        } else if (jenis == 'beli') {
            sql = `SELECT SUM(netto) AS jumlah FROM mgaptbeli WHERE tgltbeli = '${tgl2}' and hapus=0`
        } else if (jenis == 'rjual') {
            sql = `select sum(netto) as jumlah from mgartrjual where tgltrjual = '${tgl2}' and hapus=0`
        } else if (jenis == 'rbeli') {
            sql = `SELECT SUM(netto) AS jumlah FROM mgaptrbeli WHERE tgltrbeli = '${tgl2}' and hapus=0`
        } else if (jenis == 'kmasuk') {
            sql = `SELECT SUM(jmlkas) AS jumlah FROM mgkblkartukas WHERE tgltrans = '${tgl2}' and jmlkas >= 0`
        } else if (jenis == 'kkeluar') {
            sql = `SELECT SUM(abs(jmlkas)) AS jumlah FROM mgkblkartukas WHERE tgltrans = '${tgl2}' and jmlkas <=0`
        } else if (jenis == 'bmasuk') {
            sql = `SELECT SUM(jmlrek) AS jumlah FROM mgkblkartubank WHERE jmlrek>=0 and tgltrans = '${tgl2}'`
        } else if (jenis == 'bkeluar') {
            sql = `SELECT SUM(abs(jmlrek)) AS jumlah FROM mgkblkartubank WHERE jmlrek<=0 and tgltrans = '${tgl2}'`
        } else if (jenis == 'hutang') {
            sql = await query.dashboard_grafikhutang(tgl2);
        } else if (jenis == 'piutang') {
            sql = await query.dashboard_grafikpiutang(tgl2);
        } else if (jenis == 'labarugi') {
            sql = await query.dashboard_grafiklabarugi(tgl2);
        } 

        if (sql != '') {
            const data = await fun.getDataFromQuery(
                sequelize,
                sql
            );
            
            var arr_item = await data.map((list, index) => { 
                var newdate = new Date(tgl);
                var jumlah = parseFloat(list.jumlah || 0);
                total += jumlah;
                arr_data.push({
                    "tanggal": newdate,
                    "nilai" : jumlah
                })
            })
        }
    }

    res.json({
        message: "Success get data grafik " + jenis,
        total:total,
        data: arr_data
    })
}