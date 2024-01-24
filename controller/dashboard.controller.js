const db = require("../models");
const sequelize = db.sequelize;

const fun = require("../mgmx");

exports.getDataCustomer = async (req, res) => {
    let countData = await fun.countDataFromQuery(
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
    let countData = await fun.countDataFromQuery(
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
    let countData = await fun.countDataFromQuery(
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



exports.getDataWarningToday = async (req, res) => {
    // jual_lewatjt
    let count_juallewatjt = await fun.countDataFromQuery(
        `SELECT COUNT(tbl.status) AS total FROM (SELECT j.tglcreate, j.TglTJual, j.buktitjual,c.nmmcust, j.netto, j.TglJTPiut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar,IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgartbpiut p ON pd.IdTBPiut=p.IdTBPiut LEFT OUTER JOIN mgarmcust c ON j.idmcust=c.idmcust WHERE j.TglJTPiut < CURDATE() GROUP BY j.idtjual) tbl WHERE tbl.status='Belum lunas'`
    );
    const juallewatjt = await fun.getDataFromQuery(
        `SELECT * FROM (SELECT j.tglcreate, j.TglTJual, j.buktitjual,c.nmmcust, j.netto, j.TglJTPiut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar,IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgartbpiut p ON pd.IdTBPiut=p.IdTBPiut LEFT OUTER JOIN mgarmcust c ON j.idmcust=c.idmcust WHERE j.TglJTPiut < CURDATE() GROUP BY j.idtjual) tbl WHERE tbl.status='Belum lunas' order by tgltjual desc`
    );
    var arr_juallewatjt = await Promise.all(juallewatjt.map(async (list, index) => {
        return resList(list, 1);
    }))

    // jual_jthi
    let count_jualjthi = await fun.countDataFromQuery(
        `SELECT COUNT(tbl.status) AS total FROM (SELECT j.tglcreate, j.TglTJual, j.buktitjual,c.nmmcust, j.netto, j.TglJTPiut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar, IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgartbpiut p ON pd.IdTBPiut=p.IdTBPiut LEFT OUTER JOIN mgarmcust c ON j.idmcust=c.idmcust WHERE j.TglJTPiut = CURDATE() GROUP BY j.idtjual) tbl WHERE tbl.status='Belum lunas'`
    );
    const jualjthi = await fun.getDataFromQuery(
        `SELECT * FROM (SELECT j.tglcreate, j.TglTJual, j.buktitjual,c.nmmcust, j.netto, j.TglJTPiut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar, IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgartbpiut p ON pd.IdTBPiut=p.IdTBPiut LEFT OUTER JOIN mgarmcust c ON j.idmcust=c.idmcust WHERE j.TglJTPiut = CURDATE() GROUP BY j.idtjual) tbl WHERE tbl.status='Belum lunas'`
    );
    var arr_jualjthi = await Promise.all(jualjthi.map(async (list, index) => {
        return resList(list, 1);
    }))

    // jual_jt7
    let nw = 'DATE_ADD(CURDATE(), INTERVAL 6 DAY)';
    let count_jualjt7 = await fun.countDataFromQuery(
        `SELECT COUNT(tbl.status) AS total FROM (SELECT j.tglcreate, j.TglTJual, j.buktitjual,c.nmmcust, j.netto, j.TglJTPiut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar, IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgartbpiut p ON pd.IdTBPiut=p.IdTBPiut LEFT OUTER JOIN mgarmcust c ON j.idmcust=c.idmcust WHERE j.TglJTPiut between CURDATE() and ${nw} GROUP BY j.idtjual) tbl WHERE tbl.status='Belum lunas'`
    );
    const jualjt7 = await fun.getDataFromQuery(
        `SELECT * FROM (SELECT j.tglcreate, j.TglTJual, j.buktitjual,c.nmmcust, j.netto, j.TglJTPiut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar, IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgartbpiut p ON pd.IdTBPiut=p.IdTBPiut LEFT OUTER JOIN mgarmcust c ON j.idmcust=c.idmcust WHERE j.TglJTPiut between CURDATE() and ${nw} GROUP BY j.idtjual) tbl WHERE tbl.status='Belum lunas'`
    );
    var arr_jualjt7 = await Promise.all(jualjt7.map(async (list, index) => {
        return resList(list, 1);
    }))

    //jual_voideditbackdate
    let count_jualvoid = await fun.countDataFromQuery(
        `SELECT COUNT(*) AS total FROM mgartjual WHERE void = 1 AND hapus = 0`
    );
    let count_jualedit = await fun.countDataFromQuery(
        `SELECT COUNT(*) AS total FROM mgartjual WHERE countedit > 0 AND hapus = 0`
    );
    let count_jualbackdate = await fun.countDataFromQuery(
        `SELECT COUNT(*) AS total FROM mgartjual WHERE LEFT(tglcreate,10) <> LEFT(tgltjual,10) AND hapus = 0`
    );
    const jualvoideditbackdate = await fun.getDataFromQuery(
        `SELECT j.tgltjual, j.tglcreate, j.buktitjual, c.nmmcust, j.netto, IF(j.void=0,'Tidak','Ya') AS void, j.countedit, IF(LEFT(j.tglcreate,10) <> LEFT(j.tgltjual,10),'Ya','Tidak') AS backdate FROM mgartjual j LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust WHERE (LEFT(j.tglcreate,10) <> LEFT(j.tgltjual,10) AND j.hapus = 0) OR (j.countedit > 0 AND j.hapus = 0) OR (j.void = 1 AND j.hapus = 0)`
    );
    var arr_jualvoideditbackdate = await Promise.all(jualvoideditbackdate.map(async (list, index) => {
        return resListTransaksi(list, 1);
    }))

    //  ======== BELI
    // beli_lewatjt
    let count_belilewatjt = await fun.countDataFromQuery(
        `SELECT COUNT(tbl.status) AS total FROM (SELECT j.tglcreate, j.TglTBeli, j.buktitbeli,c.nmmsup, j.netto, j.TglJThut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar,IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgaptbeli j LEFT OUTER JOIN mgaptbhutd pd ON j.idtbeli = pd.idtrans LEFT OUTER JOIN mgaptbhut p ON pd.IdTBhut=p.IdTBhut LEFT OUTER JOIN mgapmsup c ON j.idmsup=c.idmsup WHERE j.TglJThut < CURDATE() GROUP BY j.idtbeli) tbl WHERE tbl.status='Belum lunas'`
    );
    const belilewatjt = await fun.getDataFromQuery(
        `SELECT * FROM (SELECT j.tglcreate, j.tgltbeli, j.buktitbeli,c.nmmsup, j.netto, j.tgljthut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar,IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgaptbeli j LEFT OUTER JOIN mgaptbhutd pd ON j.idtbeli = pd.idtrans LEFT OUTER JOIN mgaptbhut p ON pd.IdTBhut=p.IdTBhut LEFT OUTER JOIN mgapmsup c ON j.idmsup=c.idmsup WHERE j.TglJThut < CURDATE() GROUP BY j.idtbeli) tbl WHERE tbl.status='Belum lunas' ORDER BY tgltbeli DESC`
    );
    var arr_belilewatjt = await Promise.all(belilewatjt.map(async (list, index) => {
        return resList(list, 2);
    }))

    // beli_jthi
    let count_belijthi = await fun.countDataFromQuery(
        `SELECT COUNT(tbl.status) AS total FROM (SELECT j.tglcreate, j.TglTBeli, j.buktitbeli,c.nmmsup, j.netto, j.TglJThut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar,IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgaptbeli j LEFT OUTER JOIN mgaptbhutd pd ON j.idtbeli = pd.idtrans LEFT OUTER JOIN mgaptbhut p ON pd.IdTBhut=p.IdTBhut LEFT OUTER JOIN mgapmsup c ON j.idmsup=c.idmsup WHERE j.TglJThut= CURDATE() GROUP BY j.idtbeli) tbl WHERE tbl.status='Belum lunas'`
    );
    const belijthi = await fun.getDataFromQuery(
        `SELECT * FROM (SELECT j.tglcreate, j.tgltbeli, j.buktitbeli,c.nmmsup, j.netto, j.TglJThut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar,IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgaptbeli j LEFT OUTER JOIN mgaptbhutd pd ON j.idtbeli = pd.idtrans LEFT OUTER JOIN mgaptbhut p ON pd.IdTBhut=p.IdTBhut LEFT OUTER JOIN mgapmsup c ON j.idmsup=c.idmsup WHERE j.TglJThut = CURDATE() GROUP BY j.idtbeli) tbl WHERE tbl.status='Belum lunas' ORDER BY tgltbeli DESC`
    );
    var arr_belijthi = await Promise.all(belijthi.map(async (list, index) => {
        return resList(list, 2);
    }))

    // beli lewat 7 hari
    let count_belijt7 = await fun.countDataFromQuery(
        `SELECT COUNT(tbl.status) AS total FROM (SELECT j.tglcreate, j.TglTBeli, j.buktitbeli,c.nmmsup, j.netto, j.TglJThut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar,IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgaptbeli j LEFT OUTER JOIN mgaptbhutd pd ON j.idtbeli = pd.idtrans LEFT OUTER JOIN mgaptbhut p ON pd.IdTBhut=p.IdTBhut LEFT OUTER JOIN mgapmsup c ON j.idmsup=c.idmsup WHERE j.TglJThut between CURDATE() and '${nw}' GROUP BY j.idtbeli) tbl WHERE tbl.status='Belum lunas'`
    );
    const belijt7 = await fun.getDataFromQuery(
        `SELECT * FROM (SELECT j.tglcreate, j.tgltbeli, j.buktitbeli,c.nmmsup, j.netto, j.TglJThut, IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)AS jmlbayar,IF(IF(SUM(pd.JmlBayar),SUM(pd.JmlBayar),0)<j.netto,'Belum lunas','Lunas') AS STATUS FROM mgaptbeli j LEFT OUTER JOIN mgaptbhutd pd ON j.idtbeli = pd.idtrans LEFT OUTER JOIN mgaptbhut p ON pd.IdTBhut=p.IdTBhut LEFT OUTER JOIN mgapmsup c ON j.idmsup=c.idmsup WHERE j.TglJThut between CURDATE() and '${nw}' GROUP BY j.idtbeli) tbl WHERE tbl.status='Belum lunas' ORDER BY tgltbeli DESC`
    );
    var arr_belijt7 = await Promise.all(belijt7.map(async (list, index) => {
        return resList(list, 2);
    }))

    //beli voideditbackdate
    let count_belivoid = await fun.countDataFromQuery(
        `SELECT COUNT(*) AS total FROM mgaptbeli WHERE void = 1 AND hapus = 0`
    );
    let count_beliedit = await fun.countDataFromQuery(
        `SELECT COUNT(*) AS total FROM mgaptbeli WHERE countedit > 0 AND hapus = 0`
    );
    let count_belibackdate = await fun.countDataFromQuery(
        `SELECT COUNT(*) AS total FROM mgaptbeli WHERE LEFT(tglcreate,10) <> LEFT(tgltbeli,10) AND hapus = 0`
    );
    const belivoideditbackdate = await fun.getDataFromQuery(
        `SELECT j.tgltbeli, j.tglcreate, j.buktitbeli, c.nmmsup, j.netto, IF(j.void=0,'Tidak','Ya') AS void, j.countedit, IF(LEFT(j.tglcreate,10) <> LEFT(j.tgltbeli,10),'Ya','Tidak') AS backdate FROM mgaptbeli j LEFT OUTER JOIN mgapmsup c ON j.idmsup = c.idmsup WHERE (LEFT(j.tglcreate,10) <> LEFT(j.tgltbeli,10) AND j.hapus = 0) OR (j.countedit > 0 AND j.hapus = 0) OR (j.void = 1 AND j.hapus = 0)`
    );
    var arr_belivoideditbackdate = await Promise.all(belivoideditbackdate.map(async (list, index) => {
        return resListTransaksi(list, 2);
    }))


    // ===== STOCK
    let count_minstock = await fun.countDataFromQuery(
        `select count(*) as total from (select max(b.idmbrg), b.kdmbrg, b.nmmbrg, sum(ks.QtyTotal) as stock, b.QtyMinStockCabang, b.QtyMinStockGd from mginmbrg b left outer join mginlkartustock ks on b.idmbrg = ks.idmbrg WHERE b.Hapus=0 AND b.Aktif=1 group by b.idmbrg having SUM(ks.QtyTotal) < b.QtyMinStockCabang or SUM(ks.QtyTotal) < b.QtyMinStockGd or SUM(ks.QtyTotal) <=0 or isnull(SUM(ks.QtyTotal))) tbl`
    );
    const minstock = await fun.getDataFromQuery(
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

    function resData(count = 0, list = []) {
        return {
            countData: count,
            data: list
        };
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