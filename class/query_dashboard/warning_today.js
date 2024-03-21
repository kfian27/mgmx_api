const fun = require("../../mgmx");
var companyWI = fun.companyWI;

// LAPORAN PIUTANG JATUH TEMPO
exports.queryPenjualanJatuhTempo = async (companyid, jenis = 0) => { 
    // 0: lewat, 1: hari ini, 2: h-7
    var where = `  <= CURDATE()`;
    if (jenis == 1) {
        where = `  = CURDATE()`;
    } else if (jenis == 2) {
        // where = ` AND datediff(date_format(TglJTPiut, '%Y-%m-%d'), CURDATE()) = 7`;
        where = `  between CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 6 DAY)`;
    }
    var sql = ``;
    if (companyid == companyWI) {
        sql = `SELECT * FROM (
                SELECT MCabang.KdMCabang
                    , MCabang.NmMCabang
                    , MCust.KdMCust
                    , MCust.NmMCust
                    , Trans.IdMCust
                    , Trans.IdMCabangMCust
                    , MKota.IdMKota
                    , MKota.KdMKota
                    , MKota.NmMKota
                    , Trans.JenisTrans
                    , Trans.BuktiTrans
                    , Trans.IdMCabang
                    , Trans.IdTrans
                    , Trans.TglTrans
                    , Trans.JmlPiut
                    , Trans.Retur
                    , SUM(Trans.JmlBayar) AS JmlBayar
                    , Trans.TglJTPiut
                    , sum(Trans.SisaPiut) as SisaPiut
                    , Trans.JenisInvoice
                FROM (
                SELECT TJualPOS.IdMCabangMCust 
                    , TJualPOS.IdMCust 
                    , 'T' as JenisTrans 
                    , TJualPOS.BuktiTJualPOS as BuktiTrans 
                    , TJualPOS.IdMCabang 
                    , TJualPOS.IdTJualPOS as IdTrans 
                    , TJualPOS.TglTJualPOS as TglTrans 
                    , (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) as JmlPiut 
                    , 0 AS Retur
                    , ((JmlBayarTunai - Kembali) - JmlBayarKartu) AS JmlBayar
                    , TJualPOS.TglJTPiut 
                    , (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) as SisaPiut 
                    , 0 as JenisInvoice 
                FROM MGARTJualPOS TJualPOS 
                WHERE TJualPOS.Hapus = 0 
                AND TJualPOS.Void = 0 
                UNION ALL 
                SELECT TJualPOS.IdMCabangMCust 
                    , TJualPOS.IdMCust 
                    , 'T' as JenisTrans 
                    , TJualPOS.BuktiTJualPOS as BuktiTrans 
                    , TJualPOS.IdMCabang 
                    , TJualPOS.IdTJualPOS as IdTrans 
                    , TJualPOS.TglTJualPOS as TglTrans 
                    , (TJualPOS.Netto - (TJualPOS.JmlBayarTunai - TJualPOS.Kembali) - TJualPOS.JmlBayarKartu) as JmlPiut 
                    , 0 AS Retur
                    , d.JmlBayar AS JmlBayar
                    , TJualPOS.TglJTPiut 
                    , - d.JmlBayar as SisaPiut 
                    , 0 as JenisInvoice 
                FROM MGARTBPiutD d LEFT OUTER JOIN MGARTBPiut m on (d.IdMCabang = m.IdMCabang and d.IdTBPiut = m.IdTBPiut) 
                                    LEFT OUTER JOIN MGARTJualPOS TJualPOS ON (d.IdMCabangTrans = TJualPOS.IdMCabang AND d.IdTrans = TJualPOS.IdTJualPOS) 
                WHERE TJualPOS.Hapus = 0 
                AND TJualPOS.Void = 0 
                AND m.Hapus = 0 
                AND m.Void = 0 
                AND d.JenisTrans = 'T' 
                UNION ALL
                SELECT TJual.IdMCabangMCust 
                    , TJual.IdMCust 
                    , 'J' as JenisTrans 
                    , TJual.BuktiTJual as BuktiTrans 
                    , TJual.IdMCabang 
                    , TJual.IdTJual as IdTrans 
                    , TJual.TglTJual as TglTrans 
                    , TJual.JmlBayarKredit as JmlPiut 
                    , 0 AS Retur
                    , 0 AS JmlBayar
                    , TJual.TglJTPiut 
                    , TJual.JmlBayarKredit as SisaPiut 
                    , TJual.JenisTJual as JenisInvoice 
                FROM MGARTJual TJual 
                WHERE TJual.Hapus = 0 
                AND TJual.Void = 0 
                AND Coalesce(TJual.IdTRJual, 0) = 0
                UNION ALL 
                SELECT TJual.IdMCabangMCust 
                    , TJual.IdMCust 
                    , 'J' as JenisTrans 
                    , TJual.BuktiTJual as BuktiTrans 
                    , TJual.IdMCabang 
                    , TJual.IdTJual as IdTrans 
                    , TJual.TglTJual as TglTrans 
                    , TJual.JmlBayarKredit as JmlPiut 
                    , 0 AS Retur
                    , d.JmlBayar AS JmlBayar
                    , TJual.TglJTPiut 
                    , - d.JmlBayar as SisaPiut 
                    , TJual.JenisTJual as JenisInvoice 
                FROM MGARTBPiutD d LEFT OUTER JOIN MGARTBPiut m on (d.IdMCabang = m.IdMCabang and d.IdTBPiut = m.IdTBPiut) 
                                LEFT OUTER JOIN MGARTJual TJual ON (d.IdMCabangTrans = TJual.IdMCabang AND d.IdTrans = TJual.IdTJual) 
                WHERE TJual.Hapus = 0 
                AND TJual.Void = 0 
                AND m.Hapus = 0 
                AND m.Void = 0 
                AND d.JenisTrans = 'J' 
            ) Trans LEFT OUTER JOIN MGSYMCabang MCabang ON (Trans.IdMCabang = MCabang.IdMCabang)
                    LEFT OUTER JOIN MGARMCust MCust ON (Trans.IdMCabangMCust = MCust.IdMCabang and Trans.IdMCust = MCust.IdMCust)
                    LEFT OUTER JOIN MGSYMCabang MCabangMCust ON (MCust.IdMCabang = MCabangMCust.IdMCabang)
                    LEFT OUTER JOIN MGARMJenisInvoice JI on (Trans.JenisInvoice = JI.IdmJenisInvoice and Trans.IdMCabang = JI.IdMCabang)
                    LEFT OUTER JOIN MGARMKota MKota ON (MCust.IdMCabangMKota = MKota.IdMCabang AND MCust.IdMKota = MKota.IdMKota)
                WHERE MCabang.Hapus = 0
                AND MCabang.Aktif = 1
                AND MCabang.KdMCabang LIKE '%%'
                AND MCabang.NmMCabang LIKE '%%'
                AND MCust.Hapus = 0
                AND MCust.Aktif = 1
                AND MCust.KdMCust LIKE '%%'
                AND MCust.NmMCust LIKE '%%'
                AND MKota.Hapus = 0
                AND MKota.Aktif = 1
                GROUP BY MCabang.KdMCabang
                    , MCabang.NmMCabang
                    , MCust.KdMCust
                    , MCust.NmMCust
                    , Trans.IdMCust
                    , Trans.IdMCabangMCust
                    , Trans.JenisTrans
                    , Trans.BuktiTrans
                    , Trans.IdMCabang
                    , Trans.IdTrans
                    , Trans.TglTrans
                    , Trans.JmlPiut
                    , Trans.Retur
                    , Trans.TglJTPiut
                    , Trans.JenisInvoice
            ) Tbl
                WHERE SisaPiut <> 0
                AND TglJTPiut ${where}
            ORDER BY KdMCust, TglTrans`;
        
    } else {
        sql = `SELECT * FROM (
                SELECT MCabang.KdMCabang
                    , MCabang.NmMCabang
                    , MCust.KdMCust
                    , MCust.NmMCust
                    , Trans.IdMCust
                    , Trans.IdMCabangMCust
                    , Trans.JenisTrans
                    , Trans.BuktiTrans
                    , Trans.IdMCabang
                    , Trans.IdTrans
                    , Trans.IdTransD
                    , Trans.TglTrans
                    , Trans.JmlPiut
                    , Trans.TglJTPiut
                    , sum(Trans.SisaPiut) as SisaPiut
                    , Trans.JenisInvoice
                FROM (
                SELECT TJualPOS.IdMCabangMCust 
                    , TJualPOS.IdMCust 
                    , 'T' as JenisTrans 
                    , TJualPOS.BuktiTJualPOS as BuktiTrans 
                    , TJualPOS.IdMCabang 
                    , TJualPOS.IdTJualPOS as IdTrans 
                    , 0 as IdTransD 
                    , TJualPOS.TglTJualPOS as TglTrans 
                    , (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) as JmlPiut 
                    , TJualPOS.TglJTPiut 
                    , (Netto - (JmlBayarTunai - Kembali) - JmlBayarKartu) as SisaPiut 
                    , 0 as JenisInvoice 
                FROM MGARTJualPOS TJualPOS 
                WHERE TJualPOS.Hapus = 0 
                AND TJualPOS.Void = 0 
                AND TJualPOS.TglJTPiut ${where}
                UNION ALL 
                SELECT TJualPOS.IdMCabangMCust 
                    , TJualPOS.IdMCust 
                    , 'T' as JenisTrans 
                    , TJualPOS.BuktiTJualPOS as BuktiTrans 
                    , TJualPOS.IdMCabang 
                    , TJualPOS.IdTJualPOS as IdTrans 
                    , 0 as IdTransD 
                    , TJualPOS.TglTJualPOS as TglTrans 
                    , (TJualPOS.Netto - (TJualPOS.JmlBayarTunai - TJualPOS.Kembali) - TJualPOS.JmlBayarKartu) as JmlPiut 
                    , TJualPOS.TglJTPiut 
                    , - d.JmlBayar as SisaPiut 
                    , 0 as JenisInvoice 
                FROM MGARTBPiutD d LEFT OUTER JOIN MGARTBPiut m on (d.IdMCabang = m.IdMCabang and d.IdTBPiut = m.IdTBPiut) 
                                    LEFT OUTER JOIN MGARTJualPOS TJualPOS ON (d.IdMCabangTrans = TJualPOS.IdMCabang AND d.IdTrans = TJualPOS.IdTJualPOS) 
                WHERE TJualPOS.Hapus = 0 
                AND TJualPOS.Void = 0 
                AND m.Hapus = 0 
                AND m.Void = 0 
                AND d.JenisTrans = 'T' 
                AND TJualPOS.TglJTPiut ${where}
                UNION ALL
                SELECT TJual.IdMCabangMCust 
                    , TJual.IdMCust 
                    , 'J' as JenisTrans 
                    , TJual.BuktiTJual as BuktiTrans 
                    , TJual.IdMCabang 
                    , TJual.IdTJual as IdTrans 
                    , 0 AS IdTransD 
                    , TJual.TglTJual as TglTrans 
                    , TJual.JmlBayarKredit as JmlPiut 
                    , TJual.TglJTPiut 
                    , TJual.JmlBayarKredit as SisaPiut 
                    , TJual.JenisTJual as JenisInvoice 
                FROM MGARTJual TJual 
                WHERE TJual.Hapus = 0 
                AND TJual.Void = 0 
                AND Coalesce(TJual.IdTRJual, 0) = 0
                AND TJual.TglJTPiut ${where}
                UNION ALL 
                SELECT TJual.IdMCabangMCust 
                    , TJual.IdMCust 
                    , 'J' as JenisTrans 
                    , TJual.BuktiTJual as BuktiTrans 
                    , TJual.IdMCabang 
                    , TJual.IdTJual as IdTrans 
                    , 0 AS IdTransD 
                    , TJual.TglTJual as TglTrans 
                    , TJual.JmlBayarKredit as JmlPiut 
                    , TJual.TglJTPiut 
                    , - d.JmlBayar as SisaPiut 
                    , TJual.JenisTJual as JenisInvoice 
                FROM MGARTBPiutD d LEFT OUTER JOIN MGARTBPiut m on (d.IdMCabang = m.IdMCabang and d.IdTBPiut = m.IdTBPiut) 
                                    LEFT OUTER JOIN MGARTJual TJual ON (d.IdMCabangTrans = TJual.IdMCabang AND d.IdTrans = TJual.IdTJual) 
                WHERE TJual.Hapus = 0 
                AND TJual.Void = 0 
                AND m.Hapus = 0 
                AND m.Void = 0 
                AND d.JenisTrans = 'J' 
                AND TJual.TglJTPiut ${where}
                ) Trans LEFT OUTER JOIN MGSYMCabang MCabang ON (Trans.IdMCabang = MCabang.IdMCabang)
                        LEFT OUTER JOIN MGARMCust MCust ON (Trans.IdMCabangMCust = MCust.IdMCabang and Trans.IdMCust = MCust.IdMCust)
                        LEFT OUTER JOIN MGSYMCabang MCabangMCust ON (MCust.IdMCabang = MCabangMCust.IdMCabang)
                        LEFT OUTER JOIN MGARMJenisInvoice JI on (Trans.JenisInvoice = JI.IdmJenisInvoice and Trans.IdMCabang = JI.IdMCabang)
                WHERE MCabang.Hapus = 0
                AND MCabang.Aktif = 1
                AND MCabang.KdMCabang LIKE '%%'
                AND MCabang.NmMCabang LIKE '%%'
                AND MCust.Hapus = 0
                AND MCust.Aktif = 1
                AND MCust.KdMCust LIKE '%%'
                AND MCust.NmMCust LIKE '%%'
                GROUP BY MCabang.KdMCabang
                        , MCabang.NmMCabang
                        , MCust.KdMCust
                        , MCust.NmMCust
                        , Trans.IdMCust
                        , Trans.IdMCabangMCust
                        , Trans.JenisTrans
                        , Trans.BuktiTrans
                        , Trans.IdMCabang
                        , Trans.IdTrans
                        , Trans.IdTransD
                        , Trans.TglTrans
                        , Trans.JmlPiut
                        , Trans.TglJTPiut
                        , Trans.JenisInvoice
                ) Tbl
                WHERE SisaPiut <> 0
                AND TglJTPiut ${where}
                ORDER BY TglJTPiut, TglTrans
                `;
    }
    return sql;
}

exports.queryPenjualanVoidEditBackdate = async (companyid) => { 
    var sql = ``;
    if (companyid == companyWI) {
        
    }
    sql = `SELECT j.tgltjual, j.tglcreate, j.buktitjual, c.nmmcust, j.netto, IF(j.void=0,'Tidak','Ya') AS void, j.countedit, IF(LEFT(j.tglcreate,10) > LEFT(j.tgltjual,10),'Ya','Tidak') AS backdate FROM mgartjual j LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust WHERE (LEFT(j.tglcreate,10) > LEFT(j.tgltjual,10) AND j.hapus = 0) OR (j.countedit > 0 AND j.hapus = 0) OR (j.void = 1 AND j.hapus = 0)`;

    return sql;
}

// jual < create (backdate)
// LAPORAN HUTANG JATUH TEMPO
exports.queryPembelianJatuhTempo = async (companyid, jenis = 0) => { 
    var where = ` <= CURDATE()`;
    if (jenis == 1) {
        where = ` = CURDATE()`;
    } else if (jenis == 2) {
        // where = ` AND datediff(date_format(TglJTHut, '%Y-%m-%d'), CURDATE()) = 7`;
        where = ` between CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 6 DAY)`;
    }
    var sql = ``;
    if (companyid == companyWI) {
        sql = `SELECT * FROM (
                SELECT MCabang.KdMCabang
                    , MCabang.NmMCabang
                    , MSup.KdMSup
                    , MSup.NmMSup
                    , Trans.IdMSup
                    , Trans.JenisTrans
                    , Trans.BuktiTrans
                    , Trans.IdMCabang
                    , Trans.IdTrans
                    , Trans.TglTrans
                    , Trans.JmlHut
                    , MSup.Kota
                    , sum(Trans.BayarHut) as BayarHut
                    , Trans.TglJTHut
                    , sum(Trans.SisaHut) as SisaHut
                    , MSup.JenisMSup
                    , Trans.Keterangan
                FROM (
                SELECT TBeli.IdMSup 
                    , 'T' as JenisTrans 
                    , TBeli.BuktiTBeli as BuktiTrans 
                    , TBeli.IdMCabang 
                    , TBeli.IdTBeli as IdTrans 
                    , TBeli.TglTBeli as TglTrans 
                    , (TBeli.Netto - TBeli.JmlBayarTunai) AS JmlHut 
                    , TBeli.JmlBayarTunai as BayarHut
                    , TBeli.TglJTHut as TglJTHut 
                    , (TBeli.Netto - TBeli.JmlBayarTunai) AS SisaHut 
                    , Keterangan
                FROM MGAPTBeli TBeli 
                WHERE TBeli.Hapus = 0 
                AND TBeli.Void = 0 
                AND TBeli.VoidLPB = 0 
                AND TBeli.HapusLPB = 0 
                UNION ALL 
                SELECT TBeli.IdMSup 
                    , 'T' as JenisTrans 
                    , TBeli.BuktiTBeli as BuktiTrans 
                    , TBeli.IdMCabang 
                    , TBeli.IdTBeli as IdTrans 
                    , TBeli.TglTBeli as TglTrans 
                    , (TBeli.Netto - TBeli.JmlBayarTunai) as JmlHut 
                    , d.JmlBayar as BayarHut
                    , TBeli.TglJTHut as TglJTHut 
                    , - d.JmlBayar as SisaHut 
                    , TBeli.Keterangan As Keterangan
                FROM MGAPTBHutD d LEFT OUTER JOIN MGAPTBHut m on (d.IdMCabang = m.IdMCabang and d.IdTBHut = m.IdTBHut) 
                                LEFT OUTER JOIN MGAPTBeli TBeli ON (d.IdMCabangTrans = TBeli.IdMCabang AND d.IdTrans = TBeli.IdTBeli) 
                WHERE TBeli.Hapus = 0 
                AND TBeli.Void = 0 
                AND m.Hapus = 0 
                AND m.Void = 0 
                AND d.JenisTrans = 'T' 
            ) Trans 
            LEFT OUTER JOIN MGSYMCabang MCabang ON (Trans.IdMCabang = MCabang.IdMCabang)
            LEFT OUTER JOIN MGAPMSup MSup ON (Trans.IdMSup = MSup.IdMSup)
                WHERE MCabang.Hapus = 0
                AND MCabang.Aktif = 1
                AND MCabang.KdMCabang LIKE '%%'
                AND MCabang.NmMCabang LIKE '%%'
                AND MSup.Hapus = 0
                AND MSup.Aktif = 1
                AND MSup.KdMSup LIKE '%%'
                AND MSup.NmMSup LIKE '%%'
                GROUP BY MCabang.KdMCabang
                    , MCabang.NmMCabang
                    , MSup.KdMSup
                    , MSup.NmMSup
                    , Trans.IdMSup
                    , Trans.JenisTrans
                    , Trans.BuktiTrans
                    , Trans.IdMCabang
                    , Trans.IdTrans
                    , Trans.TglTrans
                    , Trans.JmlHut
                    , Trans.TglJTHut
            ) Tbl
            WHERE SisaHut <> 0
            AND TglJTHut ${where}
            ORDER BY KdMSup, TglJTHut`;
        
    } else {
        sql = `SELECT * FROM (
                SELECT MCabang.KdMCabang
                    , MCabang.NmMCabang
                    , MSup.KdMSup
                    , MSup.NmMSup
                    , Trans.IdMSup
                    , Trans.JenisTrans
                    , Trans.BuktiTrans
                    , Trans.IdMCabang
                    , Trans.IdTrans
                    , Trans.TglTrans
                    , Trans.JmlHut
                    , Trans.TglJTHut
                    , sum(Trans.SisaHut) as SisaHut
                    , MSup.JenisMSup
                FROM (
                SELECT TBeli.IdMSup 
                    , 'T' as JenisTrans 
                    , TBeli.BuktiTBeli as BuktiTrans 
                    , TBeli.IdMCabang 
                    , TBeli.IdTBeli as IdTrans 
                    , 0 AS IdTransD 
                    , TBeli.TglTBeli as TglTrans 
                    , (TBeli.Netto - TBeli.JmlBayarTunai) AS JmlHut 
                    , TBeli.TglJTHut as TglJTHut 
                    , (TBeli.Netto - TBeli.JmlBayarTunai) AS SisaHut 
                FROM MGAPTBeli TBeli 
                WHERE TBeli.Hapus = 0 
                AND TBeli.Void = 0 
                AND TBeli.VoidLPB = 0 
                AND TBeli.HapusLPB = 0 
                AND TBeli.BuktiTBeli <> ''
                AND TBeli.TglTBeli ${where}
                UNION ALL 
                SELECT TBeli.IdMSup 
                    , 'T' as JenisTrans 
                    , TBeli.BuktiTBeli as BuktiTrans 
                    , TBeli.IdMCabang 
                    , TBeli.IdTBeli as IdTrans 
                    , 0 AS IdTransD 
                    , TBeli.TglTBeli as TglTrans 
                    , (TBeli.Netto - TBeli.JmlBayarTunai) as JmlHut 
                    , TBeli.TglJTHut as TglJTHut 
                    , - d.JmlBayar as SisaHut 
                FROM MGAPTBHutD d LEFT OUTER JOIN MGAPTBHut m on (d.IdMCabang = m.IdMCabang and d.IdTBHut = m.IdTBHut) 
                                LEFT OUTER JOIN MGAPTBeli TBeli ON (d.IdMCabangTrans = TBeli.IdMCabang AND d.IdTrans = TBeli.IdTBeli) 
                WHERE TBeli.Hapus = 0 
                AND TBeli.Void = 0 
                AND m.Hapus = 0 
                AND m.Void = 0 
                AND d.JenisTrans = 'T' 
                AND m.TglTBHut ${where}
                ) Trans LEFT OUTER JOIN MGSYMCabang MCabang ON (Trans.IdMCabang = MCabang.IdMCabang)
                        LEFT OUTER JOIN MGAPMSup MSup ON (Trans.IdMSup = MSup.IdMSup)
                WHERE MCabang.Hapus = 0
                AND MCabang.Aktif = 1
                AND MCabang.KdMCabang LIKE '%%'
                AND MCabang.NmMCabang LIKE '%%'
                AND MSup.Hapus = 0
                AND MSup.Aktif = 1
                AND MSup.KdMSup LIKE '%%'
                AND MSup.NmMSup LIKE '%%'
                GROUP BY MCabang.KdMCabang
                        , MCabang.NmMCabang
                        , MSup.KdMSup
                        , MSup.NmMSup
                        , Trans.IdMSup
                        , Trans.JenisTrans
                        , Trans.BuktiTrans
                        , Trans.IdMCabang
                        , Trans.IdTrans
                        , Trans.TglTrans
                        , Trans.JmlHut
                        , Trans.TglJTHut
                ) Tbl
                WHERE SisaHut <> 0
                AND TglJTHut ${where}
                ORDER BY KdMSup, TglJTHut`;
    }
    
    return sql;
}

exports.queryPembelianVoidEditBackdate = async (companyid) => { 
    var sql = ``;
    if (companyid == companyWI) {
        
    }
    sql = `SELECT j.tgltbeli, j.tglcreate, j.buktitbeli, c.nmmsup, j.netto, IF(j.void=0,'Tidak','Ya') AS void, j.countedit, IF(LEFT(j.tglcreate,10) > LEFT(j.tgltbeli,10),'Ya','Tidak') AS backdate FROM mgaptbeli j LEFT OUTER JOIN mgapmsup c ON j.idmsup = c.idmsup WHERE (LEFT(j.tglcreate,10) > LEFT(j.tgltbeli,10) AND j.hapus = 0) OR (j.countedit > 0 AND j.hapus = 0) OR (j.void = 1 AND j.hapus = 0)`;

    return sql;
}

exports.queryMinStock = async (companyid, tanggal) => { 
    var sql = ``;
    if (companyid == companyWI) {
        
    }
    // const stock = require("../query_report/stock");

    // var qposisi_stock = await stock.queryPosisiStock(companyid, tanggal);

    // var sql = `${qposisi_stock} AND (PosQty < QtyMinStockGd OR PosQty <= 0)`
    sql = `SELECT * FROM (
            SELECT MCabang.KdMCabang, MCabang.NmMCabang
                , MBrg.KdMBrg, MBrg.NmMBrg, MBrg.Aktif
                , MJenisBrg.KdMJenisBrg, MJenisBrg.NmMJenisBrg
                , IF(IsNull(MStn1.KdMStn) = '', MStn1.KdMStn, '') as KdMStn1
                , IF(IsNull(MStn2.KdMStn) = '', MStn2.KdMStn, '') as KdMStn2
                , IF(IsNull(MStn3.KdMStn) = '', MStn3.KdMStn, '') as KdMStn3
                , IF(IsNull(MStn4.KdMStn) = '', MStn4.KdMStn, '') as KdMStn4
                , IF(IsNull(MStn5.KdMStn) = '', MStn5.KdMStn, '') as KdMStn5
                , MStock.QtyMinStockCabang as QtyMinStock
                , TablePosQty.PosQty
                , (TablePosQty.PosQty - MStock.QtyMinStockCabang) as Selisih
                , -1 * (TablePosQty.Jual + TablePosQty.Retur) AS jualretur
                , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='UKURAN_BRG'),'') AS EditUKURAN_BRG
                , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='KMK'),'') AS EditKMK
                , COALESCE((Select Nilai from MGINMBrgDGol MDGol LEFT OUTER JOIN MGINMGol MGOL ON(MGOL.idmgol=MDGOL.idmgol) where mdgol.idmbrg=MBrg.idmbrg and mgol.kdmgol='MERK'),'') AS EditMERK
            FROM (
            SELECT a.IdMCabang, a.IdMBrg, a.IdMGd, SUM(a.PosQty) AS PosQty, SUM(a.Jual) AS Jual, SUM(a.retur) AS retur FROM(
            SELECT LKartu.IdMCabang, IdMBrg, IdMGd, Sum(QtyTotal) as PosQty, 0 as Jual, 0 as Retur 
                FROM MGINLKartuStock LKartu
            WHERE TglTrans <= '${tanggal} 23:59:59'
            GROUP BY LKartu.IdMCabang, IdMBrg
            UNION ALL
            SELECT LKartu.IdMCabang, IdMBrg, IdMGd, 0 AS PosQty, SUM(LKartu.QtyTotal) AS jual, 0 AS retur
                FROM MGINLKartuStock LKartu
                WHERE TglTrans <= '${tanggal} 23:59:59'
            GROUP BY LKartu.IdMCabang, IdMBrg
            UNION ALL
            SELECT LKartu.IdMCabang, IdMBrg, IdMGd, 0 AS PosQty, 0 AS jual, SUM(LKartu.QtyTotal) AS retur
                FROM MGINLKartuStock LKartu
                WHERE TglTrans <= '${tanggal} 23:59:59'
                WHERE LKartu.JenisTrans = 'RRJ'
            GROUP BY LKartu.IdMCabang, IdMBrg) a
            LEFT OUTER JOIN MGSYMGd MGd ON (a.IdMCabang = MGd.IdMCabang AND a.IdMGd = MGd.IdMGd)
            WHERE MGd.Hapus = 0
                AND MGd.Aktif = 1
                AND MGd.KdMGd LIKE '%%'
                AND MGd.NmMGd LIKE '%%'
            GROUP BY a.IdMCabang, a.IdMBrg
            ) TablePosQty LEFT OUTER JOIN MGSYMCabang MCabang ON (TablePosQty.IdMCabang = MCabang.IdMCabang)
                        LEFT OUTER JOIN MGINMinStock MStock ON (TablePosQty.IdMBrg = MStock.IdMBrg AND TablePosQty.IdMCabang=MStock.IdMCabang)
                        LEFT OUTER JOIN MGINMBrg MBrg ON (TablePosQty.IdMBrg = MBrg.IdMBrg)
                        LEFT OUTER JOIN MGINMJenisBrg MJenisBrg ON (MBrg.IdMJenisBrg = MJenisBrg.IdMJenisBrg)
                        LEFT OUTER JOIN MGINMStn MStn1 ON (MBrg.IdMStn1 = MStn1.IdMStn)
                        LEFT OUTER JOIN MGINMStn MStn2 ON (MBrg.IdMStn2 = MStn2.IdMStn)
                        LEFT OUTER JOIN MGINMStn MStn3 ON (MBrg.IdMStn3 = MStn3.IdMStn)
                        LEFT OUTER JOIN MGINMStn MStn4 ON (MBrg.IdMStn4 = MStn4.IdMStn)
                        LEFT OUTER JOIN MGINMStn MStn5 ON (MBrg.IdMStn5 = MStn5.IdMStn)
            WHERE MCabang.Hapus = 0
            AND MCabang.Aktif = 1
            AND MCabang.KdMCabang LIKE '%%'
            AND MCabang.NmMCabang LIKE '%%'
            AND MBrg.Hapus = 0
            AND MBrg.Aktif = 1
            AND MBrg.KdMBrg LIKE '%%'
            AND MBrg.NmMBrg LIKE '%%'
            AND MBrg.Reserved_int1 <> 3
            AND MBrg.Reserved_int1 <> 2
            AND MBrg.Reserved_int1 <> 4
            AND MJenisBrg.Hapus = 0
            AND MJenisBrg.Aktif = 1
            AND MJenisBrg.KdMJenisBrg LIKE '%%'
            AND MJenisBrg.NmMJenisBrg LIKE '%%'
            AND ((PosQty < 0) OR ((MStock.QtyMinStockCabang <> 0) AND (PosQty <= MStock.QtyMinStockCabang)))
            ) AS Table1
            WHERE 
            EditUKURAN_BRG Like '%%'
            AND 
            EditKMK Like '%%'
            AND 
            EditMERK Like '%%'
            ORDER BY Table1.KdMCabang, Table1.KdMJenisBrg, Table1.NmMBrg
        `;
    return sql;
}

