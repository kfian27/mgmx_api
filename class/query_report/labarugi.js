const fun = require("../../mgmx");
var companyWI = fun.companyWI;

exports.queryLabaRugiPenjualan = async (companyid, start, end, cabang, customer, sales, barang = '') => { 
    var sql = ``;
    if (companyid == companyWI) { 
        
    }
    var qcabang = ``;
    var qcustomer = ``;
    var qsales = ``;
    var qbarang = ``;
    if (cabang != "") {
        qcabang = ` AND (MCabang.IdMCabang=${cabang})`;
    }

    if (customer != "") {
        qcustomer = ` AND (MCust.IdMCust = ${customer})`;
    }

    if (sales != "") {
        qsales = ` AND (MSales.IdMSales = ${sales})`;
    }

    if (barang != "") {
        qbarang = ` AND IdMBrg = ${barang}`;
    }
    sql = `SELECT MCabang.KdMCabang, MCabang.NmMCabang, TRLPenjualan.*
    , (NilaiJual - NilaiHPP) AS LabaRugi
    FROM (
    SELECT IdMCabang, TglTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales, NilaiJual, NilaiHPP
    FROM (
    SELECT 3 AS Idx, IdMCabang, TglTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales, SUM(Qty * HrgStn) AS NilaiJual, SUM(Qty * HPP) AS NilaiHPP
    FROM (
        SELECT m.IdMCabang, m.TglTJual AS TglTrans, m.BuktiTJual AS BuktiTrans, MCust.KdMCust, MCust.NmMCust, MSales.KdMSales, MSales.NmMSales, d.IdMBrg, d.QtyTotal AS Qty
            , (d.HrgStn- IF(ISNULL(d.DiscV), 0, d.DiscV) - ((d.HrgStn - IF(ISNULL(d.DiscV), 0, d.DiscV)) * m.discP/100)
                + ((d.HrgStn - IF(ISNULL(d.DiscV), 0, d.DiscV)
                - ((d.HrgStn - IF(ISNULL(d.DiscV), 0, d.DiscV)) * m.discP/100)) * m.PPNP/100)) AS HrgStn
            , COALESCE(d.HPP, 0) AS HPP
        FROM MGARTJualD d
            LEFT OUTER JOIN MGARTJual m ON ((d.IdMCabang = m.IdMCabang) AND (d.IdTJual = m.IdTJual))
            LEFT OUTER JOIN MGARMCust MCust ON (m.IdMCabangMCust = MCust.IdMCabang AND m.IdMCust = MCust.IdMCust)
            LEFT OUTER JOIN MGARMSales MSales ON (m.IdMCabangMSales = MSales.IdMCabang AND m.IdMSales = MSales.IdMSales)
        WHERE (m.Hapus = 0)
        AND (m.Void = 0)
        AND ((TglTJual >= '${start} 00:00:00') AND (TglTJual <= '${end} 23:59:59')) 
        AND (MCust.Hapus = 0)
        AND (MCust.KdMCust LIKE '%%')
        AND (MCust.NmMCust LIKE '%%')
        AND (MSales.Hapus = 0)
        AND (MSales.KdMSales LIKE '%%')
        AND (MSales.NmMSales LIKE '%%')
        ${qcustomer} ${qsales}
    ) TablePenjualan
    WHERE IdMBrg <> 0 ${qbarang}
    GROUP BY IdMCabang, TglTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales
    UNION ALL
    SELECT 4 AS Idx, IdMCabang, TglTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales, SUM(Qty * HrgStn) AS NilaiJual, SUM(Qty * HPP) AS NilaiHPP
    FROM (
        SELECT m.IdMCabang, m.TglTRJual AS TglTrans, m.BuktiTRJual AS BuktiTrans, MCust.KdMCust, MCust.NmMCust, MSales.KdMSales, MSales.NmMSales, d.IdMBrg, d.QtyTotal AS Qty, - (d.HrgStn) AS HrgStn
            , COALESCE(-d.HPP, 0) AS HPP
        FROM MGARTRJualD d
            LEFT OUTER JOIN MGARTRJual m ON ((d.IdMCabang = m.IdMCabang) AND (d.IdTRJual = m.IdTRJual))
            LEFT OUTER JOIN MGARTJual TJual ON (TJual.IdMCabang = m.IdMCabangTJual AND TJual.IdTJual = m.IdTJual)
            LEFT OUTER JOIN MGARMCust MCust ON (m.IdMCabangMCust = MCust.IdMCabang AND m.IdMCust = MCust.IdMCust)
            LEFT OUTER JOIN MGARMSales MSales ON (TJual.IdMCabangMSales = MSales.IdMCabang AND TJual.IdMSales = MSales.IdMSales)
        WHERE (m.IdTJual <> 0)
        AND (m.Hapus = 0) AND (m.Void = 0)
        AND ((TglTRJual >= '${start} 00:00:00') AND (TglTRJual <= '${end} 23:59:59')) 
        AND (MCust.Hapus = 0)
        AND (MCust.KdMCust LIKE '%%')
        AND (MCust.NmMCust LIKE '%%')
        AND (MSales.Hapus = 0)
        AND (MSales.KdMSales LIKE '%%')
        AND (MSales.NmMSales LIKE '%%')
        ${qcustomer} ${qsales}
    ) TableReturPenjualan
    WHERE IdMBrg <> 0 ${qbarang}
    GROUP BY IdMCabang, TglTrans, BuktiTrans, KdMCust, NmMCust, KdMSales, NmMSales
    ) SubTRLPenjualan
    ) TRLPenjualan
    LEFT OUTER JOIN MGSYMCabang MCabang ON (TRLPenjualan.IdMCabang = MCabang.IdMCabang)
    WHERE (MCabang.Hapus = 0)
    AND (MCabang.KdMCabang LIKE '%%')
    AND (MCabang.NmMCabang LIKE '%%')
    ${qcabang}
    ORDER BY KdMCabang, TglTrans, BuktiTrans`;

    return sql;
}

exports.queryProgressLabaRugi = async (companyid, periode) => { 
    var sql = ``;

    if (companyid == companyWI) { 
        
    }
    sql = `SELECT IF(((MPrk.JenisMPrkD >= 7 AND MPrk.JenisMPrkD <= 9) OR (MPrk.JenisMPrkD = 14)),
        0,
        IF((MPrk.JenisMPrkD >= 10 AND MPrk.JenisMPrkD <= 11),
            1,
            IF((MPrk.JenisMPrkD >= 12 AND MPrk.JenisMPrkD <= 13),
                2,
                -1
    ))) as IdTitle
    , IF(((MPrk.JenisMPrkD >= 7 AND MPrk.JenisMPrkD <= 9) OR (MPrk.JenisMPrkD = 14)),
        'Laba Kotor',
        IF((MPrk.JenisMPrkD >= 10 AND MPrk.JenisMPrkD <= 11),
            'Laba Usaha',
            IF((MPrk.JenisMPrkD >= 12 AND MPrk.JenisMPrkD <= 13),
                'Laba Bersih',
                ''
    ))) as Footer
    , IF((MPrk.JenisMPrkD >= 7 AND MPrk.JenisMPrkD <= 8),
        0,
        IF(((MPrk.JenisMPrkD >= 9 AND MPrk.JenisMPrkD <= 9) OR (MPrk.JenisMPrkD = 14)),
            1,
            IF((MPrk.JenisMPrkD >= 10 AND MPrk.JenisMPrkD <= 10),
                2,
                IF((MPrk.JenisMPrkD >= 11 AND MPrk.JenisMPrkD <= 11),
                    3,
                    IF((MPrk.JenisMPrkD >= 12 AND MPrk.JenisMPrkD <= 12),
                        4,
                        IF((MPrk.JenisMPrkD >= 13 AND MPrk.JenisMPrkD <= 13),
                            5,
                            -1
    )))))) as IdSubTitle
    , IF((MPrk.JenisMPrkD >= 7 AND MPrk.JenisMPrkD <= 8),
        'Penjualan',
        IF(((MPrk.JenisMPrkD >= 9 AND MPrk.JenisMPrkD <= 9) OR (MPrk.JenisMPrkD = 14)),
            'HPP',
            IF((MPrk.JenisMPrkD >= 10 AND MPrk.JenisMPrkD <= 10),
                'Biaya Penjualan',
                IF((MPrk.JenisMPrkD >= 11 AND MPrk.JenisMPrkD <= 11),
                    'Biaya Administrasi',
                    IF((MPrk.JenisMPrkD >= 12 AND MPrk.JenisMPrkD <= 12),
                        'Pendapatan Lain - lain',
                        IF((MPrk.JenisMPrkD >= 13 AND MPrk.JenisMPrkD <= 13),
                            'Biaya Lain - lain',
                            ''
    )))))) as Header
    , MPrk.IdMPrk
    , MPrk.KdMPrk
    , MPrk.NmMPrk
    , (SELECT Coalesce(Sum(JmlK - JmlD), 0) FROM MGGLLJurnalHrn WHERE JenisTrans <> 'TTB' AND JenisTrans <> 'TTH' AND Periode = MPrk.Periode-3  AND IdMPrk = MPrk.IdMPrk) as MutasiTigaBlnLalu
    , (SELECT Coalesce(Sum(JmlK - JmlD), 0) FROM MGGLLJurnalHrn WHERE JenisTrans <> 'TTB' AND JenisTrans <> 'TTH' AND Periode = MPrk.Periode-2  AND IdMPrk = MPrk.IdMPrk) as MutasiDuaBlnLalu
    , (SELECT Coalesce(Sum(JmlK - JmlD), 0) FROM MGGLLJurnalHrn WHERE JenisTrans <> 'TTB' AND JenisTrans <> 'TTH' AND Periode = MPrk.Periode-1  AND IdMPrk = MPrk.IdMPrk) as MutasiSatuBlnLalu
    , (SELECT Coalesce(Sum(JmlK - JmlD), 0) FROM MGGLLJurnalHrn WHERE JenisTrans <> 'TTB' AND JenisTrans <> 'TTH' AND Periode < MPrk.Periode AND (Extract(Year from TglTrans) <= Extract(Year from Posting.Tgl)-1) AND IdMPrk = MPrk.IdMPrk) as MutasiSDBlnLalu
    , (SELECT Coalesce(Sum(JmlK - JmlD), 0) FROM MGGLLJurnalHrn WHERE JenisTrans <> 'TTB' AND JenisTrans <> 'TTH' AND Periode = MPrk.Periode AND IdMPrk = MPrk.IdMPrk) as MutasiBlnIni
    , (SELECT Coalesce(Sum(JmlK - JmlD), 0) FROM MGGLLJurnalHrn WHERE JenisTrans <> 'TTB' AND JenisTrans <> 'TTH' AND Periode <= MPrk.Periode AND (Extract(Year from TglTrans) = Extract(Year from Posting.Tgl)) AND IdMPrk = MPrk.IdMPrk) as MutasiSDBlnIni
    , MPrk.ParentId
    , IF((SELECT Count(IdMPrk) FROM MGGLMPrk WHERE Periode = MPrk.Periode AND ParentId = MPrk.IdMPrk AND Hapus = 0) > 0, 1, 0) as IsParent
    , MPrk.JenisMPrkD
    , MPrk.Periode
    , 0 as IsGroup
    , 1 as ShowValue
    , MPrk.LevelNumber
    FROM MGGLMPrk MPrk  left outer join MGGLPosting Posting on (MPrk.Periode = Posting.Periode)
    WHERE MPrk.Hapus = 0
    AND MPrk.IsDefault = 0
    AND MPrk.Periode = ${periode}
    AND MPrk.JenisMPrkD >= 7
    AND MPrk.JenisMPrkD <= 14
    ORDER BY IdTitle, IdSubTitle, JenisMPrkD, OrderByAll, KdMPrk`;

    return sql;
}

exports.queryRugiLaba = async (companyid, periode) => { 
    var sql = ``;

    sql = `SELECT IF(((MPrk.JenisMPrkD >= 7 AND MPrk.JenisMPrkD <= 9) OR (MPrk.JenisMPrkD = 14)),
        0,
        IF((MPrk.JenisMPrkD >= 10 AND MPrk.JenisMPrkD <= 11),
            1,
            IF((MPrk.JenisMPrkD >= 12 AND MPrk.JenisMPrkD <= 13),
                12,
                -1
    ))) as IdTitle
    , IF(((MPrk.JenisMPrkD >= 7 AND MPrk.JenisMPrkD <= 9) OR (MPrk.JenisMPrkD = 14)),
        'Laba Kotor',           IF((MPrk.JenisMPrkD >= 10 AND MPrk.JenisMPrkD <= 11),
            'Laba Usaha',
            IF((MPrk.JenisMPrkD >= 12 AND MPrk.JenisMPrkD <= 13),
                'Laba Bersih',                   ''
    ))) as Footer
    , IF((MPrk.JenisMPrkD >= 7 AND MPrk.JenisMPrkD <= 8),
        0,
        IF(((MPrk.JenisMPrkD >= 9 AND MPrk.JenisMPrkD <= 9) OR (MPrk.JenisMPrkD = 14)),
            1,
            IF((MPrk.JenisMPrkD >= 10 AND MPrk.JenisMPrkD <= 10),
                2,
                IF((MPrk.JenisMPrkD >= 11 AND MPrk.JenisMPrkD <= 11),
                    3,
                    IF((MPrk.JenisMPrkD >= 12 AND MPrk.JenisMPrkD <= 12),
                        4,
                        IF((MPrk.JenisMPrkD >= 13 AND MPrk.JenisMPrkD <= 13),
                            12,
                            -1
    )))))) as IdSubTitle
    , IF((MPrk.JenisMPrkD >= 7 AND MPrk.JenisMPrkD <= 8),
        'Penjualan',
        IF(((MPrk.JenisMPrkD >= 9 AND MPrk.JenisMPrkD <= 9) OR (MPrk.JenisMPrkD = 14)),
            'HPP',
            IF((MPrk.JenisMPrkD >= 10 AND MPrk.JenisMPrkD <= 10),
                'Biaya Penjualan',
                IF((MPrk.JenisMPrkD >= 11 AND MPrk.JenisMPrkD <= 11),
                    'Biaya Administrasi',
                    IF((MPrk.JenisMPrkD >= 12 AND MPrk.JenisMPrkD <= 12),
                        'Pendapatan Lain - lain',
                        IF((MPrk.JenisMPrkD >= 13 AND MPrk.JenisMPrkD <= 13),
                            'Biaya Lain - lain',
                            ''
    )))))) as Header
    , MPrk.IdMPrk
    , MPrk.KdMPrk
    , MPrk.NmMPrk
    , Coalesce(MBlnLalu.Selisih, 0) as MutasiSDBlnLalu
    , Coalesce(MBlnIni.Selisih, 0) as MutasiBlnIni
    , Coalesce(MBlnSemua.Selisih, 0) as MutasiSDBlnIni
    , MPrk.ParentId
    , IF((SELECT Count(IdMPrk) FROM MGGLMPrk WHERE Periode = MPrk.Periode AND ParentId = MPrk.IdMPrk AND Hapus = 0) > 0, 1, 0) as IsParent
    , MPrk.JenisMPrkD
    , MPrk.Periode
    , 0 as IsGroup
    , 1 as ShowValue
    , MPrk.LevelNumber
    FROM MGGLMPrk MPrk  left outer join MGGLPosting Posting on (MPrk.Periode = Posting.Periode)
    LEFT OUTER JOIN MGGLLabaRugiMutasiPeriode MBlnLalu ON (MBlnLalu.IdMPrk = MPrk.IdMPrk AND MBlnLalu.Periode = MPrk.Periode AND MBlnLalu.Jenis = 0)
    LEFT OUTER JOIN MGGLLabaRugiMutasiPeriode MBlnIni ON (MBlnIni.IdMPrk = MPrk.IdMPrk AND MBlnIni.Periode = MPrk.Periode AND MBlnIni.Jenis = 1)
    LEFT OUTER JOIN MGGLLabaRugiMutasiPeriode MBlnSemua ON (MBlnSemua.IdMPrk = MPrk.IdMPrk AND MBlnSemua.Periode = MPrk.Periode AND MBlnSemua.Jenis = 2)
    WHERE MPrk.Hapus = 0
    AND MPrk.IsDefault = 0
    AND MPrk.Periode = :paramPeriode
    AND MPrk.JenisMPrkD >= 7
    AND MPrk.JenisMPrkD <= 14
    ORDER BY IdTitle, IdSubTitle, JenisMPrkD, OrderByAll, KdMPrk
`

    return sql;
}