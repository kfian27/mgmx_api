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
                - ((d.HrgStn - IF(ISNULL(d.DiscV), 0, d.DiscV)) * m.discP/100)) * PPNP/100)) AS HrgStn
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

exports.queryProgressLabaRugi = async (tanggal) => { 
    var sql = ``;

    return sql;
}

exports.queryRugiLaba = async (tanggal) => { 
    var sql = ``;

    return sql;
}