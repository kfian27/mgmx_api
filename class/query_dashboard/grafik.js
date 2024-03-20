const fun = require("../../mgmx");
var companyWI = fun.companyWI;

exports.queryPenjualan = async (companyid, start, end) => { 
    var sql = ``;
    // WI
    if (companyid == companyWI) {

    }

    sql = `select TglTJual as Tanggal, SUM(TJual.Netto + Coalesce(TJualLain.Netto, 0)) AS jumlah 
    FROM mgartjual TJual
    LEFT OUTER JOIN MGARTJualLain TJualLain ON (TJual.IdMCabang = TJualLain.IdMCabang AND TJual.BuktiTJual = TJualLain.BuktiAsli AND TJualLain.Hapus = 0 AND TJualLain.Void = 0)
    where TJual.Hapus = 0 AND TJual.Void = 0 AND TJual.TglTJual >= '${start} 00:00:00' AND TJual.TglTJual <= '${end} 23:59:59' 
    AND TJual.AprtBulan <> 1
    AND TJual.IdTRJual = 0
    AND TJual.AprtBulan <> 2
    group by TglTJual`


    return sql;
}

exports.queryPembelian = async (companyid, start, end) => { 
    var sql = ``;
    // WI
    if (companyid == companyWI) {

    }

    // sql = `select TglTBeli as Tanggal, SUM(Netto) AS jumlah FROM mgaptbeli where Hapus = 0 AND Void = 0 AND TglTBeli >= '${start} 00:00:00' AND TglTBeli <= '${end} 23:59:59' group by TglCreate`
    sql = `select TBeli.TglTBeli as Tanggal, IdTBeli, SUM(Netto) AS jumlah FROM mgaptbeli TBeli
    LEFT OUTER JOIN mgapmsup msup on TBeli.IdMSup = msup.IdMSup 
    WHERE TBeli.Hapus = 0 AND TBeli.Void = 0 AND TBeli.TglTBeli  >= '${start} 00:00:00' AND TBeli.TglTBeli <= '${end} 23:59:59' 
    AND TBeli.BuktiTBeli <> '' AND MSup.JenisMSup = 0 AND TBeli.IdMUserCreate > 0 
    group by TBeli.TglTBeli`

    return sql;
}

exports.queryReturJual = async (companyid, start, end) => { 
    var sql = ``;
    // WI
    if (companyid == companyWI) {

    }
    sql = `select TglTRJual as Tanggal, SUM(Netto) AS jumlah FROM mgartrjual where Hapus = 0 AND Void = 0 AND TglTRJual >= '${start} 00:00:00' AND TglTRJual <= '${end} 23:59:59' group by TglTRJual`


    return sql;
}

exports.queryReturBeli = async (companyid, start, end) => { 
    var sql = ``;
    // WI
    if (companyid == companyWI) {

    }
    sql = `select TglTRBeli as Tanggal, SUM(Netto) AS jumlah FROM mgaptrbeli where Hapus = 0 AND Void = 0 AND TglTRBeli >= '${start} 00:00:00' AND TglTRBeli <= '${end} 23:59:59' group by TglTRBeli`

    return sql;
}

exports.queryKasMasuk = async (companyid, start, end) => { 
    var sql = ``;
    // WI
    if (companyid == companyWI) {

    }
    sql = `select TGLTTRANSFER as Tanggal, SUM(TOTAL) AS jumlah FROM mgkbttransfer where JENISMREF = 'K' AND JENISTTRANSFER = 'M' AND Hapus = 0 AND Void = 0 AND TGLTTRANSFER >= '${start} 00:00:00' AND TGLTTRANSFER <= '${end} 23:59:59' group by TGLTTRANSFER`


    return sql;
}

exports.queryKasKeluar = async (companyid, start, end) => { 
    var sql = ``;
    // WI
    if (companyid == companyWI) {

    }
    sql = `select TGLTTRANSFER as Tanggal, SUM(TOTAL) AS jumlah FROM mgkbttransfer where JENISMREF = 'K' AND JENISTTRANSFER = 'K' AND Hapus = 0 AND Void = 0 AND TGLTTRANSFER >= '${start} 00:00:00' AND TGLTTRANSFER <= '${end} 23:59:59' group by TGLTTRANSFER`


    return sql;
}

exports.queryBankMasuk = async (companyid, start, end) => { 
    var sql = ``;
    // WI
    if (companyid == companyWI) {

    }
    sql = `select TGLTTRANSFER as Tanggal, SUM(TOTAL) AS jumlah FROM mgkbttransfer where JENISMREF = 'B' AND JENISTTRANSFER = 'M' AND Hapus = 0 AND Void = 0 AND TGLTTRANSFER >= '${start} 00:00:00' AND TGLTTRANSFER <= '${end} 23:59:59' group by TGLTTRANSFER`


    return sql;
}

exports.queryBankKeluar = async (companyid, start, end) => { 
    var sql = ``;
    // WI
    if (companyid == companyWI) {

    }
    sql = `select TGLTTRANSFER as Tanggal, SUM(TOTAL) AS jumlah FROM mgkbttransfer where JENISMREF = 'B' AND JENISTTRANSFER = 'K' AND Hapus = 0 AND Void = 0 AND TGLTTRANSFER >= '${start} 00:00:00' AND TGLTTRANSFER <= '${end} 23:59:59' group by TGLTTRANSFER`

    return sql;
}

exports.queryHutang = async (companyid, start, end) => { 
    var sql = ``;
    // WI
    if (companyid == companyWI) {

    }

    sql = `select TglTBHut as Tanggal, SUM(Total) AS jumlah FROM mgaptbhut where Hapus = 0 AND Void = 0 AND TglTBHut >= '${start} 00:00:00' AND TglTBHut <= '${end} 23:59:59' group by TglTBHut`


    return sql;
}

exports.queryPiutang = async (companyid, start, end) => { 
    var sql = ``;
    // WI
    if (companyid == companyWI) {

    }
    sql = `select TglTBPiut as Tanggal, SUM(Total) AS jumlah FROM mgaptbpiut where Hapus = 0 AND Void = 0 AND TglTBPiut >= '${start} 00:00:00' AND TglTBPiut <= '${end} 23:59:59' group by TglTBPiut`


    return sql;
}

exports.queryLabaRugi = async (companyid, start, end) => { 
    var sql = ``;
    // WI
    if (companyid == companyWI) {

    }

    return sql;
}



