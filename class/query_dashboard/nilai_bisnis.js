const fun = require("../../mgmx");
var companyWI = fun.companyWI;

// UNTUK NILAI DAN STOCK BARANG JADI SATU
exports.queryBarang = async (companyid, tanggal) => { 
    const q = require("../query_report/stock");
    var sql = ``;
    // WI
    var query = await q.queryPosisiStock(companyid, tanggal);
    sql = `SELECT SUM(PosValue) as total, SUM(PosQty) as total_stock FROM (${query}) tbl`;

    return sql;
}


exports.queryPiutang = async (companyid, tanggal) => { 
    const q = require("../query_report/piutang");
    var sql = ``;
    
    var query = await q.queryPosisiPiutang(companyid, tanggal);
    sql = `SELECT SUM(PosPiut) as total FROM (${query}) tbl`;
    

    return sql;
}

exports.queryHutang = async (companyid, tanggal) => { 
    const q = require("../query_report/hutang");
    var sql = ``;
    
    var query = await q.queryPosisiHutang(companyid, tanggal);
    sql = `SELECT SUM(PosHut) as total FROM (${query}) tbl`;

    return sql;
}

exports.queryKas = async (companyid, tanggal) => { 
    const q = require("../query_report/kas");
    var sql = ``;
    
    var query = await q.queryPosisiKas(companyid, tanggal);
    sql = `SELECT SUM(tbl.PosKas) as total FROM (${query}) tbl`;

    return sql;

    return sql;
}

exports.queryBank = async (companyid, tanggal) => { 
    const q = require("../query_report/bank");
    var sql = ``;
    
    var query = await q.queryPosisiBank(companyid, tanggal);
    sql = `SELECT SUM(tbl.PosRek) as total FROM (${query}) tbl`;

    return sql;

    return sql;
}