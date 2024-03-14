const fun = require("../../mgmx");
var companyWI = fun.companyWI;

exports.queryBarangTerlaku = async (companyid, start, end) => { 
    var sql = ``;

    sql = `SELECT b.nmmbrg as nama, SUM(jd.qtytotal) AS jumlah FROM mgartjuald jd LEFT OUTER JOIN mgartjual j ON j.idtjual = jd.idtjual LEFT OUTER JOIN mginmbrg b ON jd.idmbrg = b.idmbrg WHERE j.Hapus = 0 AND j.Void = 0 AND j.tgltjual >= '${start} 00:00:00' AND j.tgltjual <= '${end} 23:59:59' GROUP BY b.idmbrg ORDER BY SUM(jd.qtytotal) DESC LIMIT 10`;

    return sql;
}