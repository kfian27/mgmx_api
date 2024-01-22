const db = require("../models");
const sequelize = db.sequelize;

const formatNumberTwoComma = new Intl.NumberFormat('id-ID', {
    style: 'decimal',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
});

const formatNumberNoComma = new Intl.NumberFormat('id-ID', {
    style: 'decimal',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0
});

function formatNoComma(params = 0) {
    return formatNumberNoComma.format(params);
}

function formatTwoComma(params = 0) {
    return formatNumberTwoComma.format(params);
}

function formatNoCommaRp(params = 0) {
    return 'Rp' + formatNumberNoComma.format(params);
}

function formatTwoCommaRp(params) {
    return 'Rp' + formatNumberTwoComma.format(params);
}

let today = new Date().toJSON().slice(0, 10);

async function countDataFromQuery(query) {
    var count_data = await sequelize.query(
        query, {
            raw: false,
            plain: true
    })
    return count_data.total;
}

exports.getListCabang = async (req, res) => {
    let sql = `select IdMCabang as ID, NmMCabang as nama from mgsymcabang where aktif=1 and hapus=0`;
    const data = await sequelize.query(sql, {
        raw: false,
    });

    res.json({
        message: "Success",
        data: data[0]
    });
}

exports.getListCustomer = async (req, res) => {
    let sql = `select IdMCust as ID, NmMCust as nama from mgarmcust where aktif=1 and hapus=0`;
    const data = await sequelize.query(sql, {
        raw: false,
    });

    res.json({
        message: "Success",
        data: data[0]
    });
}

exports.getListSupplier = async (req, res) => {
    let sql = `select idmsup as ID, nmmsup as nama from mgapmsup where aktif=1 and hapus=0`;
    const data = await sequelize.query(sql, {
        raw: false,
    });

    res.json({
        message: "Success",
        data: data[0]
    });
}

exports.getListGudang = async (req, res) => {
    let sql = `select idmgd as ID, nmmgd as nama from mgsymgd where aktif=1 and hapus=0`;
    const data = await sequelize.query(sql, {
        raw: false,
    });

    res.json({
        message: "Success",
        data: data[0]
    });
}
exports.getListBarang = async (req, res) => {
    let sql = `SELECT b.idmbrg as ID, REPLACE(b.nmmbrg,'"','') as nama FROM mginlkartustock k LEFT OUTER JOIN mginmbrg b ON k.idmbrg = b.idmbrg GROUP BY b.idmbrg`;
    const data = await sequelize.query(sql, {
        raw: false,
    });

    res.json({
        message: "Success",
        data: data[0]
    });
}


exports.penjualan = async (req, res) => {

    let start = req.body.start || '2008-01-17';
    let end = req.body.end || '2024-02-17';
    let jenis = req.body.jenis || 1;

    let cabang = req.body.cabang;
    let qcabang = "";
    if (cabang && cabang != "") {
        qcabang = "and j.idmcabang=" + cabang;
    }

    let customer = req.body.customer;
    let qcustomer = "";
    if (customer && customer != "") {
        qcustomer = "and j.idmcust=" + customer;
    }

    let barang = req.body.barang;
    let qbarang = "";
    if (barang && barang != "") {
        qbarang = "and jd.idmbrg=" + barang;
    }

    let group = req.body.group;


    const count_penjualan = await countDataFromQuery(
        `SELECT COUNT(j.idtjual) as total FROM mgartjual j LEFT OUTER JOIN mgartjuald jd ON jd.idtjual = j.idtjual WHERE j.hapus=0 ${qcabang} ${qcustomer} ${qbarang} and j.tgltjual between '${start}%' and '${end}%'`
    );
    const count_produk = await countDataFromQuery(
        `SELECT SUM(jd.qtytotal) as total FROM mgartjuald jd LEFT OUTER JOIN mgartjual j ON jd.IdTJual = j.IdTJual WHERE j.tgltjual between '${start}%' and '${end}%' ${qcabang} ${qbarang} ${qcustomer}`
    );
    const count_pendapatan = await countDataFromQuery(
        `SELECT SUM(j.netto) as total FROM mgartjual j WHERE j.tgltjual between '${start}%' and '${end}%' and j.hapus=0 ${qcabang} ${qcustomer} ${qbarang}`
    );
    const count_hpp = await countDataFromQuery(
        `SELECT COALESCE((SELECT (jd.QtyTotal * b.reserved_dec1) AS hpp FROM mgartjuald jd LEFT OUTER JOIN mgartjual j ON jd.IdTJual=j.IdTJual LEFT OUTER JOIN mginmbrg b ON jd.IdMBrg = b.idmbrg WHERE j.tgltjual between '${start}%' and '${end}%' ${qbarang} ${qcustomer} ${qbarang} order by jd.IdTJualD desc limit 1),0) as total`
    );

    var count = {
        penjualan: count_penjualan,
        produk_terjual: parseFloat(count_produk),
        pendapatan: parseFloat(count_pendapatan),
        profit: parseFloat(count_pendapatan) - parseFloat(count_hpp),
    };

    if (jenis == 1) {
        async function barang_list(list) {
            let sql2 = `SELECT jd.idtjuald, jd.idmbrg, jd.qtytotal, jd.hrgstn, jd.discv, jd.subtotal, b.kdmbrg, b.nmmbrg FROM mgartjuald jd LEFT OUTER JOIN mginmbrg b ON jd.idmbrg = b.idmbrg WHERE jd.idtjual = ${list.idtjual}`;
            const brg = await sequelize.query(sql2, {
                raw: false,
            });

            var arr_brg = brg[0].map((brg, index_dua) => {
                return {
                    'id': brg.idtjuald,
                    'barcode': brg.kdmbrg,
                    'nama': brg.nmmbrg,
                    'jumlah': brg.qtytotal,
                    'harga': parseFloat(brg.hrgstn), // format
                    'diskon': parseFloat(brg.discv), // format
                    'total': parseFloat(brg.subtotal), // format
                }
            })

            return {
                "id": list.idtjual,
                "tanggal": list.tgltjual, // date_format(date_create(list.tgltjual), "m - d - Y"),
                "transaksi": list.buktitjual,
                "customer": list.nmmcust,
                "subtotal": parseFloat(list.bruto), // "Rp & nbsp; ".number_format(list.bruto, 2),
                "diskon": parseFloat(list.discv), // "Rp & nbsp; ".number_format(list.discv, 2),
                "pajak": parseFloat(list.ppnv), // "Rp & nbsp; ".number_format(list.ppnv, 2),
                "grandtotal": parseFloat(list.netto), // "Rp & nbsp; ".number_format(list.netto, 2),
                "bayar": parseFloat(list.bayar), // "Rp & nbsp; ".number_format(list.bayar, 2),
                "sisa": parseFloat(list.sisa), // number_format(list.sisa),
                "sisabayar": parseFloat(list.sisa), // "Rp ".number_format(abs(list.sisa), 2),
                "listitem": arr_brg
            }
        }

        if (group == "cabang") {
            let sql = `SELECT fin.idmcust, fin.nmmcust, fin.idmcabang, fin.nmmcabang, SUM(fin.tagihan) AS 'tagihan', SUM(fin.bayar) AS 'bayar', SUM(fin.sisa) AS 'sisa' FROM (SELECT ca.idmcabang, ca.nmmcabang, s.nmmsales, j.tgltjual AS 'Tanggal', j.buktitjual AS 'Nota', c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qcabang} ${qbarang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC) fin group by fin.idmcabang`;
            const filter = await sequelize.query(sql, {
                raw: false,
            });

            var arr_data = await Promise.all(filter[0].map(async (fil, index) => {
                let sql1 = `SELECT ca.nmmcabang,j. idtjual, j.tgltjual, j.buktitjual, c.nmmcust, s.nmmsales, j.bruto, j.discv, j.ppnv, j.netto, SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qbarang} ${qcabang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC`;
                const list = await sequelize.query(sql1, {
                    raw: false,
                });

                var arr_list = await Promise.all(list[0].map(async (list, index_satu) => {
                    return barang_list(list);
                }))

                return {
                    "nama": "Cabang : " + fil.nmmcabang,
                    "netto": parseFloat(fil.tagihan), // "Rp&nbsp;"+ number_format(fil.tagihan, 2),
                    "sisa": parseFloat(Math.abs(fil.sisa)), // "Rp&nbsp;"+ number_format(abs(fil.sisa), 2),
                    "bayar": parseFloat(fil.bayar), // "Rp&nbsp;"+ number_format(fil.bayar, 2),
                    "list": arr_list
                }
            }))

        } else if (group == "customer") {
            let sql = `SELECT fin.idmcust, fin.nmmcust, fin.idmcabang, fin.nmmcabang, SUM(fin.tagihan) AS 'tagihan', SUM(fin.bayar) AS 'bayar', SUM(fin.sisa) AS 'sisa' FROM (SELECT ca.idmcabang, ca.nmmcabang, s.nmmsales, j.tgltjual AS 'Tanggal', j.buktitjual AS 'Nota', c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qcabang} ${qbarang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC) fin GROUP BY fin.idmcust`;
            const filter = await sequelize.query(sql, {
                raw: false,
            });

            var arr_data = await Promise.all(filter[0].map(async (fil, index) => {
                let idmcust = fil.idmcust;
                let sql1 = `SELECT ca.nmmcabang,j. idtjual, j.tgltjual, j.buktitjual, c.idmcust, c.nmmcust, s.nmmsales, j.bruto, j.discv, j.ppnv, j.netto, SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qbarang} ${qcabang} ${qcustomer} and c.idmcust=${idmcust} GROUP BY j.idtjual ORDER BY j.tgltjual ASC`;
                const list = await sequelize.query(sql1, {
                    raw: false,
                });

                var arr_list = await Promise.all(list[0].map(async (list, index_satu) => {
                    var list_item = await barang_list(list);
                    list_item.sales = list.nmmsales;
                    return list_item;

                }))

                return {
                    "nama": "Customer : " + fil.nmmcust,
                    "netto": parseFloat(fil.tagihan), // "Rp&nbsp;"+ number_format(fil.tagihan, 2),
                    "sisa": parseFloat(Math.abs(fil.sisa)), // "Rp&nbsp;"+ number_format(abs(fil.sisa), 2),
                    "bayar": parseFloat(fil.bayar), // "Rp&nbsp;"+ number_format(fil.bayar, 2),
                    "list": arr_list
                }
            }))

        } else if (group == "sales") {
            let sql = `SELECT fin.idmsales, fin.nmmsales, fin.idmcust, fin.nmmcust, fin.idmcabang, fin.nmmcabang, SUM(fin.tagihan) AS 'tagihan', SUM(fin.bayar) AS 'bayar', SUM(fin.sisa) AS 'sisa' FROM (SELECT ca.idmcabang, ca.nmmcabang, s.idmsales, s.nmmsales, j.tgltjual AS 'Tanggal', j.buktitjual AS 'Nota', c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE s.aktif=1 and s.hapus=0 and j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qcabang} ${qbarang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC) fin GROUP BY fin.idmsales`;
            const filter = await sequelize.query(sql, {
                raw: false,
            });

            var arr_data = await Promise.all(filter[0].map(async (fil, index) => {
                let sql1 = `SELECT ca.idmcabang, ca.nmmcabang, s.idmsales, s.nmmsales, j.idtjual, j.tgltjual AS 'Tanggal', j.buktitjual AS 'Nota', c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE s.aktif=1 and s.hapus=0 and j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qcabang} ${qbarang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC`;
                const list = await sequelize.query(sql1, {
                    raw: false,
                });

                var arr_list = await Promise.all(list[0].map(async (list, index_satu) => {
                    var list_item = await barang_list(list);
                    list_item.sales = list.nmmsales;
                    return list_item;

                }))

                return {
                    "nama": "Sales : " + fil.nmmsales,
                    "netto": parseFloat(fil.tagihan), // "Rp&nbsp;"+ number_format(fil.tagihan, 2),
                    "sisa": parseFloat(Math.abs(fil.sisa)), // "Rp&nbsp;"+ number_format(abs(fil.sisa), 2),
                    "bayar": parseFloat(fil.bayar), // "Rp&nbsp;"+ number_format(fil.bayar, 2),
                    "list": arr_list
                }
            }))
        } else if (group == "barang") {
            let sql = `SELECT fin.idmbrg, fin.nmmbrg, fin.idmcust, fin.nmmcust, fin.idmcabang, fin.nmmcabang, SUM(fin.tagihan) AS 'tagihan', SUM(fin.bayar) AS 'bayar', SUM(fin.sisa) AS 'sisa' FROM (SELECT b.idmbrg, b.nmmbrg, ca.idmcabang, ca.nmmcabang, s.nmmsales, j.tgltjual AS 'Tanggal', j.buktitjual AS 'Nota', c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'bayar',IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang LEFT OUTER JOIN mgartjuald jd ON j.idtjual = jd.IdTJual LEFT OUTER JOIN mginmbrg b ON jd.idmbrg = b.idmbrg WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qcabang} ${qbarang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC) fin GROUP BY fin.idmbrg`;
            const filter = await sequelize.query(sql, {
                raw: false,
            });

            var arr_data = await Promise.all(filter[0].map(async (fil, index) => {
                let sql1 = `SELECT j.idtjual, j.tgltjual, j.buktitjual, j.bruto, j.discv, j.ppnv, j.netto, b.idmbrg, b.nmmbrg, ca.idmcabang, ca.nmmcabang, s.nmmsales, j.tgltjual AS 'Tanggal', j.buktitjual AS 'Nota', c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'bayar',IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang LEFT OUTER JOIN mgartjuald jd ON j.idtjual = jd.IdTJual LEFT OUTER JOIN mginmbrg b ON jd.idmbrg = b.idmbrg WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qcabang} ${qbarang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC`;
                const list = await sequelize.query(sql1, {
                    raw: false,
                });

                var arr_list = await Promise.all(list[0].map(async (list, index_satu) => {
                    var list_item = await barang_list(list);
                    list_item.sales = list.nmmsales;
                    return list_item;
                }))

                return {
                    "nama": "Barang : " + fil.nmmbrg,
                    "netto": parseFloat(fil.tagihan), // "Rp&nbsp;"+ number_format(fil.tagihan, 2),
                    "sisa": parseFloat(Math.abs(fil.sisa)), // "Rp&nbsp;"+ number_format(abs(fil.sisa), 2),
                    "bayar": parseFloat(fil.bayar), // "Rp&nbsp;"+ number_format(fil.bayar, 2),
                    "list": arr_list
                }
            }))

        } else {
            var count = {};
            var arr_data = [];
        }
    } else if (jenis == 2) {
        async function barang_list(list) {
            let sql2 = `SELECT b.kdmbrg, b.nmmbrg, g.nmmgd, jd.qtytotal, s.nmmstn, jd.hrgstn, jd.discv, jd.ppnv, (jd.qtytotal * jd.hrgstn) AS dpp, (jd.qtytotal * jd.hrgstn - jd.discv + jd.ppnv) AS subtotal FROM mgartjuald jd LEFT OUTER JOIN mginmbrg b ON b.idmbrg = jd.idmbrg LEFT OUTER JOIN mginmstn s ON b.IdMStn1 = s.idmstn LEFT OUTER JOIN mgsymgd g ON g.idmgd = jd.idmgd WHERE idtjual =  ${list.idtjual}`;
            const brg = await sequelize.query(sql2, {
                raw: false,
            });

            var arr_brg = brg[0].map((brg, index_dua) => {
                return {
                    "kode": brg.kdmbrg,
                    "nama": brg.nmmbrg,
                    "gudang": brg.nmmgd,
                    "qty": brg.qtytotal,
                    "satuan": brg.nmmstn,
                    "hargasat": parseFloat(brg.hrgstn),
                    "diskon": parseFloat(brg.discv),
                    "pajak": parseFloat(brg.ppnv),
                    "dpp": parseFloat(brg.dpp),
                    "subtotal": parseFloat(brg.subtotal)
                }
            })

            return {
                "id" : list.idtjual, 
                "tanggal" : list.tgltjual, 
                "transaksi" : list.buktitjual,
                "customer" : list.nmmcust, 
                "subtotal" : parseFloat(list.bruto), 
                "diskon" : parseFloat(list.discv), 
                "pajak" : parseFloat(list.ppnv), 
                "grandtotal" : parseFloat(list.netto),
                "sisa" : parseFloat(list.sisa), 
                "item" : arr_brg, 
                
            }
        }
        
        if (group == "cabang") {
            let sql = `select nmmcabang from mgsymcabang where aktif = 1 and hapus = 0`;
            const filter = await sequelize.query(sql, {
                raw: false,
            });

            var arr_data = await Promise.all(filter[0].map(async (fil, index) => {
                let sql1 = `SELECT j.idtjual, j.tgltjual, j.buktitjual, c.nmmcust, j.bruto, j.discv, j.ppnv, j.netto, SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qbarang} ${qcabang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC`;
                const list = await sequelize.query(sql1, {
                    raw: false,
                });

                var arr_list = await Promise.all(list[0].map(async (list, index_satu) => {
                    return barang_list(list);
                }))

                return {
                    "nama": fil.nmmcabang,
                    "list": arr_list
                }
            }))
        }
        else if (group == "customer") {
            let sql = `SELECT fin.idmcust, fin.nmmcust, fin.idmcabang, fin.nmmcabang, SUM(fin.tagihan) AS 'Tagihan', SUM(fin.bayar) AS 'Bayar', SUM(fin.sisa) AS 'Sisa' FROM (SELECT ca.idmcabang, ca.nmmcabang, s.nmmsales, j.tgltjual AS 'Tanggal', j.buktitjual AS 'Nota', c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'Tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'Bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'Sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qcabang} ${qbarang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC) fin GROUP BY fin.idmcust`;
            const filter = await sequelize.query(sql, {
                raw: false,
            });

            var arr_data = await Promise.all(filter[0].map(async (fil, index) => {
                let idmcust = fil.idmcust;
                let sql1 = `SELECT ca.nmmcabang,j. idtjual, j.tgltjual, j.buktitjual, c.idmcust, c.nmmcust, s.nmmsales, j.bruto, j.discv, j.ppnv, j.netto, SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'Bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'Sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qbarang} ${qcabang} ${qcustomer} and c.idmcust=${idmcust} GROUP BY j.idtjual ORDER BY j.tgltjual ASC`;
                const list = await sequelize.query(sql1, {
                    raw: false,
                });

                var arr_list = await Promise.all(list[0].map(async (list, index_satu) => {
                    return barang_list(list);
                }))

                return {
                    "nama": fil.nmmcust,
                    "list": arr_list
                }
            }))
        }
        else if (group == "sales") {
            let sql = `SELECT fin.idmsales, fin.nmmsales, fin.idmcust, fin.nmmcust, fin.idmcabang, fin.nmmcabang, SUM(fin.tagihan) AS 'Tagihan', SUM(fin.bayar) AS 'Bayar', SUM(fin.sisa) AS 'Sisa' FROM (SELECT ca.idmcabang, ca.nmmcabang, s.idmsales, s.nmmsales, j.tgltjual AS 'Tanggal', j.buktitjual AS 'Nota', c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'Tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'Bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'Sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE s.aktif=1 and s.hapus=0 and j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qcabang} ${qbarang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC) fin GROUP BY fin.idmsales`;
            const filter = await sequelize.query(sql, {
                raw: false,
            });

            var arr_data = await Promise.all(filter[0].map(async (fil, index) => {
                let sql1 = `SELECT j.idtjual, j.bruto, j.discv, j.ppnv, j.netto, ca.idmcabang, ca.nmmcabang, s.idmsales, s.nmmsales, j.tgltjual, j.buktitjual, c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'Tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'Bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'Sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE s.aktif=1 and s.hapus=0 and j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qcabang} ${qbarang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC`;
                const list = await sequelize.query(sql1, {
                    raw: false,
                });

                var arr_list = await Promise.all(list[0].map(async (list, index_satu) => {
                    return barang_list(list);
                }))

                return {
                    "nama": fil.nmmsales,
                    "list": arr_list
                }
            }))
        }
        else if (group == "barang") {
            let sql = `SELECT fin.idmbrg, fin.nmmbrg, fin.idmcust, fin.nmmcust, fin.idmcabang, fin.nmmcabang, SUM(fin.tagihan) AS 'Tagihan', SUM(fin.bayar) AS 'bayar', SUM(fin.sisa) AS 'Sisa' FROM (SELECT b.idmbrg, b.nmmbrg, ca.idmcabang, ca.nmmcabang, s.nmmsales, j.tgltjual AS 'Tanggal', j.buktitjual AS 'nota', c.idmcust, c.nmmcust, j.bruto AS 'subtotal', j.discv AS 'diskon', j.ppnv AS 'pajak', j.netto AS 'tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'Bayar',IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang LEFT OUTER JOIN mgartjuald jd ON j.idtjual = jd.IdTJual LEFT OUTER JOIN mginmbrg b ON jd.idmbrg = b.idmbrg WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${qcabang} ${qbarang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC) fin GROUP BY fin.idmbrg`;
            const filter = await sequelize.query(sql, {
                raw: false,
            });

            var arr_data = await Promise.all(filter[0].map(async (fil, index) => {
                let idmbrg = fil.idmbrg;
                let sql1 = `SELECT j.idtjual, j.tgltjual, j.buktitjual, j.bruto, j.discv, j.ppnv, j.netto, b.idmbrg, b.nmmbrg, ca.idmcabang, ca.nmmcabang, s.nmmsales, j.tgltjual AS 'tanggal', j.buktitjual AS 'nota', c.idmcust, c.nmmcust, j.bruto AS 'subtotal', j.discv AS 'diskon', j.ppnv AS 'pajak', j.netto AS 'tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'bayar',IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang LEFT OUTER JOIN mgartjuald jd ON j.idtjual = jd.IdTJual LEFT OUTER JOIN mginmbrg b ON jd.idmbrg = b.idmbrg WHERE j.hapus=0 AND b.idmbrg = ${idmbrg} and j.tgltjual BETWEEN '${start}' AND '${end}' ${qcabang} ${qbarang} ${qcustomer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC`;
                const list = await sequelize.query(sql1, {
                    raw: false,
                });

                var arr_list = await Promise.all(list[0].map(async (list, index_satu) => {
                    let list_item = await barang_list(list);
                    list_item.bayar = parseFloat(list.bayar);
                    return list_item;
                    
                }))

                return {
                    "nama": fil.nmmbrg,
                    "list": arr_list
                }
            }))
        } else {
            var count = {};
            var arr_data = [];
        }
    }
    res.json({
        message: "Success",
        countData: count,
        data: arr_data
    });
};

exports.pembelian = async (req, res) => {

    let start = req.body.start || '2008-01-17';
    let end = req.body.end || '2024-02-17';
    let jenis = req.body.jenis || 1;

    let cabang = req.body.cabang;
    let qcabang = "";
    if (cabang && cabang != "") {
        qcabang = "and b.idmcabang=" + cabang;
    }

    let supplier = req.body.supplier;
    let qsupplier = "";
    if (supplier && supplier != "") {
        qsupplier = "and b.idmsup=" + supplier;
    }

    let barang = req.body.barang;
    let qbarang = "";
    if (barang && barang != "") {
        qbarang = "and bb.idmbrg=" + barang;
    }

    let group = req.body.group;

    const count_pembelian = await countDataFromQuery(
        `SELECT COUNT(j.idtbeli) as total FROM mgaptbeli j LEFT OUTER JOIN mgaptbelid jd ON jd.idtbeli = j.idtbeli WHERE j.hapus=0 ${qcabang} ${qsupplier} ${qbarang} and j.tgltbeli between '${start}%' and '${end}%'`
    );
    const count_produk = await countDataFromQuery(
        `SELECT count(jd.idmbrg) as total FROM mgaptbelid jd LEFT OUTER JOIN mgaptbeli j ON jd.Idtbeli = j.Idtbeli WHERE j.tgltbeli between '${start}%' and '${end}%' ${qcabang} ${qbarang} ${qsupplier}`
    );
    const count_pengeluaran = await countDataFromQuery(
        `SELECT COALESCE((SELECT SUM(j.netto) as total FROM mgaptbeli j left outer join mgaptbelid jd on jd.idtbeli = j.idtbeli WHERE tgltbeli between '${start}%' and '${end}%' ${qcabang} ${qbarang} ${qsupplier} order by j.idtbeli desc limit 1),0) as total`
    );

    var count = {
        pembelian: count_pembelian,
        produk_dibeli: parseFloat(count_produk),
        pengeluaran: parseFloat(count_pengeluaran),
    };

    var jenis_str = '';
    if (jenis == 1) {
        jenis_str = 'summary';
        async function barang_list(list) {
            let sql2 = `SELECT jd.idtbelid, jd.idmbrg, jd.qtytotal, jd.hrgstn, jd.discv, jd.subtotal, b.kdmbrg, b.nmmbrg FROM mgaptbelid jd LEFT OUTER JOIN mginmbrg b ON jd.idmbrg = b.idmbrg WHERE jd.idtbeli = ${list.idtbeli}`;
            const brg = await sequelize.query(sql2, {
                raw: false,
            });

            var arr_brg = brg[0].map((brg, index_dua) => {
                return {
                    'id': brg.idtbelid,
                    'barcode': brg.kdmbrg,
                    'nama': brg.nmmbrg,
                    'jumlah': parseFloat(brg.qtytotal),
                    'harga': parseFloat(brg.hrgstn), // format
                    'diskon': parseFloat(brg.discv), // format
                    'total': parseFloat(brg.subtotal), // format
                }
            })

            return {
                "id": list.idtbeli,
                "tanggal": list.tgltbeli, // date_format(date_create(list.tgltjual), "m - d - Y"),
                "transaksi": list.buktitbeli,
                "supplier": list.nmmsup,
                "subtotal": parseFloat(list.bruto), // "Rp & nbsp; ".number_format(list.bruto, 2),
                "diskon": parseFloat(list.discv), // "Rp & nbsp; ".number_format(list.discv, 2),
                "pajak": parseFloat(list.ppnv), // "Rp & nbsp; ".number_format(list.ppnv, 2),
                "grandtotal": parseFloat(list.netto), // "Rp & nbsp; ".number_format(list.netto, 2),
                "bayar": parseFloat(list.bayar), // "Rp & nbsp; ".number_format(list.bayar, 2),
                "sisa": parseFloat(list.sisa), // number_format(list.sisa),
                "listitem": arr_brg
            }
        }

        if (group == "cabang") {
            let sql = `SELECT fin.nmmsup, fin.idmcabang, fin.nmmcabang, SUM(fin.bruto) AS 'subtotal', SUM(fin.ppnv) AS 'pajak', SUM(fin.discv) AS 'diskon',SUM(fin.netto) AS 'tagihan', SUM(fin.bayar) AS 'bayar', SUM(fin.sisa) AS 'sisa' FROM (SELECT b.idmcabang, c.NmMCabang, b.tgltbeli, b.buktitbeli, bb.idmbrg, bb.nmmbrg, b.idmsup, s.nmmsup, b.bruto, b.netto, b.discv, b.ppnv, b.JmlBayarTunai + IF(SUM(bd.jmlbayar)>0,SUM(bd.jmlbayar),0) AS 'bayar', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)<=0,0,b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)) AS 'sisa', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)>0,'Belum Lunas','Lunas') AS 'status' FROM mgaptbeli b LEFT OUTER JOIN mgaptbelid db ON b.idtbeli = db.idtbeli LEFT OUTER JOIN mginmbrg bb ON db.idmbrg = bb.idmbrg LEFT OUTER JOIN mgsymcabang c ON b.IdMCabang = c.idmcabang LEFT OUTER JOIN mgapmsup s ON b.IdMSup = s.idmsup LEFT OUTER JOIN mgaptbhutd bd ON b.IdTBeli = bd.idtrans WHERE b.hapus=0 AND b.tgltbeli BETWEEN '${start}' AND '${end}' ${qcabang} ${qsupplier} ${qbarang} GROUP BY b.idtbeli) fin GROUP BY fin.idmcabang`;
            const filter = await sequelize.query(sql, {
                raw: false,
            });

            var arr_data = await Promise.all(filter[0].map(async (fil, index) => {
                let idmcabang = fil.idmcabang;
                let sql1 = `SELECT c.NmMCabang, bb.nmmbrg, b.tgltbeli, b.idtbeli, b.buktitbeli, s.nmmsup, b.bruto, b.netto, b.discv, b.ppnv, b.JmlBayarTunai + IF(SUM(bd.jmlbayar)>0,SUM(bd.jmlbayar),0) AS 'bayar', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)<=0,0,b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)) AS 'sisa', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)>0,'Belum Lunas','Lunas') AS 'status' FROM mgaptbeli b LEFT OUTER JOIN mgaptbelid db ON b.idtbeli = db.idtbeli LEFT OUTER JOIN mginmbrg bb ON db.idmbrg = bb.idmbrg LEFT OUTER JOIN mgsymcabang c ON b.IdMCabang = c.idmcabang LEFT OUTER JOIN mgapmsup s ON b.IdMSup = s.idmsup LEFT OUTER JOIN mgaptbhutd bd ON b.IdTBeli = bd.idtrans WHERE b.hapus=0 AND b.tgltbeli BETWEEN '${start}' AND '${end}' and b.idmcabang = ${idmcabang} ${qcabang} ${qsupplier} ${qbarang} GROUP BY b.idtbeli`;
                const list = await sequelize.query(sql1, {
                    raw: false,
                });

                var arr_list = await Promise.all(list[0].map(async (list, index_satu) => {
                    return barang_list(list);
                }))
                
                return {
                    "nama": fil.nmmcabang,
                    "bruto": parseFloat(fil.subtotal),
                    "diskon": parseFloat(fil.diskon),
                    "pajak": parseFloat(fil.pajak),
                    "netto": parseFloat(fil.tagihan),
                    "tunai": parseFloat(fil.bayar), // "Rp&nbsp;"+ number_format(fil.bayar, 2),
                    "sisa": parseFloat(Math.abs(fil.sisa)), // "Rp&nbsp;"+ number_format(abs(fil.sisa), 2),
                    "list": arr_list
                }
            }))

        } else if (group == "supplier") {
            let sql = `SELECT fin.idmsup, fin.nmmsup, fin.idmcabang, fin.nmmcabang, SUM(fin.bruto) AS 'subtotal', SUM(fin.ppnv) AS 'pajak', SUM(fin.discv) AS 'diskon',SUM(fin.netto) AS 'tagihan', SUM(fin.bayar) AS 'bayar', SUM(fin.sisa) AS 'sisa' FROM (SELECT b.idmcabang, c.NmMCabang, b.tgltbeli, b.buktitbeli, bb.idmbrg, bb.nmmbrg, b.idmsup, s.nmmsup, b.bruto, b.netto, b.discv, b.ppnv, b.JmlBayarTunai + IF(SUM(bd.jmlbayar)>0,SUM(bd.jmlbayar),0) AS 'bayar', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)<=0,0,b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)) AS 'sisa', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)>0,'Belum Lunas','Lunas') AS 'status' FROM mgaptbeli b LEFT OUTER JOIN mgaptbelid db ON b.idtbeli = db.idtbeli LEFT OUTER JOIN mginmbrg bb ON db.idmbrg = bb.idmbrg LEFT OUTER JOIN mgsymcabang c ON b.IdMCabang = c.idmcabang LEFT OUTER JOIN mgapmsup s ON b.IdMSup = s.idmsup LEFT OUTER JOIN mgaptbhutd bd ON b.IdTBeli = bd.idtrans WHERE b.hapus=0 AND b.tgltbeli BETWEEN '${start}' AND '${end}' ${qcabang} ${qsupplier} ${qbarang} GROUP BY b.idtbeli) fin GROUP BY fin.idmsup`;
            const filter = await sequelize.query(sql, {
                raw: false,
            });

            var arr_data = await Promise.all(filter[0].map(async (fil, index) => {
                let idmsup = fil.idmsup;
                let sql1 = `SELECT c.NmMCabang, bb.nmmbrg, b.tgltbeli, b.idtbeli, b.buktitbeli, s.nmmsup, b.bruto, b.netto, b.discv, b.ppnv, b.JmlBayarTunai + IF(SUM(bd.jmlbayar)>0,SUM(bd.jmlbayar),0) AS 'bayar', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)<=0,0,b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)) AS 'sisa', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)>0,'Belum Lunas','Lunas') AS 'status' FROM mgaptbeli b LEFT OUTER JOIN mgaptbelid db ON b.idtbeli = db.idtbeli LEFT OUTER JOIN mginmbrg bb ON db.idmbrg = bb.idmbrg LEFT OUTER JOIN mgsymcabang c ON b.IdMCabang = c.idmcabang LEFT OUTER JOIN mgapmsup s ON b.IdMSup = s.idmsup LEFT OUTER JOIN mgaptbhutd bd ON b.IdTBeli = bd.idtrans WHERE b.hapus=0 AND b.tgltbeli BETWEEN '${start}' AND '${end}' and b.idmsup = ${idmsup} ${qcabang} ${qsupplier} ${qbarang} GROUP BY b.idtbeli`;
                const list = await sequelize.query(sql1, {
                    raw: false,
                });

                var arr_list = await Promise.all(list[0].map(async (list, index_satu) => {
                    return barang_list(list);
                }))

                return {
                    "nama": fil.nmmsup,
                    "bruto": parseFloat(fil.subtotal),
                    "diskon": parseFloat(fil.diskon),
                    "pajak": parseFloat(fil.pajak),
                    "netto": parseFloat(fil.tagihan),
                    "tunai": parseFloat(fil.bayar), // "Rp&nbsp;"+ number_format(fil.bayar, 2),
                    "sisa": parseFloat(Math.abs(fil.sisa)), // "Rp&nbsp;"+ number_format(abs(fil.sisa), 2),
                    "list": arr_list
                }
            }))

        } else if (group == "barang") {
            let sql = `SELECT fin.idmsup, fin.nmmsup, fin.idmbrg, fin.nmmbrg, fin.idmcabang, fin.nmmcabang, SUM(fin.bruto) AS 'subtotal', SUM(fin.ppnv) AS 'pajak', SUM(fin.discv) AS 'diskon',SUM(fin.netto) AS 'tagihan', SUM(fin.bayar) AS 'bayar', SUM(fin.sisa) AS 'sisa' FROM (SELECT b.idmcabang, c.NmMCabang, b.tgltbeli, b.buktitbeli, bb.idmbrg, bb.nmmbrg, b.idmsup, s.nmmsup, b.bruto, b.netto, b.discv, b.ppnv, b.JmlBayarTunai + IF(SUM(bd.jmlbayar)>0,SUM(bd.jmlbayar),0) AS 'bayar', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)<=0,0,b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)) AS 'sisa', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)>0,'Belum Lunas','Lunas') AS 'status' FROM mgaptbeli b LEFT OUTER JOIN mgaptbelid db ON b.idtbeli = db.idtbeli LEFT OUTER JOIN mginmbrg bb ON db.idmbrg = bb.idmbrg LEFT OUTER JOIN mgsymcabang c ON b.IdMCabang = c.idmcabang LEFT OUTER JOIN mgapmsup s ON b.IdMSup = s.idmsup LEFT OUTER JOIN mgaptbhutd bd ON b.IdTBeli = bd.idtrans WHERE b.hapus=0 AND b.tgltbeli BETWEEN '${start}' AND '${end}' ${qcabang} ${qsupplier} ${qbarang} GROUP BY b.idtbeli) fin GROUP BY fin.idmbrg`;
            const filter = await sequelize.query(sql, {
                raw: false,
            });

            var arr_data = await Promise.all(filter[0].map(async (fil, index) => {
                let idmbrg = fil.idmbrg;
                let sql1 = `SELECT c.NmMCabang, bb.nmmbrg, b.tgltbeli, b.idtbeli, b.buktitbeli, s.nmmsup, b.bruto, b.netto, b.discv, b.ppnv, b.JmlBayarTunai + IF(SUM(bd.jmlbayar)>0,SUM(bd.jmlbayar),0) AS 'bayar', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)<=0,0,b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)) AS 'sisa', IF(b.netto - b.jmlbayartunai - IF(SUM(bd.jmlbayar)>0, SUM(bd.jmlbayar),0)>0,'Belum Lunas','Lunas') AS 'status' FROM mgaptbeli b LEFT OUTER JOIN mgaptbelid db ON b.idtbeli = db.idtbeli LEFT OUTER JOIN mginmbrg bb ON db.idmbrg = bb.idmbrg LEFT OUTER JOIN mgsymcabang c ON b.IdMCabang = c.idmcabang LEFT OUTER JOIN mgapmsup s ON b.IdMSup = s.idmsup LEFT OUTER JOIN mgaptbhutd bd ON b.IdTBeli = bd.idtrans WHERE b.hapus=0 AND b.tgltbeli BETWEEN '${start}' AND '${end}' and bb.idmbrg = ${idmbrg} ${qcabang} ${qsupplier} ${qbarang} GROUP BY b.idtbeli`;
                const list = await sequelize.query(sql1, {
                    raw: false,
                });

                var arr_list = await Promise.all(list[0].map(async (list, index_satu) => {
                    var list_item = await barang_list(list);
                    list_item.sales = list.nmmsales;
                    return list_item;
                }))

                return {
                    "nama": fil.nmmbrg,
                    "bruto": parseFloat(fil.subtotal),
                    "diskon": parseFloat(fil.diskon),
                    "pajak": parseFloat(fil.pajak),
                    "netto": parseFloat(fil.tagihan),
                    "tunai": parseFloat(fil.bayar), // "Rp&nbsp;"+ number_format(fil.bayar, 2),
                    "sisa": parseFloat(Math.abs(fil.sisa)), // "Rp&nbsp;"+ number_format(abs(fil.sisa), 2),
                    "list": arr_list
                }
            }))

        } else {
            var count = {};
            var arr_data = [];
        }
    } else if (jenis == 2) {
        jenis_str = 'detail';
        async function barang_list(list) {
            let sql2 = `SELECT bb.kdmbrg, bb.nmmbrg, g.nmmgd, bd.qtytotal, bd.hrgstn, s.nmmstn, bd.discv, bd.ppnv, (bd.qtytotal * bd.hrgstn) AS dpp, (bd.qtytotal * bd.hrgstn - bd.discv + bd.ppnv) AS subtotal FROM mgaptbelid bd LEFT OUTER JOIN mgsymgd g ON bd.idmgd = g.idmgd LEFT OUTER JOIN mginmbrg bb ON bd.idmbrg = bb.idmbrg LEFT OUTER JOIN mginmstn s ON bb.idmstn1 = s.idmstn LEFT OUTER JOIN mgaptbeli b ON bd.idtbeli = b.idtbeli WHERE b.hapus=0 AND b.tgltbeli BETWEEN '${start}%' AND '${end}%' ${qcabang} ${qsupplier} ${qbarang} AND b.idtbeli = ${list.idtbeli}`;
            // let sql2 = `SELECT bb.kdmbrg, bb.nmmbrg, g.nmmgd, bd.qtytotal, bd.hrgstn, s.nmmstn, bd.discv, bd.ppnv, (bd.qtytotal * bd.hrgstn) AS dpp, (bd.qtytotal * bd.hrgstn - bd.discv + bd.ppnv) AS subtotal FROM mgaptbelid bd LEFT OUTER JOIN mgsymgd g ON bd.idmgd = g.idmgd LEFT OUTER JOIN mginmbrg bb ON bd.idmbrg = bb.idmbrg LEFT OUTER JOIN mginmstn s ON bb.idmstn1 = s.idmstn LEFT OUTER JOIN mgaptbeli b ON bd.idtbeli = b.idtbeli WHERE b.hapus=0 AND b.tgltbeli BETWEEN '${start}%' AND '${end}%' AND b.idtbeli = $l->idtbeli`;
            const brg = await sequelize.query(sql2, {
                raw: false,
            });

            var arr_brg = brg[0].map((brg, index_dua) => {
                return {
                    "kode": brg.kdmbrg,
                    "nama": brg.nmmbrg,
                    "gudang": brg.nmmgd,
                    "qty": brg.qtytotal,
                    "satuan": brg.nmmstn,
                    "hargasat": parseFloat(brg.hrgstn),
                    "diskon": parseFloat(brg.discv),
                    "pajak": parseFloat(brg.ppnv),
                    "dpp": parseFloat(brg.dpp),
                    "subtotal": parseFloat(brg.subtotal)
                }
            })

            return {
                "id" : list.idtbeli, 
                "tanggal" : list.tgltbeli, 
                "transaksi" : list.buktitbeli,
                "supplier" : list.nmmsup, 
                "subtotal" : parseFloat(list.bruto), 
                "diskon" : parseFloat(list.discv), 
                "pajak" : parseFloat(list.ppnv), 
                "grandtotal" : parseFloat(list.netto),
                "bayar" : parseFloat(list.jmlbayartunai),
                "kredit": parseFloat(list.jmlbayarkredit),
                "sisa" : parseFloat(list.jmlbayarkredit + list.jmlbayartunai - list.netto), 
                "item" : arr_brg, 
                
            }
        }
        
        if (group == "cabang") {
            let sql = `select nmmcabang from mgsymcabang where aktif = 1 and hapus = 0`;
            const filter = await sequelize.query(sql, {
                raw: false,
            });

            var arr_data = await Promise.all(filter[0].map(async (fil, index) => {
                let sql1 = `SELECT b.idtbeli, b.tgltbeli, b.buktitbeli, s.nmmsup, b.bruto, b.discv, b.ppnv, b.netto, b.jmlbayartunai, b.jmlbayarkredit, (b.netto-b.jmlbayartunai-b.jmlbayarkredit) AS STATUS FROM mgaptbeli b LEFT OUTER JOIN mgapmsup s ON b.idmsup = s.idmsup WHERE b.hapus = 0 AND b.tgltbeli BETWEEN '${start}%' AND '${end}%' ${qbarang} ${qcabang} ${qsupplier}`;
                const list = await sequelize.query(sql1, {
                    raw: false,
                });

                var arr_list = await Promise.all(list[0].map(async (list, index_satu) => {
                    return barang_list(list);
                }))

                return {
                    "nama": fil.nmmcabang,
                    "list": arr_list
                }
            }))
        }
        else if (group == "supplier") {
            let sql = `SELECT s.* FROM mgapmsup s LEFT OUTER JOIN mgaptbeli b ON s.idmsup = b.idmsup LEFT OUTER JOIN mgaptbelid bd ON b.idtbeli = bd.idtbeli WHERE s.Aktif = 1 AND s.hapus = 0 AND b.tgltbeli BETWEEN '${start}%' AND '${end}%' ${qbarang} ${qsupplier} ${qcabang} GROUP BY s.nmmsup HAVING COUNT(b.idtbeli)>0`;
            const filter = await sequelize.query(sql, {
                raw: false,
            });

            var arr_data = await Promise.all(filter[0].map(async (fil, index) => {
                let idmsup = fil.idmsup;
                let sql1 = `SELECT b.idtbeli, b.tgltbeli, b.buktitbeli, s.nmmsup, b.bruto, b.discv, b.ppnv, b.netto, b.jmlbayartunai, b.jmlbayarkredit, (b.netto-b.jmlbayartunai-b.jmlbayarkredit) AS STATUS FROM mgaptbeli b LEFT OUTER JOIN mgapmsup s ON b.idmsup = s.idmsup WHERE b.hapus=0 AND b.idmsup = ${idmsup} AND b.tgltbeli BETWEEN '${start}%' AND '${end}%' ${qcabang} ${qbarang} ${qsupplier}`;
                const list = await sequelize.query(sql1, {
                    raw: false,
                });

                var arr_list = await Promise.all(list[0].map(async (list, index_satu) => {
                    return barang_list(list);
                }))

                return {
                    "nama": fil.nmmsup,
                    "list": arr_list
                }
            }))
        }
        else if (group == "barang") {
            let sql = `SELECT b.* FROM mginmbrg b LEFT OUTER JOIN mgaptbelid bd ON b.IdMBrg = bd.idmbrg LEFT OUTER JOIN mgaptbeli bb ON bd.idtbeli = bb.idtbeli WHERE b.aktif = 1 AND b.hapus=0 AND bb.tgltbeli BETWEEN '${start}%' AND '${end}%' ${qcabang} ${qsupplier} ${qbarang} GROUP BY b.Idmbrg HAVING COUNT(bb.IdTbeli)>0`;
            const filter = await sequelize.query(sql, {
                raw: false,
            });

            var arr_data = await Promise.all(filter[0].map(async (fil, index) => {
                let idmbrg = fil.idmbrg;
                let sql1 = `SELECT b.idtbeli, b.tgltbeli, b.buktitbeli, s.nmmsup, b.bruto, b.discv, b.ppnv, b.netto, b.jmlbayartunai, b.jmlbayarkredit, (b.netto-b.jmlbayarkredit-b.jmlbayartunai) AS STATUS FROM mgaptbeli b LEFT OUTER JOIN mgapmsup s ON s.idmsup = b.idmsup LEFT OUTER JOIN mgaptbelid bd ON b.idtbeli = bd.idtbeli WHERE b.hapus = 0 AND bd.idmbrg =${idmbrg} AND b.tgltbeli BETWEEN '${start}%' AND '${end}%' ${qcabang} ${qsupplier} ${qbarang} GROUP BY b.idtbeli`;
                const list = await sequelize.query(sql1, {
                    raw: false,
                });

                var arr_list = await Promise.all(list[0].map(async (list, index_satu) => {
                    return barang_list(list);
                }))

                return {
                    "nama": fil.nmmbrg,
                    "list": arr_list
                }
            }))
        } else {
            var count = {};
            var arr_data = [];
        }
    }
    res.json({
        message: "Success " + jenis_str,
        countData: count,
        data: arr_data
    });
};

exports.stock = async (req, res) => {

    let jenis = req.body.jenis || 1;
    // posisi stock
    if (jenis == 1) {
        let date = req.body.tanggal || '2024-01-19';
        let sql = `SELECT c.idmcabang, c.nmmcabang, g.idmgd, g.nmmgd FROM mgsymcabang c LEFT OUTER JOIN mginlkartustock k ON c.idmcabang = k.idmcabang LEFT OUTER JOIN mgsymgd g ON k.idmgd = g.idmgd WHERE c.hapus = 0 AND k.tgltrans <= '${date}' GROUP BY c.nmmcabang HAVING COUNT(k.IdTrans)>0`;
        const filter = await sequelize.query(sql, {
            raw: false,
        });

        var arr_data = await Promise.all(filter[0].map(async (fil, index) => {
            let sql1 = `SELECT k.idmbrg, b.kdmbrg, b.nmmbrg, SUM(k.qtytotal) AS qty, s.nmmstn FROM mginlkartustock k LEFT OUTER JOIN mginmbrg b ON k.idmbrg = b.idmbrg LEFT OUTER JOIN mginmstn s ON b.idmstn1 = s.idmstn WHERE tgltrans <= '${date}' AND k.idmcabang = ${fil.idmcabang} AND idmgd=${fil.idmgd} GROUP BY k.idmbrg`;
            const brg = await sequelize.query(sql1, {
                raw: false,
            });

            var arr_brg = await Promise.all(brg[0].map(async (brg, index_satu) => {
                let sql2 = `SELECT s.tgltrans, s.keterangan, ss.nmmstn, s.debet, s.kredit, SUM(s.saldo) as saldo FROM (SELECT tgltrans, idmbrg, idtrans, keterangan, IF(qtytotal >= 0, qtytotal, 0) AS debet, IF(qtytotal <= 0, qtytotal, 0) AS kredit, SUM(qtytotal) AS saldo FROM mginlkartustock WHERE STR_TO_DATE(tgltrans, '%Y-%m-%d') <= '${date}' AND idmbrg = ${brg.idmbrg} GROUP BY idtrans) s LEFT OUTER JOIN mginmbrg b ON b.idmbrg = s.idmbrg LEFT OUTER JOIN mginmstn ss ON ss.idmstn = b.idmstn1 GROUP BY s.keterangan ORDER BY s.tgltrans ASC`;
                const item = await sequelize.query(sql2, {
                    raw: false,
                });

                var saldo = 0;
                var arr_item = item[0].map((item, index_dua) => {
                    saldo += parseFloat(item.saldo);
                    return {
                        'tanggal': item.tgltrans,
                        'keterangan': item.keterangan,
                        'satuan': item.nmmstn,
                        'debet': parseFloat(item.debet),
                        'kredit': parseFloat(item.kredit),
                        'saldo': parseFloat(item.saldo),
                        // 'debet': item.debet,
                        // 'kredit': item.kredit,
                        // 'saldo': item.saldo,
                    }
                })

                return {
                    "id": brg.idmbrg,
                    "kode": brg.kdmbrg, 
                    "nama": brg.nmmbrg,
                    "qty": parseFloat(brg.qty),
                    "satuan": brg.nmmstn,
                    "listitem": arr_item,
                }
            }))

            return {
                "cabang": fil.nmmcabang,
                "gudang": fil.nmmgd, // "Rp&nbsp;"+ number_format(fil.tagihan, 2),
                "list": arr_brg
            }
        }))

        res.json({
            message: "Success",
            data: arr_data
        })
    }

    // kartu stock
    else if (jenis == 2) {
        let start = req.body.start || today;
        let end = req.body.end || today;

        let cabang = req.body.cabang || "";
        let qcabang = "";
        if (cabang != "") {
            qcabang = "AND c.idmcabang=" + cabang;
        }

        let gudang = req.body.gudang || "";
        let qgudang = "";
        if (gudang != "") {
            qgudang = "AND g.idmgd = " + gudang;
        }

        let barang = req.body.barang || "";
        let qbarang = "";
        if (barang != "") {
            qbarang = "AND b.idmbrg = " + barang;
        }
        console.log('logbrg', qbarang)
        console.log('logstart', start)
        console.log('logend', end)


        let sql = `SELECT c.idmcabang, c.nmmcabang, g.idmgd, g.nmmgd, b.idmbrg, b.kdmbrg, b.NmMBrg FROM mgsymcabang c LEFT OUTER JOIN mginlkartustock k ON c.idmcabang = k.idmcabang LEFT OUTER JOIN mgsymgd g ON k.idmgd = g.idmgd LEFT OUTER JOIN mginmbrg b ON k.idmbrg = b.idmbrg WHERE c.hapus = 0 AND k.tgltrans <= '${today}' ${qcabang} ${qgudang} GROUP BY c.nmmcabang`;
        const filter = await sequelize.query(sql, {
            raw: false,
        });

        var arr_data = await Promise.all(filter[0].map(async (fil, index) => {
            console.log('fil', fil)
            let sql1 = `select b.idmbrg, b.kdmbrg, b.nmmbrg from mginmbrg b left outer join mginlkartustock k on b.idmbrg = k.idmbrg where k.idmcabang = ${fil.idmcabang} and k.idmgd = ${fil.idmgd} ${qbarang} group by b.idmbrg`;
            const brg = await sequelize.query(sql1, {
                raw: false,
            });

            var arr_brg = await Promise.all(brg[0].map(async (brg, index_satu) => {
                console.log('brg', brg)

                let sql2 = `SELECT s.tgltrans, s.keterangan, ss.nmmstn, s.debet, s.kredit, SUM(s.saldo) as saldo FROM (SELECT '${start}' as tgltrans, idmbrg, idtrans, 'Saldo awal' AS keterangan, (0) AS debet, (0) AS kredit, SUM(qtytotal) AS saldo FROM mginlkartustock WHERE STR_TO_DATE(tgltrans, '%Y-%m-%d') < '${start}' AND idmbrg = ${brg.idmbrg} UNION ALL SELECT tgltrans, idmbrg, idtrans, keterangan, IF(qtytotal >= 0, qtytotal, 0) AS debet, IF(qtytotal <= 0, qtytotal, 0) AS kredit, SUM(qtytotal) AS saldo FROM mginlkartustock WHERE STR_TO_DATE(tgltrans, '%Y-%m-%d') between '${start}' AND '${end}' AND idmbrg = ${brg.idmbrg} GROUP BY idtrans) s LEFT OUTER JOIN mginmbrg b ON b.idmbrg = s.idmbrg LEFT OUTER JOIN mginmstn ss ON ss.idmstn = b.idmstn1 GROUP BY s.keterangan ORDER BY s.tgltrans ASC`;
                const item = await sequelize.query(sql2, {
                    raw: false,
                });

                var saldo = 0;
                var arr_item = item[0].map((item, index_dua) => {
                    saldo += parseFloat(item.saldo);
                    return {
                        'tanggal': item.tgltrans,
                        'keterangan': item.keterangan,
                        'satuan': item.nmmstn,
                        'debet': parseFloat(item.debet),
                        'kredit': parseFloat(item.kredit),
                        'saldo': parseFloat(item.saldo),
                        // 'debet': item.debet,
                        // 'kredit': item.kredit,
                        // 'saldo': item.saldo,
                    }
                })

                return {
                    // "id": brg.idmbrg,
                    "kode": brg.kdmbrg, 
                    "nama": brg.nmmbrg,
                    // "qty": parseFloat(brg.qty),
                    // "satuan": brg.nmmstn,
                    "listitem": arr_item,
                }
            }))

            return {
                "cabang": fil.nmmcabang,
                "gudang": fil.nmmgd, // "Rp&nbsp;"+ number_format(fil.tagihan, 2),
                "list": arr_brg
            }
        }))

        res.json({
            message: "Success kartu",
            data: arr_data
        })
    }
    
}