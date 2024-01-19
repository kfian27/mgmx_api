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


exports.penjualan = async (req, res) => {

    let start = req.body.start ?? '2008-01-17';
    let end = req.body.end ?? '2024-02-17';
    let jenis = req.body.jenis ?? 1;

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
    const count_profit = await countDataFromQuery(
        `SELECT COALESCE((SELECT (jd.QtyTotal * b.reserved_dec1) AS hpp FROM mgartjuald jd LEFT OUTER JOIN mgartjual j ON jd.IdTJual=j.IdTJual LEFT OUTER JOIN mginmbrg b ON jd.IdMBrg = b.idmbrg WHERE j.tgltjual between '${start}%' and '${end}%' ${qbarang} ${qcustomer} ${qbarang} order by jd.IdTJualD desc limit 1),0) as total`
    );

    var count = {
        penjualan: count_penjualan,
        produk_terjual: parseFloat(count_produk),
        pendapatan: parseFloat(count_pendapatan),
        profit: parseFloat(count_profit),
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
                    'harga': formatNoCommaRp(brg.hrgstn), // format
                    'diskon': formatNoCommaRp(brg.discv), // format
                    'total': formatNoCommaRp(brg.subtotal), // format
                }
            })

            return {
                "id": list.idtjual,
                "tanggal": list.tgltjual, // date_format(date_create(list.tgltjual), "m - d - Y"),
                "transaksi": list.buktitjual,
                "customer": list.nmmcust,
                "subtotal": formatTwoCommaRp(list.bruto), // "Rp & nbsp; ".number_format(list.bruto, 2),
                "diskon": formatTwoCommaRp(list.discv), // "Rp & nbsp; ".number_format(list.discv, 2),
                "pajak": formatTwoCommaRp(list.ppnv), // "Rp & nbsp; ".number_format(list.ppnv, 2),
                "grandtotal": formatTwoCommaRp(list.netto), // "Rp & nbsp; ".number_format(list.netto, 2),
                "bayar": formatTwoCommaRp(list.bayar), // "Rp & nbsp; ".number_format(list.bayar, 2),
                "sisa": formatTwoCommaRp(list.sisa), // number_format(list.sisa),
                "sisabayar": formatTwoCommaRp(list.sisa), // "Rp ".number_format(abs(list.sisa), 2),
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
                    "netto": formatTwoCommaRp(fil.tagihan), // "Rp&nbsp;"+ number_format(fil.tagihan, 2),
                    "sisa": formatTwoCommaRp(Math.abs(fil.sisa)), // "Rp&nbsp;"+ number_format(abs(fil.sisa), 2),
                    "bayar": formatTwoCommaRp(fil.bayar), // "Rp&nbsp;"+ number_format(fil.bayar, 2),
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
                    "netto": formatTwoCommaRp(fil.tagihan), // "Rp&nbsp;"+ number_format(fil.tagihan, 2),
                    "sisa": formatTwoCommaRp(Math.abs(fil.sisa)), // "Rp&nbsp;"+ number_format(abs(fil.sisa), 2),
                    "bayar": formatTwoCommaRp(fil.bayar), // "Rp&nbsp;"+ number_format(fil.bayar, 2),
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
                    "netto": formatTwoCommaRp(fil.tagihan), // "Rp&nbsp;"+ number_format(fil.tagihan, 2),
                    "sisa": formatTwoCommaRp(Math.abs(fil.sisa)), // "Rp&nbsp;"+ number_format(abs(fil.sisa), 2),
                    "bayar": formatTwoCommaRp(fil.bayar), // "Rp&nbsp;"+ number_format(fil.bayar, 2),
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
                    "netto": formatTwoCommaRp(fil.tagihan), // "Rp&nbsp;"+ number_format(fil.tagihan, 2),
                    "sisa": formatTwoCommaRp(Math.abs(fil.sisa)), // "Rp&nbsp;"+ number_format(abs(fil.sisa), 2),
                    "bayar": formatTwoCommaRp(fil.bayar), // "Rp&nbsp;"+ number_format(fil.bayar, 2),
                    "list": arr_list
                }
            }))

        } else {
            var count = {};
            var arr_data = [];
        }
    } else if (jenis == 2) {
        async function barang_list(list) {
            let sql2 = `SELECT b.kdmbrg, b.nmmbrg, g.nmmgd, jd.QtyTotal, s.NmMStn, jd.hrgstn, jd.discv, jd.ppnv, (jd.qtytotal * jd.hrgstn) AS dpp, (jd.qtytotal * jd.hrgstn - jd.discv + jd.ppnv) AS subtotal FROM mgartjuald jd LEFT OUTER JOIN mginmbrg b ON b.idmbrg = jd.idmbrg LEFT OUTER JOIN mginmstn s ON b.IdMStn1 = s.idmstn LEFT OUTER JOIN mgsymgd g ON g.idmgd = jd.idmgd WHERE idtjual =  ${list.idtjual}`;
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
                    "hargasat": formatTwoCommaRp(brg.hrgstn),
                    "diskon": formatTwoCommaRp(brg.discv),
                    "pajak": formatTwoCommaRp(brg.ppnv),
                    "dpp": formatTwoCommaRp(brg.dpp),
                    "subtotal": formatTwoCommaRp(brg.subtotal)
                }
            })

            return {
                "id" : list.idtjual, 
                "tanggal" : list.tgltjual, 
                "transaksi" : list.buktitjual,
                "customer" : list.nmmcust, 
                "subtotal" : formatTwoCommaRp(list.bruto), 
                "diskon" : formatTwoCommaRp(list.discv), 
                "pajak" : formatTwoCommaRp(list.ppnv), 
                "grandtotal" : formatTwoCommaRp(list.netto),
                "sisa" : formatTwoComma(list.sisa), 
                "item" : arr_brg, 
                
            }
        }
        
        if (group == "cabang") {
            let sql = `select nmmcabang from mgsymcabang where aktif = 1 and hapus = 0`;
            const filter = await sequelize.query(sql, {
                raw: false,
            });

            var arr_data = await Promise.all(filter[0].map(async (fil, index) => {
                let sql1 = `SELECT j.idtjual, j.tgltjual, j.buktitjual, c.nmmcust, j.bruto, j.discv, j.ppnv, j.netto, SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'Bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'Sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${barang} ${cabang} ${customer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC`;
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
            let sql = `SELECT fin.idmcust, fin.nmmcust, fin.idmcabang, fin.nmmcabang, SUM(fin.tagihan) AS 'Tagihan', SUM(fin.bayar) AS 'Bayar', SUM(fin.sisa) AS 'Sisa' FROM (SELECT ca.idmcabang, ca.nmmcabang, s.nmmsales, j.tgltjual AS 'Tanggal', j.buktitjual AS 'Nota', c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'Tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'Bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'Sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${cabang} ${barang} ${customer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC) fin GROUP BY fin.idmcust`;
            const filter = await sequelize.query(sql, {
                raw: false,
            });

            var arr_data = await Promise.all(filter[0].map(async (fil, index) => {
                let idmcust = fil.idmcust;
                let sql1 = `SELECT ca.nmmcabang,j. idtjual, j.tgltjual, j.buktitjual, c.idmcust, c.nmmcust, s.nmmsales, j.bruto, j.discv, j.ppnv, j.netto, SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'Bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'Sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${barang} ${cabang} ${customer} and c.idmcust=${idmcust} GROUP BY j.idtjual ORDER BY j.tgltjual ASC`;
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
            let sql = `SELECT fin.idmsales, fin.nmmsales, fin.idmcust, fin.nmmcust, fin.idmcabang, fin.nmmcabang, SUM(fin.tagihan) AS 'Tagihan', SUM(fin.bayar) AS 'Bayar', SUM(fin.sisa) AS 'Sisa' FROM (SELECT ca.idmcabang, ca.nmmcabang, s.idmsales, s.nmmsales, j.tgltjual AS 'Tanggal', j.buktitjual AS 'Nota', c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'Tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'Bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'Sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE s.aktif=1 and s.hapus=0 and j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${cabang} ${barang} ${customer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC) fin GROUP BY fin.idmsales`;
            const filter = await sequelize.query(sql, {
                raw: false,
            });

            var arr_data = await Promise.all(filter[0].map(async (fil, index) => {
                let sql1 = `SELECT j.idtjual, j.bruto, j.discv, j.ppnv, j.netto, ca.idmcabang, ca.nmmcabang, s.idmsales, s.nmmsales, j.tgltjual, j.buktitjual, c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'Tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'Bayar', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'Sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang WHERE s.aktif=1 and s.hapus=0 and j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${cabang} ${barang} ${customer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC`;
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
            let sql = `SELECT fin.idmbrg, fin.nmmbrg, fin.idmcust, fin.nmmcust, fin.idmcabang, fin.nmmcabang, SUM(fin.tagihan) AS 'Tagihan', SUM(fin.bayar) AS 'Bayar', SUM(fin.sisa) AS 'Sisa' FROM (SELECT b.idmbrg, b.nmmbrg, ca.idmcabang, ca.nmmcabang, s.nmmsales, j.tgltjual AS 'Tanggal', j.buktitjual AS 'Nota', c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'Tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'Bayar',IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'Sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang LEFT OUTER JOIN mgartjuald jd ON j.idtjual = jd.IdTJual LEFT OUTER JOIN mginmbrg b ON jd.idmbrg = b.idmbrg WHERE j.hapus=0 AND j.tgltjual BETWEEN '${start}' AND '${end}' ${cabang} ${barang} ${customer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC) fin GROUP BY fin.idmbrg`;
            const filter = await sequelize.query(sql, {
                raw: false,
            });

            var arr_data = await Promise.all(filter[0].map(async (fil, index) => {
                let idmbrg = fil.idmbrg;
                let sql1 = `SELECT j.idtjual, j.tgltjual, j.buktitjual, j.bruto, j.discv, j.ppnv, j.netto, b.idmbrg, b.nmmbrg, ca.idmcabang, ca.nmmcabang, s.nmmsales, j.tgltjual AS 'Tanggal', j.buktitjual AS 'Nota', c.idmcust, c.nmmcust, j.bruto AS 'Subtotal', j.discv AS 'Diskon', j.ppnv AS 'Pajak', j.netto AS 'Tagihan', SUM(j.jmlbayartunai + IF(pd.jmlbayar>0,pd.jmlbayar,0)) AS 'Bayar',IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)<=0,0,j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)) AS 'Sisa', IF(j.netto - j.jmlbayartunai - IF(SUM(pd.jmlbayar)>0,SUM(pd.jmlbayar),0)>0,'Belum Lunas', 'Lunas') AS STATUS FROM mgartjual j LEFT OUTER JOIN mgartbpiutd pd ON j.idtjual = pd.idtrans LEFT OUTER JOIN mgarmcust c ON j.idmcust = c.idmcust LEFT OUTER JOIN mgarmsales s ON j.idmsales = s.idmsales LEFT OUTER JOIN mgsymcabang ca ON j.idmcabang = ca.idmcabang LEFT OUTER JOIN mgartjuald jd ON j.idtjual = jd.IdTJual LEFT OUTER JOIN mginmbrg b ON jd.idmbrg = b.idmbrg WHERE j.hapus=0 AND b.idmbrg = ${idmbrg} and j.tgltjual BETWEEN '${start}' AND '${end}' ${cabang} ${barang} ${customer} GROUP BY j.idtjual ORDER BY j.tgltjual ASC`;
                const list = await sequelize.query(sql1, {
                    raw: false,
                });

                var arr_list = await Promise.all(list[0].map(async (list, index_satu) => {
                    let list_item = await barang_list(list);
                    list_item.bayar = list.bayar;
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