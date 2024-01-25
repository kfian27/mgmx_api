// const db = require("../models");
// const sequelize = db.sequelize;

const fun = require("../mgmx");


exports.findAll = async (req, res) => {
    const sequelize = await fun.connection(req.datacompany);

    let sql = `SELECT c.IdMCust, c.KdMCust, c.NmMCust, c.JenisMCust, c.Alamat, c.IdMKota, c.Kota, c.KodePos, c.Telp1, c.Hp1, c.Aktif FROM mgarmcust c WHERE c.Hapus = 0`;

    let sortBy = req.body.sort_by || "TglCreate";
    let sortType = req.body.sort_type || "desc";
    let search = req.body.search || "";
    sortBy = sortBy.split(",");
    sortType = sortType.split(",");
    let sortArr = [];
    for (let index = 0; index < sortBy.length; index++) {
        let element = [sortBy[index], sortType[index]];
        sortArr.push(element);
    }
    let limit = req.body.limit || 12;
    let page = req.body.page || 1;
    let offset = (page - 1) * limit;
    let where = {};
    // let Op = db.Sequelize.Op;
    let qsearch = '';
    let qsort = '';
    let qpaginate = '';

    if (search != "") {
        // let testDate = db.Sequelize.literal("DATE_FORMAT(created_at,'%d-%m-%Y')");
        qsearch = ` AND 
        (c.kdmcust like '%${search}%' or
        c.nmmcust like '%${search}%' or
        c.alamat like '%${search}%' or
        c.kodepos like '%${search}%' or
        c.kota like '%${search}%' or
        c.telp1 like '%${search}%' or
        c.hp1 like '%${search}%')`;
    }
    qsort = ` ORDER BY `;
    qsort += `${sortBy} ${sortType}`
    qpaginate = ` LIMIT ${limit} OFFSET ${offset}`;

    let querysql = sql + qsearch + qsort + qpaginate;

    const data = await sequelize.query(querysql, {
        raw: false,
    });

    let qcount = "SELECT COUNT(IdMCust) as total FROM (" + sql + qsearch + ") tbl";
    const count_data = await sequelize.query(qcount, {
        raw: false,
        plain: true
    });

    let total_data = count_data.total;
    var total_page = Math.ceil(total_data / limit);
    var current_page = parseInt(page);
    var prev_page = 0;
    var next_page = 0;
    if (current_page > 1) {
        prev_page = current_page - 1
    }

    if (current_page < total_page) {
        next_page = current_page + 1
    }

    res.json({
        message: "Success",
        CountData: total_data,
        TotalPage: total_page,
        CurrentPage: current_page,
        PrevPage: prev_page,
        NextPage: next_page,
        data: data[0],
    });
};

exports.create = async (req, res) => {
    const sequelize = await fun.connection(req.datacompany);

    let qcount = "SELECT IdMCust as ID FROM mgarmcust ORDER BY ID DESC LIMIT 1";
    const count_data = await sequelize.query(qcount, {
        raw: false,
        plain: true
    });

    let last_id = count_data.ID || 0;
    let id = last_id + 1;

    req.body['id'] = id;

    let idmcabang = req.body.idmcabang || 0;
    let kode = req.body.kode || "";
    let nama = req.body.nama || "";
    let alamat = req.body.alamat || "";
    let kodepos = req.body.kodepos || "";
    let telp = req.body.telp || "";
    let hp = req.body.hp || "";
    let is_aktif = req.body.is_aktif || 0;


    let userid = 0;

    let sql = `insert into mgarmcust (IdMCabangMTower, idmtower, idmcabang, idmcust, kdmcust, nmmcust, alamat, kodepos, telp1, hp1, idmusercreate, tglcreate, idmuserupdate, tglupdate, aktif, hapus)
  VALUES(0, 0, ${idmcabang}, ${id},'${kode}', '${nama}', '${alamat}', '${kodepos}', '${telp}', '${hp}', '${userid}', NOW(), '${userid}', NOW(), ${is_aktif}, 0)`
    const data = await sequelize.query(sql, {
        raw: false,
    }).then(datanya => {
        res.json({
            message: "Master Customer berhasil dibuat.",
            data: req.body,
        });
    }).catch((err) => {
        res.status(500).json({
            message: err.message || "Some error occurred while updating the data.",
            data: null,
        });
    });
}

exports.update = async (req, res) => {
    const sequelize = await fun.connection(req.datacompany);

    const id = req.params.id;
    req.body['id'] = parseInt(id);

    let kode = req.body.kode || "";
    let nama = req.body.nama || "";
    let alamat = req.body.alamat || "";
    let kodepos = req.body.kodepos || "";
    let telp = req.body.telp || "";
    let hp = req.body.hp || "";
    let is_aktif = req.body.is_aktif || 0;

    let userid = 0;


    let sql = `update mgarmcust set 
    kdmcust='${kode}',
    nmmcust='${nama}',
    alamat='${alamat}',
    kodepos='${kodepos}',
    telp1='${telp}',
    hp1='${hp}',
    aktif='${is_aktif}',
    idmuserupdate='${userid}',
    tglupdate = NOW()
    where idmcust=${id}`;
    const data = await sequelize.query(sql, {
        raw: false,
    }).then(datanya => {
        res.json({
            message: "Master Customer berhasil diupdate.",
            data: req.body,
        });
    }).catch((err) => {
        res.status(500).json({
            message: err.message || "Some error occurred while updating the data.",
            data: null,
        });
    });
}

// DELETE: Menghapus data sesuai id yang dikirimkan
exports.delete = async (req, res) => {
    const sequelize = await fun.connection(req.datacompany);

    const id = req.params.id;
    req.body['id'] = parseInt(id);
    let userid = 0;
    let sql = `UPDATE mgarmcust SET Hapus = 1, idmuserupdate = ${userid}, tglupdate = NOW() WHERE idmcust = ${id}`
    const data = await sequelize.query(sql, {
        raw: false,
    }).then(datanya => {
        res.json({
            message: "Master Customer berhasil dihapus.",
            data: req.body,
        });
    }).catch((err) => {
        res.status(500).json({
            message: err.message || "Some error occurred while deleting the data.",
            data: null,
        });
    });
};