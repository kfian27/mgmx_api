const db = require("../models");
const Company = db.company;

// CREATE: untuk enambahkan data kedalam tabel company
exports.create = (req, res) => {
    // validate request
    //   if (!req.body.title) {
    //     return res.status(400).send({
    //       message: "Title can not be empty",
    //     });
    //   }
    // data yang didapatkan dari inputan oleh pengguna
    const company = req.body;

    // proses menyimpan kedalam database
    Company.create(company)
        .then((data) => {
            res.json({
                message: "Company created successfully.",
                data: data,
            });
        })
        .catch((err) => {
            res.status(500).json({
                message: err.message || "Some error occurred while creating the Company.",
                data: null,
            });
        });
};

// READ: menampilkan atau mengambil semua data sesuai model dari database
exports.findAll = async(req, res) => {
    let sortBy = req.body.sort_by || "created_at";
    let sortType = req.body.sort_type || "desc";
    let search = req.body.search || "";
    sortBy = sortBy.split(",");
    sortType = sortType.split(",");
    let sortArr = [];
    for (let index = 0; index < sortBy.length; index++) {
        let element = [sortBy[index], sortType[index]];
        sortArr.push(element);
    }
    let limit = req.body.limit || 10;
    let page = req.body.page || 1;
    let offset = (page - 1) * limit;
    let where = {};
    let Op = db.Sequelize.Op;
    if (search != "") {
        // let testDate = db.Sequelize.literal("DATE_FORMAT(created_at,'%d-%m-%Y')");
        Object.assign(where, {
            [Op.or]: [{
                    kode: {
                        [Op.like]: `%${search}%`,
                    },
                },
                {
                    nama: {
                        [Op.like]: `%${search}%`,
                    },
                },
                // {
                //     "DATE_FORMAT(created_at,'%d-%m-%Y')": {
                //         [Op.like]: `%${search}%`,
                //     },
                // },
            ],
        });
    }

    await Company.findAndCountAll({
            where: where,
            order: [sortArr],
            limit: parseInt(limit),
            offset: parseInt(offset),
            // paranoid: false,
        })
        .then((company) => {
            Object.assign(company, { test: 1 });
            res.json({
                message: "Companys retrieved successfully.",
                data: company,
            });
        })
        .catch((err) => {
            res.status(500).json({
                message: err.message || "Some error occurred while retrieving company.",
                data: null,
            });
        });
};

// UPDATE: Merubah data sesuai dengan id yang dikirimkan sebagai params
exports.update = (req, res) => {
    const id = req.params.id;
    Company.update(req.body, {
            where: { id },
        })
        .then((num) => {
            if (num == 1) {
                res.json({
                    message: "Company updated successfully.",
                    data: req.body,
                });
            } else {
                res.json({
                    message: `Cannot update company with id=${id}. Maybe company was not found or req.body is empty!`,
                    data: req.body,
                });
            }
        })
        .catch((err) => {
            res.status(500).json({
                message: err.message || "Some error occurred while updating the company.",
                data: null,
            });
        });
};

// DELETE: Menghapus data sesuai id yang dikirimkan
exports.delete = (req, res) => {
    const id = req.params.id;
    Company.destroy({
            where: { id },
        })
        .then((num) => {
            if (num == 1) {
                res.json({
                    message: "Company deleted successfully.",
                    data: req.body,
                });
            } else {
                res.json({
                    message: `Cannot delete company with id=${id}. Maybe company was not found!`,
                    data: req.body,
                });
            }
        })
        .catch((err) => {
            res.status(500).json({
                message: err.message || "Some error occurred while deleting the company.",
                data: null,
            });
        });
};

// Mengambil data sesuai id yang dikirimkan
exports.findOne = (req, res) => {
    Company.findByPk(req.params.id || 0)
        .then((company) => {
            if (company != null) {
                res.json({
                    message: "Company retrieved successfully.",
                    data: company,
                });
            } else {
                res.status(400).json({
                    message: `Cannot find company with id=${req.params.id}. Maybe company was not found!`,
                    data: {},
                });
            }
        })
        .catch((err) => {
            res.status(500).json({
                message: err.message || "Some error occurred while retrieving company.",
                data: null,
            });
        });
};