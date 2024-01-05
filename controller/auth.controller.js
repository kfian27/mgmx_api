const db = require("../models");
const User = db.user;
const Op = db.Sequelize.Op;
const jwt = require("jsonwebtoken");
const env = require("dotenv").config();

// CREATE: untuk enambahkan data kedalam tabel company
exports.doLogin = async(req, res) => {
    let email = req.body.email || "";
    let password = req.body.password || "";

    User.findAll({
            limit: 1,
            where: {
                //your where conditions, or without them if you need ANY entry
                email: {
                    [Op.like]: email,
                },
            },
            order: [
                ["created_at", "DESC"]
            ],
        })
        .then(async function(row) {
            let first = row[0];
            if (first) {
                let cekPassword = await first.verifyPassword(password);
                if (cekPassword) {
                    let token = jwt.sign(first.toJSON(), process.env.JWT_SECRET);
                    res.json({
                        message: "Login successfully.",
                        token: token,
                    });
                } else {
                    res.status(500).json({
                        message: "Email and password doest match",
                        data: null,
                    });
                }
            } else {
                res.status(500).json({
                    message: "Email and password doest match",
                    data: null,
                });
            }
        })
        .catch((err) => {
            res.status(500).json({
                message: err.message || "Email and password doest match",
                data: null,
            });
        });
};