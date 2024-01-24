const db = require("../models");
const User = db.user;
const Company = db.company;
const Op = db.Sequelize.Op;
const jwt = require("jsonwebtoken");
const env = require("dotenv").config();

// CREATE: untuk enambahkan data kedalam tabel company
exports.doLogin = async (req, res) => {
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
    order: [["created_at", "DESC"]],
  })
    .then(async function (row) {
      let first = row[0];
      if (first) {
        let cekPassword = await first.verifyPassword(password);
        if (cekPassword) {
          let token = jwt.sign(first.toJSON(), process.env.JWT_SECRET);
          let id = first.id;
          await User.update(
            { last_login: db.sequelize.fn("NOW") },
            {
              where: { id },
            }
          );
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

exports.setCompany = async (req, res) => {
  User.findAll({
    where: {
      id: req.userid,
    },
    include: [
      {
        required: true,
        model: Company,
        where: {
          id: {
            [Op.in]: [req.body.companyid],
          },
        },
      },
    ],
    raw: true,
    nest: true,
  })
    .then((user) => {
      if (user != null) {
        let token = jwt.sign(user[0], process.env.JWT_SECRET);
        res.json({
          message: "User retrieved successfully.",
          data: token,
        });
      } else {
        res.status(400).json({
          message: `Cannot find user. Maybe user was not found!`,
          data: {},
        });
      }
    })
    .catch((err) => {
      res.status(500).json({
        message: err.message || "Some error occurred while retrieving user.",
        data: null,
      });
    });
};

exports.getCompanyByUser = async (req, res) => {
  Company.findAll({
    include: [
      {
        required: true,
        model: User,
        where: {
          id: {
            [Op.in]: [req.userid],
          },
        },
      },
    ],
  })
    .then((company) => {
      if (company != null) {
        res.json({
          message: "company retrieved successfully.",
          data: company,
        });
      } else {
        res.status(400).json({
          message: `Cannot find company. Maybe user was not found!`,
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
