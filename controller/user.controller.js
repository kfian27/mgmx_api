const exp = require("constants");
const db = require("../models");
const User = db.user;
const Op = db.Sequelize.Op;

// CREATE: untuk enambahkan data kedalam tabel user
exports.create = async (req, res) => {
  // validate request
  if (!req.body.email || !req.body.password) {
    return res.status(400).send({
      message: "Email and Password can not be empty",
    });
  }

  // data yang didapatkan dari inputan oleh pengguna
  const user = req.body;
  // proses menyimpan kedalam database
  await User.create(user)
    .then(async (data) => {
      const forUpdate = await data.setPassword();
      id = data.id;
      data.update(forUpdate);
      res.json({
        message: "User created successfully.",
        data: data,
      });
    })
    .catch((err) => {
      res.status(500).json({
        message: err.message || "Some error occurred while creating the User.",
        data: null,
      });
    });
};

// READ: menampilkan atau mengambil semua data sesuai model dari database
exports.findAll = async (req, res) => {
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
  let filter = {};
  if (search != "") {
    Object.assign(filter, {
      [Op.or]: [
        {
          email: {
            [Op.like]: `%${search}%`,
          },
        },
        {
          nama: {
            [Op.like]: `%${search}%`,
          },
        },
      ],
    });
  }

  let countUser = await User.findAndCountAll({
    where: filter,
    order: [sortArr],
    paranoid: true,
  }).then((count) => {
    return count.count;
  });

  await User.findAndCountAll({
    where: filter,
    order: [sortArr],
    limit: parseInt(limit),
    offset: parseInt(offset),
    include: [
      {
        model: db.company,
      },
    ],
    paranoid: true,
  })
    .then((user) => {
      user.count = countUser;
      res.json({
        message: "Users retrieved successfully.",
        data: user,
      });
    })
    .catch((err) => {
      res.status(500).json({
        message: err.message || "Some error occurred while retrieving user.",
        data: null,
      });
    });
};

// UPDATE: Merubah data sesuai dengan id yang dikirimkan sebagai params
exports.update = async (req, res) => {
  const id = req.params.id;
  await User.update(req.body, {
    where: { id },
  })
    .then(async (num) => {
      if (num == 1) {
        let cekPassword = req.body.password || "";
        if (cekPassword != "") {
          User.findByPk(id).then(async (user) => {
            const forUpdate = await user.setPassword();
            user.update(forUpdate);
            req.body = user;
          });
        }
        res.json({
          message: "User updated successfully.",
          data: req.body,
        });
      } else {
        res.json({
          message: `Cannot update user with id=${id}. Maybe user was not found or req.body is empty!`,
          data: req.body,
        });
      }
    })
    .catch((err) => {
      res.status(500).json({
        message: err.message || "Some error occurred while updating the user.",
        data: null,
      });
    });
};

// DELETE: Menghapus data sesuai id yang dikirimkan
exports.delete = (req, res) => {
  const id = req.params.id;
  User.destroy({
    where: { id },
  })
    .then((num) => {
      if (num == 1) {
        res.json({
          message: "User deleted successfully.",
          data: req.body,
        });
      } else {
        res.json({
          message: `Cannot delete user with id=${id}. Maybe user was not found!`,
          data: req.body,
        });
      }
    })
    .catch((err) => {
      res.status(500).json({
        message: err.message || "Some error occurred while deleting the user.",
        data: null,
      });
    });
};

// Mengambil data sesuai id yang dikirimkan
exports.findOne = (req, res) => {
  User.findByPk(req.params.id, {
    include: [
      {
        model: db.company,
        required: false,
      },
    ],
  })
    .then((user) => {
      if (user != null) {
        res.json({
          message: "User retrieved successfully.",
          data: user,
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

exports.updateCompany = async (req, res) => {
  const id = req.params.id;
  let allCompany = req.body.company || [];

  if (allCompany.length == 0) {
    return res.status(400).send({
      message: "Company is required!!",
    });
  }

  db.user_company.destroy({
    where: {
      userId: {
        [Op.like]: id,
      },
    },
  });

  let forInsert = [];
  allCompany.forEach((item) => {
    let obj = {
      userId: id,
      companyId: item,
    };
    forInsert.push(obj);
  });

  await db.user_company.bulkCreate(forInsert);
  await User.findByPk(id, {
    include: [
      {
        model: db.company,
        required: false,
      },
    ],
  })
    .then((user) => {
      if (user != null) {
        res.json({
          message: "User retrieved successfully.",
          data: user,
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
