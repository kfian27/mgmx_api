const db = require("../models");
const User = db.user;
const jwt = require("jsonwebtoken");
const env = require("dotenv").config();

const authMiddleware = async (req, res, next) => {
  const Authorization = req.get("Authorization");
  if (!Authorization) {
    res
      .status(401)
      .json({
        form_name: null,
        message: "Unauthorized",
      })
      .end();
  } else {
    const token = Authorization.replaceAll("Bearer ", "");
    var decoded = jwt.verify(token, process.env.JWT_SECRET);

    const user = await User.findOne({
      where: {
        id: decoded.id,
      },
    });

    if (!user) {
      res
        .status(401)
        .json({
          form_name: null,
          message: "Unauthorized",
        })
        .end();
    } else {
      req.userid = user.id;
      next();
    }
  }
};

module.exports = authMiddleware;
