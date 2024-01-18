//untuk menambahkan path
const path = require("path");
const config = {
  destination: function (req, file, cb) {
    // cb(null, path.join(__dirname, "public/uploads"));
    cb(null, "public/uploads");
  },
  filename: function (req, file, cb) {
    cb(
      null,
      file.fieldname + "-" + Date.now() + path.extname(file.originalname)
    );
  },
};
exports.config = config;
