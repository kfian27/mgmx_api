const bookController = require("../controller/so.controller");
const router = require("express").Router();

router.get("/", bookController.findAll);

module.exports = router;
