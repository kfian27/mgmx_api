const soController = require("../controller/so.controller");
const router = require("express").Router();

router.post("/", soController.findAll);

module.exports = router;
