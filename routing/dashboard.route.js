const reportController = require("../controller/dashboard.controller");
const router = require("express").Router();
const multer = require("multer");
const upload = multer();

router.get("/getDataCustomer", upload.none(), reportController.getDataCustomer);
router.get("/getDataSupplier", upload.none(), reportController.getDataSupplier);
router.get("/getDataBarang", upload.none(), reportController.getDataBarang);
router.get("/getDataWarningToday", upload.none(), reportController.getDataWarningToday);


module.exports = router;