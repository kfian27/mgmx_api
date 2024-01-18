const reportController = require("../controller/report.controller");
const router = require("express").Router();
const multer = require("multer");
const upload = multer();

router.get("/getListCabang", upload.none(), reportController.getListCabang);
router.get("/getListCustomer", upload.none(), reportController.getListCustomer);
router.post("/penjualan", upload.none(), reportController.penjualan);
// router.post("/pembelian", upload.none(), reportController.pembelian);
// router.get("/", upload.none(), reportController.findAll);
// router.put("/:id", upload.none(), reportController.update);
// router.delete("/:id", upload.none(), reportController.delete);
// router.get("/:id", reportController.findOne);
// router.patch("/:id", upload.none(), reportController.updateCompany);

module.exports = router;