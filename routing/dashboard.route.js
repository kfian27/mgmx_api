const reportController = require("../controller/dashboard.controller");
const router = require("express").Router();
const multer = require("multer");
const upload = multer();

router.get("/getDataCustomer", upload.none(), reportController.getDataCustomer);
router.get("/getDataSupplier", upload.none(), reportController.getDataSupplier);
router.get("/getDataBarang", upload.none(), reportController.getDataBarang);
router.get("/getWarningToday", upload.none(), reportController.getWarningToday);
router.get("/getTransaksiAdjustKoreksi", upload.none(), reportController.getTransaksiAdjustKoreksi);
router.get("/getNilaiBisnis", upload.none(), reportController.getNilaiBisnis);
router.get("/getBarangTerlaku", upload.none(), reportController.getBarangTerlaku);
router.get("/getDataGrafik", upload.none(), reportController.getDataGrafik);



module.exports = router;