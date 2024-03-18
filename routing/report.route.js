const reportController = require("../controller/report.controller");
const reportV2Controller = require("../controller/reportv2.controller");
const router = require("express").Router();
const multer = require("multer");
const upload = multer();
const authMiddleware = require("../middleware/auth_company");
router.use(authMiddleware);

router.get("/getListCabang", upload.none(), reportController.getListCabang);
router.get("/getListCustomer", upload.none(), reportController.getListCustomer);
router.get("/getListSupplier", upload.none(), reportController.getListSupplier);
router.get("/getListGudang", upload.none(), reportController.getListGudang);
router.get("/getListBarang", upload.none(), reportController.getListBarang);
router.get("/getListBank", upload.none(), reportController.getListBank);
router.get("/getListKas", upload.none(), reportController.getListKas);
router.get("/getListSales", upload.none(), reportController.getListSales);
router.post("/penjualan", upload.none(), reportController.penjualan);
router.post("/pembelian", upload.none(), reportController.pembelian);
router.post("/stock", upload.none(), reportController.stock);
router.post("/kas", upload.none(), reportController.kas);
router.post("/bank", upload.none(), reportController.bank);
router.post("/hutang", upload.none(), reportController.hutang);
router.post("/piutang", upload.none(), reportController.piutang);
router.post("/laba-rugi", upload.none(), reportController.labarugi);
router.get("/tesRahman", upload.none(), reportController.tesRahman);

router.post("/stock/v2", upload.none(), reportV2Controller.stock);
// router.post("/pembelian", upload.none(), reportController.pembelian);
// router.get("/", upload.none(), reportController.findAll);
// router.put("/:id", upload.none(), reportController.update);
// router.delete("/:id", upload.none(), reportController.delete);
// router.get("/:id", reportController.findOne);
// router.patch("/:id", upload.none(), reportController.updateCompany);

module.exports = router;