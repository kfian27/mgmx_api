const penjualan = require("../controller/penjualan.controller");
const router = require("express").Router();
const multer = require("multer");
const storageConfig = require("../config/upload_file.config");
const path = require("path");
const diskStorage = multer.diskStorage(storageConfig.config);
const upload = multer({ storage: diskStorage });
const authMiddleware = require("../middleware/auth_company");
router.use(authMiddleware);

router.post("/",upload.none(), penjualan.findAll);
router.post("/create",upload.none(), penjualan.create);
router.get("/view/:id", penjualan.view);
router.put("/real/:id", penjualan.real);
router.put("/update/:id", penjualan.update);
router.delete("/delete/:id", penjualan.delete);

module.exports = router;
