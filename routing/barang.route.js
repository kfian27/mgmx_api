const barangController = require("../controller/barang.controller");
const router = require("express").Router();
const multer = require("multer");
const storageConfig = require("../config/upload_file.config");
const path = require("path");
const diskStorage = multer.diskStorage(storageConfig.config);
const upload = multer({ storage: diskStorage });

const authMiddleware = require("../middleware/auth_company");
router.use(authMiddleware);

router.post("/", barangController.findAll);
router.post("/create", upload.single("gambar"), barangController.create);
router.put("/:id", upload.single("gambar"), barangController.update);
router.delete("/:id", upload.none(), barangController.delete);

module.exports = router;
