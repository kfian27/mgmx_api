const barangController = require("../controller/barang.controller");
const router = require("express").Router();
const multer = require("multer");
const upload = multer();

router.post("/", barangController.findAll);
router.post("/create", upload.none(), barangController.create);
router.put("/:id", barangController.update);
router.delete("/:id", upload.none(), barangController.delete);

module.exports = router;
