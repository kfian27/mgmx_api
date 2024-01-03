const satuanController = require("../controller/satuan.controller");
const router = require("express").Router();
const multer = require("multer");
const upload = multer();

router.post("/", satuanController.findAll);
router.post("/create", upload.none(), satuanController.create);
router.put("/:id", satuanController.update);
router.delete("/:id", upload.none(), satuanController.delete);

module.exports = router;
