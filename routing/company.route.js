const companyControler = require("../controller/company.controller");
const router = require("express").Router();
const multer = require("multer");
const storageConfig = require("../config/upload_file.config");
const path = require("path");
const diskStorage = multer.diskStorage(storageConfig.config);
const upload = multer({ storage: diskStorage });

router.post("/", upload.single("logo"), companyControler.create);
router.get("/", upload.none(), companyControler.findAll);
router.put("/:id", upload.none(), companyControler.update);
router.delete("/:id", upload.none(), companyControler.delete);
router.get("/:id", companyControler.findOne);

module.exports = router;
