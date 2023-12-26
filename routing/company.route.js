const companyControler = require("../controller/company.controller");
const router = require("express").Router();
const multer = require("multer");
const upload = multer();

router.post("/", companyControler.create);
router.get("/", upload.none(), companyControler.findAll);
router.put("/:id", upload.none(), companyControler.update);
router.delete("/:id", upload.none(), companyControler.delete);
router.get("/:id", companyControler.findOne);

module.exports = router;