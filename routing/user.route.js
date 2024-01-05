const userControler = require("../controller/user.controller");
const router = require("express").Router();
const multer = require("multer");
const upload = multer();

router.post("/", upload.none(), userControler.create);
router.get("/", upload.none(), userControler.findAll);
router.put("/:id", upload.none(), userControler.update);
router.delete("/:id", upload.none(), userControler.delete);
router.get("/:id", userControler.findOne);
router.patch("/:id", upload.none(), userControler.updateCompany);

module.exports = router;