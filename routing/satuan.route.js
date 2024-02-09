const router = require("express").Router();
const multer = require("multer");
const upload = multer();
const authMiddleware = require("../middleware/auth_company");
router.use(authMiddleware);
const satuanController = require("../controller/satuan.controller");

router.post("/", satuanController.findAll);
router.post("/create", upload.none(), satuanController.create);
router.put("/:id", satuanController.update);
router.delete("/:id", upload.none(), satuanController.delete);

module.exports = router;
