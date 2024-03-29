const customerController = require("../controller/customer.controller");
const router = require("express").Router();
const multer = require("multer");
const upload = multer();
const authMiddleware = require("../middleware/auth_company");
router.use(authMiddleware);

router.post("/", customerController.findAll);
router.post("/create", upload.none(), customerController.create);
router.put("/:id", customerController.update);
router.delete("/:id", upload.none(), customerController.delete);

module.exports = router;
