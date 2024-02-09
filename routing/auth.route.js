const authController = require("../controller/auth.controller");
const router = require("express").Router();
const multer = require("multer");
const upload = multer();

router.post("/user/do_login", upload.none(), authController.doLogin);

const authMiddleware = require("../middleware/auth");
router.use(authMiddleware);
router.post("/user/set_company", upload.none(), authController.setCompany);
router.get("/user/get_company", upload.none(), authController.getCompanyByUser);

module.exports = router;
