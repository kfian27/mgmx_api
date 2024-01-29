const soController = require("../controller/so.controller");
const router = require("express").Router();
const authMiddleware = require("../middleware/auth_company");
router.use(authMiddleware);

router.post("/", soController.findAll);

module.exports = router;
