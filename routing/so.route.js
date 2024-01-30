const soController = require("../controller/so.controller");
const router = require("express").Router();
const authMiddleware = require("../middleware/auth_company");
router.use(authMiddleware);

router.post("/", soController.findAll);
router.post("/create", soController.createSO);
router.get("/view/:id", soController.viewSO);
router.put("/real/:id", soController.realSO);
router.delete("/delete/:id", soController.deleteSO);

module.exports = router;
