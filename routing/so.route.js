const soController = require("../controller/so.controller");
const router = require("express").Router();
const multer = require("multer");
const storageConfig = require("../config/upload_file.config");
const path = require("path");
const diskStorage = multer.diskStorage(storageConfig.config);
const upload = multer({ storage: diskStorage });
const authMiddleware = require("../middleware/auth_company");
router.use(authMiddleware);

router.post("/",upload.none(), soController.findAll);
router.post("/create",upload.none(), soController.createSO);
router.get("/view/:id", soController.viewSO);
router.put("/real/:id", soController.realSO);
router.put("/update/:id", soController.updateSO);
router.delete("/delete/:id", soController.deleteSO);

module.exports = router;
