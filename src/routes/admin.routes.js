const express = require("express");
const {
  addSite,
  updateSiteStatus,
  getSitesList,
  deleteSite,
  login,
  setConsumers,
} = require("../controllers/admin.controller");
const router = express.Router();
const { protect } = require("../middleware/auth.middleware");
router.post("/login", login);
router.post("/add-site", protect, addSite);
router.get("/get-sites", getSitesList);
router.post("/update-site-status", protect, updateSiteStatus);
router.get("/delete-site/:id", protect, deleteSite);
router.post("/set-consumer", protect, setConsumers);

module.exports = router;
