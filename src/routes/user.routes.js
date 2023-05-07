const express = require("express");
const {
    scrapUserDetails, deleteUserDetails
} = require("../controllers/user.controller");
const router = express.Router();
const {
    protect
} = require("../middleware/auth.middleware");
router.post('/scrap', scrapUserDetails);
router.post('/delete', deleteUserDetails);

module.exports = router;