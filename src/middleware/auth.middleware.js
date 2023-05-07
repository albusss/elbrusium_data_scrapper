const jwt = require("jsonwebtoken");
const connection = require("../db/db.connection");
const {
    QueryTypes
} = require("sequelize");
// Protect Routes
exports.protect = async (req, res, next) => {
    let token;
    if (
        req.headers.authorization &&
        req.headers.authorization.startsWith("Bearer")
    ) {
        token = req.headers.authorization.split(" ")[1];
    }
    if (!token) {
        return res.status(401).json({
            status: false,
            message: "Inavlid Token , Not authorized to access this route"
        });
    }
    try {
        const decoded = jwt.verify(token, process.env.JWT_SECRET);
        let {
            user_id,
            email
        } = decoded

        let user = await connection.query(`select * from users where user_id = '${user_id}' AND admin="Y"`, {
            type: QueryTypes.SELECT
        })
        if (user && user.length > 0) {
            req.user_id = user_id;
            next();
        } else {
            res.status(401).json({
                status: false,
                message: "Not authorize to access this route"
            });
        }
    } catch (err) {
        return res.status(401).json({
            status: false,
            message: "Not authorized to access this route"
        });
    }
}