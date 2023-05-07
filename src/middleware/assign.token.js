const jwt = require('jsonwebtoken');

const getAccessToken = async (user) => {
    return await jwt.sign({ user_id: user.user_id, email: user.email }, process.env.JWT_SECRET, {
        expiresIn: process.env.JWT_EXPIRE,
    });
}

module.exports = getAccessToken