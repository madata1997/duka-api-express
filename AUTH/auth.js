require('dotenv').config();
const jwt = require('jsonwebtoken');

function authenticateToken (req, res, next) {
    const authHeader = req.headers['authorization']
    const token = authHeader && authHeader.split(' ')[1]
    if (token == null)
        return res.sendStatus(401);
    jwt.verify(token, process.env.ACCESS_TOKEN, (err, response) => {
        if (err)
            return res.sendStatus(403);
        res.locals = response;
        next();
    })
}

// Middleware to check user roles
function checkRole(...roles) {
    return (req, res, next) => {
        const userRole = res.locals.roleName;

        if (!roles.includes(userRole)) {
            return res.status(403).json({ message: 'Access denied: Not allowed to access this' });
        }

        next();
    };
}


module.exports = { authenticateToken: authenticateToken, checkRole:checkRole }