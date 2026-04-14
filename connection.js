const mysql = require('mysql2/promise');
require('dotenv').config();
const date = require('date-and-time');

const now = new Date();

let connection;

(async () => {
  try {
    connection = await mysql.createConnection({
      port: process.env.DB_PORT,
      host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0
    });

    console.log('Connected to the database at', date.format(now, 'YYYY-MM-DD HH:mm:ss'));
  } catch (err) {
    console.error('Failed to connect to the database:', err.message);
  }
})();

module.exports = {
  getConnection: async () => connection
};
