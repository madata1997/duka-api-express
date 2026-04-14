const mysql = require('mysql2/promise');
require('dotenv').config();

let connection;

const getConnection = async () => {
  if (!connection) {
    connection = await mysql.createConnection({
      host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
      port: process.env.DB_PORT
    });

    console.log('✅ DB Connected');
  }

  return connection;
};

module.exports = { getConnection };
