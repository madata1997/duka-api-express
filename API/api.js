const express = require('express');
const router = express.Router();
const multer = require('multer');
const path = require('path');
var uuid = require('uuid');
const jwt = require('jsonwebtoken');
const nodemailer = require('nodemailer');
require('dotenv').config();
const date = require('date-and-time')
const now = new Date();
const XLSX = require('xlsx');
const twilio = require('twilio');
const moment = require('moment-timezone');
const cron = require('node-cron');
const bcrypt = require('bcrypt');
const axios = require('axios');
const ExcelJS = require('exceljs');
const speakeasy = require('speakeasy');
const qrcode = require('qrcode');
const dayjs = require('dayjs');

// PREPARE DATABASE BACKUP =======================================

const { exec } = require('child_process');
const util = require('util');
const execAsync = util.promisify(exec);

// locations =============================================
const geoip = require('geoip-lite');

// EXCEL FILE DATA ===========================================
const readXlsxFile = require('read-excel-file/node');

// Files Upload Express =========================================
const fileUpload = require('express-fileupload');
const uploadOpts = {
    useTempFiles: true,
    tempFileDir: ''
}

// Nodejs Schedule =====================================
const schedule = require('node-schedule');

// Print A4
const PDFDocument = require('pdfkit');


// Include EJS Report
let ejs = require('ejs');

// Include html PDF
let pdf = require('html-pdf');

// Include FS
var fs = require('fs');



// User Authentication && Authorization
var auth = require('../AUTH/auth');

const { default: dayOfWeek } = require('date-and-time/plugin/day-of-week');

const { getConnection }  = require('../connection');


// Twilio Configuration

const accountSid = process.env.TWILIO_ACCOUNT_SID;
const authToken  = process.env.TWILIO_AUTH_TOKEN;
const twilioPhone = process.env.TWILIO_PHONE;
const client = twilio(accountSid, authToken);

// Configure Thermo Printer

const ThermalPrinter = require("node-thermal-printer").printer;
const PrinterTypes = require("node-thermal-printer").types;


// SYSTEM AUTOMATION AND ASYNC START HERE ======================================


// UPDATE AFTER UNSECCESSFULLY LOGIN ATTEMPTS

cron.schedule('* * * * *', async () => {
  const currentTime = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

  let connection;

  try {
    connection = await getConnection();
    
    const sql = `
      UPDATE users
      SET 
        loginAttempts = 0,
        expiresAt = NULL,
        accountDisabled = 'false',
        attemptStatus = 'false'
      WHERE expiresAt IS NOT NULL
        AND expiresAt <= ?
        AND attemptStatus = 'true'
    `;

    const [result] = await connection.query(sql, [currentTime]);

    console.log(` Reset ${result.affectedRows} user(s) at ${currentTime}`);
  } catch (err) {
    console.error(' Error running cron job:', err.message);
  }
}, {
    timezone: 'Africa/Nairobi' 
});


// AUTO DELETE MAILS OLDER THAN 1 MONTH


cron.schedule(
  '* * * * *', // Runs every minute
  async () => {
    let conn;

    try {
      conn = await getConnection();

      // Force MySQL timezone 
      await conn.query(`SET time_zone = '+03:00'`);

      // Log server & DB time
      const [dbTime] = await conn.query(`SELECT NOW() AS now`);
      console.log(`🕒 Cron started | Server: ${new Date().toISOString()} | DB: ${dbTime[0].now}`);

      // Count mails older than 1 month
      const [rowsToDelete] = await conn.query(`
        SELECT COUNT(*) AS count
        FROM mails
        WHERE date < DATE_SUB(NOW(), INTERVAL 1 MONTH)
      `);

      console.log(` Old mails to delete: ${rowsToDelete[0].count}`);

      // Delete old mails
      const [result] = await conn.query(`
        DELETE FROM mails
        WHERE date < DATE_SUB(NOW(), INTERVAL 1 MONTH)
      `);

      console.log(`️ Deleted ${result.affectedRows} old mails`);

    } catch (error) {
      console.error(' Error deleting old mails:', error);
    } 
  },
  {
    timezone: 'Africa/Nairobi' // Matches system logic
  }
);



// AUTO DELETE LOGS OLDER THAN 6 MONTHS =============================


cron.schedule(
  '* * * * *', // Runs every 1 minute
  async () => {
    let conn;

    try {
      conn = await getConnection();

      // Force MySQL timezone (EAT)
      await conn.query(`SET time_zone = '+03:00'`);

      // Log server & DB time
      const [dbTime] = await conn.query(`SELECT NOW() AS now`);
      console.log(
        ` Cron started | Server: ${new Date().toISOString()} | DB: ${dbTime[0].now}`
      );

      // Count logs older than 1 month
      const [rowsToDelete] = await conn.query(`
        SELECT COUNT(*) AS count
        FROM logs
        WHERE createdAt < DATE_SUB(NOW(), INTERVAL 1 MONTH)
      `);

      console.log(`Logs to delete: ${rowsToDelete[0].count}`);

      // Delete logs older than 1 month
      const [result] = await conn.query(`
        DELETE FROM logs
        WHERE createdAt < DATE_SUB(NOW(), INTERVAL 1 MONTH)
      `);

      console.log(` Deleted ${result.affectedRows} old logs`);

    } catch (error) {
      console.error(' Error deleting old logs:', error);
    } 
  },
  {
    timezone: 'Africa/Nairobi'
  }
);




// AUTO DELETE EXPIRED PRODUCT DATA AFTER 30 DAY OF EXPIRE ================================


cron.schedule(
  '* * * * *', // Runs every minute
  async () => {
    let conn;

    try {
      conn = await getConnection();

      // Force MySQL timezone to EAT
      await conn.query(`SET time_zone = '+03:00'`);

      // Log server & DB time
      const [dbTime] = await conn.query(`SELECT NOW() AS now`);
      console.log(
        ` Product cleanup cron started | Server: ${new Date().toISOString()} | DB: ${dbTime[0].now}`
      );

      // Fetch warehouses with auto delete enabled
      const [warehouses] = await conn.query(`
        SELECT id
        FROM warehouses
        WHERE auto_delete_expired_product = 1
      `);

      console.log(` Warehouses to process: ${warehouses.length}`);

      for (const warehouse of warehouses) {
        const warehouseId = warehouse.id;

        // Delete products expired more than 30 days ago
        const [result] = await conn.query(`
          DELETE FROM products
          WHERE warehouse_id = ?
            AND expire_date IS NOT NULL
            AND product_status != 'true'
            AND DATE(expire_date) < DATE_SUB(CURDATE(), INTERVAL 30 DAY)
        `, [warehouseId]);

        console.log(
          `🗑️ Warehouse ${warehouseId}: Deleted ${result.affectedRows} expired product(s)`
        );
      }

      console.log(' Warehouse expired product cleanup completed.');

    } catch (err) {
      console.error(' Error during warehouse cleanup:', err);
    } 
  },
  {
    timezone: 'Africa/Nairobi'
  }
);




// CHECK IF FINANCIAL YEAR REACHED 31/12 EVERY YEAR ==========


cron.schedule(
  '* * * * *', // Every 31st Dec at 23:59
  async () => {

    let connection;

    try {
      connection = await getConnection();

      // Force MySQL timezone (EAT)
      await connection.query(`SET time_zone = '+03:00'`);

      // Log server & DB time
      const [dbTime] = await connection.query(`SELECT NOW() AS now`);
      console.log(` FY Close Cron started | Server: ${new Date().toISOString()} | DB: ${dbTime[0].now}`);

      const currentTimestamp = dbTime[0].now;

      // Get current year & next FY safely
      const currentYear = new Date(dbTime[0].now).getFullYear();
      const nextFYName = String(currentYear + 1);

      // Fetch active & expired FYs
      const [expiredActiveFYs] = await connection.query(`
        SELECT id, store_id, name
        FROM fy_cycle
        WHERE isActive = 1
          AND expireAt <= ?
      `, [currentTimestamp]);

      console.log(`FYs to close: ${expiredActiveFYs.length}`);

      for (const fy of expiredActiveFYs) {
        const { id, store_id, name } = fy;

        // Check if next FY already exists
        const [nextFY] = await connection.query(`
          SELECT id
          FROM fy_cycle
          WHERE name = ? AND store_id = ?
          LIMIT 1
        `, [nextFYName, store_id]);

        // Close current FY (always close once expired)
        await connection.query(`
          UPDATE fy_cycle
          SET isActive = 0,
              closedAt = ?
          WHERE id = ?
        `, [currentTimestamp, id]);

        if (nextFY.length === 0) {
          console.log(` Closed FY "${name}" for Store ${store_id}. Next FY "${nextFYName}" not found.`);
        } else {
          console.log(`Closed FY "${name}" for Store ${store_id}. Next FY exists.`);
        }
      }

      console.log(' Financial Year close job completed.');

    } catch (error) {
      console.error(' Error in FY Close job:', error);
    } 
  },
  {
    timezone: 'Africa/Nairobi'
  }
);



// Daily at 1:00 AM — retry missed FY closings


cron.schedule(
  '0 1 * * *', // Daily at 01:00 AM
  async () => {

    let connection;

    try {
      connection = await getConnection();

      // Force MySQL timezone (EAT)
      await connection.query(`SET time_zone = '+03:00'`);

      // Log server & DB time
      const [dbTime] = await connection.query(`SELECT NOW() AS now`);
      const currentTimestamp = dbTime[0].now;

      console.log(` FY Recovery started | DB Time: ${currentTimestamp}`);

      // Determine next FY safely from DB time
      const currentYear = new Date(currentTimestamp).getFullYear();
      const nextFYName = String(currentYear + 1);

      // Find expired but still active FYs
      const [expiredActiveFYs] = await connection.query(`
        SELECT id, store_id, name
        FROM fy_cycle
        WHERE isActive = 1
          AND expireAt <= ?
      `, [currentTimestamp]);

      console.log(` FYs to recover: ${expiredActiveFYs.length}`);

      for (const fy of expiredActiveFYs) {
        const { id, store_id, name } = fy;

        // Always close expired FY (recovery logic)
        await connection.query(`
          UPDATE fy_cycle
          SET isActive = 0,
              closedAt = ?
          WHERE id = ?
        `, [currentTimestamp, id]);

        // Check if next FY exists (info only)
        const [nextFY] = await connection.query(`
          SELECT id
          FROM fy_cycle
          WHERE name = ? AND store_id = ?
          LIMIT 1
        `, [nextFYName, store_id]);

        if (nextFY.length === 0) {
          console.log(` Recovered & closed FY "${name}" for Store ${store_id} (Next FY missing)`);
        } else {
          console.log(` Recovered & closed FY "${name}" for Store ${store_id}`);
        }
      }

      console.log(' FY Recovery task completed.');

    } catch (error) {
      console.error(' FY Recovery error:', error);
    } 
  },
  {
    timezone: 'Africa/Nairobi'
  }
);




// Create new Financial Year 

cron.schedule(
  '0 0 1 1 *', // Every Jan 1st at 00:00
  async () => {

    let connection;

    try {
      connection = await getConnection();

      await connection.query(`SET time_zone = '+03:00'`);

      const [dbTime] = await connection.query(`SELECT NOW() AS now`);
      const currentTimestamp = dbTime[0].now;

      const currentYear = new Date(currentTimestamp).getFullYear();
      const newFYName = String(currentYear);

      const newFYStart = `${newFYName}-01-01 00:00:00`;
      const newFYExpire = `${newFYName}-12-31 23:59:59`;

      console.log(`📅 FY Auto-Creation | Year ${newFYName}`);

      // GET STORES
      const [stores] = await connection.query(`
        SELECT id 
        FROM stores
        WHERE status = 'true'
      `);

      console.log(`🏬 Stores found: ${stores.length}`);

      for (const store of stores) {
        const storeId = store.id;

        // CHECK FY EXISTS
        const [[existingFY]] = await connection.query(`
          SELECT id
          FROM fy_cycle
          WHERE store_id = ? AND name = ?
          LIMIT 1
        `, [storeId, newFYName]);

        if (existingFY) {
          console.log(` FY ${newFYName} already exists | Store ${storeId}`);
          continue;
        }

        // CLOSE ACTIVE FY (SAFETY)
        await connection.query(`
          UPDATE fy_cycle
          SET isActive = 0,
              closedAt = ?
          WHERE store_id = ? AND isActive = 1
        `, [currentTimestamp, storeId]);

        // INSERT NEW FY
        await connection.query(`
          INSERT INTO fy_cycle
            (store_id, name, isActive, startedAt, closedAt, expireAt)
          VALUES (?, ?, 1, ?, NULL, ?)
        `, [
          storeId,
          newFYName,
          newFYStart,
          newFYExpire
        ]);

        console.log(`✅ FY ${newFYName} created | Store ${storeId}`);
      }

      console.log('🎉 Fiscal Year auto-creation completed');

    } catch (error) {
      console.error(' FY Cron Error:', error);
    }
  },
  {
    timezone: 'Africa/Nairobi'
  }
);



// CHECK FOR EXPIRE PRODUCTS AND CHANGE THEIR STATUS ============


cron.schedule(
  '59 23 * * *', // Every day at 00:00 (midnight)
  async () => {

    let connection;

    try {
      connection = await getConnection();

      // Force MySQL timezone (EAT)
      await connection.query(`SET time_zone = '+03:00'`);

      // Get DB current date
      const [dbTime] = await connection.query(`SELECT CURDATE() AS today`);
      const today = dbTime[0].today;

      console.log(` Expiry Check Started | DB Date: ${today}`);

      const [result] = await connection.query(`
        UPDATE products
        SET product_status = 'false'
        WHERE expire_date IS NOT NULL
          AND expire_date != 'null'
          AND DATE(expire_date) < ?
          AND product_status != 'false'
      `, [today]);

      console.log(` ${result.affectedRows} product(s) marked as expired`);

    } catch (error) {
      console.error(' Error updating expired product status:', error);
    }
  },
  {
    timezone: 'Africa/Nairobi'
  }
);




// CHECK FOR STOCK AVAILABILITY AND CHANGE THEIR STATUS ============


cron.schedule(
  '* * * * *', // Runs every minute
  async () => {

    let connection;

    try {
      connection = await getConnection();

      // Force DB timezone (EAT)
      await connection.query(`SET time_zone = '+03:00'`);

      // DB time = source of truth
      const [dbTime] = await connection.query(`
        SELECT CURDATE() AS today, NOW() AS now
      `);

      const today = dbTime[0].today;

      const [result] = await connection.query(`
        UPDATE products 
        SET product_status = 'false'
        WHERE expire_date IS NOT NULL
          AND expire_date < ?
          AND product_status != 'false'
      `, [today]);

      console.log(` ${dbTime[0].now} | ${result.affectedRows} product(s) marked as expired`);

    } catch (error) {
      console.error(' Expired product cron error:', error);
    } 
  },
  {
    timezone: 'Africa/Nairobi'
  }
);



// INSERT REPORT OF THE STORE SUPPORT DAILY REPORT


cron.schedule(
  '59 23 * * *', // Every day at 23:59
  async () => {
    let conn;

    try {
      conn = await getConnection();

      // Force DB timezone
      await conn.query(`SET time_zone = '+03:00'`);

      const now = moment().tz('Africa/Nairobi');
      const start = now.clone().startOf('day').format('YYYY-MM-DD HH:mm:ss');
      const end = now.clone().endOf('day').format('YYYY-MM-DD HH:mm:ss');
      const reportDate = now.format('YYYY-MM-DD');
      const timestamp = now.format('YYYY-MM-DD HH:mm:ss');

      console.log(` Daily Report Cron Started | Date: ${reportDate}`);

      // Fetch stores with auto daily report enabled
      const [stores] = await conn.query(`
        SELECT id AS store_id
        FROM stores
        WHERE auto_daily_report = 1
      `);

      for (const { store_id } of stores) {

        // Get warehouses with activity today
        const [warehouses] = await conn.query(`
          SELECT DISTINCT warehouse_id FROM (
            SELECT warehouse_id FROM sales 
            WHERE store_id = ? AND created_at BETWEEN ? AND ?
            UNION
            SELECT warehouse_id FROM purchases 
            WHERE store_id = ? AND created_at BETWEEN ? AND ?
            UNION
            SELECT warehouse_id FROM expenses 
            WHERE store_id = ? AND created_at BETWEEN ? AND ?
          ) combined
        `, [
          store_id, start, end,
          store_id, start, end,
          store_id, start, end
        ]);

        for (const { warehouse_id } of warehouses) {

          // Prevent duplicate daily report
          const [exists] = await conn.query(`
            SELECT id FROM daily_reports
            WHERE store_id = ? AND warehouse_id = ? AND DATE(report_date) = ?
          `, [store_id, warehouse_id, reportDate]);

          if (exists.length > 0) {
            console.log(` Report already exists for Store ${store_id}, Warehouse ${warehouse_id}`);
            continue;
          }

          // Sales
          const [[sales]] = await conn.query(`
            SELECT IFNULL(SUM(grand_total), 0) AS total_sales
            FROM sales
            WHERE sale_status = 'APPROVED'
              AND store_id = ?
              AND warehouse_id = ?
              AND created_at BETWEEN ? AND ?
          `, [store_id, warehouse_id, start, end]);

          // Purchases
          const [[purchases]] = await conn.query(`
            SELECT IFNULL(SUM(grand_total), 0) AS total_purchases
            FROM purchases
            WHERE purchase_status = 'APPROVED'
              AND store_id = ?
              AND warehouse_id = ?
              AND created_at BETWEEN ? AND ?
          `, [store_id, warehouse_id, start, end]);

          // Expenses
          const [[expenses]] = await conn.query(`
            SELECT IFNULL(SUM(amount), 0) AS total_expenses
            FROM expenses
            WHERE approved = 1
              AND store_id = ?
              AND warehouse_id = ?
              AND created_at BETWEEN ? AND ?
          `, [store_id, warehouse_id, start, end]);

          // Insert daily report
          await conn.query(`
            INSERT INTO daily_reports (
              store_id,
              warehouse_id,
              report_date,
              total_sales,
              total_purchases,
              total_expenses,
              created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
          `, [
            store_id,
            warehouse_id,
            reportDate,
            sales.total_sales,
            purchases.total_purchases,
            expenses.total_expenses,
            timestamp
          ]);

          console.log(`Report saved | Store ${store_id}, Warehouse ${warehouse_id}`);
        }
      }

      console.log('Daily Report Generation Completed');

    } catch (err) {
      console.error('CRON ERROR - DAILY REPORT:', err);
    } 
  },
  {
    timezone: 'Africa/Nairobi'
  }
);



// UPDATE STATUS FOR ALL USER PASSWORD EXPIRED

cron.schedule('* * * * *', async () => {
  console.log(' Running password expiration check...');

  let conn;
  try {
      
      conn = await getConnection();
      
      // Force DB timezone
      await conn.query(`SET time_zone = '+03:00'`);
      
        // Current full timestamp (YYYY-MM-DD HH:mm:ss)
    const date = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');
      
    /* =========================
       EXPIRED PASSWORDS (≥ 90 DAYS)
    ========================= */
    const [expiredUsers] = await conn.query(`
      SELECT id, name, email
      FROM users
      WHERE last_password_change IS NOT NULL
        AND DATEDIFF(CURDATE(), last_password_change) >= 90
        AND mustChangePassword = 'false'
    `);

    if (expiredUsers.length === 0) {
      console.log(' No expired passwords found');
      return;
    }

    for (const user of expiredUsers) {
      await conn.query(`
        UPDATE users
        SET mustChangePassword = 'true', last_password_change = ?
        WHERE id = ?
      `, [date, user.id]);
    }

    console.log(` ${expiredUsers.length} users marked for password change`);

  } catch (error) {
    console.error(' Password expiry cron error:', error);
  } 
},
{
  timezone: 'Africa/Nairobi'
}
);



// SENT EMAIL TO STORE NEARLY TO EXPIRE =======================================


cron.schedule('50 4 * * *', async () => {
  let conn;

  try {
    conn = await getConnection();
    const now = moment().tz('Africa/Nairobi').startOf('day');

    /** -----------------------------
     * STORE EXPIRY HANDLING
     * ----------------------------- */

    // Fetch all active stores
    const [stores] = await conn.query(`SELECT * FROM stores WHERE status = 'active'`);

    const twilioClient = twilio(process.env.TWILIO_SID, process.env.TWILIO_AUTH_TOKEN);

    for (const store of stores) {
      // Calculate store expiry date
      let expiryDate = moment(store.start_date);
      switch (store.duration_type.toLowerCase()) {
        case 'days':
          expiryDate.add(store.duration_value, 'days');
          break;
        case 'months':
          expiryDate.add(store.duration_value, 'months');
          break;
        case 'years':
          expiryDate.add(store.duration_value, 'years');
          break;
        default:
          console.warn(`Unknown duration_type for store ${store.name}: ${store.duration_type}`);
          continue;
      }

      const daysRemaining = expiryDate.diff(now, 'days');

      // Send reminder if 5 days or fewer remaining
      if (daysRemaining <= 5 && daysRemaining > 0 && (!store.last_reminder_sent || moment(store.last_reminder_sent).format('YYYY-MM-DD') !== now.format('YYYY-MM-DD'))) {
        const message = `Store "${store.name}" will expire in ${daysRemaining} day(s). Please renew to avoid service interruption.`;

        try {
          await twilioClient.messages.create({
            body: message,
            from: process.env.TWILIO_PHONE_NUMBER,
            to: store.phone
          });

          await conn.query(`UPDATE stores SET last_reminder_sent = ? WHERE id = ?`, [now.format('YYYY-MM-DD HH:mm:ss'), store.id]);
          console.log(`SMS reminder sent to store "${store.name}"`);
        } catch (err) {
          console.error(`Failed to send SMS to store "${store.name}":`, err.message);
        }
      }

      // Block store and all related users if expired
      if (daysRemaining <= 0 && store.status !== 'blocked') {
        // Block store
        await conn.query(`UPDATE stores SET status = 'blocked' WHERE id = ?`, [store.id]);

        // Find all users linked to this store via user_stores
        const [users] = await conn.query(`SELECT user_id FROM user_stores WHERE store_id = ?`, [store.id]);
        const userIds = users.map(u => u.user_id);
        if (userIds.length > 0) {
          await conn.query(`UPDATE users SET account_disabled = 'true' WHERE id IN (?)`, [userIds]);
        }

        console.log(`Store "${store.name}" expired. Store blocked and ${userIds.length} related users disabled.`);
      }
    }

    console.log('Store expiry check completed.');

  } catch (error) {
    console.error('[CRON ERROR - STORE EXPIRY HANDLER]', error.message);
  }
}, {
  timezone: 'Africa/Nairobi'
});


// SENT MAIL TO NOTIFY USERS WHO ARE NEARLY PASSWORD TO EXPIRE =================


cron.schedule(
  '55 4 * * *', // Daily at 04:55 AM
  async () => {
    let conn;

    try {
      conn = await getConnection();

      // Force DB timezone to EAT
      await conn.query(`SET time_zone = '+03:00'`);

      const [dbTime] = await conn.query(`SELECT NOW() AS now`);
      const currentTimestamp = dbTime[0].now;

      console.log(` Password expiry cron started | DB Time: ${currentTimestamp}`);

      /* =========================
         EMAIL CONFIGURATION
      ========================= */
      const [emailConfig] = await conn.query(`
        SELECT host, port, username, password
        FROM system_mail_configuration
        LIMIT 1
      `);

      if (!emailConfig.length) {
        console.warn(' No email configuration found. Job aborted.');
        return;
      }

      const { host, port, username, password } = emailConfig[0];

      const transporter = nodemailer.createTransport({
        host,
        port: Number(port),
        secure: Number(port) === 465,
        auth: { user: username, pass: password }
      });

      /* =========================
         EXPIRED PASSWORDS (≥ 90 DAYS)
      ========================= */
      const [expiredUsers] = await conn.query(`
        SELECT id, name, email
        FROM users
        WHERE last_password_change IS NOT NULL
          AND DATEDIFF(CURDATE(), last_password_change) >= 90
          AND mustChangePassword = 'false'
      `);

      for (const user of expiredUsers) {
        await conn.query(`
          UPDATE users
          SET mustChangePassword = 'true'
          WHERE id = ?
        `, [user.id]);

        const mailText = `
Hello ${user.name},

Your password has expired and must be changed before your next login.

Regards,
DUKA ENTERPRISES PORTAL
        `.trim();

        try {
          await transporter.sendMail({
            from: username,
            to: user.email,
            subject: ' Password Expired - Action Required',
            text: mailText
          });

          await conn.query(`
            INSERT INTO mails (email, message, date, status)
            VALUES (?, ?, ?, 'true')
          `, [user.email || '', mailText, currentTimestamp]);

        } catch (err) {
          await conn.query(`
            INSERT INTO mails (email, message, date, status)
            VALUES (?, ?, ?, 'false')
          `, [user.email || '', mailText, currentTimestamp]);
        }
      }

      /* =========================
         NEAR EXPIRY REMINDER (85–89 DAYS)
      ========================= */
      const [nearExpiryUsers] = await conn.query(`
        SELECT id, name, email
        FROM users
        WHERE last_password_change IS NOT NULL
          AND DATEDIFF(CURDATE(), last_password_change) BETWEEN 85 AND 89
          AND mustChangePassword = 'false'
      `);

      for (const user of nearExpiryUsers) {
        const mailText = `
Hello ${user.name},

Your password will expire in less than 5 days.
Please change it to avoid login issues.

Regards,
DUKA ENTERPRISES PORTAL
        `.trim();

        try {
          await transporter.sendMail({
            from: username,
            to: user.email,
            subject: ' Password Expiry Reminder',
            text: mailText
          });

          await conn.query(`
            INSERT INTO mails (email, message, date, status)
            VALUES (?, ?, ?, 'true')
          `, [user.email || '', mailText, currentTimestamp]);

        } catch (err) {
          await conn.query(`
            INSERT INTO mails (email, message, date, status)
            VALUES (?, ?, ?, 'false')
          `, [user.email || '', mailText, currentTimestamp]);
        }
      }

      console.log(' Password expiry enforcement & reminders completed.');

    } catch (error) {
      console.error(' CRON ERROR - PASSWORD EXPIRY HANDLER:', error);
    } 
  },
  {
    timezone: 'Africa/Nairobi'
  }
);



// SENT WAREHOUSES SUMMARY REPORT ============================

cron.schedule(
  '59 23 * * *', // Daily at 23:59
  async () => {
    console.log(' Running daily warehouse summary reports...');
    let conn;

    try {
      conn = await getConnection();

      // Force DB timezone
      await conn.query(`SET time_zone = '+03:00'`);

      const now = moment().tz('Africa/Nairobi');
      const today = now.format('YYYY-MM-DD');
      const start = `${today} 00:00:00`;
      const end = `${today} 23:58:00`;
      const currentTimestamp = now.format('YYYY-MM-DD HH:mm:ss');

      const [stores] = await conn.query(`
        SELECT id FROM stores WHERE status = 'true'
      `);

      for (const store of stores) {
        const storeId = store.id;

        // ================= SMS CONFIG =================
        const [[smsConfig]] = await conn.query(
          `SELECT * FROM sms_configuration WHERE store_id = ? LIMIT 1`,
          [storeId]
        );
        if (!smsConfig) continue;

        const encodedAuth = Buffer
          .from(`${smsConfig.username}:${smsConfig.password}`)
          .toString('base64');

        // ================= MAIL CONFIG =================
        const [[mailConfig]] = await conn.query(
          `SELECT * FROM mail_configuration WHERE store_id = ? LIMIT 1`,
          [storeId]
        );
        if (!mailConfig) continue;

        const transporter = nodemailer.createTransport({
          host: mailConfig.host,
          port: parseInt(mailConfig.port),
          secure: parseInt(mailConfig.port) === 465,
          auth: {
            user: mailConfig.username,
            pass: mailConfig.password
          }
        });

        // ================= WAREHOUSES =================
        const [warehouses] = await conn.query(`
          SELECT 
            w.id AS warehouseId,
            w.name AS warehousename,
            w.auto_send_summary_report,
            s.phone,
            s.email
          FROM warehouses w
          JOIN stores s ON s.id = w.storeId
          WHERE s.id = ?
        `, [storeId]);

        for (const wh of warehouses) {
          if (wh.auto_send_summary_report !== 1) continue;
          if (!wh.email || !/\S+@\S+\.\S+/.test(wh.email)) continue;
          if (!wh.phone || wh.phone.length < 8) continue;

          const warehouseId = wh.warehouseId;

          // ================= SALES =================
          const [[sales]] = await conn.query(`
            SELECT 
              IFNULL(SUM(grand_total),0) AS total_sales,
              IFNULL(SUM(total_cost),0) AS total_sales_cost,
              IFNULL(SUM(grand_total - total_cost),0) AS sales_profit,
              MAX(fy_id) AS fy_id
            FROM sales
            WHERE sale_status='APPROVED'
              AND warehouse_id=?
              AND created_at BETWEEN ? AND ?
          `, [warehouseId, start, end]);

          const [[purchases]] = await conn.query(`
            SELECT IFNULL(SUM(grand_total),0) AS total_purchases
            FROM purchases
            WHERE purchase_status='APPROVED'
              AND warehouse_id=?
              AND created_at BETWEEN ? AND ?
          `, [warehouseId, start, end]);

          const [[expenses]] = await conn.query(`
            SELECT IFNULL(SUM(amount),0) AS total_expenses
            FROM expenses
            WHERE approved=1
              AND warehouse_id=?
              AND expense_date BETWEEN ? AND ?
          `, [warehouseId, start, end]);

          const [[adjustments]] = await conn.query(`
            SELECT IFNULL(SUM(qty_adjusted),0) AS total_adjusted_qty
            FROM stock_adjustments
            WHERE adjust_status='APPROVED'
              AND warehouse_id=?
              AND adjusted_at BETWEEN ? AND ?
          `, [warehouseId, start, end]);

          // ================= INSERT DAILY REPORT =================
          await conn.query(`
  INSERT INTO warehouse_daily_report (
    report_date, warehouse_id, fy_id,
    total_sales, total_sales_cost, sales_profit,
    total_purchases, total_expenses, total_adjusted_qty,
    created_at, updated_at
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  ON DUPLICATE KEY UPDATE
    fy_id = VALUES(fy_id),
    total_sales = VALUES(total_sales),
    total_sales_cost = VALUES(total_sales_cost),
    sales_profit = VALUES(sales_profit),
    total_purchases = VALUES(total_purchases),
    total_expenses = VALUES(total_expenses),
    total_adjusted_qty = VALUES(total_adjusted_qty),
    updated_at = VALUES(updated_at)
`, [
  today,
  warehouseId,
  sales.fy_id || 0,
  sales.total_sales,
  sales.total_sales_cost,
  sales.sales_profit,
  purchases.total_purchases,
  expenses.total_expenses,
  adjustments.total_adjusted_qty,
  currentTimestamp,
  currentTimestamp
]);


          // ================= MESSAGE =================
          const message =
`RIPOTI YA MAUZO ${today}
Duka: ${wh.warehousename}
Mauzo: TZS ${sales.total_sales.toLocaleString()}
Faida: TZS ${sales.sales_profit.toLocaleString()}
Manunuzi: TZS ${purchases.total_purchases.toLocaleString()}
Matumizi: TZS ${expenses.total_expenses.toLocaleString()}
Adjustment: ${adjustments.total_adjusted_qty.toLocaleString()}`;

          // ================= EMAIL =================
          try {
            await transporter.sendMail({
              from: mailConfig.username,
              to: wh.email,
              subject: 'RIPOTI YA MAUZO',
              text: message
            });

            await conn.query(
              `INSERT INTO mails (email, message, date, status)
               VALUES (?, ?, ?, 'true')`,
              [wh.email, message, currentTimestamp]
            );
          } catch {
            await conn.query(
              `INSERT INTO mails (email, message, date, status)
               VALUES (?, ?, ?, 'false')`,
              [wh.email, message, currentTimestamp]
            );
          }

          // ================= SMS =================
          try {
            await axios.post(
              smsConfig.api_url,
              { from: smsConfig.sender_name, to: wh.phone, text: message },
              {
                headers: {
                  Authorization: `Basic ${encodedAuth}`,
                  'Content-Type': 'application/json'
                },
                timeout: 10000
              }
            );

            await conn.query(`
              INSERT INTO sms (store_id, phone, message, status, date)
              VALUES (?, ?, ?, 'true', ?)
            `, [storeId, wh.phone, message, currentTimestamp]);

          } catch {
            await conn.query(`
              INSERT INTO sms (store_id, phone, message, status, date)
              VALUES (?, ?, ?, 'false', ?)
            `, [storeId, wh.phone, message, currentTimestamp]);
          }
        }
      }

      console.log(' Daily warehouse summary completed');

    } catch (err) {
      console.error(' CRON job error:', err);
    } 
  },
  {
    timezone: 'Africa/Nairobi'
  }
);




// SUBSCRIPTION REMAINDER ==============================

cron.schedule('0 9 * * *', async () => {
  let conn;

  try {
    conn = await getConnection();

    // Use Nairobi time as "today"
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');
    const todayDateOnly = new Date(now.split(' ')[0]); // YYYY-MM-DD only

    const [reminders] = await conn.query(`
      SELECT sr.*, s.email, s.end_date, s.name as store_name
      FROM subscription_reminders sr
      JOIN stores s ON s.id = sr.store_id
      WHERE sr.reminder_type = 'email'
    `);

    const emailsSent = [];

    for (const reminder of reminders) {
      const { store_id, email, end_date, days_before_expiry, store_name } = reminder;

      const expiry = new Date(end_date);

      // calculate reminder date
      const reminderTargetDate = new Date(expiry);
      reminderTargetDate.setDate(expiry.getDate() - days_before_expiry);

      const reminderDateOnly = new Date(reminderTargetDate.toISOString().split('T')[0]);

      if (todayDateOnly.getTime() === reminderDateOnly.getTime()) {
        
        const subject = `Kumbusho la Muda wa Usajili kwa ${store_name}`;
const message = `
  Mpendwa,

 Unakumbushwa kwamba usajili wako wa <strong>${store_name}</strong> 
  utaisha mnamo tarehe <strong>${end_date}</strong>.

  Tafadhali fanya malipo ya usajili wako kabla ya kuisha, ili kuepuka usumbufu wa huduma.

  Asante.
`;


        try {
          await sendMail(email, subject, message);
          emailsSent.push(email);

          // Insert new record into reminders log using Nairobi server time
          await conn.query(
            `INSERT INTO subscription_reminders_log 
             (store_id, email, reminder_type, reminder_date, days_before_expiry) 
             VALUES (?, ?, 'email', ?, ?)`,
            [store_id, email, now, days_before_expiry]
          );

        } catch (err) {
          console.error(` Failed to send email to ${email}:`, err.message);
        }
      }
    }

    console.log(`[CRON] Sent ${emailsSent.length} reminder emails.`);
  } catch (err) {
    console.error('[CRON] Reminder job error:', err.message);
  }
}, {
  timezone: 'Africa/Nairobi'
});



// AUTO UPDATE EXPIRED USER ACCOUNT ======================

cron.schedule('* * * * *', async () => {

  let conn;
  
  try {
    // Get current Nairobi datetime with full time format
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

     conn = await getConnection();
    if (!conn) {
      console.error('[CRON] No DB connection');
      return;
    }


    // Find expired stores before now
    const [expiredStores] = await conn.query(`
      SELECT id FROM stores 
      WHERE 
        end_date != 'null' 
        AND end_date != 0 
        AND end_date < ? 
        AND status = 'true'
    `, [now]);

    if (expiredStores.length === 0) {
      console.log('No expired stores found.');
      return;
    }

    const storeIds = expiredStores.map(store => store.id);

    // Disable the expired stores
    await conn.query(`UPDATE stores SET status = 'false' WHERE id IN (?)`, [storeIds]);

    // Get all user_ids assigned to these expired stores
    const [assignedUsers] = await conn.query(`
      SELECT DISTINCT user_id FROM user_stores WHERE store_id IN (?)
    `, [storeIds]);

    const userIds = assignedUsers.map(user => user.user_id);

    if (userIds.length > 0) {
      // Disable the users assigned to expired stores
      await conn.query(`
        UPDATE users 
        SET accountExpDate = 'null', accountExpireStatus = 'true' 
        WHERE id IN (?)
      `, [userIds]);
      console.log(` Disabled ${userIds.length} users related to expired stores.`);
    }

  } catch (err) {
    console.error('[CRON] Error:', err);
  }
}, {
  timezone: 'Africa/Nairobi'
});


// REAL TIME SENT SUMMARY SALES REPORT TO STORE OWNER =================



// DATABASE BACKUP SCHEDULING ======================

// Function to insert metadata into backups table

const backupDir = path.join(__dirname, 'DATABASE_BACKUPS');
if (!fs.existsSync(backupDir)) {
  fs.mkdirSync(backupDir, { recursive: true });
}

async function insertBackupRecord(fileName, filePath) {
  const conn = await getConnection();

  await conn.query(`
    INSERT INTO backups (file_name, file_path) VALUES (?, ?)
  `, [fileName, filePath]);
}

// Create backup and log to DB
async function createBackup() {
  const date = new Date().toISOString().replace(/:/g, '-').slice(0, 19);
  const fileName = `backup-${date}.sql`;
  const filePath = path.join(backupDir, fileName);

  const mysqldumpPath = `"F:\\XAMPP\\mysql\\bin\\mysqldump"`; 
  const command = `${mysqldumpPath} -h${process.env.DB_HOST} -P${process.env.DB_PORT} -u${process.env.DB_USERNAME} --password=${process.env.DB_PASSWORD} ${process.env.DB_NAME} > "${filePath}"`;

  exec(command, (error) => {
    if (error) {
      console.error('❌ Backup failed:', error.message);
      return;
    }
   insertBackupRecord(fileName, filePath);
  });
}

// Run daily at 2:00 AM
cron.schedule('12 23 * * *', () => {
  console.log('⏰ Scheduled database backup running...');
  createBackup();
});


// DELETE FOR UNPAID SUBSCRIBER =============================

async function deleteUnpaidStores() {
  const conn = await getConnection();
  const oneMonthAgo = moment().tz('Africa/Nairobi').subtract(1, 'month').format('YYYY-MM-DD HH:mm:ss');

  try {
    const [storesToDelete] = await conn.query(`
      SELECT s.id, s.user_id FROM stores s
      LEFT JOIN store_payments p ON s.id = p.store_id AND p.payment_date > ?
      WHERE p.id IS NULL
    `, [oneMonthAgo]);

    if (storesToDelete.length === 0) {
      console.log("No unpaid stores to delete.");
      return;
    }

    for (const store of storesToDelete) {
      await conn.beginTransaction();

      try {
        const [users] = await conn.query(`SELECT user_id FROM user_stores WHERE store_id = ?`, [store.id]);

        // Delete user_stores
        for (const user of users) {
          await conn.query(`DELETE FROM user_stores WHERE user_id = ?`, [user.user_id]);
        }

        // Delete users
        await conn.query(`DELETE FROM users WHERE id = ?`, [store.user_id]);

        // Delete orphan payments (safety)
        await conn.query(`DELETE FROM store_payments WHERE store_id = ?`, [store.id]);

        // Delete store
        await conn.query(`DELETE FROM stores WHERE id = ?`, [store.id]);

        await conn.commit();
        console.log(`Deleted unpaid store ID: ${store.id}`);

      } catch (innerErr) {
        await conn.rollback();
        console.error(`Error deleting store ID ${store.id}:`, innerErr.message);
      }
    }
  } catch (err) {
    console.error('Failed to fetch or delete unpaid stores:', err.message);
  }
}

// Run daily at 2am Nairobi time
cron.schedule('0 2 * * *', () => {
  deleteUnpaidStores().catch(console.error);
}, {
  timezone: 'Africa/Nairobi'
});

// Backup also runs at 2 AM Nairobi time (change time as needed)
cron.schedule('0 2 * * *', () => {
  console.log('⏰ Scheduled database backup running...');
  createBackup();
}, {
  timezone: 'Africa/Nairobi'
});


// RUNNING A SCHEDULES EVERY SECONDS TO CHECK USER IF DISABLED ========================

async function updateLoginAttempt() {
  

  try {
      
      const conn = await getConnection();
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    const [result] = await conn.query(`
      UPDATE users 
      SET 
        loginAttempts = 0, 
        expiresAt = 'null', 
        accountDisabled = 'false', 
        attemptStatus = 'false' 
      WHERE expiresAt IS NOT NULL AND expiresAt <= ?
    `, [now]);

    console.log(`Login attempts reset for ${result.affectedRows} user(s) at ${now}`);
  } catch (err) {
    console.error('Failed to run login attempt reset schedule:', err.message);
  }
}

// Run every minute (cron doesn't support per-second scheduling)
cron.schedule('* * * * *', () => {
  updateLoginAttempt().catch(console.error);
}, {
  timezone: 'Africa/Nairobi'
});







// CRONS AND SCHEDULES =========================================



// AUTO UPDATE STORES STATUS IF STORE HAVE EXPIRE DATE ========================



// Run every day at 00:30 AM Nairobi time
cron.schedule('* * * * *', async () => {
  console.log('⏰ Running daily store status update...');
 
 
  try {
    conn = await getConnection();
    
    
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    const [result] = await conn.query(`
      UPDATE stores 
      SET status = 'Expired' 
      WHERE status = 'true' 
        AND end_date != 'null' 
        AND end_date < ?
    `, [now]);

  } catch (err) {
    console.error(' Error updating store statuses:', err);
  } 
 
}, {
  timezone: 'Africa/Nairobi'
});







// USER MANAGEMENT API


// Login with Barcode Scanner ==================

// Generate QR Code for a User (Login Flow)

router.get('/2fa/setup/login', async (req, res) => {
  try {
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    const { phone } = req.query;

    if (!phone) {
      return res.status(400).json({ message: 'Phone number is required' });
    }

    // Generate secret
    const secret = speakeasy.generateSecret({
      name: `DukaEnterPrisePortal (${phone})`,
      length: 32,
    });

    // Generate QR code image (data URL)
    const qrCode = await qrcode.toDataURL(secret.otpauth_url);

    const conn = await getConnection();

    // Save secret and enable 2FA flag
    await conn.query(
      `UPDATE users SET twofa_secret = ?, is_2fa_enabled = 1 WHERE phone = ?`,
      [secret.base32, phone]
    );

    // Only return the QR code image for scanning — no secret returned
    return res.json({ qrCode });

  } catch (err) {
    console.error('Error generating 2FA QR code:', err);
    return res.status(500).json({ message: 'Failed to generate QR code' });
  }
});



// verify user account ================================

router.post('/verify/user/account', async (req, res) => {
  const { uuid, password } = req.body;
  const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');
   
  let conn;
  try {
    conn = await getConnection();

    // Check if token exists and is valid
    const [rows] = await conn.query(
      `SELECT id, activation_token 
       FROM users 
       WHERE activation_token = ?`,
      [uuid]
    );

    if (rows.length === 0 ) {
      return res.json({ message: "No token" });
    }

    const user = rows[0];

    const saltRounds = 12;
    const hashedPassword = await bcrypt.hash(password, saltRounds);
   
    // Update password & mark account verified
    await conn.query(
      `UPDATE users 
       SET activation_token = 'null', isFirstLogin = 'false', last_password_change = ?, password = ?, mustChangePassword = 'false', userStatus = 'true' 
       WHERE id = ?`,
      [now, hashedPassword, user.id]
    );

    await conn.query(
      `INSERT INTO password_history (user_id, password_hash, changed_at) VALUES (?, ?, ?)`,
      [user.id, hashedPassword, now]
    );

    res.status(200).json({ message: "Account verified successfully. You can now log in." });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Server error" });
  } 
});




// Verify 2FA Token and issue JWT


router.post('/2fa/verify', async (req, res) => {
  const { phone, token } = req.body;

  try {
    const conn = await getConnection();

    const [rows] = await conn.query(`
      SELECT id, name, role, twofa_secret, accessAllStores, accessAllWarehouses, profileCompleted 
      FROM users WHERE phone=?
    `, [phone]);

    if (!rows.length) return res.status(404).json({ message: 'User not found' });

    const user = rows[0];

    const verified = speakeasy.totp.verify({
      secret: user.twofa_secret,
      encoding: 'base32',
      token,
      window: 1
    });

    if (!verified) {
      return res.status(401).json({
        success: false,
        message: 'Invalid 2FA token. Please scan QR Code again.'
      });
    }

    const payload = {
      id: user.id,
      name: user.name,
      role: user.role,
      isSuperUser: user.role === 1,
      accessAllStores: user.accessAllStores === 1,
      accessAllWarehouses: user.accessAllWarehouses === 1,
      profileCompleted: user.profileCompleted
    };

    const accessToken = jwt.sign(payload, process.env.ACCESS_TOKEN, { expiresIn: '30m' });

    return res.json({ success: true, token: accessToken });

  } catch (err) {
    console.error('2FA verification error:', err);
    return res.status(500).json({ message: 'Server error' });
  }
});



// User Login Authentication and Authorization

router.post('/login', async (req, res) => {
  let conn;

  try {
    const nowMoment = moment().tz('Africa/Nairobi');
    const now = nowMoment.format('YYYY-MM-DD HH:mm:ss');
    const todayDate = nowMoment.format('YYYY-MM-DD');
    const expiresAt = nowMoment.clone().add(15, 'minutes').format('YYYY-MM-DD HH:mm:ss');

    conn = await getConnection();
    const { phoneNumber, password } = req.body;

    // Get full user info + role in one query
    const [users] = await conn.query(`
      SELECT u.*, r.name AS roleName 
      FROM users u
      INNER JOIN roles r ON u.role = r.id
      WHERE u.phone = ?`, [phoneNumber]);

    if (!users.length) {
      return res.status(400).json({ message: `Phone number ${phoneNumber} not registered.` });
    }

    const user = users[0];

    // Account lock check
    if (user.accountDisabled === 'true') {
      return res.status(403).json({ message: 'Account is temporarily locked. Please try later.' });
    }

    // Check password
    const isMatch = await bcrypt.compare(password, user.password);
    if (!isMatch) {
      const newAttempts = (parseInt(user.loginAttempts || '0') + 1);
      await conn.query(`UPDATE users SET loginAttempts = ? WHERE phone = ?`, [newAttempts, phoneNumber]);

      if (newAttempts >= 3) {
        await conn.query(`UPDATE users SET accountDisabled='true', attemptStatus='true', expiresAt=? WHERE phone=?`, [expiresAt, phoneNumber]);
        return res.status(403).json({ message: 'Account locked for 15 minutes due to multiple failed login attempts.' });
      }

      return res.status(401).json({ message: 'Incorrect phone number or password.' });
    }

    // Reset login attempts on success
    await conn.query(`UPDATE users SET loginAttempts=0, attemptStatus='false' WHERE phone=?`, [phoneNumber]);

    // Check account expiry
    if (user.accountExpireStatus === 'true' && user.accountExpDate && moment(user.accountExpDate).isSameOrBefore(todayDate)) {
      return res.status(403).json({ message: `${user.name}, your account has expired!` });
    }

    // Handle 2FA
    if (user.is_2fa_enabled === 1) {
      return res.status(200).json({
        requires2FA: true,
        phone: user.phone,
        name: user.name,
        message: 'Two-Factor Authentication required.',
        profileCompleted: user.profileCompleted
      });
    }

    // Determine store access (skip for superuser)
    let store = null;
    const isSuperUser = user.role === 1;

    if (!isSuperUser) {
      const [storeResult] = await conn.query(`
        SELECT s.id, s.status, s.end_date 
        FROM user_stores us
        INNER JOIN stores s ON us.store_id = s.id
        WHERE us.user_id = ? LIMIT 1
      `, [user.id]);

      store = storeResult[0];

      // SKIP CHECK IF THE STORE EXPIRED OR INACTIVE
      /* 
      
      if (!store || store.status !== 'true' || (store.end_date && moment(store.end_date).isBefore(nowMoment))) {
        return res.status(403).json({ message: 'Assigned store inactive or expired. Contact admin.' });
      } 
      
      */

    }

    // Get user warehouses
    const [userWarehouses] = await conn.query(`SELECT warehouse_id FROM user_warehouses WHERE user_id=?`, [user.id]);
    const warehouseIds = userWarehouses.map(w => w.warehouse_id);

    // Create JWT (30 minutes expiry)
    const payload = {
      id: user.id,
      name: user.name,
      role: user.role,
      isFirstLogin: user.isFirstLogin,
      mustChangePassword: user.mustChangePassword,
      isSuperUser,
      accessAllStores: user.accessAllStores === 1,
      accessAllWarehouses: user.accessAllWarehouses === 1,
      profileCompleted: user.profileCompleted,
      storeId: store?.id || null,
      warehouses: warehouseIds
    };

    const accessToken = jwt.sign(payload, process.env.ACCESS_TOKEN, { expiresIn: '30m' });

    // Log login
    await conn.query(`
      INSERT INTO logs (user_id, store_id, action, description, createdAt, createdBy)
      VALUES (?, ?, 'LOGIN VERIFICATION', ?, ?, ?)
    `, [user.id, store?.id || null, `${user.name} (${user.id}) logged in successfully`, now, user.name]);

    // Update last active
    await conn.query(`UPDATE users SET lastActive=?, loggedIn='TRUE' WHERE id=?`, [now, user.id]);

    // Response
    return res.status(200).json({
      token: accessToken,
      isFirstLogin: user.isFirstLogin,
      mustChangePassword: user.mustChangePassword,
      userStatus: user.userStatus,
      accountDisabled: user.accountDisabled,
      warehouseIds,
      storeId: store?.id || null,
      name: user.name,
      profileCompleted: user.profileCompleted
    });

  } catch (err) {
    console.error('Login Error:', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});




// VERIFY USER ACCOUNT ====================================

router.post('/verifyUserAccount', async (req, res) => {
  let connection;

  try {
    connection = await getConnection();
    const { phoneNumber } = req.body;

    if (!phoneNumber) {
      return res.status(400).json({ message: "Phone number is required." });
    }

    // Check if phone number exists
    const [result] = await connection.execute(
      'SELECT * FROM users WHERE phone = ?',
      [phoneNumber]
    );

    if (result.length === 0) {
      return res.status(400).json({ message: "Phone number does not match our records." });
    }

    const user = result[0];

    // Update data and flag for change
    await connection.execute(
      `UPDATE users SET isFirstLogin = "false" WHERE id = ?`,
      [user.id]
    );

    return res.status(200).json({ message: ' Account verified successfully.' });

  } catch (error) {
    console.error('Verify User Error:', error);
    return res.status(500).json({ message: 'Something went wrong. Please try again later.' });
  }
});


// FORGOT PASSWORD REQUEST ====================================

router.post('/forgotPassword', async (req, res) => {
  let connection;

  try {
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');
    connection = await getConnection();
    const { mode, phoneNumber, email } = req.body;

    const otp = Math.floor(100000 + Math.random() * 900000);
    const hashedPassword = await bcrypt.hash(otp.toString(), 12);

    let user = null;
    let store_id = null;

    // ==================== PHONE MODE ====================
    if (mode === 'phone') {
      if (!phoneNumber) {
        return res.status(400).json({ error: 'Phone number is required.' });
      }

      const [result] = await connection.query('SELECT * FROM users WHERE phone = ?', [phoneNumber]);
      if (result.length === 0) {
        return res.status(200).json({ message: 'Phone number does not match our records.' });
      }

      user = result[0];

      const [stores] = await connection.execute(
        'SELECT store_id FROM user_stores WHERE user_id = ? LIMIT 1',
        [user.id]
      );
      store_id = stores[0]?.store_id;

      if (!store_id) {
        return res.status(400).json({ error: 'Store ID not found for user.' });
      }

      const [smsConfig] = await connection.execute(
        'SELECT * FROM sms_configuration WHERE store_id = ? LIMIT 1',
        [store_id]
      );

      if (smsConfig.length === 0) {
        return res.status(500).json({ error: 'SMS configuration not found.' });
      }

      const { api_url, sender_name, username, password } = smsConfig[0];
      const smsText = `Ndg ${user.name} neno siri jipya ni ${otp}`;
      const encodedAuth = Buffer.from(`${username}:${password}`).toString('base64');

      try {
        await axios.post(api_url, {
          from: sender_name,
          text: smsText,
          to: user.phone
        }, {
          headers: {
            'Authorization': `Basic ${encodedAuth}`,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
          },
          timeout: 10000
        });

        await connection.execute(
          `INSERT INTO sms (store_id, phone, message, date, status) VALUES (?, ?, ?, ?, ?)`,
          [store_id, user.phone || '', smsText, now, 'true']
        );

        await connection.execute(
          `UPDATE users SET password = ?, mustChangePassword = "true" WHERE id = ?`,
          [hashedPassword, user.id]
        );

        return res.status(200).json({ message: 'Temporary password sent via SMS.' });

      } catch (smsError) {
        console.error('SMS Sending Failed:', smsError.message);

        await connection.execute(
          `INSERT INTO sms (store_id, phone, message, date, status) VALUES (?, ?, ?, ?, ?)`,
          [store_id, user.phone || '', smsText, now, 'false']
        );

        return res.status(500).json({ error: 'Failed to send SMS. Try again later.' });
      }
    }

    // ==================== EMAIL MODE ====================
    else if (mode === 'email') {
      if (!email) {
        return res.status(400).json({ error: 'Email is required.' });
      }

      const [result] = await connection.query('SELECT * FROM users WHERE email = ?', [email]);
      if (result.length === 0) {
        return res.status(200).json({ message: 'Email does not match our records.' });
      }

      user = result[0];

      const [stores] = await connection.execute(
        'SELECT store_id FROM user_stores WHERE user_id = ? LIMIT 1',
        [user.id]
      );
      store_id = stores[0]?.store_id;

      if (!store_id) {
        return res.status(400).json({ error: 'Store ID not found for user.' });
      }

      const [emailConfig] = await connection.execute(
        'SELECT * FROM mail_configuration WHERE store_id = ? LIMIT 1',
        [store_id]
      );

      if (emailConfig.length === 0) {
        return res.status(500).json({ error: 'Email configuration not found.' });
      }

      const { host, port, username, password } = emailConfig[0];

      const transporter = nodemailer.createTransport({
        host: host,
        port: parseInt(port),
        secure: parseInt(port) === 465,
        auth: {
          user: username,
          pass: password
        }
      });

      const mailText = `Dear ${user.name},\n\nYour new temporary password is: ${otp}\n\nPlease login and change your password.\n\nThanks.`;

      try {
        await transporter.sendMail({
          from: username,
          to: user.email,
          subject: 'Reset Password',
          text: mailText
        });

        await connection.execute(
          `INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, ?)`,
          [user.email || '', mailText, now, 'true']
        );

        await connection.execute(
          `UPDATE users SET password = ?, mustChangePassword = "true" WHERE id = ?`,
          [hashedPassword, user.id]
        );

        return res.status(200).json({ message: 'Temporary password sent via Email.' });

      } catch (mailError) {
        console.error('Email Sending Failed:', mailError.message);

        await connection.execute(
          `INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, ?)`,
          [user.email || '', mailText, now, 'false']
        );

        return res.status(500).json({ error: 'Failed to send Email. Try again later.' });
      }
    }

    return res.status(400).json({ error: 'Invalid reset mode provided.' });

  } catch (error) {
    console.error('❌ Unexpected Error:', error);
    return res.status(500).json({ error: 'Something went wrong. Try again later.' });
  }
});

// GET USER ROLE =======================

router.get('/get-user-role', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id; // Token middleware should set this

  let connection;
  try {
    connection = await getConnection();

    const [rows] = await connection.query(
      `SELECT r.name AS name 
       FROM users u 
       JOIN roles r ON u.role = r.id 
       WHERE u.id = ? LIMIT 1`,
      [userId]
    );

    if (rows.length > 0) {
      res.json({ role: rows[0].name });
    } else {
      res.status(404).json({ message: 'User or role not found' });
    }
  } catch (err) {
    console.error('Error fetching user role:', err);
    res.status(500).json({ message: 'Internal server error' });
  }
});


// < =================== PENDING REQUEST SENDS BY USER ================================= >


router.get('/get/app/notifications', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const formattedDate = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

  let connection;

  try {
    connection = await getConnection();

    // Get role name
    const [[{ name: roleName } = {}]] = await connection.query(
      'SELECT name FROM roles WHERE id = (SELECT role FROM users WHERE id = ?)',
      [userId]
    );

    if (!roleName) {
      return res.json({ message: 'Invalid role access.' });
    }

    const isAdmin = roleName === 'ADMIN';
    const isManager = roleName === 'MANAGER';

    // if (!isAdmin && !isManager) {
    //  return res.status(403).json({ message: 'Access denied. Only MANAGER or ADMIN allowed.' });
    // } 

    // SUPER ADMIN: full access, no filtering
    if (isAdmin) {
      const [[{ count: newSales }]] = await connection.query(`SELECT COUNT(*) as count FROM sales WHERE sale_status = 'DRAFT'`);
      const [[{ count: cancelSales }]] = await connection.query(`SELECT COUNT(*) as count FROM sales WHERE sale_status = 'AWAIT'`);
      const [[{ count: expiredProduct }]] = await connection.query(`SELECT COUNT(*) as count FROM products WHERE expire_date < ?`, [formattedDate]);
      const [[{ count: outstockProduct }]] = await connection.query(`SELECT COUNT(*) as count FROM products WHERE qty <= 0 `);
      const [[{ count: newPurchases }]] = await connection.query(`SELECT COUNT(*) as count FROM purchases WHERE purchase_status = 'PENDING'`);
      const [[{ count: lowStockProducts }]] = await connection.query(`SELECT COUNT(*) as count FROM products WHERE qty <= 10`);
      const [[{ count: helpCenter }]] = await connection.query(`SELECT COUNT(*) as count FROM help_desk WHERE status = 'open'`);
      const [[{ count: newUser }]] = await connection.query(`SELECT COUNT(*) as count FROM users WHERE userStatus = 'false'`);
      const [[{ count: stockAdjust }]] = await connection.query(`SELECT COUNT(*) as count FROM stock_adjustments WHERE adjust_status = 'PENDING'`);

      return res.json({
        newSales,
        cancelSales,
        expiredProduct,
        outstockProduct,
        newPurchases,
        lowStockProducts,
        helpCenter,
        newUser,
        stockAdjust
      });
    }

    // MANAGER: Filter by assigned stores and warehouses
    const [storeRows] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
    );
    const [warehouseRows] = await connection.query(
      'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
    );

    const storeIds = storeRows.map(r => r.store_id);
    const warehouseIds = warehouseRows.map(r => r.warehouse_id);

    if (storeIds.length === 0 && warehouseIds.length === 0) {
      return res.json({
        newSales: 0,
        cancelSales: 0,
        expiredProduct: 0,
        outstockProduct: 0,
        newPurchases: 0,
        lowStockProducts: 0,
        newUser: 0,
        stockAdjust: 0
      });
    }

    const filters = [];
    const params = [];

    if (storeIds.length > 0) {
      filters.push(`store_id IN (${storeIds.map(() => '?').join(',')})`);
      params.push(...storeIds);
    }

    if (warehouseIds.length > 0) {
      filters.push(`warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
      params.push(...warehouseIds);
    }

    const filterClause = filters.length ? `WHERE ${filters.join(' AND ')}` : '';

    // NEW SALES ========
    const [[{ count: newSales }]] = await connection.query(
      `SELECT COUNT(*) as count FROM sales ${filterClause} AND sale_status = 'DRAFT'`,
      params
    );

    // REQUEST FOR CANCEL SALES ========
    const [[{ count: cancelSales }]] = await connection.query(
      `SELECT COUNT(*) as count FROM sales ${filterClause} AND sale_status = 'AWAIT'`,
      params
    );

    // CHECK EXPIRED PRODUCT ========
    params.push(formattedDate);
    const [[{ count:  expiredProduct}]] = await connection.query(
      `SELECT COUNT(*) as count FROM products ${filterClause} AND expire_date < ?`,
      params
    );

    // CHECK OUTSTOCK PRODUCT ========
    const [[{ count:  outstockProduct}]] = await connection.query(
      `SELECT COUNT(*) as count FROM products ${filterClause} AND qty <= 0`,
      params
    );

    // COUNT NEW PURCHASES =======
    const [[{ count: newPurchases }]] = await connection.query(
      `SELECT COUNT(*) as count FROM purchases ${filterClause} AND purchase_status = 'PENDING'`,
      params
    );

    // COUNT LOW STOCK PRODUCTS
    const [[{ count: lowStockProducts }]] = await connection.query(
      `SELECT COUNT(*) as count FROM products ${filterClause ? `${filterClause} AND` : 'WHERE'} qty <= 10`,
      params
    );


    // COUNT NEW USER AND UN APPROVED USER
    const [[{ count: newUser }]] = await connection.query(
      `SELECT COUNT(*) as count FROM users WHERE userStatus = 'false'`
    );

    // COUNT STOCK TO ADJUST
    const [[{ count: stockAdjust }]] = await connection.query(
      `SELECT COUNT(*) as count FROM stock_adjustments ${filterClause ? `${filterClause} AND` : 'WHERE'} adjust_status = 'PENDING'`,
      params
    );
    
    return res.json({
      newSales,
      cancelSales,
      expiredProduct,
      outstockProduct,
      newPurchases,
      lowStockProducts,
      newUser,
      stockAdjust
    });

  } catch (error) {
    console.error(' Notification fetch error:', error);
    return res.status(500).json({ error: 'Failed to load notifications' });
  }
});


// Change Password From the Default to normal

router.post('/change/default/password/tonormal', async (req, res) => {
  const { oldPassword, newPassword, phoneNumber } = req.body;

  let connection;
  try {
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');
    connection = await getConnection();

    // Get user by phone
    const [users] = await connection.query(
      'SELECT * FROM users WHERE phone = ?',
      [phoneNumber]
    );

    if (!users || users.length === 0) {
      return res.json({ message: "User not found/Provided details fail to match our records" });
    }

    const user = users[0];

    // Check if user is mapped to DEMO STORE
    const [storeCheck] = await connection.query(
      `
      SELECT s.name
      FROM user_stores us
      INNER JOIN stores s ON s.id = us.store_id
      WHERE us.user_id = ?
      `,
      [user.id]
    );

    const isMappedToDemo = storeCheck.some(row => row.name === 'DEMO STORE');

    // Prevent password change for users mapped to DEMO STORE
    if (isMappedToDemo) {
      return res.status(403).json({ message: "Password change is not allowed for DEMO accounts" });
    }

    // Allow password change for superadmin (not mapped to any store) or normal users
    const passwordMatch = await bcrypt.compare(oldPassword, user.password);
    if (!passwordMatch) {
      return res.status(400).json({ message: "Incorrect old password" });
    }

    const [history] = await connection.query(
      `SELECT password_hash FROM password_history WHERE user_id = ? ORDER BY changed_at DESC LIMIT 3`,
      [user.id]
    );

    for (const record of history) {
      const match = await bcrypt.compare(newPassword, record.password_hash);
      if (match) {
        return res.json({ message: "New password must not match any of your last 3 passwords" });
      }
    }

    const hashedNewPassword = await bcrypt.hash(newPassword, 12);

    await connection.query(
      'UPDATE users SET last_password_change = ?, password = ?, mustChangePassword = "false" WHERE phone = ?',
      [now, hashedNewPassword, phoneNumber]
    );

    await connection.query(
      `INSERT INTO password_history (user_id, password_hash, changed_at) VALUES (?, ?, ?)`,
      [user.id, hashedNewPassword, now]
    );

    return res.status(200).json({ message: "Password changed successfully" });

  } catch (error) {
    console.error('Password change error:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});



// User Change Password his/her wishes

router.post('/change/normal/password', auth.authenticateToken, async (req, res) => {
  const { oldPassword, newPassword } = req.body;

  let connection;
  try {
    connection = await getConnection();
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    // Get user by authenticated ID
    const [users] = await connection.query(
      'SELECT * FROM users WHERE id = ?',
      [res.locals.id]
    );

    if (!users || users.length === 0) {
      return res.json({ message: "User not found" });
    }

    const user = users[0];

    // Check if user is mapped to "DEMO STORE"
    const [demoStoreUsers] = await connection.query(
      `SELECT 1 FROM user_stores us
       INNER JOIN stores s ON s.id = us.store_id
       WHERE us.user_id = ? AND s.name = 'DEMO STORE'`,
      [user.id]
    );

    if (demoStoreUsers.length > 0) {
      return res.status(403).json({ message: "Users mapped to DEMO STORE cannot change their password" });
    }

    // Compare old password
    const passwordMatch = await bcrypt.compare(oldPassword, user.password);
    if (!passwordMatch) {
      return res.status(400).json({ message: "Incorrect old password" });
    }

    // Check last 3 passwords from history
    const [history] = await connection.query(
      `SELECT password_hash FROM password_history WHERE user_id = ? ORDER BY changed_at DESC LIMIT 3`,
      [user.id]
    );

    for (const record of history) {
      const reused = await bcrypt.compare(newPassword, record.password_hash);
      if (reused) {
        return res.json({ message: "New password must not match any of your last 3 passwords" });
      }
    }

    // Hash new password
    const hashedNewPassword = await bcrypt.hash(newPassword, 12);

    // Update user's password
    await connection.query(
      'UPDATE users SET last_password_change = ?, password = ? WHERE id = ?',
      [now, hashedNewPassword, user.id]
    );

    // Insert new password into history
    await connection.query(
      `INSERT INTO password_history (user_id, password_hash, changed_at) VALUES (?, ?, ?)`,
      [user.id, hashedNewPassword, now]
    );

    return res.status(200).json({ message: "Password changed successfully" });

  } catch (error) {
    console.error('Password change error:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
});


// Get User Profile =====================

router.get('/get/users/profile', auth.authenticateToken, async (req, res) => {
  const tokenPayload = res.locals;
  let connection;

  try {
    connection = await getConnection();

    // Get user info
    const [userRows] = await connection.query(
      `SELECT 
          u.id AS id,
          u.name AS fullname, 
          u.phone AS phone, 
          u.email AS email,  
          u.createDate AS create_date, 
          r.name AS rolename
        FROM users u
        JOIN roles r ON r.id = u.role
        WHERE u.id = ?`,
      [tokenPayload.id]
    );

    if (userRows.length === 0) {
      return res.status(404).json({ message: `No user found` });
    }

    const user = userRows[0];

    // Get user stores
    const [storeRows] = await connection.query(
      `SELECT s.id, s.name 
       FROM user_stores us 
       JOIN stores s ON s.id = us.store_id 
       WHERE us.user_id = ?`,
      [tokenPayload.id]
    );

    // Get user warehouses
    const [warehouseRows] = await connection.query(
      `SELECT w.id, w.name 
       FROM user_warehouses uw 
       JOIN warehouses w ON w.id = uw.warehouse_id 
       WHERE uw.user_id = ?`,
      [tokenPayload.id]
    );

    const data = {
      fullName: user.fullname,
      phoneNumber: user.phone,
      email: user.email,
      date: user.create_date,
      role: user.rolename,
      stores: storeRows,
      warehouses: warehouseRows,
    };

    return res.status(200).json(data);
  } catch (err) {
    console.error('Database error:', err);
    return res.status(500).json({ error: "An error occurred while retrieving the data." });
  }
});



// Get Stores based on assigned users
router.get('/regular-user/get/stores/assigned', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  
  try {
    const connection = await getConnection();

    let storesQuery;
    let params = [];

      // Regular users only see assigned stores
      storesQuery = `
        SELECT s.name as name, s.email as email, s.phone, s.id, u.id, u.name as username 
        FROM stores s 
        INNER JOIN user_stores us ON s.id = us.store_id 
        INNER JOIN users u ON u.id = us.user_id 
        WHERE us.user_id = ?`;
      params = [userId];
    

    const [result] = await connection.query(storesQuery, params);
    res.json(result);
  } catch (err) {
    res.status(500).json({ message: 'Error fetching assigned stores' });
  }
});


// Get Stores based on assigned users
router.get('/stores/assigned', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  try {
    const connection = await getConnection();

    let storesQuery;
    let params = [];

    if (roleId === 1 || roleId === '1') {
      // Superadmin can see all stores
      storesQuery = `SELECT id, name FROM stores`;
    } else {
      // Regular users only see assigned stores
      storesQuery = `
        SELECT s.id, s.name 
        FROM stores s 
        INNER JOIN user_stores us ON s.id = us.store_id 
        WHERE us.user_id = ?`;
      params = [userId];
    }

    const [stores] = await connection.query(storesQuery, params);
    res.json(stores);
  } catch (err) {
    res.status(500).json({ message: 'Error fetching assigned stores' });
  }
});


// Get Warehouses by store name
router.get('/warehouses/by-stores/:store', auth.authenticateToken, async (req, res) => {
  const storeId = req.params.store;
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const isSuperAdmin = (roleId === 1 || roleId === '1');

  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT 
        w.id, w.name, w.storeId,
        w.supports_barcode, w.supports_beep, w.customer_field, 
        w.supplier_field, w.send_sale_sms, w.send_purchase_sms, 
        w.send_low_qty_sms, w.send_sms_every_week_sale, w.batch_number, 
        w.show_discount_field, w.show_vat_field, w.show_transport_field
      FROM warehouses w
    `;

    let whereClause = `WHERE w.storeId = ?`;
    let params = [storeId];

    if (!isSuperAdmin) {
      query += `
        INNER JOIN user_warehouses uw ON uw.warehouse_id = w.id
        INNER JOIN user_stores us ON us.store_id = w.storeId
      `;
      whereClause += ` AND uw.user_id = ? AND us.user_id = ?`;
      params.push(userId, userId);
    }

    const finalQuery = `${query} ${whereClause}`;

    const [result] = await connection.execute(finalQuery, params);
    res.json(result);
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: 'Failed to fetch' });
  }
});


// Get User Stores
router.get('/user/stores', auth.authenticateToken, async (req, res) => {
  let conn;
  const userId = res.locals.id;

  try {
    conn = await getConnection();

    // Get user role and role name
    const [userRows] = await conn.query(
      `SELECT u.role, u.id as userId, u.name as fullname, r.name AS role_name 
       FROM users u 
       JOIN roles r ON r.id = u.role 
       WHERE u.id = ?`, 
      [userId]
    );

    const userRole = userRows[0]?.role_name;
    let stores = [];
    let warehouses = [];

    if (userRole === 'ADMIN') {
      // Admin: fetch all stores and warehouses
      [stores] = await conn.query(`SELECT id, name FROM stores ORDER BY name`);
      [warehouses] = await conn.query(`SELECT id, name, storeId FROM warehouses ORDER BY name`);
    } else {
      // Regular user: fetch only assigned stores
      const [assignedStores] = await conn.query(
        `SELECT s.id, s.name 
         FROM user_stores us 
         JOIN stores s ON s.id = us.store_id 
         WHERE us.user_id = ? 
         ORDER BY s.name`, 
        [userId]
      );

      const storeIds = assignedStores.map(s => s.id);
      stores = assignedStores;

      if (storeIds.length > 0) {
        const placeholders = storeIds.map(() => '?').join(',');
        [warehouses] = await conn.query(
          `SELECT id, name, storeId 
           FROM warehouses 
           WHERE storeId IN (${placeholders}) 
           ORDER BY name`, 
          storeIds
        );
      }
    }

    res.status(200).json({ stores, warehouses });

  } catch (err) {
    console.error('Error fetching assigned stores and warehouses:', err);
    res.status(500).json({ message: 'Failed to fetch stores and warehouses' });
  }
});


// MENU MODULE API

// Get Sidebar Menu
router.get('/sidebarMenu', auth.authenticateToken, async (req, res) => {
  let connection;
  
  try {
    connection = await getConnection(); // get promise-based connection

    // Get user's role
    const [userRows] = await connection.query(`SELECT role FROM users WHERE id = ?`, [res.locals.id]);
    const roleId = userRows[0]?.role;
    if (!roleId) return res.status(404).json({ error: 'Role not found' });

    // Get permissions
    const [permRows] = await connection.query(`SELECT menu, submenu, access FROM permissions WHERE role = ?`, [roleId]);

    const menuAccessMap = {};
    const submenuAccessMap = {};

    permRows.forEach(row => {
      if (row.submenu) {
        if (!submenuAccessMap[row.submenu]) submenuAccessMap[row.submenu] = new Set();
        submenuAccessMap[row.submenu].add(row.access);
      } else {
        if (!menuAccessMap[row.menu]) menuAccessMap[row.menu] = new Set();
        menuAccessMap[row.menu].add(row.access);
      }
    });

    const allowedMenus = [...new Set(permRows.map(p => p.menu).filter(Boolean))];
    const allowedSubmenus = [...new Set(permRows.map(p => p.submenu).filter(Boolean))];

    // Prevent query with empty IN ()
    if (allowedMenus.length === 0) {
      return res.json([]); // No menu permissions, return empty menu
    }

    const [menus] = await connection.query(`SELECT * FROM menus WHERE name IN (?)`, [allowedMenus]);

    let submenus = [];
    if (allowedSubmenus.length > 0) {
      [submenus] = await connection.query(
        `SELECT * FROM submenus WHERE menu IN (?) AND name IN (?)`,
        [allowedMenus, allowedSubmenus]
      );
    }

    const submenuMap = {};
    submenus.forEach(sub => {
      const accessArray = Array.from(submenuAccessMap[sub.name] || []);
      const submenuItem = {
        label: sub.name,
        routeLink: sub.route,
        access: accessArray
      };
      if (!submenuMap[sub.menu]) submenuMap[sub.menu] = [];
      submenuMap[sub.menu].push(submenuItem);
    });

    const menuTree = menus.map(menu => ({
      label: menu.name,
      routeLink: menu.route,
      icon: menu.icon || '',
      access: Array.from(menuAccessMap[menu.name] || []),
      items: submenuMap[menu.name] || []
    }));

    res.json(menuTree);
  } catch (err) {
    console.error('Error loading sidebar menu:', err);
    res.status(500).json({ message: 'Failed to load sidebar menu', error: err.message });
  } 
});


// ADD TANZANIA DISTRICTS ==============================

router.post('/addDistricts', auth.authenticateToken, async (req, res) => {
  let conn;

  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

  try {
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');
    const { name, region } = req.body;

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Optional: Simulate delay
    await new Promise(resolve => setTimeout(resolve, 3000));

      // Only allow super admins
      if (!isSuperAdmin) {
        return res.status(403).json({ error: 'Access denied. Super admin only.' });
      }

    // Check for existing 
    const [existing] = await conn.query(
      `SELECT * FROM districts WHERE name = ? `,
      [name]
    );

    if (existing.length > 0) {
      return res.status(409).json({
        message: `Name "${name}" already existing `
      });
    }

    // Optional: Another delay before insert
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Insert new 
    const [result] = await conn.query(
      `INSERT INTO districts (name, region_id)
       VALUES (?, ?)`,
      [name, region]
    );

    return res.status(201).json({
      message: `District of name "${name}" created successfully! `
     });

  } catch (err) {
    console.error('Add Error:', err);
    res.status(500).json({ error: 'Internal server error' });
  } 
});


// Update Districts ===================================
router.put('/updateDistricts/:id', auth.authenticateToken, async (req, res) => {
  let conn;

  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

  try {
    const { name, region } = req.body;
    const id = req.params.id;

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

     // Only allow super admins
     if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }

    // Update 
    const [result] = await conn.query(
      `UPDATE districts SET name = ?, region_id = ? WHERE id = ?`,
      [name, region, id]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ message: "Id not found " });
    }

    return res.status(200).json({
      message: `"${name}" updated successfully! `
    });

  } catch (err) {
    console.error('Update Error:', err);
    res.status(500).json({ error: 'Internal server error' });
  } 
});


// Get District Lists
router.get('/getDistrictsList', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Use async/await with MySQL query
    const [results] = await conn.query('SELECT * FROM districts ORDER BY name ASC');

    // Return the lists
    res.json(results);

  } catch (err) {
    console.error('Error fetching list:', err);
    return res.status(500).json({ error: 'Internal server error' });
  } 
});


// Delete Districts Data ==============================

router.delete('/deleteDistricts/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const id = req.params.id;
    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');


    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Execute the query to delete 
    const [result] = await conn.query(
      "DELETE FROM districts WHERE id = ?",
      [id]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ message: `Id ${id} not found` });
    }

    return res.status(200).json({ message: 'Deleted successfully ' });

  } catch (err) {
    console.error('Deleting Error:', err);
    return res.status(500).json({ error: 'Internal server error', details: err });
  } 
});

// ADD TANZANIA REGION ==============================

router.post('/addRegion', auth.authenticateToken, async (req, res) => {
  let conn;

  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

  try {
    const { name } = req.body;

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Optional: Simulate delay
    await new Promise(resolve => setTimeout(resolve, 3000));

      // Only allow super admins
      if (!isSuperAdmin) {
        return res.status(403).json({ error: 'Access denied. Super admin only.' });
      }

    // Check for existing menu
    const [existing] = await conn.query(
      `SELECT * FROM regions WHERE name = ? `,
      [name]
    );

    if (existing.length > 0) {
      return res.status(409).json({
        message: `Name "${name}" already existing `
      });
    }

    // Optional: Another delay before insert
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Insert new 
    const [result] = await conn.query(
      `INSERT INTO regions (name)
       VALUES (?)`,
      [name]
    );

    return res.status(201).json({
      message: `Region of name "${name}" created successfully! `
     });

  } catch (err) {
    console.error('Add Error:', err);
    res.status(500).json({ error: 'Internal server error' });
  } 
});


// Update Region ===================================
router.put('/updateRegion/:id', auth.authenticateToken, async (req, res) => {
  let conn;

  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

  try {
    const { name } = req.body;
    const id = req.params.id;

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

     // Only allow super admins
     if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }

    // Update 
    const [result] = await conn.query(
      `UPDATE regions SET name = ? WHERE id = ?`,
      [name, id]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ message: "Id not found " });
    }

    return res.status(200).json({
      message: `"${name}" updated successfully! `
    });

  } catch (err) {
    console.error('Update Error:', err);
    res.status(500).json({ error: 'Internal server error' });
  } 
});


// Delete Region ==============================

router.delete('/deleteRegion/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const id = req.params.id;
    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');


    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Execute the query to delete 
    const [result] = await conn.query(
      "DELETE FROM regions WHERE id = ?",
      [id]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ message: `Id ${id} not found` });
    }

    return res.status(200).json({ message: 'Deleted successfully ' });

  } catch (err) {
    console.error('Deleting Error:', err);
    return res.status(500).json({ error: 'Internal server error', details: err });
  } 
});


// DELETE PAY ACCOUNT ==============================

router.delete('/delete/pay/account/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const id = req.params.id;
    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');


    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Execute the query to delete 
    const [result] = await conn.query(
      "DELETE FROM deposit_accounts WHERE id = ?",
      [id]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ message: `Id ${id} not found` });
    }

    return res.status(200).json({ message: 'Deleted successfully ' });

  } catch (err) {
    console.error('Deleting Error:', err);
    return res.status(500).json({ error: 'Internal server error', details: err });
  } 
});


// DeleteOwnership plan ==============================

router.delete('/delete/ownershipPlan/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const id = req.params.id;
    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');


    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Execute the query to delete 
    const [result] = await conn.query(
      "DELETE FROM ownership_plans WHERE id = ?",
      [id]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ message: `Id ${id} not found` });
    }

    return res.status(200).json({ message: 'Deleted successfully ' });

  } catch (err) {
    console.error('Deleting Error:', err);
    return res.status(500).json({ error: 'Internal server error', details: err });
  } 
});


// CREATE PAY ACCOUNT ===============================

router.post('/create/pay/account', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');
    const { type, warehouse, banks, bankNo, mobile, pay_number } = req.body;

    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');
   
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Optional: Simulate delay
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }

    // Check for existing
    const [existing] = await conn.query(
      'SELECT id FROM deposit_accounts WHERE mobile = ? AND banks = ? AND pay_number = ? AND bankNo = ? AND type = ? AND warehouse_id = ? ',
      [mobile, banks, pay_number, bankNo, type, warehouse ]
    );

    if (existing.length > 0) {
      return res.json({
        message: `Data already exists `
      });
    }

    // Optional: Another delay before insert
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Insert new 
    await conn.query(
      `INSERT INTO deposit_accounts (warehouse_id, type, banks, bankNo, mobile, pay_number)
      VALUES (?, ?, ?, ?, ?, ?)`,
     [warehouse, type, banks || null, bankNo || null, mobile || null, pay_number || null]
   
    );

    res.json({
      message: `Pay account created successfully!`,
    });

  } catch (err) {
    console.error('Add Error:', err);
    res.status(500).json({ error: 'Internal server error' });
  } 
});


// UPDATE PAY ACCOUNT =========================
router.put('/update/pay/account/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {

  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');
    
  const { type, warehouse, banks, bankNo, mobile, pay_number } = req.body;

    const id = req.params.id;

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }


    // Update 
    const [result] = await conn.query(
      `UPDATE deposit_accounts SET warehouse_id = ?, type = ?, banks = ?,
      bankNo = ?, mobile = ?, pay_number = ?
      WHERE id = ?`,
     [warehouse, type, banks || null, bankNo || null, mobile || null, pay_number || null, id]
   
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ message: "Id not found" });
    }

    return res.status(200).json({
      message: ` Updated successfully! `
    });

  } catch (err) {
    console.error('Update Error:', err);
    res.status(500).json({ error: 'Internal server error' });
  } 
});

// GET PAY ACCOUNT LISTS

router.get('/get/pay/accounts', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const { warehouseId } = req.query;


  let connection;

  try {
    connection = await getConnection();

    // Base query
    let query = `
      SELECT da.*,  
        w.name AS warehousename, w.id AS warehouse_id
      FROM deposit_accounts da
      JOIN warehouses w ON w.id = da.warehouse_id
    `;

    const params = [];
    const whereConditions = [];

    // If the user is not an admin, apply restrictions based on their assigned stores and warehouses
    if (!(roleId === 1 || roleId === '1')) {
      
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      // If user has no stores or warehouses assigned, return an empty response
      if (warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      // Conditions for stores and warehouses assigned to the user
      const accessConditions = [];

      if (warehouseIds.length > 0) {
        accessConditions.push(`da.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }

      if ( warehouseIds.length > 0) {
        whereConditions.push(`(da.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
        params.push( ...warehouseIds);
      } 
      else if (warehouseIds.length > 0) {
        whereConditions.push(`da.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }

    }

    // Admin doesn't need store/warehouse filters, apply optional filters if passed
  
    if (warehouseId) {
      whereConditions.push(`da.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // If there are any where conditions, add them to the query
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sort the result 
    query += ` ORDER BY da.type ASC`;

    const [result] = await connection.query(query, params);
    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});


// CREATE OWNERSHIP PLAN SUBSCRIBER ===============================

router.post('/create/ownershipPlan', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const { 
      duration_type, amount, type
    } = req.body;

    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');
   
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Optional: Simulate delay
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }

    // Check for existing menu
    const [existing] = await conn.query(
      'SELECT id FROM ownership_plans WHERE plan_type = ? AND duration_type = ? ',
      [type, duration_type ]
    );

    if (existing.length > 0) {
      return res.status(409).json({
        message: `Ownership plan already exists `
      });
    }

    // Optional: Another delay before insert
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Insert new 
    await conn.query(
      `INSERT INTO ownership_plans (plan_type, duration_type, duration_value, amount)
      VALUES (?, ?, ?, ?)`,
     [type, duration_type || null, 1, amount]
   
    );

    return res.status(201).json({
      message: `Ownership plan of type "${type}" created successfully!`,
    });

  } catch (err) {
    console.error('Add Error:', err);
    res.status(500).json({ error: 'Internal server error' });
  } 
});

// UPDATE OWNERSHIP PLAN SUBSCRIBER =========================
router.put('/update/OwnershipPlan/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {

  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');
    
    const { 
      duration_type, amount, type
    } = req.body;

    const id = req.params.id;

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }


    // Update 
    const [result] = await conn.query(
      `UPDATE ownership_plans SET duration_type = ?, amount = ?, plan_type = ? 
      WHERE id = ?`,
     [duration_type || null, amount, type, id]
   
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ message: "Id not found" });
    }

    return res.status(200).json({
      message: ` "${type}" updated successfully!`
    });

  } catch (err) {
    console.error('Update Error:', err);
    res.status(500).json({ error: 'Internal server error' });
  } 
});


// Get Districts Lists =======================================
router.get('/get/getDistrictsList', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Use async/await with MySQL query
    const [results] = await conn.query('SELECT * FROM districts ORDER BY name asc');

    // Return the lists
    res.json(results);

  } catch (err) {
    console.error('Error fetching list:', err);
    return res.status(500).json({ error: 'Internal server error' });
  } 
});


// Get Ownership plan Lists =======================================
router.get('/get/ownershipPlanLists', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Use async/await with MySQL query
    const [results] = await conn.query('SELECT * FROM ownership_plans ORDER BY amount DESC');

    // Return the lists
    res.json(results);

  } catch (err) {
    console.error('Error fetching list:', err);
    return res.status(500).json({ error: 'Internal server error' });
  } 
});


// ADD SCHEDULE CATEGORY ==============================

router.post('/addScheduleCategory', auth.authenticateToken, async (req, res) => {
  let conn;

  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

  try {
    const { name } = req.body;

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Optional: Simulate delay
    await new Promise(resolve => setTimeout(resolve, 3000));

      // Only allow super admins
      if (!isSuperAdmin) {
        return res.status(403).json({ error: 'Access denied. Super admin only.' });
      }

    // Check for existing menu
    const [existing] = await conn.query(
      `SELECT * FROM schedule_category WHERE name = ? `,
      [name]
    );

    if (existing.length > 0) {
      return res.json({
        message: `Schedule category with name "${name}" already existing`
      });
    }

    // Optional: Another delay before insert
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Insert new 
    const [result] = await conn.query(
      `INSERT INTO schedule_category (name)
       VALUES (?)`,
      [name]
    );

    return res.status(201).json({
      message: `Schedule category "${name}" created successfully!`
     });

  } catch (err) {
    console.error('Add Error:', err);
    res.status(500).json({ error: 'Internal server error' });
  } 
});


// Update Schedule Category ===================================
router.put('/updateScheduleCategory/:id', auth.authenticateToken, async (req, res) => {
  let conn;

  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

  try {
    const { name } = req.body;
    const id = req.params.id;

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

     // Only allow super admins
     if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }

    // Update 
    const [result] = await conn.query(
      `UPDATE schedule_category SET name = ? WHERE id = ?`,
      [name, id]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ message: "Id not found " });
    }

    return res.status(200).json({
      message: `"${name}" updated successfully! `
    });

  } catch (err) {
    console.error('Update Error:', err);
    res.status(500).json({ error: 'Internal server error' });
  } 
});

// Get Schedule Category Lists
router.get('/get/scheduleCategoryList', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Use async/await with MySQL query
    const [results] = await conn.query('SELECT * FROM schedule_category ORDER BY name ASC');

    // Return the lists
    res.json(results);

  } catch (err) {
    console.error('Error fetching list:', err);
    return res.status(500).json({ error: 'Internal server error' });
  } 
});


// Delete Schedule Category ==============================

router.delete('/deleteScheduleCategory/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const id = req.params.id;
    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');


    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Execute the query to delete 
    const [result] = await conn.query(
      "DELETE FROM schedule_category WHERE id = ?",
      [id]
    );

    if (result.affectedRows === 0) {
      return res.json({ message: `Id ${id} not found` });
    }

    return res.status(200).json({ message: 'Deleted successfully ' });

  } catch (err) {
    console.error('Deleting Error:', err);
    return res.status(500).json({ error: 'Internal server error', details: err });
  } 
});


// Delete Schedule ==============================

router.delete('/deleteSchedule/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const id = req.params.id;
    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');


    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Execute the query to delete 
    const [result] = await conn.query(
      "DELETE FROM schedules WHERE id = ?",
      [id]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ message: `Id ${id} not found` });
    }

    return res.status(200).json({ message: 'Deleted successfully' });

  } catch (err) {
    console.error('Deleting Error:', err);
    return res.status(500).json({ error: 'Internal server error', details: err });
  } 
});

// Add a new time schedule ===============================

router.post('/addSchedule', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const { 
      name,
      type,
      cron_time,
      start_time,
      end_time,
      enabled,
      store,
      days, 
      hours, 
      minutes
    } = req.body;
   
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Optional: Simulate delay
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Check for existing menu
    const [existing] = await conn.query(
      'SELECT id FROM schedules WHERE name = ? AND type = ? AND store_id = ?',
      [name, type, store]
    );

    if (existing.length > 0) {
      return res.json({
        message: `Schedule already exists `
      });
    }

    // Optional: Another delay before insert
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Insert new 
    await conn.query(
      `INSERT INTO schedules (day, minute, hour, name, type, cron_pattern, start_time, end_time, enabled, store_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
     [days || null, minutes || null, hours || null, name, type, cron_time || null, start_time || null, end_time || null, enabled, store]
   
    );

    return res.status(201).json({
      message: `Schedule "${name}" created successfully!`,
    });

  } catch (err) {
    console.error('Add Error:', err);
    res.status(500).json({ error: 'Internal server error' });
  } 
});


router.put('/updateSchedule/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    
    const { 
      name,
      type,
      cron_time,
      start_time,
      end_time,
      enabled,
      store, days, hours, minutes
     } = req.body;
    const id = req.params.id;

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    // Update 
    const [result] = await conn.query(
      `UPDATE schedules SET day = ?, minute = ?, hour = ?, name = ?, type = ?, cron_pattern = ?, start_time = ?, end_time = ?, enabled = ?, store_id = ? 
      WHERE id = ?`,
     [days || null, minutes || null, hours || null, name, type, cron_time || null, start_time || null, end_time || null, enabled, store, id]
   
    );

    if (result.affectedRows === 0) {
      return res.json({ message: "Id not found" });
    }

    return res.status(200).json({
      message: ` "${name}" updated successfully! `
    });

  } catch (err) {
    console.error('Update Error:', err);
    res.status(500).json({ error: 'Internal server error' });
  } 
});


// Get Schedule Lists
router.get('/get/scheduleLists', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const { storeId } = req.query;

  let connection;

  try {
    connection = await getConnection();

    // Base query
    let query = `
      SELECT sh.*, 
        s.name AS storename, s.id AS store_id
      FROM schedules sh
      JOIN stores s ON s.id = sh.store_id
    `;

    const params = [];
    const whereConditions = [];

    // If the user is not an admin, apply restrictions based on their assigned stores and warehouses
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      
      const storeIds = storeRows.map(r => r.store_id);
      
      // If user has no stores or warehouses assigned, return an empty response
      if (storeIds.length === 0 ) {
        return res.json({ array: [] });
      }

      // Conditions for stores and warehouses assigned to the user
      const accessConditions = [];

      if (storeIds.length > 0) {
        accessConditions.push(`sh.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (storeIds.length > 0 ) {
        whereConditions.push(`(sh.store_id IN (${storeIds.map(() => '?').join(',')}))`);
        params.push(...storeIds);
      } else if (storeIds.length > 0) {
        whereConditions.push(`sh.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

    }

    // Admin doesn't need store/warehouse filters, apply optional filters if passed
    if (storeId) {
      whereConditions.push(`sh.store_id = ?`);
      params.push(storeId);
    }

    // If there are any where conditions, add them to the query
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sort the result 
    query += ` ORDER BY sh.name ASC`;

    const [result] = await connection.query(query, params);
    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});

  
// Add a new menu or submenu
router.post('/addMenu', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const { name, route, icon, description } = req.body;
    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');


    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection


    // Get next order_index
    const [countRows] = await conn.query(
      `SELECT COUNT(*) AS count FROM menus`
    );
    const order_index = countRows[0].count + 1;

    // Optional: Simulate delay
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }

    // Check for existing menu
    const [existing] = await conn.query(
      `SELECT * FROM menus WHERE name = ? OR route = ? OR order_index = ?`,
      [name, route, order_index]
    );

    if (existing.length > 0) {
      return res.json({
        message: `Menu with name "${name}", route "${route}", or order index "${order_index}" already exists.`
      });
    }

    // Optional: Another delay before insert
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Step 3: Insert new menu
    const [result] = await conn.query(
      `INSERT INTO menus (description, name, route, icon, status, deletable, order_index)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [description, name, route, icon, "true", "false", order_index]
    );

    return res.status(201).json({
      message: `Menu "${name}" created successfully!`,
      menuId: result.insertId
    });

  } catch (err) {
    console.error('Add Menu Error:', err);
    res.status(500).json({ error: 'Internal server error' });
  } 
});



// Update Menu
router.put('/updateMenu/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

    const { name, route, icon, description } = req.body;
    const id = req.params.id;

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }

    // Update menu
    const [result] = await conn.query(
      `UPDATE menus SET name = ?, route = ?, icon = ?, description = ? WHERE id = ?`,
      [name, route, icon, description, id]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ message: "Menu not found" });
    }

    return res.status(200).json({
      message: `Menu "${name}" updated successfully!`
    });

  } catch (err) {
    console.error('Update Menu Error:', err);
    res.status(500).json({ error: 'Internal server error' });
  } 
});

// Get Users Assigned to Warehouse ========================

router.get('/get/users/assigned/warehouses', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  let conn;
  try {
    conn = await getConnection();

    const [warehouseRows] = await conn.query(
      'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?',
      [userId]
    );

    const warehouseIds = warehouseRows.map(r => r.warehouse_id);

    if (warehouseIds.length === 0) {
      // No warehouses assigned to user — return empty array
      return res.json([]);
    }

    const [results] = await conn.query(
      `
        SELECT DISTINCT u.*
        FROM users u
        JOIN user_warehouses uw ON uw.user_id = u.id
        WHERE uw.warehouse_id IN (?)
        ORDER BY u.name ASC
      `,
      [warehouseIds]
    );

    res.json(results);
  } catch (err) {
    console.error('Error fetching users by warehouses:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});



// Get Bank Lists ======================================

router.get('/getBankList', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Use async/await with MySQL query
    const [results] = await conn.query('SELECT * FROM banks ORDER BY name ASC');

    // Return the list
    res.json(results);

  } catch (err) {
    console.error('Error fetching list:', err);
    return res.status(500).json({ error: 'Internal server error' });
  } 
});



// Get Mobile Lists ======================================

router.get('/getMobileList', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Use async/await with MySQL query
    const [results] = await conn.query('SELECT * FROM mobile_services ORDER BY name ASC');

    // Return the list
    res.json(results);

  } catch (err) {
    console.error('Error fetching list:', err);
    return res.status(500).json({ error: 'Internal server error' });
  } 
});


// Get Mail Logs Lists
router.get('/get/mail/logs/list', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Use async/await with MySQL query
    const [results] = await conn.query('SELECT * FROM mails ORDER BY date DESC');

    // Return the list of menus
    res.json(results);

  } catch (err) {
    console.error('Error fetching list:', err);
    return res.status(500).json({ error: 'Internal server error' });
  } 
});



// Get Menu Lists Viewed Public
router.get('/get/menuList/view/public', async (req, res) => {
  let conn;
  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Use async/await with MySQL query
    const [results] = await conn.query('SELECT * FROM menus ORDER BY order_index ASC');

    // Return the list of menus
     return res.status(200).json({
      success: true,
      data: results
    });

  } catch (err) {
    console.error('Error fetching menu list:', err);
    return res.status(500).json({ error: 'Internal server error' });
  } 
});




// Get Menu Lists
router.get('/getMenuList', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Use async/await with MySQL query
    const [results] = await conn.query('SELECT * FROM menus ORDER BY order_index ASC');

    // Return the list of menus
    res.json(results);

  } catch (err) {
    console.error('Error fetching menu list:', err);
    return res.status(500).json({ error: 'Internal server error' });
  } 
});


// Get submenus by menu
router.get('/getSubmenusByName/:menu', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const menu = req.params.menu;

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Use async/await with MySQL query
    const [submenus] = await conn.query(
      'SELECT id, name, route FROM submenus WHERE menu = ?',
      [menu]
    );

    res.json(submenus);

  } catch (err) {
    console.error('Error fetching submenus:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});



// Update Menu Status
router.post('/updateMenuStatus', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }

    const { id, status } = req.body;

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Execute the query to update the menu status
    const [result] = await conn.query(
      "UPDATE menus SET status=? WHERE id=?",
      [status, id]
    );

    if (result.affectedRows === 0) {
      return res.json({ message: `${id} not found` });
    }

    return res.status(200).json({ message: 'Successfully updated' });

  } catch (err) {
    console.error('Error updating menu status:', err);
    return res.status(500).json({ error: 'Internal server error' });
  } 
});


// Update Menu Deletable
router.post('/updateMenuDeletable', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }

    const { id, deletable } = req.body;

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Execute the query to update the menu deletable status
    const [result] = await conn.query(
      "UPDATE menus SET deletable=? WHERE id=?",
      [deletable, id]
    );

    if (result.affectedRows === 0) {
      return res.json({ message: `${id} not found` });
    }

    return res.status(200).json({ message: 'Successfully updated' });

  } catch (err) {
    console.error('Error updating menu deletable status:', err);
    return res.status(500).json({ error: 'Internal server error' });
  } 
});



// Delete Menu
router.delete('/deleteMenu/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {

    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');


    const id = req.params.id;

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Execute the query to delete the menu
    const [result] = await conn.query(
      "DELETE FROM menus WHERE id = ?",
      [id]
    );

    if (result.affectedRows === 0) {
      return res.json({ message: `Menu with id ${id} not found` });
    }

    return res.status(200).json({ message: 'Menu deleted successfully' });

  } catch (err) {
    console.error('Error deleting menu:', err);
    return res.status(500).json({ error: 'Internal server error', details: err });
  } 
});


// Add a new submenu
router.post('/addSubMenu', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    let roleId = res.locals.role;
    const isSuperAdmin = (roleId === 1 || roleId === '1');

    const { menu, name, route, icon } = req.body;

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }

    // Get a new connection from the pool
    conn = await getConnection();

    // Step 1: Get next order_index
    const [orderResult] = await conn.query(`SELECT MAX(order_index) AS max FROM submenus`);
    const order_index = (orderResult[0].max || 0) + 1;

    await new Promise(resolve => setTimeout(resolve, 3000)); // Optional delay

    // Step 2: Check if the submenu already exists by name or route
    const [existing] = await conn.query(
      `SELECT * FROM submenus WHERE name = ? OR route = ?`,
      [name, route]
    );

    if (existing.length > 0) {
      return res.json({
        message: `Submenu with name "${name}" or route "${route}" already exists`
      });
    }

    await new Promise(resolve => setTimeout(resolve, 3000)); // Optional delay

    //Insert new submenu
    await conn.query(
      `INSERT INTO submenus (name, route, icon, status, deletable, menu, order_index)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [name, route, icon, 'true', 'false', menu, order_index]
    );

    return res.status(201).json({
      message: `Submenu "${name}" created successfully!`
    });

  } catch (err) {
    console.error('Error adding submenu:', err); // Show actual backend error
    res.status(500).json({ error: 'Internal server error' });
  }
});



// Update Sub Menu
router.put('/updateSubMenu/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

    const { name, route, icon, menu } = req.body;
    const id = req.params.id;

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }

    // Update submenu
    const [result] = await conn.query(
      `UPDATE submenus SET menu = ?, name = ?, route = ?, icon = ? WHERE id = ?`,
      [menu, name, route, icon, id]
    );

    if (result.affectedRows === 0) {
      return res.json({ message: "Submenu not found" });
    }

    return res.status(200).json({
      message: `Submenu "${name}" updated successfully!`
    });

  } catch (err) {
    console.error('Error updating submenu:', err);
    res.status(500).json({ error: 'Internal server error' });
  } 
});


// Get Password changes history ===================

router.get('/get/password/change/history', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Use async/await with MySQL query
    const [results] = await conn.query(`
      SELECT * FROM password_history WHERE user_id = ?  ORDER BY changed_at DESC`,
      [res.locals.id]
    );

    // Return the list of 
    res.json(results);
  } catch (err) {
    console.error('Error fetching :', err);
    return res.status(500).json({ error: 'Internal server error' });
  } 
});


// Get SubMenu Lists
router.get('/getSubMenuList', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Use async/await with MySQL query
    const [results] = await conn.query(`
      SELECT * FROM submenus ORDER BY order_index ASC`);

    // Return the list of submenus
    res.json(results);
  } catch (err) {
    console.error('Error fetching submenus:', err);
    return res.status(500).json({ error: 'Internal server error' });
  } 
});


// Update SubMenu Status
router.post('/updateSubMenuStatus', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds


    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }

    const { id, status } = req.body;
// Get a new connection from the pool
conn = await getConnection(); // get promise-based connection

    const [result] = await conn.query(
      "UPDATE submenus SET status=? WHERE id=?",
      [status, id]
    );

    if (result.affectedRows === 0) {
      return res.json({ message: `${id} not found` });
    } else {
      return res.status(200).json({ message: `Successfully updated` });
    }
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Internal server error' });
  } 
});


// Update SubMenu Deletable
router.post('/updateSubMenuDeletable', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds


    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }

    const { id, deletable } = req.body;

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    const [result] = await conn.query(
      "UPDATE submenus SET deletable=? WHERE id=?",
      [deletable, id]
    );

    if (result.affectedRows === 0) {
      return res.json({ message: `Submenu with id ${id} not found` });
    } else {
      return res.status(200).json({ message: `Successfully updated` });
    }
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Internal server error' });
  } 
});


// Delete SubMenu
router.delete('/deleteSubMenu/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

    const id = req.params.id;

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }

    // Get a new connection from the pool
conn = await getConnection(); // get promise-based connection

    const [result] = await conn.query(
      "DELETE FROM submenus WHERE id = ?",
      [id]
    );

    if (result.affectedRows === 0) {
      return res.json({ message: `Submenu with id ${id} not found` });
    } else {
      return res.status(200).json({ message: `Submenu deleted successfully` });
    }
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Internal server error', details: err });
  } 
});


// Add a new menu access
router.post('/addMenuAccess', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

    const { menu, access, submenu } = req.body;
    const submenuValue = submenu || null; // treat empty string as null

    // Simulate delay (for demonstration purposes)
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds


    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ error: 'Access denied. Super admin only.' });
    }

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Check if access already exists
    const [existing] = await conn.query(
      `SELECT * FROM menuaccess WHERE access = ? AND menu = ? AND (submenu = ? OR (submenu IS NULL AND ? IS NULL))`,
      [access, menu, submenuValue, submenuValue]
    );

    if (existing.length > 0) {
      return res.json({
        message: `Menu access with the name "${access}" already exists`
      });
    }

    // Insert menu access
    await new Promise(resolve => setTimeout(resolve, 3000)); // Optional delay

    await conn.query(
      `INSERT INTO menuaccess (menu, submenu, access, status, deletable)
       VALUES (?, ?, ?, ?, ?)`,
      [menu, submenuValue, access, 'true', 'false']
    );

    return res.status(201).json({
      message: `Menu access "${access}" created successfully!`
    });

  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Internal server error', details: err });
  } 
});


// Update Menu Access
router.put('/updateMenuAccess/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {

    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

    const { access, menu, submenu } = req.body;
    const id = req.params.id;

    // Simulate delay (for demonstration purposes)
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds


    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ message: 'Access denied. Super admin only.' });
    }


    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Update menu access
    const [result] = await conn.query(
      `UPDATE menuaccess SET submenu = ?, menu = ?, access = ? WHERE id = ?`,
      [submenu, menu, access, id]
    );

    if (result.affectedRows === 0) {
      return res.json({ message: "Menu access not found" });
    }

    return res.status(200).json({
      message: `Menu access "${access}" updated successfully!`
    });

  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Internal server error', details: err });
  } 
});


// Get Menu Access Lists
router.get('/getMenuAccessList', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Fetch menu access data from the database
    const [results] = await conn.query(`
      SELECT * FROM menuaccess ORDER BY menu ASC
    `);

    // Return the list of menu accesses
    res.json(results);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Internal server error', details: err });
  } 
});



// Get Menu Access Lists
router.get('/menuAccess', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Fetch menu access data from the database
    const [menuAccesses] = await conn.query(
      'SELECT id, menu, access, submenu FROM menuaccess ORDER BY menu ASC'
    );

    // Return the list of menu accesses
    res.status(200).json(menuAccesses);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Internal server error', details: error });
  } 
});



// Update SubMenu Status
router.post('/updateMenuAccessStatus', auth.authenticateToken, async (req, res) => {
  let conn;
  try {

    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ message: 'Access denied. Super admin only.' });
    }

    const { id, status } = req.body;

    // Update the status of the menu access
    const [result] = await conn.query(
      "UPDATE menuaccess SET status=? WHERE id=? ",
      [status, id]
    );

    if (result.affectedRows === 0) {
      return res.json({ message: `Menu access with id ${id} not found` });
    }

    return res.status(200).json({ message: `Successfully updated status` });

  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Internal server error', details: err });
  } 
});



// Update Menu Access Deletable
router.post('/updateMenuAccessDeletable', auth.authenticateToken, async (req, res) => {
  let conn;
  try {

    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ message: 'Access denied. Super admin only.' });
    }

    const { id, deletable } = req.body;

    // Update the deletable flag of the menu access
    const [result] = await conn.query(
      "UPDATE menuaccess SET deletable=? WHERE id=? ",
      [deletable, id]
    );

    if (result.affectedRows === 0) {
      return res.json({ message: `Menu access with id ${id} not found` });
    }

    return res.status(200).json({ message: `Successfully updated deletable status` });

  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Internal server error', details: err });
  } 
});


// Delete Menu Access
router.delete('/deleteMenuAccess/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const id = req.params.id;

    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');


    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ message: 'Access denied. Super admin only.' });
    }

    const [result] = await conn.query(
      "DELETE FROM menuaccess WHERE id = ?",
      [id]
    );

    if (result.affectedRows === 0) {
      return res.json({ message: `Menu access with id ${id} not found` });
    } else {
      return res.status(200).json({ message: `Menu access deleted successfully` });
    }
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Internal server error', details: err });
  } 
});


// Respond to a cancel Request Sales =================

router.post('/respond/cancel/sales/request', auth.authenticateToken, async (req, res) => {
  const conn = await getConnection();
  const { respondRemark, id } = req.body;

  const approverId = res.locals.id;

  try {
    await new Promise(resolve => setTimeout(resolve, 3000)); // delay

    const [[{ name: roleName } = {}]] = await conn.query(
      'SELECT name FROM roles WHERE id = (SELECT role FROM users WHERE id = ?)',
      [approverId]
    );

    if (!roleName) {
      return res.json({ message: 'Invalid role access.' });
    }

    const isAdmin = roleName === 'ADMIN';
    const isManager = roleName === 'MANAGER';

    if (!isAdmin && !isManager) {
      return res.json({ message: 'Access denied. Only MANAGER or ADMIN allowed.' });
    }

    // Fetch all sale items for this sale
    const [saleItems] = await conn.query(`SELECT product_id, quantity FROM sale_items WHERE sale_id = ?`, [id]);

    if (saleItems.length === 0) {
      return res.json({ message: 'No sale items found for this sale.' });
    }

    // Get the user who created the sale
    const [[sale]] = await conn.query(`SELECT user_id FROM sales WHERE id = ?`, [id]);
    if (!sale || sale.user_id === approverId) {
      return res.json({ message: 'You cannot approve your own sold items.' });
    }

    if (respondRemark === 'ROLLBACK') {
      // Set sale back to draft
      await conn.query(`UPDATE sales SET sale_status = 'DRAFT', respondRemark = ? WHERE id = ?`, [respondRemark, id]);

      // Update deposit status
      await conn.query(`UPDATE pending_deposits SET status = 'pending' WHERE sale_id = ?`, [id]);
    } else {
      // Restore quantities back to products table
      for (const item of saleItems) {
        await conn.query(
          `UPDATE products SET qty = qty + ? WHERE id = ?`,
          [item.quantity, item.product_id]
        );
      }

      // Delete sale_items
      await conn.query(`DELETE FROM sale_items WHERE sale_id = ?`, [id]);

      // Delete the sale
      await conn.query(`DELETE FROM sales WHERE id = ?`, [id]);
    }

    res.json({ message: 'Successfully processed cancellation!' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Database error while canceling sale.' });
  }
});


// Add a new role
router.post('/addRole', auth.authenticateToken, async (req, res) => {
  const conn = await getConnection();
  const tokenPayload = res.locals;
  const { name, stores } = req.body;
  const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');


  if (!name || !stores || !Array.isArray(stores) || stores.length === 0) {
    return res.status(400).json({ message: 'Invalid data' });
  }

  try {
    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.json({ message: 'Access denied. Super admin only.' });
    }

    // Check if role name already exists (case insensitive)
    const [existingRole] = await conn.query(`SELECT * FROM roles WHERE LOWER(name) = LOWER(?)`, [name]);
    if (existingRole.length > 0) {
      return res.json({ message: 'Role name already exists' });
    }

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    // Insert role
    const [roleResult] = await conn.query(`INSERT INTO roles (name, createDate, createBy, updateBy, status) VALUES (?, ?, ?, ?, ?)`, 
      [name, now, tokenPayload.name, 'null', 'true']);
    const roleId = roleResult.insertId;

    // Insert store mappings
    const storeValues = stores.map(storeId => [roleId, storeId]);
    await conn.query(`INSERT INTO role_stores (role_id, store_id) VALUES ?`, [storeValues]);

    res.status(201).json({ message: 'Role added successfully' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Database error while adding role' });
  }
});


// Update Role
router.put('/updateRole/:id', auth.authenticateToken, async (req, res) => {
  let conn;

  try {
    const { name, stores } = req.body;
    const roleId = parseInt(req.params.id);
    const updatedBy = res.locals.name;
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    // Prevent updating Admin role (protect ID 1)
    if (roleId === 1) {
      return res.status(403).json({ message: 'You are not allowed to update the Admin role.' });
    }

    await new Promise(resolve => setTimeout(resolve, 3000)); // Simulate delay

    conn = await getConnection();

    // Update role name
    const [updateResult] = await conn.query(
      `UPDATE roles SET name = ?, updateBy = ? WHERE id = ?`,
      [name, updatedBy, roleId]
    );

    if (updateResult.affectedRows === 0) {
      return res.status(404).json({ message: 'Role not found.' });
    }

    // For non-Admin roles only, update store assignments
    if (roleId !== 1) {
      await conn.query(`DELETE FROM role_stores WHERE role_id = ?`, [roleId]);

      if (stores.length > 0) {
        const storeValues = stores.map(storeId => [roleId, storeId]);
        await conn.query(`INSERT INTO role_stores (role_id, store_id) VALUES ?`, [storeValues]);
      }
    }

    res.status(200).json({ message: `Role "${name}" updated successfully.` });

  } catch (err) {
    console.error('Error updating role:', err);
    res.status(500).json({ error: 'Internal server error.' });
  }
});


// Get Role Lists
router.get('/getRoleList', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    conn = await getConnection();
    const tokenPayload = res.locals;

    const assignedStores = tokenPayload.assignedStores || [];
    const accessAll = tokenPayload.accessAllStores;

    // Include roles with or without assigned stores (LEFT JOIN)
    let query = `
      SELECT 
        r.id, 
        r.name, 
        r.status,
        GROUP_CONCAT(DISTINCT rs.store_id) AS store_ids,
        GROUP_CONCAT(DISTINCT s.name) AS store_names
      FROM roles r 
      LEFT JOIN role_stores rs ON rs.role_id = r.id 
      LEFT JOIN stores s ON s.id = rs.store_id
    `;

    let queryParams = [];

    if (!accessAll && assignedStores.length > 0) {
      const placeholders = assignedStores.map(() => '?').join(',');
      query += `
        WHERE (rs.store_id IN (${placeholders}) OR r.id = 1)
      `;
      queryParams.push(...assignedStores);
    }

    query += `
      GROUP BY r.id 
      ORDER BY r.name ASC
    `;

    const [results] = await conn.query(query, queryParams);

    const formattedResults = results.map(role => ({
      ...role,
      stores: role.store_ids ? role.store_ids.split(',').map(id => parseInt(id)) : [],
      store_names: role.store_names ? role.store_names.split(',') : []
    }));

    res.json(formattedResults);
  } catch (err) {
    console.error('Error fetching role list:', err);
    return res.status(500).json({ error: 'Internal Server Error' });
  }
});



// Update Role Status
router.post('/updateRoleStatus', auth.authenticateToken, async (req, res) => {
  let conn;
  try {

    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds (simulating delay)
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ message: 'Access denied. Super admin only.' });
    }

    const { id, status } = req.body;

    // Ensure both id and status are provided
    if (!id || !status) {
      return res.status(400).json({ message: 'Role id and status are required' });
    }

    // Update the role's status
    const [result] = await conn.query(
      "UPDATE roles SET status = ? WHERE id = ?",
      [status, id]
    );

    if (result.affectedRows === 0) {
      return res.json({ message: `Role with id ${id} not found` });
    }

    return res.status(200).json({ message: `Role status updated successfully` });
  } catch (err) {
    console.error('Error updating role status:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
});


// Delete Role
router.delete('/deleteRole/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {

    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

    const id = req.params.id;
// Get a new connection from the pool
conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds 
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ message: 'Access denied. Super admin only.' });
    }

    const [result] = await conn.query(
      "DELETE FROM roles WHERE id = ?",
      [id]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ message: ` ${id} Not found` });
    } else {
      return res.status(200).json({ message: `Success` });
    }
  } catch (err) {
    return res.status(500).json({ message: 'Server error', details: err });
  }
});

// Get Permission by role id
router.get('/getPermissionsByRole/:roleId', auth.authenticateToken, async (req, res) => {
  const { roleId } = req.params;
  const tokenPayload = res.locals;
  let conn;

  try {
    conn = await getConnection();

    // Add delay (3s) if required
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Support multiple stores from tokenPayload.assignedStores
    let rows = [];

    if (tokenPayload.accessAllStores) {
      // If user has access to all stores
      [rows] = await conn.query(
        'SELECT storeId, menu, access, submenu FROM permissions WHERE role = ?',
        [roleId]
      );
    } else {
      // If limited to assigned stores
      const assignedStores = tokenPayload.assignedStores || [tokenPayload.storeId];

      if (!Array.isArray(assignedStores) || assignedStores.length === 0) {
        return res.json({ message: 'No assigned stores to fetch permissions for.' });
      }

      [rows] = await conn.query(
        `SELECT storeId, menu, access, submenu 
         FROM permissions 
         WHERE role = ? AND storeId IN (?)`,
        [roleId, assignedStores]
      );
    }

    res.json(rows);
  } catch (err) {
    console.error('Error fetching permissions:', err);
    res.status(500).json({ message: 'Internal server error' });
  }
});


// Save Permission ==============================

router.post('/savePermissions', auth.authenticateToken, async (req, res) => {
  const tokenPayload = res.locals;
  const { name, permissions, stores } = req.body; // stores = array of selected store IDs
  let conn;

  try {
    conn = await getConnection();

    // Check if role exists
    const [existing] = await conn.query('SELECT id FROM roles WHERE name = ?', [name]);
    if (existing.length === 0) {
      return res.json({ message: 'Role not found' });
    }

    const roleId = existing[0].id;

    // Delete ONLY existing permissions and permission_stores for this role and selected stores
    if (Array.isArray(stores) && stores.length > 0) {
      const storePlaceholders = stores.map(() => '?').join(',');
      const deleteParams = [roleId, ...stores];

      await conn.query(
        `DELETE FROM permissions WHERE role = ? AND storeId IN (${storePlaceholders})`,
        deleteParams
      );

      await conn.query(
        `DELETE FROM permission_stores WHERE role = ? AND store_id IN (${storePlaceholders})`,
        deleteParams
      );
    }

    // Insert new permissions for selected stores
    if (Array.isArray(permissions) && permissions.length > 0 && Array.isArray(stores) && stores.length > 0) {
      const permissionValues = [];

      for (const storeId of stores) {
        for (const p of permissions) {
          permissionValues.push([storeId, roleId, p.menu, p.submenu, p.access]);
        }
      }

      if (permissionValues.length > 0) {
        await conn.query(
          `INSERT INTO permissions (storeId, role, menu, submenu, access) VALUES ?`,
          [permissionValues]
        );
      }
    }

    // Insert into permission_stores
    if (Array.isArray(stores) && stores.length > 0) {
      const storeValues = stores.map(storeId => [roleId, storeId]);
      await conn.query(
        `INSERT INTO permission_stores (role, store_id) VALUES ?`,
        [storeValues]
      );
    }

    return res.status(200).json({ message: 'Permissions saved successfully!' });

  } catch (err) {
    console.error('Save Permission Error:', err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});


// Get Menu Permissions
router.get('/getMenuPermissions', auth.authenticateToken, async (req, res) => {

  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    const conn = conn.promise();
    const tokenPayload = res.locals;
    
    // Fetch the user's role
    const roleId = tokenPayload.role; // assuming you're storing the roleId in the JWT token

    // Get menu permissions based on the role
    const [permRows] = await conn.query(
      `SELECT submenu, access FROM permissions WHERE role = ?`,
      [roleId]
    );

    if (permRows.length === 0) {
      return res.json({ message: 'No permissions found for this role' });
    }

    // Format the response 
    const permissions = permRows.map(row => ({
      submenu: row.submenu,
      access: row.access,
    }));

    res.json(permissions);
  } catch (err) {
    console.error('Error fetching permissions:', err);
    res.status(500).json({ message: 'Server error fetching permissions' });
  }
});


// USER MODULES API

// Get Roles Assigned to stores exceptionally to Superadmin
router.get('/getAssignedRoleStoreExceptionAdmin', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Fetch menu access data from the database
    const [result] = await conn.query(
      `
  SELECT 
  r.id, 
  r.name, 
  GROUP_CONCAT(rs.store_id) AS store_ids
  FROM roles r
  JOIN role_stores rs ON rs.role_id = r.id
  WHERE r.name != 'ADMIN'
  GROUP BY r.id;

      `
    );

    // Return the list 
    res.status(200).json(result);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Internal server error', details: error });
  } 
});


// Add User to the database =================================

router.post('/users/add', auth.authenticateToken, async (req, res) => {
  let conn;

  try {
    conn = await getConnection();
    const {
      name,
      phone,
      email,
      role,
      stores,         // array
      warehouses      // array
    } = req.body;

    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    await new Promise(resolve => setTimeout(resolve, 3000));

    const [existing] = await conn.query(`SELECT id FROM users WHERE phone = ?`, [phone]);
    if (existing.length > 0) {
      return res.json({ message: `User with phone number ${phone} already exists.` });
    }

    const [lastUser] = await conn.query(`SELECT id FROM users ORDER BY id DESC LIMIT 1`);
    let newUserId = lastUser.length > 0 ? lastUser[0].id + 1 : 1;

    const saltRounds = 12;
    const password = Math.floor(100000 + Math.random() * 900000).toString();
    const hashedPassword = await bcrypt.hash(password, saltRounds);
    const activation_token = uuid.v1();

    // Get related store data (two_factor_auth and end_date)
    const storeIds = stores.map(() => '?').join(',');
    const [storeDetails] = await conn.query(
      `SELECT id, two_factor_auth, ownership, end_date FROM stores WHERE id IN (${storeIds})`,
      stores
    );

    // Determine if any assigned store has two_factor_auth enabled
    const enable2FA = storeDetails.some(s => s.two_factor_auth === 1) ? 1 : 0;

    // Insert user
    await conn.query(
      `INSERT INTO users (
        profileCompleted, last_password_change, id, activation_token, name, email, phone, password, createDate, updateDate,
        createBy, updateBy, userStatus, loggedIn, lastActive,
        accountDisabled, loginAttempts, attemptStatus, expiresAt,
        accountExpDate, accountExpireStatus, userDeletable,
        digitalSignature, isFirstLogin, mustChangePassword,
        role, accessAllStores, accessAllWarehouses, is_2fa_enabled
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        1,
        now,
        newUserId,
        activation_token,
        name,
        email,
        phone,
        hashedPassword,
        now,
        'null',                
        res.locals.name,
        'null',                
        'false',
        'false',
        'null',
        'false',
        0,
        'false',
        'null',                
        storeDetails[0].end_date,        
        'false',
        'false',
        'null',
        'true',
        'true',
        role,
        0,
        0,
        enable2FA            
      ]
    );

    const userId = newUserId;

    // Assign stores
    if (stores && stores.length > 0) {
      const storeInserts = stores.map(storeId => [userId, storeId]);
      await conn.query(`INSERT INTO user_stores (user_id, store_id) VALUES ?`, [storeInserts]);
    }

    // Assign warehouses
    if (warehouses && warehouses.length > 0) {
      const warehouseInserts = warehouses.map(warehouseId => [userId, warehouseId]);
      await conn.query(`INSERT INTO user_warehouses (user_id, warehouse_id) VALUES ?`, [warehouseInserts]);
    }

    // Log creation
    await conn.query(
      `INSERT INTO logs (
        user_id, store_id, action, description, createdAt, createdBy
      ) VALUES (?, ?, ?, ?, ?, ?)`,
      [
        res.locals.id,
        stores[0], // main store
        'CREATE USER',
        `User ${name} (${userId}) was created`,
        now,
        res.locals.name
      ]
    );

    // Mail config
    const [emailConfig] = await conn.execute('SELECT * FROM system_mail_configuration LIMIT 1');
    const [frontBase] = await conn.execute('SELECT * FROM front_end_base_url LIMIT 1');
    if (emailConfig.length === 0 || frontBase.length === 0) {
      return res.status(500).json({ message: 'Mail or Frontend config missing' });
    }

    const { host, port, username, password: mailPass } = emailConfig[0];
    const { baseUrl } = frontBase[0];
    const activationLink = `${baseUrl}/auth/activate-account/${activation_token}`;

    const mailText = `Ndg ${name},\n\nAkaunti yako imeundwa kwenye mfumo wa SHARED DUKA ENTERPRISES PORTAL.\nBofya kiungo hapa chini kuthibitisha akaunti yako:\n\n👉 ${activationLink}\n\n Tumia Namba ya simu: ${phone} kama username \n\nAsante.`;

    const transporter = nodemailer.createTransport({
      host,
      port: parseInt(port),
      secure: parseInt(port) === 465,
      auth: { user: username, pass: mailPass }
    });

    try {
      await transporter.sendMail({
        from: username,
        to: email,
        subject: 'Umeundwa kwenye mfumo - Thibitisha akaunti yako',
        text: mailText
      });

      await conn.query(`INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, ?)`, [email, mailText, now, 'true']);
    } catch (mailError) {
      await conn.query(`INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, ?)`, [email, mailText, now, 'false']);
    }

    return res.json({ message: `User created with ID ${newUserId} and email sent to ${email}` });

  } catch (err) {
    console.error('Error inserting user:', err);
    res.status(500).json({ message: 'Server error while adding user' });
  }
});



// Disable 2fa to user associated to store that support 2fa

router.post('/users/disable/2fa', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;

  try {
    // Delay (optional)
    await new Promise(resolve => setTimeout(resolve, 3000));

    const connection = await getConnection();

    // Get store support status for each user
    const placeholders = ids.map(() => '?').join(',');
    const [storeResults] = await connection.query(
      `
      SELECT us.user_id, s.two_factor_auth 
      FROM user_stores us
      JOIN stores s ON us.store_id = s.id
      WHERE us.user_id IN (${placeholders})
      `,
      ids
    );

    // Filter users with 2FA-supported stores
    const eligibleUserIds = storeResults
      .filter(row => row.two_factor_auth === 1)
      .map(row => row.user_id);

    // Update only eligible users
    if (eligibleUserIds.length > 0) {
      const eligiblePlaceholders = eligibleUserIds.map(() => '?').join(',');
      const [result] = await connection.query(
        `UPDATE users SET is_2fa_enabled = 0 WHERE id IN (${eligiblePlaceholders})`,
        eligibleUserIds
      );

      const skipped = ids.filter(id => !eligibleUserIds.includes(id));
      res.json({
        message: `${result.affectedRows} user(s) had 2FA disabled.`,
        skipped: skipped,
        eligible: eligibleUserIds
      });
    } else {
      res.json({ message: `No users eligible for 2FA` });
    }
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Failed to disable 2FA'});
  }
});


// Get Users List

router.get('/users/list', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT 
        u.accountDisabled,
        u.accountExpireStatus,
        u.accountExpDate,
        u.id,
        u.name,
        u.phone,
        u.email,
        u.userStatus,
        u.is_2fa_enabled,
        r.name AS role_name,
        u.createDate,
        GROUP_CONCAT(DISTINCT s.name) AS stores,
        GROUP_CONCAT(DISTINCT w.id) AS warehouse_id, 
        GROUP_CONCAT(DISTINCT w.name) AS warehouses,
        TIMESTAMPDIFF(SECOND, u.createDate, NOW()) AS seconds_since_created
      FROM users u
      JOIN roles r ON u.role = r.id
      LEFT JOIN user_stores us ON u.id = us.user_id
      LEFT JOIN stores s ON us.store_id = s.id
      LEFT JOIN user_warehouses uw ON u.id = uw.user_id
      LEFT JOIN warehouses w ON uw.warehouse_id = w.id
    `;

    const conditions = [`u.id != ?`, `r.name != 'ADMIN'`];
    const params = [userId];

    const isSuperAdmin = roleId === 1 || roleId === '1';

    if (!isSuperAdmin) {
      const [storeRows] = await connection.query(
        `SELECT store_id FROM user_stores WHERE user_id = ?`, [userId]
      );
      const [warehouseRows] = await connection.query(
        `SELECT warehouse_id FROM user_warehouses WHERE user_id = ?`, [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ users: [] });
      }

      if (storeIds.length > 0) {
        conditions.push(`us.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (warehouseIds.length > 0) {
        conditions.push(`uw.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    if (conditions.length > 0) {
      query += ` WHERE ${conditions.join(' AND ')}`;
    }

    query += ` GROUP BY u.id ORDER BY u.createDate DESC`;

    const [users] = await connection.query(query, params);

    // Convert seconds_since_created to human-readable duration
    const formatDuration = (seconds) => {
      const units = [
        { label: 'y', value: 365 * 24 * 3600 },
        { label: 'm', value: 30 * 24 * 3600 },
        { label: 'w', value: 7 * 24 * 3600 },
        { label: 'd', value: 24 * 3600 },
        { label: 'h', value: 3600 },
        { label: 'm', value: 60 },
        { label: 's', value: 1 }
      ];

      let result = '';
      for (const unit of units) {
        const count = Math.floor(seconds / unit.value);
        if (count > 0 || result) {
          result += `${count}${unit.label} `;
          seconds %= unit.value;
        }
      }

      return result.trim() || '0s';
    };

    users.forEach(user => {
      user.sinceCreated = formatDuration(user.seconds_since_created);
      delete user.seconds_since_created;
    });

    return res.json({ users });

  } catch (err) {
    console.error('[ERROR] /users/list:', err);
    return res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});



// Get Active Users List

router.get('/active/users/list', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const { storeId, warehouseId, date } = req.query;

  let connection;
  try {
    connection = await getConnection();

    // 1) Build the “warehouse filter” join if a warehouseId was passed in
    let warehouseFilterJoin = '';
    const filterParams = [];
    if (warehouseId) {
      warehouseFilterJoin = `
        JOIN user_warehouses uf
          ON uf.user_id = u.id
         AND uf.warehouse_id = ?
      `;
      filterParams.push(warehouseId);
    }

    // 2) Base SELECT + joins for aggregation
    let query = `
      SELECT 
        u.id,
        u.name,
        u.phone,
        u.email,
        u.userStatus,
        r.name AS role_name,
        GROUP_CONCAT(DISTINCT s.name) AS stores,
        GROUP_CONCAT(DISTINCT w.name) AS warehouses,
        TIMESTAMPDIFF(SECOND, u.createDate, NOW()) AS seconds_since_created
      FROM users u
      JOIN roles r ON u.role = r.id
      LEFT JOIN user_stores us ON u.id = us.user_id
      LEFT JOIN stores s ON us.store_id = s.id
      LEFT JOIN user_warehouses uw ON u.id = uw.user_id
      LEFT JOIN warehouses w ON uw.warehouse_id = w.id
      ${warehouseFilterJoin}
    `;

    const conditions = [
      `u.id != ?`,
      `u.userStatus = 'true'`,
      `r.name != 'ADMIN'`
    ];
    const params = [userId, ...filterParams];

    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        `SELECT store_id FROM user_stores WHERE user_id = ?`, [userId]
      );
      const [warehouseRows] = await connection.query(
        `SELECT warehouse_id FROM user_warehouses WHERE user_id = ?`, [userId]
      );
      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ users: [] });
      }
      if (storeIds.length) {
        conditions.push(`us.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }
      if (warehouseIds.length) {
        conditions.push(`uw.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    if (storeId) {
      conditions.push(`us.store_id = ?`);
      params.push(storeId);
    }

    if (date && typeof date === 'string') {
      try {
        const { start, end } = JSON.parse(date);
        if (start && end) {
          conditions.push(`DATE(u.createDate) BETWEEN ? AND ?`);
          params.push(start, end);
        }
      } catch (e) {
        console.warn('Invalid date filter, skipping');
      }
    }

    if (conditions.length) {
      query += ' WHERE ' + conditions.join(' AND ');
    }

    query += ' GROUP BY u.id ORDER BY u.createDate DESC';

    const [users] = await connection.query(query, params);

    // Add duration field
    function formatDuration(seconds) {
      const years = Math.floor(seconds / (365 * 24 * 3600));
      seconds %= 365 * 24 * 3600;
      const months = Math.floor(seconds / (30 * 24 * 3600));
      seconds %= 30 * 24 * 3600;
      const weeks = Math.floor(seconds / (7 * 24 * 3600));
      seconds %= 7 * 24 * 3600;
      const days = Math.floor(seconds / (24 * 3600));
      seconds %= 24 * 3600;
      const hours = Math.floor(seconds / 3600);
      seconds %= 3600;
      const minutes = Math.floor(seconds / 60);
      seconds %= 60;

      return `${years}y ${months}m ${weeks}w ${days}d ${hours}h ${minutes}m ${seconds}s`;
    }

    users.forEach(user => {
      user.sinceCreated = formatDuration(user.seconds_since_created);
      delete user.seconds_since_created;
    });

    res.json({ users });

  } catch (err) {
    console.error('Error loading users:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});


// Expired User List  

router.get('/expired/users/list', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const { storeId, warehouseId, date } = req.query;

  let connection;

  try {
    connection = await getConnection();

    const params = [userId];
    const conditions = [
      `u.id != ?`,
      `u.accountExpireStatus = 'true'`,
      `u.accountExpireStatus IS NOT NULL`,
      `u.accountExpireStatus != 'null'`,
      `r.name != 'ADMIN'`
    ];

    let warehouseFilterJoin = '';
    if (warehouseId) {
      warehouseFilterJoin = `
        JOIN user_warehouses uf ON uf.user_id = u.id AND uf.warehouse_id = ?
      `;
      params.push(warehouseId);
    }

    let query = `
      SELECT 
        u.id,
        u.name,
        u.phone,
        u.email,
        u.userStatus,
        r.name AS role_name,
        GROUP_CONCAT(DISTINCT s.name) AS stores,
        GROUP_CONCAT(DISTINCT w.name) AS warehouses,
        TIMESTAMPDIFF(SECOND, u.createDate, NOW()) AS seconds_since_created
      FROM users u
      JOIN roles r ON u.role = r.id
      LEFT JOIN user_stores us ON u.id = us.user_id
      LEFT JOIN stores s ON us.store_id = s.id
      LEFT JOIN user_warehouses uw ON u.id = uw.user_id
      LEFT JOIN warehouses w ON uw.warehouse_id = w.id
      ${warehouseFilterJoin}
    `;

    // Non-superadmin filtering
    const isSuperAdmin = roleId === 1 || roleId === '1';
    if (!isSuperAdmin) {
      const [storeRows] = await connection.query(
        `SELECT store_id FROM user_stores WHERE user_id = ?`, [userId]
      );
      const [warehouseRows] = await connection.query(
        `SELECT warehouse_id FROM user_warehouses WHERE user_id = ?`, [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ users: [] });
      }

      if (storeIds.length > 0) {
        conditions.push(`us.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (warehouseIds.length > 0) {
        conditions.push(`uw.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    // Optional store filter
    if (storeId) {
      conditions.push(`us.store_id = ?`);
      params.push(storeId);
    }

    // Optional date filter
    if (date && typeof date === 'string') {
      try {
        const { start, end } = JSON.parse(date);
        if (start && end) {
          conditions.push(`DATE(u.createDate) BETWEEN ? AND ?`);
          params.push(start, end);
        }
      } catch (err) {
        console.warn('Invalid date filter JSON:', date);
      }
    }

    // WHERE clause
    if (conditions.length) {
      query += ' WHERE ' + conditions.join(' AND ');
    }

    query += ' GROUP BY u.id ORDER BY u.createDate DESC';

    const [users] = await connection.query(query, params);

    // Format duration
    const formatDuration = (seconds) => {
      const units = [
        { label: 'y', value: 365 * 24 * 3600 },
        { label: 'm', value: 30 * 24 * 3600 },
        { label: 'w', value: 7 * 24 * 3600 },
        { label: 'd', value: 24 * 3600 },
        { label: 'h', value: 3600 },
        { label: 'm', value: 60 },
        { label: 's', value: 1 }
      ];

      return units.reduce((acc, unit) => {
        const count = Math.floor(seconds / unit.value);
        if (count > 0 || acc.length > 0) {
          acc.push(`${count}${unit.label}`);
        }
        seconds %= unit.value;
        return acc;
      }, []).join(' ') || '0s';
    };

    users.forEach(user => {
      user.sinceCreated = formatDuration(user.seconds_since_created);
      delete user.seconds_since_created;
    });

    res.json({ users });

  } catch (err) {
    console.error('Error loading expired users:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});


// Blocked User List

router.get('/blocked/users/list', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const { storeId, warehouseId, date } = req.query;

  let connection;

  try {
    connection = await getConnection();

    const params = [userId];
    const conditions = [
      `u.id != ?`,
      `u.accountDisabled = 'true'`,
      `r.name != 'ADMIN'`
    ];

    // Optional warehouse filter JOIN
    let warehouseFilterJoin = '';
    if (warehouseId) {
      warehouseFilterJoin = `
        JOIN user_warehouses uf
          ON uf.user_id = u.id
         AND uf.warehouse_id = ?
      `;
      params.push(warehouseId);
    }

    let query = `
      SELECT 
        u.id,
        u.name,
        u.phone,
        u.email,
        u.userStatus,
        r.name AS role_name,
        GROUP_CONCAT(DISTINCT s.name) AS stores,
        GROUP_CONCAT(DISTINCT w.name) AS warehouses,
        TIMESTAMPDIFF(SECOND, u.createDate, NOW()) AS seconds_since_created
      FROM users u
      JOIN roles r ON u.role = r.id
      LEFT JOIN user_stores us ON u.id = us.user_id
      LEFT JOIN stores s ON us.store_id = s.id
      LEFT JOIN user_warehouses uw ON u.id = uw.user_id
      LEFT JOIN warehouses w ON uw.warehouse_id = w.id
      ${warehouseFilterJoin}
    `;

    // Access restriction for non-superadmin
    const isSuperAdmin = roleId === 1 || roleId === '1';
    if (!isSuperAdmin) {
      const [storeRows] = await connection.query(
        `SELECT store_id FROM user_stores WHERE user_id = ?`, [userId]
      );
      const [warehouseRows] = await connection.query(
        `SELECT warehouse_id FROM user_warehouses WHERE user_id = ?`, [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ users: [] });
      }

      if (storeIds.length > 0) {
        conditions.push(`us.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (warehouseIds.length > 0) {
        conditions.push(`uw.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    // Optional store filter
    if (storeId) {
      conditions.push(`us.store_id = ?`);
      params.push(storeId);
    }

    // Optional date filter
    if (date && typeof date === 'string') {
      try {
        const { start, end } = JSON.parse(date);
        if (start && end) {
          conditions.push(`DATE(u.createDate) BETWEEN ? AND ?`);
          params.push(start, end);
        }
      } catch (e) {
        console.warn('Invalid date filter JSON:', date);
      }
    }

    if (conditions.length > 0) {
      query += ` WHERE ${conditions.join(' AND ')}`;
    }

    query += ' GROUP BY u.id ORDER BY u.createDate DESC';

    const [users] = await connection.query(query, params);

    // Convert seconds to readable format
    const formatDuration = (seconds) => {
      const units = [
        { label: 'y', value: 365 * 24 * 3600 },
        { label: 'm', value: 30 * 24 * 3600 },
        { label: 'w', value: 7 * 24 * 3600 },
        { label: 'd', value: 24 * 3600 },
        { label: 'h', value: 3600 },
        { label: 'm', value: 60 },
        { label: 's', value: 1 }
      ];

      return units.reduce((acc, unit) => {
        const count = Math.floor(seconds / unit.value);
        if (count > 0 || acc.length > 0) {
          acc.push(`${count}${unit.label}`);
        }
        seconds %= unit.value;
        return acc;
      }, []).join(' ') || '0s';
    };

    users.forEach(user => {
      user.sinceCreated = formatDuration(user.seconds_since_created);
      delete user.seconds_since_created;
    });

    res.json({ users });

  } catch (err) {
    console.error('Error loading blocked users:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});



// Pending User Lists


router.get('/pending/users/list', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const { storeId, warehouseId, date } = req.query;

  let connection;
  try {
    connection = await getConnection();

    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    const conditions = [
      `u.id != ?`,
      `u.userStatus = 'false'`,
      `r.name != 'ADMIN'`
    ];
    const params = [userId];

    // Optional JOIN for warehouse filter
    let warehouseJoin = '';
    if (warehouseId) {
      warehouseJoin = `
        JOIN user_warehouses uf ON uf.user_id = u.id AND uf.warehouse_id = ?
      `;
      params.push(warehouseId);
    }

    let query = `
      SELECT 
        u.id,
        u.name,
        u.phone,
        u.email,
        u.userStatus,
        r.name AS role_name,
        GROUP_CONCAT(DISTINCT s.name) AS stores,
        GROUP_CONCAT(DISTINCT w.name) AS warehouses,
        TIMESTAMPDIFF(SECOND, u.createDate, NOW()) AS seconds_since_created
      FROM users u
      JOIN roles r ON u.role = r.id
      LEFT JOIN user_stores us ON u.id = us.user_id
      LEFT JOIN stores s ON us.store_id = s.id
      LEFT JOIN user_warehouses uw ON u.id = uw.user_id
      LEFT JOIN warehouses w ON uw.warehouse_id = w.id
      ${warehouseJoin}
    `;

    const isSuperAdmin = roleId === 1 || roleId === '1';

    if (!isSuperAdmin) {
      const [storeRows] = await connection.query(
        `SELECT store_id FROM user_stores WHERE user_id = ?`, [userId]
      );
      const [warehouseRows] = await connection.query(
        `SELECT warehouse_id FROM user_warehouses WHERE user_id = ?`, [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ users: [] });
      }

      if (storeIds.length) {
        conditions.push(`us.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (warehouseIds.length) {
        conditions.push(`uw.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    if (storeId) {
      conditions.push(`us.store_id = ?`);
      params.push(storeId);
    }

    if (date && typeof date === 'string') {
      try {
        const { start, end } = JSON.parse(date);
        if (start && end) {
          conditions.push(`DATE(u.createDate) BETWEEN ? AND ?`);
          params.push(start, end);
        }
      } catch (err) {
        console.warn('⚠️ Invalid date JSON format:', date);
      }
    }

    if (conditions.length) {
      query += ` WHERE ${conditions.join(' AND ')}`;
    }

    query += ` GROUP BY u.id ORDER BY u.createDate DESC`;

    const [users] = await connection.query(query, params);

    // Format seconds to readable time since created
    const formatDuration = (seconds) => {
      const units = [
        { label: 'y', value: 365 * 24 * 3600 },
        { label: 'm', value: 30 * 24 * 3600 },
        { label: 'w', value: 7 * 24 * 3600 },
        { label: 'd', value: 24 * 3600 },
        { label: 'h', value: 3600 },
        { label: 'm', value: 60 },
        { label: 's', value: 1 }
      ];

      return units.reduce((acc, unit) => {
        const count = Math.floor(seconds / unit.value);
        if (count > 0 || acc.length > 0) {
          acc.push(`${count}${unit.label}`);
        }
        seconds %= unit.value;
        return acc;
      }, []).join(' ') || '0s';
    };

    users.forEach(user => {
      user.sinceCreated = formatDuration(user.seconds_since_created);
      delete user.seconds_since_created;
    });

    res.json({ users });

  } catch (err) {
    console.error('Error fetching pending users:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});



// Delete Users Data

router.post('/users/delete', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;             
  

  try {
    // Wait 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Perform the bulk 
    const placeholders = ids.map(() => '?').join(',');
    const sql = `DELETE FROM users WHERE id IN (${placeholders})`;
    const connection = await getConnection();
    const [result] = await connection.query(sql, ids);

    res.json({ message: `${result.affectedRows} Deleted successfully! `});
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Delete failed', error: err.message });
  }
});

// Lock Users Data

router.post('/users/lock', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;             

  try {
    // Wait 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Perform the bulk 
    const placeholders = ids.map(() => '?').join(',');
    const sql = `UPDATE users SET userStatus = 'false' WHERE id IN (${placeholders})`;
    const connection = await getConnection();
    const [result] = await connection.query(sql, ids);

    res.json({ message: `${result.affectedRows} Locked successfully! `});
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Lock failed', error: err.message });
  }
});

// Unlock Users Data

router.post('/users/unlock', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;             

  try {
    // Wait 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Perform the bulk 
    const placeholders = ids.map(() => '?').join(',');
    const sql = `UPDATE users SET userStatus = 'true' WHERE id IN (${placeholders})`;
    const connection = await getConnection();
    const [result] = await connection.query(sql, ids);

    res.json({ message: `${result.affectedRows} Unlocked successfully! `});
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'unlock failed', error: err.message });
  }
});


// Enable 2fa to user associated to store that support 2fa

router.post('/users/enable/2fa', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;

  try {
    // Delay (optional)
    await new Promise(resolve => setTimeout(resolve, 3000));

    const connection = await getConnection();

    // Get store support status for each user
    const placeholders = ids.map(() => '?').join(',');
    const [storeResults] = await connection.query(
      `
      SELECT us.user_id, s.two_factor_auth 
      FROM user_stores us
      JOIN stores s ON us.store_id = s.id
      WHERE us.user_id IN (${placeholders})
      `,
      ids
    );

    // Filter users with 2FA-supported stores
    const eligibleUserIds = storeResults
      .filter(row => row.two_factor_auth === 1)
      .map(row => row.user_id);

    // Update only eligible users
    if (eligibleUserIds.length > 0) {
      const eligiblePlaceholders = eligibleUserIds.map(() => '?').join(',');
      const [result] = await connection.query(
        `UPDATE users SET is_2fa_enabled = 1 WHERE id IN (${eligiblePlaceholders})`,
        eligibleUserIds
      );

      const skipped = ids.filter(id => !eligibleUserIds.includes(id));
      res.json({
        message: `${result.affectedRows} user(s) had 2FA enabled.`,
        skipped: skipped,
        eligible: eligibleUserIds
      });
    } else {
      res.json({ message: `No users eligible for 2FA` });
    }
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Failed to enable 2FA'});
  }
});


// Default Users Password
router.post('/users/default/password', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;             
  
  try {
    // Wait 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Hash password
    const password = '123456';
    const saltRounds = 12;
    const hashedPassword = await bcrypt.hash(password, saltRounds);


    // Perform the bulk 
const placeholders = ids.map(() => '?').join(',');
const sql = `UPDATE users SET password = ?, mustChangePassword = 'false' WHERE id IN (${placeholders})`;
const connection = await getConnection();
const [result] = await connection.query(sql, [hashedPassword, ...ids]);


    res.json({ message: `${result.affectedRows} password updated successfully! `});
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'default password failed', error: err.message });
  }
});


// Get users by id
router.get('/users/get/by/:id', auth.authenticateToken, async (req, res) => {
  const { id } = req.params;
  const connection = await getConnection();

  try {
    // Get user basic info
    const [userRows] = await connection.query(`SELECT * FROM users WHERE id = ?`, [id]);
    if (userRows.length === 0) return res.status(404).json({ message: 'User not found' });

    const user = userRows[0];

    // Get assigned stores
    const [storeRows] = await connection.query(
      `SELECT store_id FROM user_stores WHERE user_id = ?`,
      [id]
    );

    const assignedStores = storeRows.map(row => row.store_id);

    // Get assigned warehouses
    const [warehouseRows] = await connection.query(
      `SELECT warehouse_id FROM user_warehouses WHERE user_id = ?`,
      [id]
    );

    const assignedWarehouses = warehouseRows.map(row => row.warehouse_id);

    // Return combined user info
    res.json({
      ...user,
      stores: assignedStores,
      warehouses: assignedWarehouses
    });

  } catch (error) {
    console.error('Error fetching user:', error);
    res.status(500).json({ message: 'Internal server error' });
  }
});

// Update Users Data
router.put('/users/update/:id', auth.authenticateToken, async (req, res) => {
  const { id } = req.params;
  const { name, phone, email, store, role, warehouses } = req.body;
  const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

  const trx =  await getConnection();

  try {
    await trx.beginTransaction();
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Update user info
    await trx.query(
      `UPDATE users 
       SET updateDate = ?, updateBy = ?, name = ?, phone = ?, email = ?, role = ? 
       WHERE id = ?`,
      [now, res.locals.name, name, phone, email, role, id]
    );

    // Insert into logs
    await trx.query(
      `INSERT INTO logs (
          user_id, store_id, action, description, createdAt, createdBy
      ) VALUES (?, ?, ?, ?, ?, ?)`,
      [
        res.locals.id,             // ID of the user performing the action
        store,                    // Store IDs
        'UPDATE USER',             // Action type
        `User ${name} (${id}) was updated`, // Description
        now,                // Timestamp
        res.locals.name           // Name of the user who did the action
      ]
    );

    // Remove old store and warehouse links
    await trx.query(`DELETE FROM user_stores WHERE user_id = ?`, [id]);
    await trx.query(`DELETE FROM user_warehouses WHERE user_id = ?`, [id]);

    // Insert new store assignment
    await trx.query(`INSERT INTO user_stores (user_id, store_id) VALUES (?, ?)`, [id, store]);

    // Insert new warehouse assignments
    if (warehouses && warehouses.length > 0) {
      const warehouseValues = warehouses.map(wid => [id, wid]);
      await trx.query(`INSERT INTO user_warehouses (user_id, warehouse_id) VALUES ?`, [warehouseValues]);
    }

    await trx.commit();
    res.json({ message: 'User updated successfully' });

  } catch (error) {
    res.status(500).json({ message: 'Something went wrong' });
  } 
});

// CUSTOMER MODULE API

// Create Customer
router.post('/customers/add', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const { name, phone, store, warehouse } = req.body;
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Optional: Simulate delay
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Check for existing
    const [existing] = await conn.query(
      `SELECT * FROM customers WHERE name = ? OR phone = ? `,
      [name, phone]
    );

    if (existing.length <= 0) {
      
    // Optional: Another delay before insert
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Step 3: Insert new 
    const [result] = await conn.query(
      `INSERT INTO customers (name, phone, store_id, warehouse_id, created_at, created_by, customer_status)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [name, phone, store, warehouse, now, res.locals.name, 'true']
    );

    const id = result.insertId;

    // Insert into logs
    await conn.query(
      `INSERT INTO logs (
          user_id, store_id, action, description, createdAt, createdBy
      ) VALUES (?, ?, ?, ?, ?, ?)`,
      [
        res.locals.id,             // ID of the user performing the action
        store,                    // Store IDs
        'CREATE CUSTOMER',             // Action type
        `Customer ${name} (${id}) was created`, // Description
        now,                // Timestamp
        res.locals.name           // Name of the user who did the action
      ]
    );

 res.json({
      message: `Customer of "${name}" created successfully!`
    });
}
  } catch (err) {
    res.status(500).json({ message: 'Internal server error!' });
  } 
});


// Get Customers Data
router.get('/customers/list', auth.authenticateToken, async (req, res) => {
  

const userId = res.locals.id;
  const roleId = res.locals.role;
  const { storeId, warehouseId } = req.query;


  let connection;

  try {
    connection = await getConnection();

    // Base query
    let query = `
      SELECT c.*, 
        s.name AS store_name, s.id AS store_id, 
        w.name AS warehouse_name, w.id AS warehouse_id
      FROM customers c
      JOIN stores s ON s.id = c.store_id
      JOIN warehouses w ON w.id = c.warehouse_id
    `;

    const params = [];
    const whereConditions = [];

    // If the user is not an admin, apply restrictions based on their assigned stores and warehouses
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      // If user has no stores or warehouses assigned, return an empty response
      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      // Conditions for stores and warehouses assigned to the user
      const accessConditions = [];

      if (storeIds.length > 0) {
        accessConditions.push(`c.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (warehouseIds.length > 0) {
        accessConditions.push(`c.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }

      if (storeIds.length > 0 && warehouseIds.length > 0) {
        whereConditions.push(`(c.store_id IN (${storeIds.map(() => '?').join(',')}) AND c.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
        params.push(...storeIds, ...warehouseIds);
      } else if (storeIds.length > 0) {
        whereConditions.push(`c.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } else if (warehouseIds.length > 0) {
        whereConditions.push(`c.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }

    }

    // Admin doesn't need store/warehouse filters, apply optional filters if passed
    if (storeId) {
      whereConditions.push(`c.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`c.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // If there are any where conditions, add them to the query
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sort the result 
    query += ` ORDER BY c.name ASC`;

    const [result] = await connection.query(query, params);
    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }


});


// SUPPLIER MODULE API
// Create Supplier
router.post('/suppliers/add', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const {email, name, phone, store, warehouse } = req.body;
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection


    // Optional: Simulate delay
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Check for existing
    const [existing] = await conn.query(
      `SELECT * FROM suppliers WHERE name = ? OR phone = ? `,
      [name, phone]
    );

    if (existing.length > 0) {
      return res.json({
        message: `Name "${name}", phone "${phone}" already exists.`
      });
    }

    // Optional: Another delay before insert
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Insert new 
    const [result] =  await conn.query(
      `INSERT INTO suppliers (email, name, phone, store_id, warehouse_id, created_at, created_by, supplier_status)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [email, name, phone, store, warehouse, now, res.locals.name, 'true']
    );

    const id = result.insertId;

    // Insert into logs
    await conn.query(
      `INSERT INTO logs (
          user_id, store_id, action, description, createdAt, createdBy
      ) VALUES (?, ?, ?, ?, ?, ?)`,
      [
        res.locals.id,             // ID of the user performing the action
        store,                    // Store IDs
        'CREATE SUPPLIER',             // Action type
        `Supplier ${name} (${id}) was created`, // Description
        now,                // Timestamp
        res.locals.name           // Name of the user who did the action
      ]
    );

    res.json({
      message: `Supplier of "${name}" created successfully!`
    });

  } catch (err) {
    res.status(500).json({ error: 'Internal server error!' });
  } 
});


// PRODUCTS MODULE API

// Manage Brand

// Add Brand
router.post('/products/add/brand', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const { name, store, warehouse } = req.body;
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Optional: Simulate delay
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Check for existing
    const [existing] = await conn.query(
      `SELECT * FROM product_brands WHERE name = ? AND warehouse_id = ?`,
      [name, warehouse]
    );

    if (existing.length > 0) {
    return res.json({
        message: `Name "${name}" already exists.`
      });
    }

    else {

    // Optional: Another delay before insert
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Step 3: Insert new 
    const [result] = await conn.query(
      `INSERT INTO product_brands ( name, store_id, warehouse_id, brand_create_date, brand_create_by, brand_status)
       VALUES ( ?, ?, ?, ?, ?, ?)`,
      [ name, store, warehouse, now, res.locals.name, 'true']
    );

    const id = result.insertId;

    // Insert into logs
    await conn.query(
      `INSERT INTO logs (
          user_id, store_id, action, description, createdAt, createdBy
      ) VALUES (?, ?, ?, ?, ?, ?)`,
      [
        res.locals.id,             // ID of the user performing the action
        store,                    // Store IDs
        'CREATE BRAND',             // Action type
        `Brand ${name} (${id}) was created`, // Description
        now,                // Timestamp
        res.locals.name           // Name of the user who did the action
      ]
    );
    return res.status(200).json({ message: `Brand of "${name}" created successfully!` });
    }

  } catch (err) {
   res.json({ message: 'Internal server error!' });
  } 
});

// GET TOP SELLING 10 ROWS

router.get('/get/top/selling/lists/ten/row', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const { storeId, warehouseId } = req.query;

  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT 
        p.name AS name, 
        SUM(si.quantity) AS totalSales
      FROM sales sa
      JOIN sale_items si ON si.sale_id = sa.id
      LEFT JOIN products p ON p.id = si.product_id
    `;

    const params = [];
    const whereConditions = [`sa.sale_status = 'APPROVED'`];

    // User access filter
    if (roleId !== 1 && roleId !== '1') {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      if (storeIds.length > 0) {
        whereConditions.push(`sa.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (warehouseIds.length > 0) {
        whereConditions.push(`sa.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    // Optional filters from query
    if (storeId) {
      whereConditions.push(`sa.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`sa.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // Apply WHERE conditions
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Group by product and limit to top 10
    query += `
      GROUP BY p.id
      ORDER BY totalSales DESC
      LIMIT 10
    `;

    const [result] = await connection.query(query, params);
    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching top selling products:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});



// Get Top Selling Lists

router.get('/get/top/selling/lists', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const { storeId, warehouseId } = req.query;

  let connection;

  try {
    connection = await getConnection();

    // Base query
    let query = `
      SELECT 
        p.name AS name, 
        COUNT(si.id) AS totalSales,
        sa.sale_status,
        s.name AS storename, s.id AS store_id, 
        w.name AS warehousename, w.id AS warehouse_id
      FROM sales sa
      JOIN sale_items si ON si.sale_id = sa.id
      LEFT JOIN products p ON p.id = si.product_id
      JOIN stores s ON s.id = sa.store_id
      JOIN warehouses w ON w.id = sa.warehouse_id
    `;

    const params = [];
    const whereConditions = [`sa.sale_status = 'APPROVED'`];

    // User access control for non-admins
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      if (storeIds.length > 0) {
        whereConditions.push(`sa.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (warehouseIds.length > 0) {
        whereConditions.push(`sa.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    // Optional filters
    if (storeId) {
      whereConditions.push(`sa.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`sa.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // Final WHERE clause
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Group and sort by total sales
    query += `
      GROUP BY p.id, sa.sale_status, s.id, w.id
      ORDER BY totalSales DESC
    `;

    const [result] = await connection.query(query, params);
    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching top selling lists:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});


// Get Best Supplier Lists

router.get('/get/best/suppliers/supplied', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const { storeId, warehouseId } = req.query;

  let connection;

  try {
    connection = await getConnection();

    // Base query
    let query = `
      SELECT 
        su.name AS name, 
        COUNT(*) AS orders, 
        SUM(pu.grand_total) AS total,
        pu.purchase_status, 
        s.name AS storename, 
        s.id AS store_id, 
        w.name AS warehousename, 
        w.id AS warehouse_id
      FROM purchases pu
      JOIN stores s ON s.id = pu.store_id
      JOIN warehouses w ON w.id = pu.warehouse_id
      JOIN suppliers su ON su.id = pu.supplier_id
    `;

    const params = [];
    const whereConditions = [`pu.purchase_status = 'APPROVED'`];

    // Apply filters for non-admin users
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      if (storeIds.length > 0) {
        whereConditions.push(`pu.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (warehouseIds.length > 0) {
        whereConditions.push(`pu.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    // Apply optional filters for admin or general user
    if (storeId) {
      whereConditions.push(`pu.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`pu.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // Final WHERE clause
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Grouping and ordering
    query += `
      GROUP BY su.id, pu.purchase_status, s.id, w.id
      ORDER BY total DESC
    `;

    const [result] = await connection.query(query, params);
    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching best suppliers:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});


// Get Best Customer Lists

router.get('/get/best/sales/customers', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const { storeId, warehouseId } = req.query;

  let connection;

  try {
    connection = await getConnection();

    // Base query
    let query = `
      SELECT 
        c.name AS name, 
        COUNT(*) AS orders, 
        SUM(sa.grand_total) AS total,
        sa.sale_status, 
        s.name AS storename, 
        s.id AS store_id, 
        w.name AS warehousename, 
        w.id AS warehouse_id
      FROM sales sa
      JOIN stores s ON s.id = sa.store_id
      JOIN warehouses w ON w.id = sa.warehouse_id
      JOIN customers c ON c.id = sa.customer_id
    `;

    const params = [];
    const whereConditions = [`sa.sale_status = 'APPROVED'`];

    // Apply filters for non-admin users
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      if (storeIds.length > 0) {
        whereConditions.push(`sa.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (warehouseIds.length > 0) {
        whereConditions.push(`sa.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    // Apply optional filters for admin or general user
    if (storeId) {
      whereConditions.push(`sa.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`sa.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // Final WHERE clause
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Grouping and ordering
    query += `
      GROUP BY c.id, sa.sale_status, s.id, w.id
      ORDER BY total DESC
    `;

    const [result] = await connection.query(query, params);
    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching best sales customers:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});



// Get Products Brand
router.get('/get/products/brand', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const { storeId, warehouseId } = req.query;


  let connection;

  try {
    connection = await getConnection();

    // Base query
    let query = `
      SELECT b.*, 
      s.name AS storename, s.id AS store_id, 
      w.name AS warehousename, w.id AS warehouse_id
      FROM product_brands b
      JOIN stores s ON s.id = b.store_id
      JOIN warehouses w ON w.id = b.warehouse_id
    `;

    const params = [];
    const whereConditions = [];

    // If the user is not an admin, apply restrictions based on their assigned stores and warehouses
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      // If user has no stores or warehouses assigned, return an empty response
      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      // Conditions for stores and warehouses assigned to the user
      const accessConditions = [];

      if (storeIds.length > 0) {
        accessConditions.push(`b.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (warehouseIds.length > 0) {
        accessConditions.push(`b.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }

      if (storeIds.length > 0 && warehouseIds.length > 0) {
        whereConditions.push(`(b.store_id IN (${storeIds.map(() => '?').join(',')}) AND b.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
        params.push(...storeIds, ...warehouseIds);
      } else if (storeIds.length > 0) {
        whereConditions.push(`b.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } else if (warehouseIds.length > 0) {
        whereConditions.push(`b.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    
    }

    // Admin doesn't need store/warehouse filters, apply optional filters if passed
    if (storeId) {
      whereConditions.push(`b.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`b.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // If there are any where conditions, add them to the query
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sort the result 
    query += ` ORDER BY b.name ASC`;

    const [result] = await connection.query(query, params);
    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});



// Get Only Active Product Lists Data
router.get('/get/products/active/lists', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  let connection;
  try {
    connection = await getConnection();

    // Base query
    let query = `
      SELECT 
        p.batch_no, p.barcode_no, p.id, p.name, p.qty, p.cost, p.price, p.imei_serial, p.expire_date,
        p.vat, p.discount, p.product_create_date, p.product_create_by,
        p.product_update_date, p.product_update_by, p.product_status, p.product_qty_alert,
        p.refNumber, p.store_id, p.warehouse_id, p.category_id, p.brand_id, p.unit_id, 
        s.name AS storename, 
        w.name AS warehousename
      FROM 
        products p
      JOIN 
        stores s ON s.id = p.store_id
      JOIN 
        warehouses w ON w.id = p.warehouse_id
      WHERE 
        p.product_status = 'true'
    `;

    const params = [];

    if (!(roleId === 1 || roleId === '1')) {
      // NOT superadmin - apply user assigned stores/warehouses
      const [storeRows] = await connection.query(
        `SELECT store_id FROM user_stores WHERE user_id = ?`, [userId]
      );
      const [warehouseRows] = await connection.query(
        `SELECT warehouse_id FROM user_warehouses WHERE user_id = ?`, [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ active: [] });
      }

      const conditions = [];

      if (storeIds.length) {
        conditions.push(`p.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (warehouseIds.length) {
        conditions.push(`p.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }

      if (conditions.length > 0) {
        // Combine with existing WHERE using AND (...)
        query += ` AND (` + conditions.join(' OR ') + `)`;
      }
    } 
    // ELSE superadmin - no filter

    query += ` ORDER BY p.name ASC`;

    const [result] = await connection.query(query, params);
    res.json({ array: result });

  } catch (err) {
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});


// Get Active Product Brand
router.get('/get/products/active/brand', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  let connection;
  try {
    connection = await getConnection();

    // Base query
    let query = `
      SELECT 
        b.id, 
        b.name, 
        b.store_id, 
        b.warehouse_id,  
        b.brand_status, 
        s.name AS storename, 
        w.name AS warehousename
      FROM 
        product_brands b
      JOIN 
        stores s ON s.id = b.store_id
      JOIN 
        warehouses w ON w.id = b.warehouse_id
      WHERE 
        b.brand_status = 'true'
    `;

    const params = [];

    if (!(roleId === 1 || roleId === '1')) {
      // NOT superadmin - apply user assigned stores/warehouses
      const [storeRows] = await connection.query(
        `SELECT store_id FROM user_stores WHERE user_id = ?`, [userId]
      );
      const [warehouseRows] = await connection.query(
        `SELECT warehouse_id FROM user_warehouses WHERE user_id = ?`, [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ active: [] });
      }

      const conditions = [];

      if (storeIds.length) {
        conditions.push(`b.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (warehouseIds.length) {
        conditions.push(`b.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }

      if (conditions.length > 0) {
        // Combine with existing WHERE using AND (...)
        query += ` AND (` + conditions.join(' OR ') + `)`;
      }
    } 
    // ELSE superadmin - no filter

    query += ` ORDER BY b.name ASC`;

    const [result] = await connection.query(query, params);
    res.json({ brand: result });

  } catch (err) {
    res.json({ message: 'Something went wrong', error: err.message });
  }
});



// Lock Products Brand Data
router.post('/products/brand/lock', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;             
  

  try {
    // Wait 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Perform the bulk 
    const placeholders = ids.map(() => '?').join(',');
    const sql = `UPDATE product_brands SET brand_status = 'false' WHERE id IN (${placeholders})`;
    const connection = await getConnection();
    const [result] = await connection.query(sql, ids);

    res.json({ message: `${result.affectedRows} Locked successfully! `});
  } catch (err) {
    console.error(err);
    res.json({ message: 'Lock failed', error: err.message });
  }
});

// Unlock Products Brand Data
router.post('/products/brand/unlock', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;             

  try {
    // Wait 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Perform the bulk 
    const placeholders = ids.map(() => '?').join(',');
    const sql = `UPDATE product_brands SET brand_status = 'true' WHERE id IN (${placeholders})`;
    const connection = await getConnection();
    const [result] = await connection.query(sql, ids);

    res.json({ message: `${result.affectedRows} Unlocked successfully! `});
  } catch (err) {
    console.error(err);
    res.json({ message: 'Unlock failed', error: err.message });
  }
});


// Unlock Products Brand Data
router.post('/products/brand/delete', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;             
 
  try {
    // Wait 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Perform the bulk 
    const placeholders = ids.map(() => '?').join(',');
    const sql = `DELETE FROM product_brands WHERE id IN (${placeholders})`;
    const connection = await getConnection();
    const [result] = await connection.query(sql, ids);

    res.json({ message: `${result.affectedRows} Deleted successfully! `});
  } catch (err) {
    console.error(err);
    res.json({ message: 'Delete failed', error: err.message });
  }
});

// Update Product brand
router.put('/products/brand/update/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const { name, store, warehouse } = req.body;
    const id = req.params.id;
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    //  Update 
    const [result] = await conn.query(
      `UPDATE product_brands SET name = ?, store_id = ?, warehouse_id = ? WHERE id = ?`,
      [name, store, warehouse, id]
    );

    // Insert into logs
    await conn.query(
      `INSERT INTO logs (
          user_id, store_id, action, description, createdAt, createdBy
      ) VALUES (?, ?, ?, ?, ?, ?)`,
      [
        res.locals.id,             // ID of the user performing the action
        store,                    // Store IDs
        'UPDATE BRAND',             // Action type
        `Brand ${name} (${id}) was updated`, // Description
        now,                // Timestamp
        res.locals.name           // Name of the user who did the action
      ]
    );

    if (result.affectedRows === 0) {
      res.json({ message: "Id not found" });
    }

    res.json({
      message: `Brand "${name}" updated successfully!`
    });

  } catch (err) {
    console.error('Update Error:', err);
    res.status(500).json({ error: 'Internal server error' });
  } 
});


// Manage Unit

// Add Unit
router.post('/products/add/unit', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const { name, short_name,
      base_unit, operator_symbol, operator_value
     } = req.body;

     const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Optional: Simulate delay
    await new Promise(resolve => setTimeout(resolve, 3000));

  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

  // Only allow super admins
  if (!isSuperAdmin) {
  return res.status(403).json({ message: 'Access denied.' });
  }

    // Check for existing
    const [existing] = await conn.query(
      `SELECT * FROM product_units WHERE name = ? `,
      [name]
    );

    if (existing.length > 0) {
      return res.json({
        message: `Name "${name}" already exists.`
      });
    }

    else {

    // Optional: Another delay before insert
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Insert new 
    await conn.query(
      `INSERT INTO product_units (operator_value, operator_symbol, base_unit, short_name,  name, unit_create_date, unit_create_by, unit_status)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [operator_value, operator_symbol, base_unit, short_name, name, now, res.locals.name, 'true']
    );

    return res.status(201).json({
      message: `Unit of "${name}" created successfully!`
    });
  }

  } catch (err) {
    res.status(500).json({ message: 'Internal server error!' });
  } 
});


router.get('/get/products/unit', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Use async/await with MySQL query
    const [results] = await conn.query('SELECT * FROM product_units ORDER BY name ASC');

    // Return the list 
    res.json(results);

  } catch (err) {
    return res.status(500).json({ message: 'Internal server error' });
  } 
});


router.get('/get/products/active/unit', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Use async/await with MySQL query
    const [results] = await conn.query('SELECT * FROM product_units WHERE unit_status = "true" ORDER BY name ASC');

    // Return the list 
    res.json(results);

  } catch (err) {
   return res.status(500).json({ message: 'Internal server error' });
  } 
});

// Lock Products Unit Data
router.post('/products/unit/lock', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;             
 
  try {
    // Wait 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

  // Only allow super admins
  if (!isSuperAdmin) {
    return res.status(403).json({ message: 'Access denied.' });
  }

    // Perform the bulk 
    const placeholders = ids.map(() => '?').join(',');
    const sql = `UPDATE product_units SET unit_status = 'false' WHERE id IN (${placeholders})`;
    const connection = await getConnection();
    const [result] = await connection.query(sql, ids);

    res.json({ message: `${result.affectedRows} Locked successfully! `});
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Lock failed', error: err.message });
  }
});


// Unlock Products Unit Data
router.post('/products/unit/unlock', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;             
  
  try {
    // Wait 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

     roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

    if (!isSuperAdmin) {
      return res.status(403).json({ message: 'Access denied.' });
    }

    // Perform the bulk 
    const placeholders = ids.map(() => '?').join(',');
    const sql = `UPDATE product_units SET unit_status = 'true' WHERE id IN (${placeholders})`;
    const connection = await getConnection();
    const [result] = await connection.query(sql, ids);

    res.json({ message: `${result.affectedRows} Unlocked successfully! `});
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Unlock failed', error: err.message });
  }
});


// Delete Products Unit Data
router.post('/products/unit/delete', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;             

  try {
    // Wait 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

  if (!isSuperAdmin) {
    return res.status(403).json({ message: 'Access denied.' });
  }

    // Perform the bulk 
    const placeholders = ids.map(() => '?').join(',');
    const sql = `DELETE FROM product_units WHERE id IN (${placeholders})`;
    const connection = await getConnection();
    const [result] = await connection.query(sql, ids);

    res.json({ message: `${result.affectedRows} Deleted successfully! `});
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Delete failed', error: err.message });
  }
});


// Update Product unit
router.put('/products/unit/update/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const { name, short_name,
      base_unit, operator_symbol, operator_value } = req.body;
    const id = req.params.id;

    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    if (!isSuperAdmin) {
      return res.status(403).json({ message: 'Access denied' });
    }

    // Update 
    const [result] = await conn.query(
      `UPDATE product_units SET name = ?, short_name = ?, base_unit = ?, operator_symbol = ?, operator_value = ? WHERE id = ?`,
      [name, short_name, base_unit, operator_symbol, operator_value, id]
    );

    if (result.affectedRows === 0) {
      res.json({ message: "Id not found" });
    }

    res.json({
      message: `Unit "${name}" updated successfully!`
    });

  } catch (err) {
    console.error('Update Error:', err);
    res.status(500).json({ error: 'Internal server error' });
  } 
});


// Add Category
router.post('/products/add/category', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const { name, store, warehouse } = req.body;
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');


    conn = await getConnection();

    await new Promise(resolve => setTimeout(resolve, 3000));

    const [existing] = await conn.query(
      `SELECT * FROM product_category WHERE name = ? AND warehouse_id = ? `,
      [name, warehouse]
    );

    if (existing.length > 0) {
      return res.json({ message: `Name "${name}" already exists.` });
    }

    await new Promise(resolve => setTimeout(resolve, 3000));

    const [result] = await conn.query(
      `INSERT INTO product_category ( name, store_id, warehouse_id, category_create_date, category_create_by, category_status)
       VALUES ( ?, ?, ?, ?, ?, ?)`,
      [ name, store, warehouse, now, res.locals.name, 'true']
    );

    const categoryId = result.insertId;

    await conn.query(
      `INSERT INTO logs (
          user_id, store_id, action, description, createdAt, createdBy
      ) VALUES (?, ?, ?, ?, ?, ?)`,
      [
        res.locals.id,
        store,
        'CREATE CATEGORY',
        `Category ${name} (${categoryId}) was created`,
        now,
        res.locals.name
      ]
    );

    res.json({ message: `Category of "${name}" created successfully!` });

  } catch (err) {
    console.error('Error creating category:', err);
    res.status(500).json({ message: 'Internal server error!' });
  }
});


// Get Products Category
router.get('/get/products/category', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const { storeId, warehouseId } = req.query;


  let connection;

  try {
    connection = await getConnection();

    // Base query
    let query = `
      SELECT c.*, 
      s.name AS storename, s.id AS store_id, 
      w.name AS warehousename, w.id AS warehouse_id
      FROM product_category c
      JOIN stores s ON s.id = c.store_id
      JOIN warehouses w ON w.id = c.warehouse_id
    `;

    const params = [];
    const whereConditions = [];

    // If the user is not an admin, apply restrictions based on their assigned stores and warehouses
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      // If user has no stores or warehouses assigned, return an empty response
      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      // Conditions for stores and warehouses assigned to the user
      const accessConditions = [];

      if (storeIds.length > 0) {
        accessConditions.push(`c.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (warehouseIds.length > 0) {
        accessConditions.push(`c.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }

      if (storeIds.length > 0 && warehouseIds.length > 0) {
        whereConditions.push(`(c.store_id IN (${storeIds.map(() => '?').join(',')}) AND c.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
        params.push(...storeIds, ...warehouseIds);
      } else if (storeIds.length > 0) {
        whereConditions.push(`c.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } else if (warehouseIds.length > 0) {
        whereConditions.push(`c.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    
    }

    // Admin doesn't need store/warehouse filters, apply optional filters if passed
    if (storeId) {
      whereConditions.push(`c.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`c.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // If there are any where conditions, add them to the query
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sort the result 
    query += ` ORDER BY c.name ASC`;

    const [result] = await connection.query(query, params);
    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});


// Get Product Active Category
router.get('/get/products/active/category', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  let connection;
  try {
    connection = await getConnection();

    // Base query
    let query = `
      SELECT 
        c.id, 
        c.name, 
        c.store_id, 
        c.warehouse_id, 
        c.category_create_date,
        c.category_create_by, 
        c.category_status, 
        s.name AS storename, 
        w.name AS warehousename
      FROM 
        product_category c
      JOIN 
        stores s ON s.id = c.store_id
      JOIN 
        warehouses w ON w.id = c.warehouse_id
      WHERE 
        c.category_status = 'true'
    `;

    const params = [];

    if (!(roleId === 1 || roleId === '1')) {
      // NOT superadmin - apply user assigned stores/warehouses
      const [storeRows] = await connection.query(
        `SELECT store_id FROM user_stores WHERE user_id = ?`, [userId]
      );
      const [warehouseRows] = await connection.query(
        `SELECT warehouse_id FROM user_warehouses WHERE user_id = ?`, [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ active: [] });
      }

      const conditions = [];

      if (storeIds.length) {
        conditions.push(`c.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (warehouseIds.length) {
        conditions.push(`c.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }

      if (conditions.length > 0) {
        // Combine with existing WHERE using AND (...)
        query += ` AND (` + conditions.join(' OR ') + `)`;
      }
    } 
    // ELSE superadmin - no filter

    query += ` ORDER BY c.name ASC`;

    const [result] = await connection.query(query, params);
    res.json({ active: result });

  } catch (err) {
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});



// Lock Products Category Data
router.post('/products/category/lock', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;             

  try {
    // Wait 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Perform the bulk 
    const placeholders = ids.map(() => '?').join(',');
    const sql = `UPDATE product_category SET category_status = 'false' WHERE id IN (${placeholders})`;
    const connection = await getConnection();
    const [result] = await connection.query(sql, ids);

    res.json({ message: `${result.affectedRows} Locked successfully! `});
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Lock failed', error: err.message });
  }
});


// Unlock Products Category Data
router.post('/products/category/unlock', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;             
  
  try {
    // Wait 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Perform the bulk 
    const placeholders = ids.map(() => '?').join(',');
    const sql = `UPDATE product_category SET category_status = 'true' WHERE id IN (${placeholders})`;
    const connection = await getConnection();
    const [result] = await connection.query(sql, ids);

    res.json({ message: `${result.affectedRows} Unlocked successfully! `});
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Unlock failed', error: err.message });
  }
});


// Delete Products Category Data
router.post('/products/category/delete', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;             
  
  try {
    // Wait 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Perform the bulk 
    const placeholders = ids.map(() => '?').join(',');
    const sql = `DELETE FROM product_category WHERE id IN (${placeholders})`;
    const connection = await getConnection();
    const [result] = await connection.query(sql, ids);

    res.json({ message: `${result.affectedRows} Deleted successfully! `});
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Delete failed', error: err.message });
  }
});


// Update Product category
router.put('/products/category/update/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const { name, store, warehouse } = req.body;
    const id = req.params.id;
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    // Step 3: Update 
    const [result] = await conn.query(
      `UPDATE product_category SET name = ?, store_id = ?, warehouse_id = ? WHERE id = ?`,
      [name, store, warehouse, id]
    );

    // Insert into logs
    await conn.query(
      `INSERT INTO logs (
          user_id, store_id, action, description, createdAt, createdBy
      ) VALUES (?, ?, ?, ?, ?, ?)`,
      [
        res.locals.id,             // ID of the user performing the action
        store,                    // Store IDs
        'UPDATE PRODUCT CATEGORY',             // Action type
        `Category ${name} (${id}) was updated`, // Description
        now,                // Timestamp
        res.locals.name           // Name of the user who did the action
      ]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ message: "Id not found" });
    }

    res.json({
      message: `Category "${name}" updated successfully!`
    });

  } catch (err) {
    console.error('Update Error:', err);
    res.status(500).json({ error: 'Internal server error' });
  } 
});


// Create Product Manually
router.post('/products/create/product', auth.authenticateToken, async (req, res) => {
  const { items, store, warehouse } = req.body;

  let connection;
  try {
    connection = await getConnection();
    await connection.beginTransaction();

    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    const store_id = store;
    const warehouse_id = warehouse;

    // Fetch warehouse settings
    const [warehouseRows] = await connection.query(
      `SELECT supports_barcode, batch_number FROM warehouses WHERE id = ? LIMIT 1`,
      [warehouse_id]
    );

    const supportsBarcode = warehouseRows[0]?.supports_barcode === 1;
    const supportsBatch = warehouseRows[0]?.batch_number === 1;

    for (const item of items) {
      const {
        category,
        brand,
        unit,
        name,
        totalQty,
        cost,
        price,
        imeiSerial,
        expireDate,
        vat,
        discount,
        batchno,
        barcode
      } = item;

      //  Conditional check for existing products
      let existingCheckQuery = '';
      let existingParams = [];

      if (supportsBarcode || supportsBatch) {
        // Check based on name + IMEI or expireDate
        existingCheckQuery = `SELECT id FROM products WHERE store_id = ? AND warehouse_id = ? AND ( batch_no = ? OR barcode_no = ? )`;
        existingParams = [store_id, warehouse_id, batchno, barcode];
      } else {
        // Check based only on name if warehouse doesn't support barcode/batch
        existingCheckQuery = `SELECT id FROM products WHERE name = ? AND store_id = ? AND warehouse_id = ?`;
        existingParams = [name, store_id, warehouse_id];
      }

      const [existing] = await connection.query(existingCheckQuery, existingParams);

      if (existing.length > 0) {
        // Wait and skip duplicate
        await new Promise(resolve => setTimeout(resolve, 3000));
        continue;
      }

      // Generate Reference Number
      
      const refNumber = Math.floor(100000000 + Math.random() * 900000);
    
      await new Promise(resolve => setTimeout(resolve, 3000));

      // Insert new product
      const insertQuery = `
        INSERT INTO products (
          store_id, warehouse_id, category_id, brand_id, unit_id, refNumber,
          name, qty, cost, price, imei_serial, expire_date, vat, discount,
          product_create_date, product_create_by, product_update_date, product_update_by,
          product_status, product_qty_alert, batch_no, barcode_no
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `;

      const values = [
        store_id,
        warehouse_id,
        category,
        brand,
        unit,
        refNumber,
        name,
        totalQty,
        cost,
        price,
        imeiSerial || 'null',
        expireDate || 'null',
        vat ?? 0,
        discount ?? 0,
        now,
        res.locals.name || 'system',
        null,
        null,
        'true',
        0,
        batchno || 'null',
        barcode || 'null'
      ];

      await connection.query(insertQuery, values);
    }

    // Log the action
    await connection.query(
      `INSERT INTO logs (
          user_id, store_id, action, description, createdAt, createdBy
      ) VALUES (?, ?, ?, ?, ?, ?)`,
      [
        res.locals.id,
        store_id,
        'CREATE PRODUCT',
        `Product was created`,
        now,
        res.locals.name
      ]
    );

    await connection.commit();

    return res.status(200).json({ success: true, message: 'Product data submitted successfully' });

  } catch (error) {
    if (connection) await connection.rollback();
    console.error('Error during product data submission:', error);
    return res.status(500).json({ success: false, message: 'Failed to submit product data' });
  }
});



// Generate Category, Brands and Units Dropdowns

router.post('/generate/products/template', auth.authenticateToken, async (req, res) => {
  let connection;

  try {
    const { warehouseId } = req.body;

    if (!warehouseId) {
      return res.status(400).json({ success: false, message: 'Missing warehouseId in request.' });
    }

    connection = await getConnection();

    // Fetch warehouse config
    const [[warehouse]] = await connection.query(
      `SELECT 
         supports_barcode, 
         batch_number, 
         expire_date_field,
         show_vat_field, 
         show_discount_field 
       FROM warehouses 
       WHERE id = ?`,
      [warehouseId]
    );

    if (!warehouse) {
      return res.json({ success: false, message: 'Warehouse not found.' });
    }

    // Dynamically include columns
    const includeBarcode = warehouse.supports_barcode === 1;
    const includeBatch = warehouse.batch_number === 1;
    const includeExpire = warehouse.expire_date_field === 1;
    const includeVAT = warehouse.show_vat_field === 1;
    const includeDiscount = warehouse.show_discount_field === 1;

    const [categories] = await connection.query(`SELECT id, name FROM product_category WHERE category_status = 'true'`);
    const [brands] = await connection.query(`SELECT id, name FROM product_brands WHERE brand_status = 'true'`);
    const [units] = await connection.query(`SELECT id, name FROM product_units WHERE unit_status = 'true'`);

    const workbook = new ExcelJS.Workbook();
    const sheet = workbook.addWorksheet('Product Upload');
    const dataSheet = workbook.addWorksheet('Data');
    dataSheet.state = 'veryHidden';

    // Build dynamic headers
    const headers = ['name', 'category', 'brand', 'unit', 'qty', 'cost', 'price'];
    if (includeBarcode) headers.push('barcodeNo');
    if (includeBatch) headers.push('batchNo');
    if (includeExpire) headers.push('expireDate');
    if (includeVAT) headers.push('vat');
    if (includeDiscount) headers.push('discount');

    sheet.addRow(headers);
    sheet.getRow(1).font = { bold: true };

    // Add dropdown data
    const addList = (title, items, col) => {
      dataSheet.getCell(`${col}1`).value = title;
      items.forEach((item, i) => {
        dataSheet.getCell(`${col}${i + 2}`).value = item.name;
      });
    };

    addList('Categories', categories, 'A');
    addList('Brands', brands, 'B');
    addList('Units', units, 'C');

    // Add dropdowns to main sheet
    const addDropdown = (col, sourceCol, itemCount) => {
      const range = `Data!$${sourceCol}$2:$${sourceCol}$${itemCount + 1}`;
      for (let i = 2; i <= 100; i++) {
        sheet.getCell(`${col}${i}`).dataValidation = {
          type: 'list',
          allowBlank: false,
          formulae: [range],
          showErrorMessage: true,
          errorTitle: 'Invalid Input',
          error: 'Please select a valid value from the list.'
        };
      }
    };

    addDropdown('B', 'A', categories.length); // category
    addDropdown('C', 'B', brands.length);     // brand
    addDropdown('D', 'C', units.length);      // unit

    // Set expireDate format if used
    if (includeExpire) {
      const colIndex = headers.indexOf('expireDate') + 1;
      for (let i = 2; i <= 100; i++) {
        sheet.getCell(i, colIndex).numFmt = 'yyyy-mm-dd';
      }
    }

    // Set column widths
    sheet.columns = headers.map(header => ({
      header,
      width: header.length < 10 ? 15 : header.length + 5
    }));

    // Excel download response
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename=product_template.xlsx');

    await workbook.xlsx.write(res);
    res.end();

  } catch (err) {
    console.error('Excel generation error:', err);
    res.status(500).json({ success: false, message: 'Failed to generate template.' });
  }
});


// Insert Data by Excels

router.post('/excel/import/products/bulk-upload', auth.authenticateToken, async (req, res) => {
  const { items, storeWarehouse } = req.body;
  const user_id = res.locals.id;
  const createdBy = res.locals.name;

  const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms)); // used only where absolutely needed

  let connection;
  try {
    connection = await getConnection();
    await connection.beginTransaction();

    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    const store_id = storeWarehouse.store;
    const warehouse_id = storeWarehouse.warehouse;

    // Fetch warehouse settings
    const [warehouseSettings] = await connection.query(
      `SELECT supports_barcode, batch_number FROM warehouses WHERE id = ? LIMIT 1`, [warehouse_id]
    );

    const supportsBarcode = warehouseSettings[0]?.supports_barcode === 1;
    const supportsBatch = warehouseSettings[0]?.batch_number === 1;

    // Mappings
    const [categories] = await connection.query('SELECT id, name FROM product_category');
    const [brands] = await connection.query('SELECT id, name FROM product_brands');
    const [units] = await connection.query('SELECT id, name, operator_symbol, operator_value FROM product_units');

    const categoryMap = Object.fromEntries(categories.map(c => [c.name.trim().toLowerCase(), c.id]));
    const brandMap = Object.fromEntries(brands.map(b => [b.name.trim().toLowerCase(), b.id]));
    const unitMap = {};
    units.forEach(u => {
      unitMap[u.name.trim().toLowerCase()] = {
        id: u.id,
        operator_symbol: u.operator_symbol,
        operator_value: u.operator_value
      };
    });

    const skippedDuplicates = [];
    const skippedInvalid = [];
    const localDuplicates = new Set();

    let [[{ count }]] = await connection.query(
      `SELECT COUNT(*) as count FROM products WHERE DATE(product_create_date) = CURDATE() AND warehouse_id = ?`, 
      [warehouse_id]
    );
    let sequence = count;

    for (const item of items) {
      const {
        category, brand, unit, name, qty, cost, price,
        imeiSerial, expireDate, vat, discount,
        batchNo, barcodeNo
      } = item;

      const productName = String(name || '').trim();
      const lowerName = productName.toLowerCase();

      const category_id = categoryMap[String(category || '').trim().toLowerCase()];
      const brand_id = brandMap[String(brand || '').trim().toLowerCase()];
      const unitObj = unitMap[String(unit || '').trim().toLowerCase()];

      const localKey = `${lowerName}-${store_id}-${warehouse_id}`;
      if (localDuplicates.has(localKey)) {
        skippedDuplicates.push(`${productName} (duplicate in Excel)`);
        continue;
      }
      localDuplicates.add(localKey);

      if (!productName || !qty || !cost || !price || !category_id || !brand_id || !unitObj) {
        skippedInvalid.push(`${productName} (missing or invalid fields)`);
        continue;
      }

      if (parseFloat(price) <= parseFloat(cost)) {
        skippedInvalid.push(`${productName} (Price must be greater than cost)`);
        continue;
      }

      if (expireDate) {
        const inputDate = new Date(expireDate);
        const today = new Date(); today.setHours(0, 0, 0, 0);
        if (inputDate < today) {
          skippedInvalid.push(`${productName} (Expire date is in the past)`);
          continue;
        }
      }

      // Duplicate DB Check
      let existingCheckQuery = '';
      let existingParams = [];

      const duplicateConditions = [];
      if (supportsBatch && batchNo) {
        duplicateConditions.push('batch_no = ?');
        existingParams.push(batchNo);
      }
      if (supportsBarcode && barcodeNo) {
        duplicateConditions.push('barcode_no = ?');
        existingParams.push(barcodeNo);
      }

      if (duplicateConditions.length) {
        existingCheckQuery = `
          SELECT id FROM products 
          WHERE store_id = ? AND warehouse_id = ? AND (${duplicateConditions.join(' OR ')})
        `;
        existingParams.unshift(store_id, warehouse_id);
      } else {
        existingCheckQuery = `
          SELECT id FROM products 
          WHERE name = ? AND store_id = ? AND warehouse_id = ?
        `;
        existingParams = [productName, store_id, warehouse_id];
      }

      const [existing] = await connection.query(existingCheckQuery, existingParams);
      if (existing.length > 0) {
        skippedDuplicates.push(`${productName} (already exists in DB)`);
        continue;
      }

      // Generate Reference Number
      sequence++;
      const refNumber = 100000000 + Math.floor(Math.random() * 900000);

      // Final Qty with unit operator
      let finalQty = parseFloat(qty);
      if (unitObj.operator_symbol === '*' && unitObj.operator_value) {
        finalQty *= unitObj.operator_value;
      } else if (unitObj.operator_symbol === '/' && unitObj.operator_value) {
        finalQty /= unitObj.operator_value;
      }

      await connection.query(`
        INSERT INTO products (
          store_id, warehouse_id, category_id, brand_id, unit_id, refNumber,
          name, qty, cost, price, imei_serial, expire_date, vat, discount,
          product_create_date, product_create_by, product_status, product_qty_alert,
          batch_no, barcode_no
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        store_id, warehouse_id, category_id, brand_id, unitObj.id, refNumber,
        productName, finalQty, cost, price, imeiSerial || 'null',
        expireDate || 'null', vat ?? 0, discount ?? 0, now,
        createdBy, 'true', 0, batchNo || 'null', barcodeNo || 'null'
      ]);

      // Log entry
      await connection.query(`
        INSERT INTO logs (
          user_id, store_id, action, description, createdAt, createdBy
        ) VALUES (?, ?, ?, ?, ?, ?)`, 
         [
        user_id,
        store_id,
        'CREATE PRODUCT',
        `Product ${productName} imported via Excel`,
        now,
        createdBy
      ]);
    }

    await connection.commit();

    return res.status(200).json({
      success: true,
      message: 'Products imported successfully',
      skippedDuplicates,
      skippedInvalid
    });

  } catch (error) {
    if (connection) await connection.rollback();
    console.error('Bulk Upload Error:', error);
    return res.status(500).json({
      success: false,
      message: 'Bulk upload failed',
      error: error.message
    });
  }
});



// SMS TEMPLATE MODULE API

// Get sms templates
router.get('/get/sms/template', auth.authenticateToken, async (req, res) => {
  const { category, store, warehouse, type } = req.query;

  let connection = await getConnection();

  try {
    const [rows] = await connection.query(
      `SELECT * FROM sms_templates WHERE category = ? AND store_id = ? AND warehouse_id = ? AND type = ?`,
      [category, store, warehouse, type]
    );

    res.json(rows[0] || {}); // return single template or empty object
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Failed to fetch template' });
  }
});


// Save (insert or update) template
router.post('/save/update/sms/template', async (req, res) => {
  const { store, warehouse, type, category, message } = req.body;
  let connection;
  connection = await getConnection();
  try {

     // delay
  await new Promise(resolve => setTimeout(resolve, 3000));
    const [existing] = await connection.query(
      'SELECT id, store_id, warehouse_id FROM sms_templates WHERE store_id = ? AND warehouse_id = ? AND type = ? AND category = ?',
      [store, warehouse, type, category]
    );

     // delay
  await new Promise(resolve => setTimeout(resolve, 3000));
    if (existing.length > 0) {
      await connection.query(
        'UPDATE sms_templates SET message = ? WHERE id = ? AND store_id = ? AND warehouse_id = ?',
        [message, existing[0].id, existing[0].store_id, existing[0].warehouse_id]
      );
      res.json({ success: true, message: 'Updated existing template' });
    } else {
      await connection.query(
        'INSERT INTO sms_templates (store_id, warehouse_id, type, category, message) VALUES (?, ?, ?, ?, ?)',
        [store, warehouse, type, category, message]
      );
      res.json({ success: true, message: 'Inserted new template' });
    }
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});


// GET POS SALES ITEMS BY ID
router.get('/get/posgetSaleItemsBySaleId/:id', auth.authenticateToken, async (req, res) => {
  const { id } = req.params;
  const connection = await getConnection();

  try {
    const [rows] = await connection.execute(
      `SELECT * FROM sale_items WHERE sale_id = ?`,
      [id]
    );

    if (rows.length === 0) {
      return res.status(404).json({ message: 'Not found' });
    }

    res.json(rows);
  } catch (err) {
    res.status(500).json({ message: 'Failed to fetch', error: err.message });
  }
});


// GET POS SALES BY ID
router.get('/get/posgetSalesById/:id', auth.authenticateToken, async (req, res) => {
  const { id } = req.params;
  const connection = await getConnection();

  try {
    const [rows] = await connection.execute(
      `SELECT * FROM sales WHERE id = ?`,
      [id]
    );

    if (rows.length === 0) {
      return res.status(404).json({ message: 'Not found' });
    }

    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ message: 'Failed to fetch', error: err.message });
  }
});


// GET sms details by id
router.get('/get/mail/byId/:mail', auth.authenticateToken, async (req, res) => {
  const { mail } = req.params;
  const connection = await getConnection();

  try {
    const [rows] = await connection.execute(
      `SELECT * FROM mails WHERE id = ?`,
      [mail]
    );

    if (rows.length === 0) {
      return res.json({ message: 'Mail not found' });
    }

    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ message: 'Failed to fetch mail', error: err.message });
  }
});


// GET sms details by id
router.get('/get/sms/byId/:sms', auth.authenticateToken, async (req, res) => {
  const { sms } = req.params;
  const connection = await getConnection();

  try {
    const [rows] = await connection.execute(
      `SELECT * FROM sms WHERE id = ?`,
      [sms]
    );

    if (rows.length === 0) {
      return res.json({ message: 'SMS not found' });
    }

    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ message: 'Failed to fetch sms', error: err.message });
  }
});


// GET product details by id
router.get('/get/products/byId/:product', auth.authenticateToken, async (req, res) => {
  const { product } = req.params;
  const connection = await getConnection();

  try {
    const [rows] = await connection.execute(
      `SELECT * FROM products WHERE id = ?`,
      [product]
    );

    if (rows.length === 0) {
      return res.json({ message: 'Product not found' });
    }

    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ message: 'Failed to fetch product', error: err.message });
  }
});


// Get customers by warehouse
router.get('/get/customers/warehouse/:warehouseId', auth.authenticateToken, async (req, res) => {
  
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const warehouseId = req.params.warehouseId;

  const isSuperAdmin = (roleId === 1 || roleId === '1');

  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT 
        c.id, c.name, c.warehouse_id, c.store_id
      FROM customers c
    `;

    let whereClause = `WHERE c.warehouse_id = ?`;
    let params = [warehouseId];

    if (!isSuperAdmin) {
      query += `
        INNER JOIN user_warehouses uw ON uw.warehouse_id = c.warehouse_id AND uw.user_id = ?
        INNER JOIN user_stores us ON us.store_id = c.store_id AND us.user_id = ?
      `;
      whereClause += ` AND uw.user_id = ? AND us.user_id = ?`;
      params = [userId, userId, warehouseId, userId, userId]; // 5 total params
    }

    const finalQuery = `${query} ${whereClause}`;

    const [result] = await connection.execute(finalQuery, params);
    res.json(result);
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: 'Failed to fetch' });
  }

});


// Get Products by warehouse and barcode

router.get('/get/product/barcode/:barcode', auth.authenticateToken, async (req, res) => {
  const { barcode } = req.params;
  const { warehouseId } = req.query;

  try {
    const conn = await getConnection();
    const [rows] = await conn.execute(
      `SELECT p.id, p.name, p.warehouse_id, p.store_id, p.refNumber, p.qty, p.price, p.cost,
      p.batch_no, p.barcode_no, p.discount, p.vat, p.product_status
      FROM products p
      WHERE p.product_status = 'true' AND p.barcode_no = ? AND p.warehouse_id = ? LIMIT 1`,
      [barcode, warehouseId]
    );

    if (rows.length > 0) res.json(rows[0]);
    else res.json({ message: 'Product not found' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Server error' });
  }
});


// Get products by warehouse
router.get('/get/products/warehouse/:warehouseId', auth.authenticateToken, async (req, res) => {
  
const userId = res.locals.id;
  const roleId = res.locals.role;
  const warehouseId = req.params.warehouseId;

  const isSuperAdmin = (roleId === 1 || roleId === '1');

  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT 
      p.id, p.name, p.warehouse_id, p.store_id, p.refNumber, p.qty, p.price, p.cost,
      p.batch_no, p.barcode_no, p.discount, p.vat
      FROM products p
    `;

    let whereClause = `WHERE p.warehouse_id = ? AND p.product_status = "true" `;
    let params = [warehouseId];

    if (!isSuperAdmin) {
      query += `
        INNER JOIN user_warehouses uw ON uw.warehouse_id = p.warehouse_id AND uw.user_id = ?
        INNER JOIN user_stores us ON us.store_id = p.store_id AND us.user_id = ?
      `;
      whereClause += ` AND uw.user_id = ? AND us.user_id = ?`;
      params = [userId, userId, warehouseId, userId, userId]; // 5 total params
    }

    const finalQuery = `${query} ${whereClause}`;

    const [result] = await connection.execute(finalQuery, params);
    res.json(result);
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: 'Failed to fetch' });
  }

});


// Low Stock Products ===============================

router.get('/get/low/stock/product/lists/data', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    storeId,
    warehouseId,
    sortBy = 'product_create_date',
    sortOrder = 'DESC',
  } = req.query;

  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT 
        p.id, p.store_id, p.warehouse_id, p.category_id, p.batch_no, p.barcode_no,
        p.brand_id, p.unit_id, p.refNumber, p.name, p.qty, p.cost, 
        p.price, p.imei_serial, p.expire_date, p.vat, p.discount, 
        p.product_create_date, p.product_create_by, p.product_update_date, 
        p.product_update_by, p.product_status, p.product_qty_alert,

        s.name AS storename,
        s.id AS store_id,
        w.name AS warehousename,
        w.id AS warehouse_id

      FROM products p
      JOIN stores s ON s.id = p.store_id
      JOIN warehouses w ON w.id = p.warehouse_id
    `;

    const params = [];
    const whereConditions = [];

    // Always fetch products with qty <= 10
    whereConditions.push(`p.qty <= 10`);

    // Role-based filtering
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      if (storeIds.length > 0 && warehouseIds.length > 0) {
        whereConditions.push(`(p.store_id IN (${storeIds.map(() => '?').join(',')}) AND p.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
        params.push(...storeIds, ...warehouseIds);
      } else if (storeIds.length > 0) {
        whereConditions.push(`p.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } else if (warehouseIds.length > 0) {
        whereConditions.push(`p.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    // Optional store/warehouse filters
    if (storeId) {
      whereConditions.push(`p.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`p.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // Final WHERE clause
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sorting
    const columnMap = {
      created_at: 'p.product_create_date',
      product: 'p.name'
    };

    const orderBy = columnMap[sortBy] || 'p.product_create_date';
    const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    query += ` ORDER BY ${orderBy} ${orderDir}`;

    // Execute query
    const [result] = await connection.query(query, params);

    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching low-stock products:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});


// Out of Stock ===============================

router.get('/get/outofstock/product/lists/data', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    storeId,
    warehouseId,
    sortBy = 'product_create_date',
    sortOrder = 'DESC',
  } = req.query;

  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT 
        p.id, p.store_id, p.warehouse_id, p.category_id, p.batch_no, p.barcode_no,
        p.brand_id, p.unit_id, p.refNumber, p.name, p.qty, p.cost, 
        p.price, p.imei_serial, p.expire_date, p.vat, p.discount, 
        p.product_create_date, p.product_create_by, p.product_update_date, 
        p.product_update_by, p.product_status, p.product_qty_alert,

        s.name AS storename,
        s.id AS store_id,
        w.name AS warehousename,
        w.id AS warehouse_id

      FROM products p
      JOIN stores s ON s.id = p.store_id
      JOIN warehouses w ON w.id = p.warehouse_id
    `;

    const params = [];
    const whereConditions = [];

    // Always fetch products with qty <= 0
    whereConditions.push(`p.qty <= 0`);

    // Role-based filtering
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      if (storeIds.length > 0 && warehouseIds.length > 0) {
        whereConditions.push(`(p.store_id IN (${storeIds.map(() => '?').join(',')}) AND p.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
        params.push(...storeIds, ...warehouseIds);
      } else if (storeIds.length > 0) {
        whereConditions.push(`p.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } else if (warehouseIds.length > 0) {
        whereConditions.push(`p.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    // Optional store/warehouse filters
    if (storeId) {
      whereConditions.push(`p.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`p.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // Final WHERE clause
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sorting
    const columnMap = {
      created_at: 'p.product_create_date',
      product: 'p.name'
    };

    const orderBy = columnMap[sortBy] || 'p.product_create_date';
    const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    query += ` ORDER BY ${orderBy} ${orderDir}`;

    // Execute query
    const [result] = await connection.query(query, params);

    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching out-of-stock products:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});


// Get Expired Product Lists Data

router.get('/get/expired/product/lists/data', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    storeId,
    warehouseId,
    filterType,
    startDate,
    endDate,
    sortBy = 'product_create_date',
    sortOrder = 'DESC',
  } = req.query;

  let connection;

  try {
    connection = await getConnection();

    // Current time in Africa/Nairobi timezone
    const now = moment().tz('Africa/Nairobi');
    const formattedDate = now.format('YYYY-MM-DD');

    let query = `
      SELECT 
        p.id, p.store_id, p.warehouse_id, p.category_id, p.batch_no, p.barcode_no,
        p.brand_id, p.unit_id, p.refNumber, p.name, p.qty, p.cost, 
        p.price, p.imei_serial, p.expire_date, p.vat, p.discount, 
        p.product_create_date, p.product_create_by, p.product_update_date, 
        p.product_update_by, p.product_status, p.product_qty_alert,

        s.name AS storename,
        w.name AS warehousename

      FROM products p
      JOIN stores s ON s.id = p.store_id
      JOIN warehouses w ON w.id = p.warehouse_id
    `;

    const params = [];
    const whereConditions = [];

    // Only fetch expired products
    whereConditions.push(`p.expire_date < ?`);
    params.push(formattedDate);

    // Role-based restrictions (except superadmin)
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      if (storeIds.length > 0 && warehouseIds.length > 0) {
        whereConditions.push(`(p.store_id IN (${storeIds.map(() => '?').join(',')}) AND p.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
        params.push(...storeIds, ...warehouseIds);
      } else if (storeIds.length > 0) {
        whereConditions.push(`p.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } else if (warehouseIds.length > 0) {
        whereConditions.push(`p.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    // Store & warehouse filters
    if (storeId) {
      whereConditions.push(`p.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`p.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // Optional filterType or custom date range
    const today = moment().tz('Africa/Nairobi');
    let start = null;
    let end = null;

    if (filterType) {
      switch (filterType) {
        case 'active':
          whereConditions.push(`p.product_status = 'true'`);
          break;
        case 'locked':
          whereConditions.push(`p.product_status = 'false'`);
          break;
        case 'today':
          start = today.clone().startOf('day');
          end = today.clone().endOf('day');
          break;
        case 'yesterday':
          start = today.clone().subtract(1, 'day').startOf('day');
          end = today.clone().subtract(1, 'day').endOf('day');
          break;
        case 'week':
          start = today.clone().startOf('week');
          end = today.clone().endOf('week');
          break;
        case 'last_week':
          start = today.clone().subtract(1, 'week').startOf('week');
          end = today.clone().subtract(1, 'week').endOf('week');
          break;
        case 'month':
          start = today.clone().startOf('month');
          end = today.clone().endOf('month');
          break;
        case 'year':
          start = today.clone().startOf('year');
          end = today.clone().endOf('year');
          break;
        case 'last_year':
          start = today.clone().subtract(1, 'year').startOf('year');
          end = today.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_2_years':
          start = today.clone().subtract(2, 'year').startOf('year');
          end = today.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_3_years':
          start = today.clone().subtract(3, 'year').startOf('year');
          end = today.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_4_years':
          start = today.clone().subtract(4, 'year').startOf('year');
          end = today.clone().subtract(1, 'year').endOf('year');
          break;
      }
    } else if (startDate && endDate) {
      start = moment.tz(startDate, 'Africa/Nairobi').startOf('day');
      end = moment.tz(endDate, 'Africa/Nairobi').endOf('day');
    }

    if (start && end) {
      whereConditions.push(`p.product_create_date BETWEEN ? AND ?`);
      params.push(start.format('YYYY-MM-DD HH:mm:ss'), end.format('YYYY-MM-DD HH:mm:ss'));
    }

    // Final WHERE clause
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sorting
    const columnMap = {
      created_at: 'p.product_create_date',
      product: 'p.name'
    };

    const orderBy = columnMap[sortBy] || 'p.product_create_date';
    const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    query += ` ORDER BY ${orderBy} ${orderDir}`;

    // Execute query
    const [result] = await connection.query(query, params);
    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching expired products:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});



// GET product by QR (which may contain product ID)

router.get('/product/qr/:ref/:warehouseId', auth.authenticateToken, async (req, res) => {
    
  const { ref, warehouseId } = req.params;
  const conn = await getConnection();

  try {
    const [rows] = await conn.query(
      `SELECT * FROM products WHERE barcode_no = ?  AND warehouse_id = ? LIMIT 1`,
      [ref, warehouseId]
    );

    if (!rows.length) return res.json({ message: 'Product not found' });

    res.json(rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Internal server error' });
  }
});




// Get Product Lists Data

router.get('/get/product/lists/data', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    storeId,
    warehouseId,
    filterType,
    startDate,
    endDate,
    sortBy = 'product_create_date',
    sortOrder = 'DESC',
  } = req.query;

  let connection;

  try {
    connection = await getConnection();

    const now = moment().tz('Africa/Nairobi');
    const formattedDate = now.format('YYYY-MM-DD');

    // Base query
    let query = `
      SELECT 
        p.id, p.store_id, p.warehouse_id, p.category_id, p.batch_no, p.barcode_no,
        p.brand_id, p.unit_id, p.refNumber, p.name, p.qty, p.cost, 
        p.price, p.imei_serial, p.expire_date, p.vat, p.discount, 
        p.product_create_date, p.product_create_by, p.product_update_date, 
        p.product_update_by, p.product_status, p.product_qty_alert,
        s.name AS storename,
        w.name AS warehousename
      FROM products p
      JOIN stores s ON s.id = p.store_id
      JOIN warehouses w ON w.id = p.warehouse_id
    `;

    const params = [];
    const whereConditions = [];

    // Only non-expired and in-stock products
    whereConditions.push(`p.expire_date >= ?`);
    params.push(formattedDate);
    whereConditions.push(`p.qty > 0`);

    // Role-based filtering (for non-superadmins)
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query('SELECT store_id FROM user_stores WHERE user_id = ?', [userId]);
      const [warehouseRows] = await connection.query('SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]);

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      if (storeIds.length > 0 && warehouseIds.length > 0) {
        whereConditions.push(`(p.store_id IN (${storeIds.map(() => '?').join(',')}) AND p.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
        params.push(...storeIds, ...warehouseIds);
      } else if (storeIds.length > 0) {
        whereConditions.push(`p.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } else if (warehouseIds.length > 0) {
        whereConditions.push(`p.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    // Direct filter override
    if (storeId) {
      whereConditions.push(`p.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`p.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // Filter by product_create_date (by filterType or manual dates)
    let start = null;
    let end = null;
    const today = moment().tz('Africa/Nairobi');

    if (filterType) {
      switch (filterType) {
        case 'active': whereConditions.push(`p.product_status = 'true'`); break;
        case 'locked': whereConditions.push(`p.product_status = 'false'`); break;
        case 'today':
          start = today.clone().startOf('day');
          end = today.clone().endOf('day');
          break;
        case 'yesterday':
          start = today.clone().subtract(1, 'day').startOf('day');
          end = today.clone().subtract(1, 'day').endOf('day');
          break;
        case 'week':
          start = today.clone().startOf('week');
          end = today.clone().endOf('week');
          break;
        case 'last_week':
          start = today.clone().subtract(1, 'week').startOf('week');
          end = today.clone().subtract(1, 'week').endOf('week');
          break;
        case 'month':
          start = today.clone().startOf('month');
          end = today.clone().endOf('month');
          break;
        case 'year':
          start = today.clone().startOf('year');
          end = today.clone().endOf('year');
          break;
        case 'last_year':
          start = today.clone().subtract(1, 'year').startOf('year');
          end = today.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_2_years':
          start = today.clone().subtract(2, 'year').startOf('year');
          end = today.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_3_years':
          start = today.clone().subtract(3, 'year').startOf('year');
          end = today.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_4_years':
          start = today.clone().subtract(4, 'year').startOf('year');
          end = today.clone().subtract(1, 'year').endOf('year');
          break;
      }
    } else if (startDate && endDate) {
      start = moment.tz(startDate, 'Africa/Nairobi').startOf('day');
      end = moment.tz(endDate, 'Africa/Nairobi').endOf('day');
    }

    if (start && end) {
      whereConditions.push(`p.product_create_date BETWEEN ? AND ?`);
      params.push(start.format('YYYY-MM-DD HH:mm:ss'), end.format('YYYY-MM-DD HH:mm:ss'));
    }

    // Final WHERE clause
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sorting
    const columnMap = {
      created_at: 'p.product_create_date',
      product: 'p.name'
    };

    const orderBy = columnMap[sortBy] || 'p.product_create_date';
    const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    query += ` ORDER BY ${orderBy} ${orderDir}`;

    // Execute query
    const [result] = await connection.query(query, params);

    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching product:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});


// Get Product Lists

router.get('/get/products/list', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const isSuperAdmin = (roleId === 1 || roleId === '1');

  let { page, limit, search } = req.query;
  page = parseInt(page) || 1;
  limit = parseInt(limit) || 10;
  const offset = (page - 1) * limit;
  const searchQuery = `%${search || ''}%`;

  let connection;

  try {
    connection = await getConnection();

    let baseQuery = `
      FROM products p
    `;
    let whereClause = `WHERE p.name LIKE ?`;
    let params = [searchQuery, offset, limit];
    let countParams = [searchQuery];

    if (!isSuperAdmin) {
      baseQuery += `
        INNER JOIN user_stores us ON us.store_id = p.store_id AND us.user_id = ?
        INNER JOIN user_warehouses uw ON uw.warehouse_id = p.warehouse_id AND uw.user_id = ?
      `;
      params = [searchQuery, userId, userId, offset, limit];
      countParams = [searchQuery, userId, userId];
    }

    const dataQuery = `
      SELECT 
        p.id, p.name, p.qty, p.cost, p.price, p.imei_serial, p.expire_date,
        p.vat, p.discount, p.product_create_date, p.product_create_by,
        p.product_update_date, p.product_update_by, p.product_status, p.product_qty_alert,
        p.store_id, p.warehouse_id, p.category_id, p.brand_id, p.unit_id
      ${baseQuery}
      ${whereClause}
      LIMIT ?, ?
    `;

    const countQuery = `
      SELECT COUNT(*) AS total
      ${baseQuery}
      ${whereClause}
    `;

    const [products] = await connection.execute(dataQuery, params);
    const [countResult] = await connection.execute(countQuery, countParams);

    res.json({
      data: products,
      total: countResult[0].total,
      page,
      limit
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Failed to fetch products.' });
  }
});


// Get supplier by warehouse
router.get('/get/suppliers/warehouse/:warehouseId', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const warehouseId = req.params.warehouseId;

  const isSuperAdmin = (roleId === 1 || roleId === '1');

  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT 
        s.id, s.name, s.warehouse_id, s.store_id
      FROM suppliers s
    `;

    let whereClause = `WHERE s.warehouse_id = ?`;
    let params = [warehouseId];

    if (!isSuperAdmin) {
      query += `
        INNER JOIN user_warehouses uw ON uw.warehouse_id = s.warehouse_id AND uw.user_id = ?
        INNER JOIN user_stores us ON us.store_id = s.store_id AND us.user_id = ?
      `;
      whereClause += ` AND uw.user_id = ? AND us.user_id = ?`;
      params = [userId, userId, warehouseId, userId, userId]; // 5 total params
    }

    const finalQuery = `${query} ${whereClause}`;

    const [result] = await connection.execute(finalQuery, params);
    res.json(result);
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: 'Failed to fetch' });
  }
});


// Get All Suppliers List Based On Assigned Warehouses and Stores
router.get('/get/suppliers/list/all', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const isSuperAdmin = (roleId === 1 || roleId === '1');

  let connection;
  try {
    connection = await getConnection();

    let query = `
      SELECT * FROM suppliers s
    `;

    const params = [];

    if (!isSuperAdmin) {
      query += `
        INNER JOIN user_stores us ON us.store_id = s.store_id AND us.user_id = ?
        INNER JOIN user_warehouses uw ON uw.warehouse_id = s.warehouse_id AND uw.user_id = ?
      `;
      params.push(userId, userId);
    }

    const [rows] = await connection.execute(query, params);
    res.json(rows);

  } catch (err) {
    res.status(500).json({ message: 'Failed to fetch suppliers' });
  } 
});


// Get All Assigned Warehouses to Users
router.get('/get/assigned/warehouses', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const isSuperAdmin = (roleId === 1 || roleId === '1');

  let connection;
  try {
    connection = await getConnection();

    let query = `
      SELECT w.id, w.name, w.storeId,
      w.supports_barcode, w.supports_beep, w.customer_field, 
      w.supplier_field, w.send_sale_sms, w.send_purchase_sms, w.send_low_qty_sms, 
      w.send_sms_every_week_sale, w.batch_number,
      w.show_discount_field, w.show_vat_field, w.show_transport_field,
      w.expire_date_field
      FROM warehouses w
    `;

    const params = [];

    if (!isSuperAdmin) {
      query += `
        INNER JOIN user_warehouses uw ON uw.warehouse_id = w.id
        WHERE uw.user_id = ?
      `;
      params.push(userId);
    }

    const [rows] = await connection.execute(query, params);
    res.json(rows);

  } catch (err) {
    console.error('Error fetching warehouses:', err);
    res.status(500).json({ message: 'Failed to fetch assigned warehouses.' });
  } 
});


// SALES MODULE API ==================================

// Check quantity before update =========================
router.get('/checking/product/qty/:id', auth.authenticateToken, async (req, res) => {
  const productId = req.params.id;

  let db;

  db = await getConnection();

  const [product] = await db.query(
    'SELECT qty FROM products WHERE id = ?',
    [productId]
  );

  if (!product.length) {
    return res.status(404).json({ message: 'Product not found' });
  }

  res.json({ stock: product[0].qty });
});


// Add Sales carts ===============================

router.post('/sales/cart/add', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const { productId, quantity, price, discount, vat, cost } = req.body;


  const qty = parseFloat(quantity) || 0;
  const unitPrice = parseFloat(price) || 0;
  const unitDiscount = parseFloat(discount) || 0;
  const unitVat = parseFloat(vat) || 0;
  const unitCost = parseFloat(cost) || 0;
  const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

  const subtotal = ((unitPrice * qty) + unitVat) - unitDiscount;
  const totalCost = unitCost * qty;
  const connection = await getConnection();
  try {
    await connection.beginTransaction();

    const [productRows] = await connection.execute(
      `SELECT id, name, qty FROM products WHERE id = ?`,
      [productId]
    );
    if (!productRows.length) {
      return res.json({ message: 'Product not found ' });
    }

    const [existing] = await connection.execute(
      `SELECT quantity FROM sale_carts WHERE user_id = ? AND product_id = ?`,
      [userId, productId]
    );

    if (existing.length) {
      const newQty = parseFloat(existing[0].quantity) + qty;
      const newSubtotal = ((unitPrice * newQty ) + unitVat) - unitDiscount ;
      const newCost = unitCost * newQty;
      await connection.execute(
        `UPDATE sale_carts
         SET totalCost = ?, quantity = ?, price = ?, discount = ?, vat = ?, subtotal = ?, updated_at = ?
         WHERE user_id = ? AND product_id = ?`,
        [newCost, newQty, unitPrice, unitDiscount, unitVat, newSubtotal, now, userId, productId]
      );
    } else {
      await connection.execute(
        `INSERT INTO sale_carts (cost, totalCost, user_id, product_id, quantity, price, discount, vat, subtotal, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [unitCost, totalCost, userId, productId, qty, unitPrice, unitDiscount, unitVat, subtotal, now, now]
      );
    }

    await connection.commit();
    res.json({ message: 'Cart updated successfully' });

  } catch (err) {
    await connection.rollback();
    console.error('Cart error:', err);
    res.status(500).json({ message: 'Failed to update cart', error: err.message });
  }
});



// Update Sales Cart Quantity
router.post('/sales/cart/update-quantity', auth.authenticateToken, async (req, res) => {
  const { productId, quantity } = req.body;
  const userId = res.locals.id;
  const connection = await getConnection();
  const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

  try {
    // Get current cart item
    let [cartItem] = await connection.query(`
      SELECT * FROM sale_carts WHERE user_id = ? AND product_id = ?
    `, [userId, productId]);

    if (!cartItem || cartItem.length === 0) {
      return res.json({ message: 'Product not in cart' });
    }

    // Fetch product details for calculations
    let [productRows] = await connection.query(`
      SELECT price, vat, discount, cost FROM products WHERE id = ?
    `, [productId]);

    if (!productRows || productRows.length === 0) {
      return res.json({ message: 'Product not found ' });
    }

    const product = productRows[0];
    const newQty = cartItem[0].quantity + quantity;
   
    if (newQty <= 0) {
      // Remove if quantity goes to zero or less
      await connection.query(`DELETE FROM sale_carts WHERE user_id = ? AND product_id = ?`, [userId, productId]);
      return res.json({ message: 'Product removed from cart' });
    }

    // Calculate values
    const price = parseFloat(product.price);
    const cost = parseFloat(product.cost);
    const discount = parseFloat(product.discount || 0);
    const vat = parseFloat(product.vat || 0);

    const subtotal = ((price * newQty) + vat) - discount;
    const newCost = cost * newQty;
    
    // Update cart with new quantity and calculated values
    await connection.query(`
      UPDATE sale_carts 
      SET quantity = ?, subtotal = ?, totalCost = ? 
      WHERE user_id = ? AND product_id = ?
    `, [newQty, subtotal, newCost, userId, productId]);

    res.json({ message: 'Cart updated successfully', subtotal });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Failed to update cart quantity ❌' });
  }
});


// Update Purchases Cart Cost
router.post('/purchases/cart/update-cost', auth.authenticateToken, async (req, res) => {
  const { productId, cost } = req.body;
  const userId = res.locals.id;
  const connection = await getConnection();
  const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

  try {
    // Get current cart item
    let [cartItem] = await connection.query(`
      SELECT * FROM purchase_carts WHERE user_id = ? AND product_id = ?
    `, [userId, productId]);

    if (!cartItem || cartItem.length === 0) {
      return res.status(404).json({ message: 'Product not in cart' });
    }

    const qty = cartItem[0].quantity;

    if (cost <= 0) {
      // Remove if quantity goes to zero or less
      await connection.query(`DELETE FROM purchase_carts WHERE user_id = ? AND product_id = ?`, [userId, productId]);
      return res.json({ message: 'Product removed from cart' });
    }

    // Calculate values    
    const subtotal = cost * qty;

    // Update cart with new cost and calculated values
    await connection.query(`
      UPDATE purchase_carts 
      SET cost = ?, subtotal = ? 
      WHERE user_id = ? AND product_id = ?
    `, [cost, subtotal, userId, productId]);

    res.json({ message: 'Cart updated successfully', subtotal });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Failed to update cart cost' });
  }
});


// Update Purchases Cart Quantity
router.post('/purchases/cart/update-quantity', auth.authenticateToken, async (req, res) => {
  const { productId, quantity } = req.body;
  const userId = res.locals.id;
  const connection = await getConnection();

  try {
    // Get current cart item
    let [cartItem] = await connection.query(`
      SELECT * FROM purchase_carts WHERE user_id = ? AND product_id = ?
    `, [userId, productId]);

    if (!cartItem || cartItem.length === 0) {
      return res.status(404).json({ message: 'Product not in cart ' });
    }

    // Fetch product details for calculations
    let [productRows] = await connection.query(`
      SELECT cost, vat, discount FROM products WHERE id = ?
    `, [productId]);

    if (!productRows || productRows.length === 0) {
      return res.status(404).json({ message: 'Product not found ' });
    }

    const product = productRows[0];
    const newQty = cartItem[0].quantity + quantity;

    if (newQty <= 0) {
      // Remove if quantity goes to zero or less
      await connection.query(`DELETE FROM purchase_carts WHERE user_id = ? AND product_id = ?`, [userId, productId]);
      return res.json({ message: 'Product removed from cart' });
    }

    // Calculate values
    const cost = parseFloat(product.cost);
    const discount = parseFloat(product.discount || 0);
    const vat = parseFloat(product.vat || 0);

    const subtotal = (cost * newQty) - discount + vat;

    // Update cart with new quantity and calculated values
    await connection.query(`
      UPDATE purchase_carts 
      SET quantity = ?, subtotal = ? 
      WHERE user_id = ? AND product_id = ?
    `, [newQty, subtotal, userId, productId]);

    res.json({ message: 'Cart updated successfully ', subtotal });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Failed to update cart quantity ' });
  }
});



// Update Purchases Cart Cost
router.post('/purchases/cart/update-cost', auth.authenticateToken, async (req, res) => {
  const { productId, cost } = req.body;
  const userId = res.locals.id;
  const connection = await getConnection();

  try {
    // Validate cost input
    if (cost == null || isNaN(cost)) {
      return res.status(400).json({ message: 'Invalid cost value ' });
    }

    // Get current cart item
    const [cartItemRows] = await connection.query(`
      SELECT * FROM purchase_carts WHERE user_id = ? AND product_id = ?
    `, [userId, productId]);

    if (!cartItemRows || cartItemRows.length === 0) {
      return res.status(404).json({ message: 'Product not in cart ' });
    }

    const cartItem = cartItemRows[0];

    // Fetch product details
    const [productRows] = await connection.query(`
      SELECT price, vat, discount FROM products WHERE id = ?
    `, [productId]);

    if (!productRows || productRows.length === 0) {
      return res.status(404).json({ message: 'Product not found ' });
    }

    const product = productRows[0];
    const quantity = cartItem.quantity;

    // Remove if cost is zero or less
    if (cost <= 0) {
      await connection.query(`
        DELETE FROM purchase_carts 
        WHERE user_id = ? AND product_id = ?
      `, [userId, productId]);
      return res.json({ message: 'Product removed from cart ' });
    }

    // Reject if cost is less than system price (optional business rule)
    if (cost < product.price) {
      return res.status(400).json({ message: 'Cost cannot be less than system price ' });
    }

    const discount = parseFloat(product.discount || 0);
    const vat = parseFloat(product.vat || 0);

    const subtotal = (cost * quantity) - discount + vat;

    // Update cart item
    await connection.query(`
      UPDATE purchase_carts 
      SET cost = ?, subtotal = ? 
      WHERE user_id = ? AND product_id = ?
    `, [cost, subtotal, userId, productId]);

    res.json({ message: 'Cart updated successfully ', subtotal });

  } catch (err) {
    console.error('Error updating purchase cart cost:', err);
    res.status(500).json({ message: 'Failed to update cart cost ' });
  }
});



// Get Sales Carts
router.get('/get/sales/cart', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;

  const connection = await getConnection();
  try {
    const [rows] = await connection.execute(
      `SELECT s.id, s.product_id, s.quantity, p.name, p.qty as qty_available,
      s.price, s.discount, s.vat, s.subtotal, s.cost, s.totalCost
       FROM sale_carts s
       JOIN products p ON s.product_id = p.id
       WHERE s.user_id = ?`,
      [userId]
    );

    res.json(rows);
  } catch (err) {
    res.status(500).json({ message: 'Failed to fetch cart ' });
  }
});


// Clear All Sales Cart
router.delete('/sales-cart/clear', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const connection = await getConnection();

  try {
    await connection.query(`DELETE FROM sale_carts WHERE user_id = ?`, [userId]);
    res.json({ message: 'Cart cleared successfully ' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Failed to clear cart ' });
  }
});

// Delete Sales Cart Row Items by id
router.delete('/sales/cart/:id', auth.authenticateToken, async (req, res) => {
  const { id } = req.params;
  // Perform deletion logic
  const userId = res.locals.id;
  const connection = await getConnection();

  try {
    await connection.query(`DELETE FROM sale_carts WHERE user_id = ? AND id = ? `, [userId, id]);
    res.json({ message: 'Removed successfully ' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Failed to delete ' });
  }
});


// Create Sales Returns ====================================


router.post('/create/sales/return', auth.authenticateToken, async (req, res) => {
  const connection = await getConnection();

  try {
    await connection.beginTransaction();
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    const { sale_id, items } = req.body;
    const created_by = res.locals.name;

    const [sales] = await connection.execute(
      `SELECT * FROM sales WHERE id = ?`,
      [sale_id]
    );

    if (sales.length === 0) {
      await connection.rollback();
      return res.json({ message: "Sale not found" });
    }

    
    const { store_id, warehouse_id, user_id } = sales[0];

    if (user_id === res.locals.id) {
      return res.json({ message: 'You cannot return your own sales created !!!.' });
    }

    const [fyRows] = await connection.execute(
      `SELECT id FROM fy_cycle WHERE store_id = ? AND isActive = 1 LIMIT 1`,
      [store_id]
    );

    const fy_id = fyRows[0]?.id;
    if (!fy_id) {
      await connection.rollback();
      return res.json({ message: "Active financial year not found" });
    }

    let computedReturnTotal = 0;
    let computedReturnCost = 0;

    for (const item of items) {
      const product_id = Number(item.product_id);
      const returnQty = Number(item.qty);
      const returnPrice = Number(item.price);

      if (!product_id || !returnQty || returnQty < 1) {
        await connection.rollback();
        return res.json({ message: `Invalid return data for product ${item.product_id}` });
      }

      const [saleItemRows] = await connection.execute(
        `SELECT quantity, cost, price FROM sale_items WHERE sale_id = ? AND product_id = ?`,
        [sale_id, product_id]
      );

      const saleItem = saleItemRows[0];
      if (!saleItem) {
        await connection.rollback();
        return res.status(400).json({ message: `Product ${product_id} not found in sale` });
      }

      const soldQty = Number(saleItem.quantity);
      const cost = Number(saleItem.cost);
      const price = Number(saleItem.price);

      if (returnQty > soldQty) {
        await connection.rollback();
        return res.json({
          message: `Cannot return ${returnQty} units for product ${product_id}, only ${soldQty} sold`
        });
      }

      const newQty = soldQty - returnQty;
      const newSubtotal = price * newQty;
      const newTotalCost = cost * newQty;

      // Update sale_items correctly
      await connection.execute(`
        UPDATE sale_items 
        SET quantity = ?, subtotal = ?, totalCost = ?
        WHERE sale_id = ? AND product_id = ?
      `, [newQty, newSubtotal, newTotalCost, sale_id, product_id]);

      item.subtotal = returnQty * returnPrice;
      item.return_cost = returnQty * cost;

      computedReturnTotal += item.subtotal;
      computedReturnCost += item.return_cost;

      // Restore product stock
      await connection.execute(`
        UPDATE products 
        SET qty = qty + ? 
        WHERE id = ? AND warehouse_id = ?
      `, [returnQty, product_id, warehouse_id]);
    }

    const status = 'APPROVED';
    const [returnResult] = await connection.execute(`
      INSERT INTO sale_returns 
      (sale_id, store_id, warehouse_id, fy_id, return_total, return_cost, created_at, created_by, status)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, [
      sale_id,
      store_id,
      warehouse_id,
      fy_id,
      computedReturnTotal,
      computedReturnCost,
      now,
      created_by,
      status
    ]);

    const return_id = returnResult.insertId;

    for (const item of items) {
      const { product_id, qty, price, subtotal, return_reason } = item;
      await connection.execute(`
        INSERT INTO sale_return_items 
        (return_id, product_id, quantity, price, return_reason, subtotal)
        VALUES (?, ?, ?, ?, ?, ?)
      `, [return_id, product_id, qty, price, return_reason || '', subtotal]);
    }

    await connection.execute(`
      UPDATE sales 
      SET 
        total = total - ?, 
        grand_total = grand_total - ?, 
        total_cost = total_cost - ?
      WHERE id = ?
    `, [computedReturnTotal, computedReturnTotal, computedReturnCost, sale_id]);

    const [depositCheck] = await connection.execute(`
      SELECT id, payment_received FROM deposits WHERE sale_id = ? LIMIT 1
    `, [sale_id]);

    const [pendingdepositCheck] = await connection.execute(`
      SELECT id, amount FROM pending_deposits WHERE sale_id = ? LIMIT 1
    `, [sale_id]);

    if (pendingdepositCheck.length > 0) {
      const currentAmount = Number(pendingdepositCheck[0].amount);
      const newAmount = Math.max(0, currentAmount - computedReturnTotal);
      await connection.execute(`
        UPDATE pending_deposits 
        SET amount = ? 
        WHERE id = ?
      `, [newAmount, pendingdepositCheck[0].id]);
    }

    if (depositCheck.length > 0) {
      const currentAmount = Number(depositCheck[0].amount);
      const newAmount = Math.max(0, currentAmount - computedReturnTotal);
      await connection.execute(`
        UPDATE deposits 
        SET payment_received = ? 
        WHERE id = ?
      `, [newAmount, depositCheck[0].id]);
    }

    await connection.commit();

    // === SEND SMS TO OWNER ===
    const [storeInfoRows] = await connection.execute(
      `SELECT s.id as storeId, w.name as warehousename, s.name as storename, s.phone as ownerPhone, s.email as email
       FROM warehouses w JOIN stores s ON s.id = w.storeId WHERE w.id = ?`,
      [warehouse_id]
    );
    const storeInfo = storeInfoRows[0];

    const [smsConfig] = await connection.execute(`SELECT * FROM sms_configuration WHERE store_id = ? LIMIT 1`, [store_id]);
    const { api_url, sender_name, username, password } = smsConfig[0];
    const encodedAuth = Buffer.from(`${username}:${password}`).toString('base64');
    const smsText = ` Duka: ${storeInfo.warehousename}, Marejesho ya mauzo (ID: ${return_id}) yameidhinishwa na ${res.locals.name}. Jumla yaliyorejeshwa: ${computedReturnTotal.toFixed(2)}.`;

    const payload = {
      from: sender_name,
      text: smsText,
      to: storeInfo.ownerPhone
    };

    try {
      await axios.post(api_url, payload, {
        headers: {
          'Authorization': `Basic ${encodedAuth}`,
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        timeout: 10000
      });

      await connection.execute(
        `INSERT INTO sms (store_id, phone, message, status, date) VALUES (?, ?, ?, ?, ?)`,
        [store_id, storeInfo.ownerPhone, smsText, 'true', now]
      );
    } catch (err) {
      console.error('OWNER SMS failed:', err.message);
      await connection.execute(
        `INSERT INTO sms (store_id, phone, message, status, date) VALUES (?, ?, ?, ?, ?)`,
        [store_id, storeInfo.ownerPhone, smsText, 'false', now]
      );
    }


    const [emailConfig] = await connection.execute(
            'SELECT * FROM mail_configuration WHERE store_id = ? LIMIT 1',
            [store_id]
          );
    
          if (emailConfig.length > 0) {
            const transporter = nodemailer.createTransport({
              host: emailConfig[0].host,
              port: parseInt(emailConfig[0].port),
              secure: parseInt(emailConfig[0].port) === 465,
              auth: {
                user: emailConfig[0].username,
                pass: emailConfig[0].password
              }
            });
    
            try {
              await transporter.sendMail({
                from: emailConfig[0].username,
                to: storeInfo.email,
                subject: `MAREJESHO YA BIDHAA YENYE NAMBA YA MAUZO #${sale_id}`,
                text: smsText
              });
    
              await connection.execute(
                `INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, ?)`,
                [storeInfo.email || '', smsText, now, 'true']
              );
            } catch (mailError) {
              await connection.execute(
                `INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, ?)`,
                [storeInfo.email || '', smsText, now, 'false']
              );
            }
          }

    res.json({ message: 'Sales return processed successfully' });

  } catch (error) {
    await connection.rollback();
    console.error('[SALES RETURN ERROR]', error);
    res.status(500).json({ message: 'Internal server error', error: error.message });
  }
});



// Tuma Meseji kwa mteja ikimjulisha au kumpa akaunti namba ya benki

router.post('/send/bank-payment-sms/sales', auth.authenticateToken, async (req, res) => {
  const { phone, amount, warehouse } = req.body;

  try {
    const connection = await getConnection();
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    // Get store + warehouse info
    const [storeInfoRows] = await connection.execute(
      `SELECT s.id AS storeId, w.lipa_namba AS lipa_namba, w.akaunti_namba AS akaunti_namba, w.name AS warehousename, s.name AS storename, s.phone AS ownerPhone
       FROM warehouses w
       JOIN stores s ON s.id = w.storeId
       WHERE w.id = ?`,
      [warehouse]
    );

    if (storeInfoRows.length === 0) {
      return res.json({ success: false, message: 'Store/Warehouse not found' });
    }

    const storeInfo = storeInfoRows[0];
    const store_id = storeInfo.storeId;

    // Get SMS Configuration for this store
    const [smsConfigRows] = await connection.execute(
      `SELECT api_url, sender_name, username, password
       FROM sms_configuration
       WHERE store_id = ?
       ORDER BY id DESC LIMIT 1`,
      [store_id]
    );

    if (smsConfigRows.length === 0) {
      return res.status(404).json({ success: false, message: 'SMS configuration not found for store' });
    }

    const { api_url, sender_name, username, password } = smsConfigRows[0];
    const encodedAuth = Buffer.from(`${username}:${password}`).toString('base64');

    const smsText = `Ghala: ${storeInfo.warehousename}, lipa kiasi hiki: ${Number(amount).toFixed(2)} TZS, kupitia namba hii: ${storeInfo.akaunti_namba}. Asante`;

    const payload = {
      from: sender_name,
      text: smsText,
      to: phone
    };

    try {
      await axios.post(api_url, payload, {
        headers: {
          'Authorization': `Basic ${encodedAuth}`,
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        timeout: 10000
      });

      // Save SMS log
      await connection.execute(
        `INSERT INTO sms (store_id, phone, message, date, status) VALUES (?, ?, ?, ?, ?)`,
        [store_id, phone, smsText, now, 'true']
      );

      return res.json({ message: 'SMS sent successfully' });

    } catch (err) {
      console.error('SMS sending failed:', err.message);

      await connection.execute(
        `INSERT INTO sms (store_id, phone, message, date, status) VALUES (?, ?, ?, ?, ?)`,
        [store_id, phone, smsText, now, 'false']
      );

      return res.status(500).json({ success: false, message: 'SMS sending failed', error: err.message });
    }

  } catch (err) {
    console.error(' Server error:', err);
    return res.status(500).json({ success: false, message: 'Internal server error', error: err.message });
  }
});


// Tuma Meseji kwa mteja ikimjulisha au kumpa lipa namba au namba ya kulipia

router.post('/send/mobile-payment-sms/sales', auth.authenticateToken, async (req, res) => {
  const { phone, amount, warehouse } = req.body;

  try {
    const connection = await getConnection();
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    // Get store + warehouse info
    const [storeInfoRows] = await connection.execute(
      `SELECT s.id AS storeId, w.lipa_namba AS lipa_namba, w.akaunti_namba AS akaunti_namba, w.name AS warehousename, s.name AS storename, s.phone AS ownerPhone
       FROM warehouses w
       JOIN stores s ON s.id = w.storeId
       WHERE w.id = ?`,
      [warehouse]
    );

    if (storeInfoRows.length === 0) {
      return res.status(404).json({ success: false, message: 'Store/Warehouse not found' });
    }

    const storeInfo = storeInfoRows[0];
    const store_id = storeInfo.storeId;

    // Get SMS Configuration for this store
    const [smsConfigRows] = await connection.execute(
      `SELECT api_url, sender_name, username, password
       FROM sms_configuration
       WHERE store_id = ?
       ORDER BY id DESC LIMIT 1`,
      [store_id]
    );

    if (smsConfigRows.length === 0) {
      return res.status(404).json({ success: false, message: 'SMS configuration not found for store' });
    }

    const { api_url, sender_name, username, password } = smsConfigRows[0];
    const encodedAuth = Buffer.from(`${username}:${password}`).toString('base64');

    const smsText = `Ghala: ${storeInfo.warehousename}, lipa kiasi hiki: ${Number(amount).toFixed(2)} TZS, kupitia namba hii: ${storeInfo.lipa_namba}. Asante`;

    const payload = {
      from: sender_name,
      text: smsText,
      to: phone
    };

    try {
      await axios.post(api_url, payload, {
        headers: {
          'Authorization': `Basic ${encodedAuth}`,
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        timeout: 10000
      });

      // Save SMS log
      await connection.execute(
        `INSERT INTO sms (store_id, phone, message, date, status) VALUES (?, ?, ?, ?, ?)`,
        [store_id, phone, smsText, now, 'true']
      );

      return res.json({ message: 'SMS sent successfully' });

    } catch (err) {
      console.error('SMS sending failed:', err.message);

      await connection.execute(
        `INSERT INTO sms (store_id, phone, message, date, status) VALUES (?, ?, ?, ?, ?)`,
        [store_id, phone, smsText, now, 'false']
      );

      return res.status(500).json({ success: false, message: 'SMS sending failed', error: err.message });
    }

  } catch (err) {
    console.error('❌ Server error:', err);
    return res.status(500).json({ success: false, message: 'Internal server error', error: err.message });
  }
});



// Create Sales ==============================================



router.post('/sales/create/sale', auth.authenticateToken, async (req, res) => {
  const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');
  const datePart = moment(now).format('YYYYMMDD');

  const userId = res.locals.id;
  const userName = res.locals.name;
  const {
    items,
    warehouse,
    customer = 0,
    orderTax = 0,
    orderDiscount = 0,
    modalData = {}
  } = req.body;

  const { pay_name, mobile_txn_id, bank_txn_id } = modalData;

  const connection = await getConnection();
  try {
    await connection.beginTransaction();

    let availableItems = [];
    let skippedItems = [];
    const invoiceNo = Math.floor(100000000 + Math.random() * 900000);

    // Validate stock
    for (const item of items) {
      const [productRows] = await connection.execute(
        `SELECT name, qty, cost FROM products WHERE id = ?`, [item.product_id]
      );

      const product = productRows[0];
      if (!product) continue;

      if (item.quantity > product.qty) {
        skippedItems.push({
          product_id: item.product_id,
          product_name: product.name,
          requested: item.quantity,
          available: product.qty
        });
      } else {
        availableItems.push({
          ...item,
          product_name: product.name,
          availableQty: product.qty,
          cost: product.cost
        });
      }
    }

    if (availableItems.length === 0) {
      await connection.rollback();
      return res.json({
        message: 'No items could be sold due to insufficient stock.',
        skippedItems
      });
    }

    const total = availableItems.reduce((sum, item) => sum + parseFloat(item.subtotal || 0), 0);
    const totalCost = availableItems.reduce((sum, item) => sum + (item.cost * item.quantity), 0);
    const grandTotal = total;

    const [warehouseRows] = await connection.execute(`SELECT * FROM warehouses WHERE id = ?`, [warehouse]);
    const store_id = warehouseRows[0].storeId;

    const [fyRows] = await connection.execute(`SELECT id FROM fy_cycle WHERE store_id = ? AND isActive = 1 LIMIT 1`, [store_id]);
    const fy_id = fyRows[0].id;

    const [smsConfig] = await connection.execute(`SELECT * FROM sms_configuration WHERE store_id = ? LIMIT 1`, [store_id]);
    const { api_url, sender_name, username, password } = smsConfig[0];

    const [[{ count }]] = await connection.execute(
      `SELECT COUNT(*) as count FROM sales WHERE DATE(created_at) = CURDATE() AND warehouse_id = ?`, [warehouse]
    );
    const refNumber = `SALES-${datePart}-${String(count + 1).padStart(4, '0')}`;

    const [unapproved] = await connection.query(`
      SELECT * FROM pending_deposits
      WHERE status = 'pending'
      AND DATE(created_at) < CURDATE() AND warehouse_id = ?
      ORDER BY created_at ASC LIMIT 1
    `, [warehouse]);

    if (unapproved.length > 0) {
      await connection.rollback();
      return res.json({
        message: `You must approve all previous day pending deposits before adding a new sale.`
      });
    }

    const [saleResult] = await connection.execute(
      `INSERT INTO sales 
        (invoiceNo, refNumber, user_id, customer_id, store_id, warehouse_id, fy_id, total, order_discount, order_tax, grand_total, total_cost, created_at, created_by, updated_at, updated_by, sale_status, payment_method, mobile_txn_id, bank_txn_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        invoiceNo, refNumber, userId, customer, store_id, warehouse, fy_id,
        total, orderDiscount, orderTax, grandTotal, totalCost,
        now, userName, now, userName, 'DRAFT', pay_name, mobile_txn_id || 0, bank_txn_id || 0
      ]
    );

    const saleId = saleResult.insertId;

    await connection.query(
      "INSERT INTO pending_deposits (created_at, warehouse_id, sale_id, amount) VALUES (?, ?, ?, ?)",
      [now, warehouse, saleId, grandTotal]
    );

    for (const item of availableItems) {
      await connection.execute(
        `UPDATE products SET qty = qty - ? WHERE id = ?`,
        [item.quantity, item.product_id]
      );

      await connection.execute(
        `INSERT INTO sale_items (sale_id, product_id, price, cost, totalCost, quantity, discount, vat, subtotal, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [saleId, item.product_id, item.price, item.cost, item.totalCost, item.quantity, item.discount, item.vat, item.subtotal, now]
      );

      await connection.execute(
        `INSERT INTO logs (user_id, store_id, action, description, createdAt, createdBy)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [
          userId, store_id, 'CREATE SALE',
          `Sold product #${item.product_id}, ref: #${refNumber} qty: ${item.quantity}, subtotal: ${item.subtotal}`,
          now, userName
        ]
      );
    }

    const [templateRows] = await connection.execute(
      `SELECT * FROM sms_templates WHERE type = 'SALES' AND store_id = ? LIMIT 1`,
      [store_id]
    );
    const template = templateRows[0]?.message;

    const [storeInfoRows] = await connection.execute(
      `SELECT s.id as storeId, w.name as warehousename, s.email as email, s.name as storename, s.phone as ownerPhone
       FROM warehouses w JOIN stores s ON s.id = w.storeId WHERE w.id = ?`,
      [warehouse]
    );
    const storeInfo = storeInfoRows[0];
    const encodedAuth = Buffer.from(`${username}:${password}`).toString('base64');

    if (warehouseRows[0].send_sale_sms === 1 && storeInfo?.ownerPhone) {
      const smsText = template
        .replace('{{store}}', storeInfo.storename)
        .replace('{{warehouse}}', storeInfo.warehousename)
        .replace('{{total}}', grandTotal.toFixed(2))
        .replace('{{username}}', userName)
        .replace('{{date}}', now);

      const payload = {
        from: sender_name,
        text: smsText,
        to: storeInfo.ownerPhone
      };

      try {
        await axios.post(api_url, payload, {
          headers: {
            'Authorization': `Basic ${encodedAuth}`,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
          },
          timeout: 10000
        });

        await connection.execute(
          `INSERT INTO sms (store_id, phone, message, date, status) VALUES (?, ?, ?, ?, ?)`,
          [store_id, storeInfo.ownerPhone, smsText, now, 'true']
        );
      } catch (err) {
        console.error(' OWNER SMS failed:', err.message);
        await connection.execute(
          `INSERT INTO sms (store_id, phone, message, status, date) VALUES (?, ?, ?, ?, ?)`,
          [store_id, storeInfo.ownerPhone, smsText, 'false', now]
        );
      }

      const [emailConfig] = await connection.execute(
        'SELECT * FROM mail_configuration WHERE store_id = ? LIMIT 1',
        [store_id]
      );

      if (emailConfig.length > 0) {
        const transporter = nodemailer.createTransport({
          host: emailConfig[0].host,
          port: parseInt(emailConfig[0].port),
          secure: parseInt(emailConfig[0].port) === 465,
          auth: {
            user: emailConfig[0].username,
            pass: emailConfig[0].password
          }
        });

        try {
          await transporter.sendMail({
            from: emailConfig[0].username,
            to: storeInfo.email,
            subject: 'Mauzo ya Bidhaa',
            text: smsText
          });

          await connection.execute(
            `INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, ?)`,
            [storeInfo.email || '', smsText, now, 'true']
          );
        } catch (mailError) {
          await connection.execute(
            `INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, ?)`,
            [storeInfo.email || '', smsText, now, 'false']
          );
        }
      }
    }

    await connection.execute(`DELETE FROM sale_carts WHERE user_id = ?`, [userId]);
    await connection.commit();

    // Check if warehouse supports TRA
     
    if (warehouseRows[0].tra_enabled === 1) {
  let traPayload = null;
  let vfdData = null;

  try {
    // Get TRA Configuration
    const [traConfig] = await connection.execute(
      'SELECT * FROM tra_configuration WHERE store_id = ? LIMIT 1',
      [store_id]
    );

    // Get Sale by ID
    const [saleRows] = await connection.execute(
      `SELECT * FROM sales WHERE id = ?`, [saleId]
    );
    const sale = saleRows[0];

    // Get Sale Items
    const [saleItems] = await connection.execute(
      `SELECT si.*, p.name as product_name FROM sale_items si 
       JOIN products p ON p.id = si.product_id 
       WHERE sale_id = ?`, [saleId]
    );

    // Create TRA Payload
    traPayload = {
      invoiceNumber: sale.invoiceNo,
      datetime: now,
      customer: { 
        tin: traConfig[0].tin      
      },
      items: saleItems.map(item => ({
        name: item.product_name,
        qty: item.quantity,
        price: item.price,
        tax: item.vat || 0
      })),
      totalAmount: sale.grand_total
    };

    // Send to TRA
    const vfdRes = await axios.post(`${traConfig[0].api_url}`, traPayload, {
      headers: {
        Authorization: `Basic ${Buffer.from(`${traConfig[0].username}:${traConfig[0].password}`).toString('base64')}`,
        'Content-Type': 'application/json'
      }
    });

    vfdData = vfdRes.data;

    // Update Sale
    await connection.execute(
      `UPDATE sales SET fiscal_receipt_no = ?, fiscal_code = ?, qr_code = ?, fiscal_submission_status = ? WHERE id = ?`,
      [vfdData.receiptNo, vfdData.fiscalCode, vfdData.qrCode, 'SUCCESS', saleId]
    );

    // Log Success
    await connection.execute(
      `INSERT INTO tra_logs (created_at, store_id, warehouse_id, sale_id, request_data, response_data, status) 
      VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [
        now,
        store_id,
        warehouse,
        saleId,
        JSON.stringify(traPayload),      
        JSON.stringify(vfdData),        
        'SUCCESS'
      ]
    );

  } catch (traError) {
    console.error('TRA Fiscalization failed:', traError.message);

    await connection.execute(
      `UPDATE sales SET fiscal_submission_status = ? WHERE id = ?`,
      ['FAILED', saleId]
    );

    // Log Failure
    await connection.execute(
      `INSERT INTO tra_logs (created_at, store_id, warehouse_id, sale_id, request_data, response_data, status) 
      VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [
        now,
        store_id,
        warehouse,
        saleId,
        JSON.stringify(traPayload || {}),      
        JSON.stringify(vfdData || { error: traError.message }),        
        'FAILED'
      ]
    );
  }
}


    return res.status(200).json({
      message: `Sale completed successfully`,
      saleId,
      skippedItems
    });

  } catch (error) {
    await connection.rollback();
    console.error('Sale error:', error.message);
    res.status(500).json({ message: 'Eror inserting sales', error: error.message });
  }
});




// Get TRA Logs

router.get('/get/success/tra/logs', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    storeId,
    filterType,
    startDate,
    endDate,
    sortBy = 'created_at',
    sortOrder = 'DESC'
  } = req.query;

  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT
        tl.id,
        tl.store_id,
        tl.sale_id,
        tl.request_data,
        tl.response_data,
        tl.status,
        tl.created_at,
        s.name AS storename,
        w.name AS warehousename
      FROM tra_logs tl
      JOIN stores s ON tl.store_id = s.id
      JOIN warehouses w ON tl.warehouse_id = w.id
    `;

    const params = [];
    const whereConditions = [];
    whereConditions.push(`tl.status = 'SUCCESS'`);

    // Role-based filtering
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);

      if (storeIds.length === 0) {
        return res.json({ array: [], totalCount: 0 });
      }

      whereConditions.push(`tl.store_id IN (${storeIds.map(() => '?').join(',')})`);
      params.push(...storeIds);
    }

    // Store filter
    if (storeId) {
      whereConditions.push(`tl.store_id = ?`);
      params.push(storeId);
    }

    // Date filtering with moment + timezone
    const tz = 'Africa/Nairobi';
    let start, end;

    if (filterType) {
      const now = moment().tz(tz);

      switch (filterType) {
        case 'today':
          start = now.clone().startOf('day');
          end = now.clone().endOf('day');
          break;
        case 'yesterday':
          start = now.clone().subtract(1, 'day').startOf('day');
          end = now.clone().subtract(1, 'day').endOf('day');
          break;
        case 'week':
          start = now.clone().startOf('week');
          end = now.clone().endOf('week');
          break;
        case 'last_week':
          start = now.clone().subtract(1, 'week').startOf('week');
          end = now.clone().subtract(1, 'week').endOf('week');
          break;
        case 'month':
          start = now.clone().startOf('month');
          end = now.clone().endOf('month');
          break;
        case 'year':
          start = now.clone().startOf('year');
          end = now.clone().endOf('year');
          break;
      }
    } else if (startDate && endDate) {
      start = moment.tz(startDate, tz).startOf('day');
      end = moment.tz(endDate, tz).endOf('day');
    }

    if (start && end) {
      whereConditions.push(`tl.created_at BETWEEN ? AND ?`);
      params.push(start.format('YYYY-MM-DD HH:mm:ss'), end.format('YYYY-MM-DD HH:mm:ss'));
    }

    // Apply WHERE
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sorting
    const validColumns = ['created_at', 'id', 'status'];
    const orderBy = validColumns.includes(sortBy) ? `tl.${sortBy}` : 'tl.created_at';
    const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    query += ` ORDER BY ${orderBy} ${orderDir}`;

    // Execute
    const [result] = await connection.query(query, params);

    res.json({
      array: result,
      totalCount: result.length
    });

  } catch (err) {
    console.error('Error fetching TRA logs', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});


// Get Failed TRA Logs

router.get('/get/failed/tra/logs', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    storeId,
    warehouseId,
    filterType,
    startDate,
    endDate,
    sortBy = 'created_at',
    sortOrder = 'DESC'
  } = req.query;

  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT
        tl.id,
        tl.store_id,
        tl.warehouse_id,
        tl.sale_id,
        tl.request_data,
        tl.response_data,
        tl.status,
        tl.created_at,
        s.name AS storename,
        w.name AS warehousename
      FROM tra_logs tl
      JOIN stores s ON tl.store_id = s.id
      JOIN warehouses w ON tl.warehouse_id = w.id
    `;

    const params = [];
    const whereConditions = [];
    whereConditions.push(`tl.status = 'FAILED'`);

    // Role-based filtering
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);

      if (storeIds.length === 0) {
        return res.json({ array: [], totalCount: 0 });
      }

      whereConditions.push(`tl.store_id IN (${storeIds.map(() => '?').join(',')})`);
      params.push(...storeIds);
    }

    // Store filter
    if (storeId) {
      whereConditions.push(`tl.store_id = ?`);
      params.push(storeId);
    }

    // Warehouse filter
    if (warehouseId) {
        whereConditions.push(`tl.warehouse_id = ?`);
        params.push(warehouseId);
      }

    // Date filtering with moment + timezone
    const tz = 'Africa/Nairobi';
    let start, end;

    if (filterType) {
      const now = moment().tz(tz);

      switch (filterType) {
        case 'today':
          start = now.clone().startOf('day');
          end = now.clone().endOf('day');
          break;
        case 'yesterday':
          start = now.clone().subtract(1, 'day').startOf('day');
          end = now.clone().subtract(1, 'day').endOf('day');
          break;
        case 'week':
          start = now.clone().startOf('week');
          end = now.clone().endOf('week');
          break;
        case 'last_week':
          start = now.clone().subtract(1, 'week').startOf('week');
          end = now.clone().subtract(1, 'week').endOf('week');
          break;
        case 'month':
          start = now.clone().startOf('month');
          end = now.clone().endOf('month');
          break;
        case 'year':
          start = now.clone().startOf('year');
          end = now.clone().endOf('year');
          break;
      }
    } else if (startDate && endDate) {
      start = moment.tz(startDate, tz).startOf('day');
      end = moment.tz(endDate, tz).endOf('day');
    }

    if (start && end) {
      whereConditions.push(`tl.created_at BETWEEN ? AND ?`);
      params.push(start.format('YYYY-MM-DD HH:mm:ss'), end.format('YYYY-MM-DD HH:mm:ss'));
    }

    // Apply WHERE
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sorting
    const validColumns = ['created_at', 'id', 'status'];
    const orderBy = validColumns.includes(sortBy) ? `tl.${sortBy}` : 'tl.created_at';
    const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    query += ` ORDER BY ${orderBy} ${orderDir}`;

    // Execute
    const [result] = await connection.query(query, params);

    res.json({
      array: result,
      totalCount: result.length
    });

  } catch (err) {
    console.error('Error fetching TRA logs', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});



// Get Failed TRA Logs

router.get('/get/failed/tra/logs', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    storeId,
    warehouseId,
    filterType,
    startDate,
    endDate,
    sortBy = 'created_at',
    sortOrder = 'DESC'
  } = req.query;

  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT
        tl.id,
        tl.store_id,
        tl.warehouse_id,
        tl.sale_id,
        tl.request_data,
        tl.response_data,
        tl.status,
        tl.created_at,
        s.name AS storename,
        w.name AS warehousename
      FROM tra_logs tl
      JOIN stores s ON tl.store_id = s.id
      JOIN warehouses w ON tl.warehouse_id = w.id
    `;

    const params = [];
    const whereConditions = [];
    whereConditions.push(`tl.status = 'FAILED'`);

    // Role-based filtering
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);

      if (storeIds.length === 0) {
        return res.json({ array: [], totalCount: 0 });
      }

      whereConditions.push(`tl.store_id IN (${storeIds.map(() => '?').join(',')})`);
      params.push(...storeIds);
    }

    // Store filter
    if (storeId) {
      whereConditions.push(`tl.store_id = ?`);
      params.push(storeId);
    }

    // Store filter
    if (warehouseId) {
      whereConditions.push(`tl.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // Date filtering with moment + timezone
    const tz = 'Africa/Nairobi';
    let start, end;

    if (filterType) {
      const now = moment().tz(tz);

      switch (filterType) {
        case 'today':
          start = now.clone().startOf('day');
          end = now.clone().endOf('day');
          break;
        case 'yesterday':
          start = now.clone().subtract(1, 'day').startOf('day');
          end = now.clone().subtract(1, 'day').endOf('day');
          break;
        case 'week':
          start = now.clone().startOf('week');
          end = now.clone().endOf('week');
          break;
        case 'last_week':
          start = now.clone().subtract(1, 'week').startOf('week');
          end = now.clone().subtract(1, 'week').endOf('week');
          break;
        case 'month':
          start = now.clone().startOf('month');
          end = now.clone().endOf('month');
          break;
        case 'year':
          start = now.clone().startOf('year');
          end = now.clone().endOf('year');
          break;
      }
    } else if (startDate && endDate) {
      start = moment.tz(startDate, tz).startOf('day');
      end = moment.tz(endDate, tz).endOf('day');
    }

    if (start && end) {
      whereConditions.push(`tl.created_at BETWEEN ? AND ?`);
      params.push(start.format('YYYY-MM-DD HH:mm:ss'), end.format('YYYY-MM-DD HH:mm:ss'));
    }

    // Apply WHERE
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sorting
    const validColumns = ['created_at', 'id', 'status'];
    const orderBy = validColumns.includes(sortBy) ? `tl.${sortBy}` : 'tl.created_at';
    const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    query += ` ORDER BY ${orderBy} ${orderDir}`;

    // Execute
    const [result] = await connection.query(query, params);

    res.json({
      array: result,
      totalCount: result.length
    });

  } catch (err) {
    console.error('Error fetching TRA logs', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});




// Delete TRA Response Logs

router.post('/delete/tra/vfd/response', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;             
  

  try {
    // Wait 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

  // Only allow super admins
  if (!isSuperAdmin) {
    return res.json({ message: 'Access denied. Super admin only.' });
  }

    // Perform the bulk 
    const placeholders = ids.map(() => '?').join(',');
    const sql = `DELETE FROM tra_logs WHERE id IN (${placeholders})`;
    const connection = await getConnection();
    const [result] = await connection.query(sql, ids);

    res.json({ message: `Deleted ${result.affectedRows} successfully `});
  } catch (err) {
    console.error(err);
    res.json({ message: 'Failed', error: err.message });
  }
});


// Push Failed TRA Response Logs

router.post('/push/failed/tra/vfd/response', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;

  try {
    await new Promise(resolve => setTimeout(resolve, 3000)); // Wait 3 sec

    const connection = await getConnection();

    let successCount = 0;
    let failedCount = 0;

    for (const id of ids) {
      try {

        // Get tra_log by id
        const [rows] = await connection.execute(
          'SELECT * FROM tra_logs WHERE id = ? AND status = ? LIMIT 1',
          [id, 'FAILED']
        );

        if (rows.length === 0) {
          // Either not found or not failed, skip
          failedCount++;
          continue;
        }

        const log = rows[0];

        // Parse request_data JSON
        let requestData;
        try {
          requestData = JSON.parse(log.request_data);
        } catch {
          failedCount++;
          continue;
        }

        // Get tra_configuration for the store
        const [configRows] = await connection.execute(
          'SELECT * FROM tra_configuration WHERE store_id = ? LIMIT 1',
          [log.store_id]
        );

        if (configRows.length === 0) {
          failedCount++;
          continue;
        }
        const config = configRows[0];

        // Send POST to TRA API
        const axiosConfig = {
          headers: {
            Authorization: `Basic ${Buffer.from(`${config.username}:${config.password}`).toString('base64')}`,
            'Content-Type': 'application/json',
          },
          timeout: 10000
        };

        const response = await axios.post(config.api_url, requestData, axiosConfig);

        const responseData = response.data;

        // Update the tra_log record to success
        await connection.execute(
          `UPDATE tra_logs SET 
          response_data = ?, 
          status = 'SUCCESS'
          WHERE id = ?`,
          [JSON.stringify(responseData), id]
        );

        successCount++;

      } catch (err) {
        console.error(`Failed to push log id ${id}:`, err.message);
        failedCount++;
      }
    }

    if (failedCount > 0) {
      return res.json({ message: `Retry finished with ${successCount} success and ${failedCount} failed.` });
    }

    return res.json({ message: `All ${successCount} logs retried successfully.` });

  } catch (err) {
    console.error('Error in push failed tra vfd:', err);
    return res.status(500).json({ message: 'Failed', error: err.message });
  }
});



// Tuma ombi la msaada (help desk) ==========================


router.post('/omba/msaada/helpdesk', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const { category, message, store, } = req.body;
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');
  
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    await new Promise(resolve => setTimeout(resolve, 3000));

    // Insert new 
    await conn.query(
      `INSERT INTO help_desk (user_id, store_id, category, message, status, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [res.locals.id, store, category, message, 'open', now, 'null']
    );

    return res.status(201).json({
      message: `Send successfully, wait for response !`
    });

  } catch (err) {
    res.status(500).json({ message: 'Internal server error' });
  } 
});



// Get Msaada List Niliyotuma =============

router.get('/get/msaada/list/niliyotuma', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    conn = await getConnection();

    // Get all help desk tickets for the user
    const [tickets] = await conn.query(
      'SELECT * FROM help_desk WHERE user_id = ? ORDER BY created_at DESC',
      [res.locals.id]
    );

    if (tickets.length === 0) {
      return res.json([]);
    }

    // Get all responses related to these tickets
    // Collect all ticket IDs
    const ticketIds = tickets.map(t => t.id);
    const placeholders = ticketIds.map(() => '?').join(',');

    const [responses] = await conn.query(
      `SELECT * FROM help_desk_responses WHERE help_id IN (${placeholders}) ORDER BY created_at ASC`,
      ticketIds
    );

    // Map responses by help_id for easy lookup
    const responsesByHelpId = responses.reduce((acc, resp) => {
      if (!acc[resp.help_id]) acc[resp.help_id] = [];
      acc[resp.help_id].push(resp);
      return acc;
    }, {});

    // Attach responses array to each ticket
    const ticketsWithResponses = tickets.map(ticket => ({
      ...ticket,
      responses: responsesByHelpId[ticket.id] || []
    }));

    // Return enriched ticket list
    res.json(ticketsWithResponses);

  } catch (err) {
    console.error('Error fetching help desk list:', err);
    res.status(500).json({ message: 'Internal server error' });
  }
});



// Delete My Help Desk ====================

router.delete('/delete/myhelp/desk/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {

    const id = req.params.id;
// Get a new connection from the pool
conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds 
    await new Promise(resolve => setTimeout(resolve, 3000));

    const [result] = await conn.query(
      "DELETE FROM help_desk WHERE id = ?",
      [id]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ message: ` ${id} Not found` });
    } else {
      return res.status(200).json({ message: `Success` });
    }
  } catch (err) {
    return res.status(500).json({ message: 'Server error', details: err });
  }
});




// Respond Request from help center =================

router.post('/respond/request/from/help/center', auth.authenticateToken, async (req, res) => {
  const conn = await getConnection();
  const { respond, id } = req.body;

  const approverId = res.locals.id;
  const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');
  
  try {
    await new Promise(resolve => setTimeout(resolve, 3000)); // delay

    const [[{ name: roleName } = {}]] = await conn.query(
      'SELECT name FROM roles WHERE id = (SELECT role FROM users WHERE id = ?)',
      [approverId]
    );

    if (!roleName) {
      return res.json({ message: 'Invalid role access.' });
    }

    const isAdmin = roleName === 'ADMIN';

    if (!isAdmin) {
      return res.json({ message: 'Access denied. Only ADMIN allowed.' });
    }

    // Insert new ====================== 
    await conn.query(
      `INSERT INTO help_desk_responses (help_id, responder_id, message, created_at)
       VALUES (?, ?, ?, ?)`,
      [id, res.locals.id, respond, now]
    );


    // Update help desk status ===================

    await conn.query(
      `UPDATE help_desk SET status = 'closed', updated_at = ? WHERE id = ?`,
      [now, id]
    );
    

    res.json({ message: 'Successfully!' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Database error ' });
  }
});




// Get All help Desk Request list =========

router.get('/get/msaada/list/zilizotumwa', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    conn = await getConnection();

    // Get all help desk tickets (no user_id filter)
    const [tickets] = await conn.query(
      `SELECT hd.*, s.name AS storename, u.name AS username, u.phone AS userphone, 
              u.email AS useremail
       FROM help_desk hd
       JOIN users u ON u.id = hd.user_id
       JOIN stores s ON s.id = hd.store_id
       ORDER BY hd.created_at DESC`
    );

    if (tickets.length === 0) {
      return res.json({
        counts: { open: 0, closed: 0, in_progress: 0 },
        tickets: []
      });
    }

    // Get counts by status for ALL tickets
    const [statusCounts] = await conn.query(
      `SELECT status, COUNT(*) AS count 
       FROM help_desk
       GROUP BY status`
    );

    const counts = {
      open: 0,
      closed: 0,
      in_progress: 0
    };
    statusCounts.forEach(row => {
      if (row.status === 'open') counts.open = row.count;
      else if (row.status === 'closed') counts.closed = row.count;
      else if (row.status === 'in_progress') counts.in_progress = row.count;
    });

    // Get all responses for these tickets
    const ticketIds = tickets.map(t => t.id);
    const placeholders = ticketIds.map(() => '?').join(',');

    let responsesByHelpId = {};
    if (ticketIds.length > 0) {
      const [responses] = await conn.query(
        `SELECT * FROM help_desk_responses 
         WHERE help_id IN (${placeholders})
         ORDER BY created_at ASC`,
        ticketIds
      );

      responsesByHelpId = responses.reduce((acc, resp) => {
        if (!acc[resp.help_id]) acc[resp.help_id] = [];
        acc[resp.help_id].push(resp);
        return acc;
      }, {});
    }

    // Attach responses to tickets
    const ticketsWithResponses = tickets.map(ticket => ({
      ...ticket,
      responses: responsesByHelpId[ticket.id] || []
    }));

    res.json({
      counts,
      tickets: ticketsWithResponses
    });

  } catch (err) {
    console.error('Error fetching help desk list:', err);
    res.status(500).json({ message: 'Internal server error' });
  }
});


// DRAFT SALES LISTS GROUP BY DATE ======================

router.get('/draft-sale-lists/grouped-by-date', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

  let connection;

  try {
    connection = await getConnection();

    // Use moment-timezone to ensure consistent date filtering
    const todayStart = moment().tz('Africa/Nairobi').startOf('day').format('YYYY-MM-DD HH:mm:ss');
    const todayEnd = moment().tz('Africa/Nairobi').endOf('day').format('YYYY-MM-DD HH:mm:ss');

    let query = `
      SELECT
        DATE(s.created_at) AS created_date,
        s.sale_status,
        s.warehouse_id,
        w.name AS warehousename,
        COUNT(*) AS total_transactions,
        SUM(s.grand_total) AS total_grand_sales,
        SUM(s.total) AS total_before_vat_discount,
        SUM(s.total_cost) AS total_cost_item,
        SUM(s.order_discount) AS total_order_discount,
        SUM(s.order_tax) AS total_order_tax,
        fc.name AS fy_name
      FROM sales s
      JOIN warehouses w ON s.warehouse_id = w.id
      LEFT JOIN fy_cycle fc ON fc.id = s.fy_id
      WHERE s.sale_status = 'DRAFT'
        AND s.created_at BETWEEN ? AND ?
    `;

    const params = [todayStart, todayEnd];

    if (!isSuperAdmin) {
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?',
        [userId]
      );

      const warehouseIds = warehouseRows.map(row => row.warehouse_id);
      if (warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      const placeholders = warehouseIds.map(() => '?').join(', ');
      query += ` AND s.warehouse_id IN (${placeholders})`;
      params.push(...warehouseIds);
    }

    query += `
      GROUP BY s.warehouse_id, created_date
      ORDER BY created_date DESC
    `;

    const [result] = await connection.query(query, params);

    res.json({ array: result });

  } catch (err) {
    console.error(' Error fetching draft sales summary:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});




// APPROVED SALES DEPOSIT GROUP BY DATE ======================

router.get('/approved-deposits/grouped-by-date', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    warehouseId,
    filterType,
    startDate,
    endDate,
    sortBy = 'deposited_at',
    sortOrder = 'DESC',
  } = req.query;

  const timezone = 'Africa/Nairobi';
  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT
        d.deposited_at as deposit_date, 
        d.warehouse_id, d.deposit_account_id,
        COUNT(*) as total_deposit, d.deposited_by,
        SUM(d.payment_received) AS total_amount,
        d.status,
        w.id as warehouse_id,
        w.name as warehousename,
        fc.id as fyc_id, fc.name as fy_name,
        da.id, da.type, da.mobile, da.banks, da.pay_number, da.bankNo
      FROM deposits d
      JOIN warehouses w ON d.warehouse_id = w.id
      JOIN deposit_accounts da ON da.id = d.deposit_account_id
      LEFT JOIN fy_cycle fc ON fc.id = d.fy_id
    `;

    const params = [];
    const whereConditions = [`d.status = 'approved'`];

    // Role-based warehouse filtering
    if (!(roleId === 1 || roleId === '1')) {
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const warehouseIds = warehouseRows.map(r => r.warehouse_id);
      if (warehouseIds.length > 0) {
        whereConditions.push(`d.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      } else {
        return res.json({ array: [] }); // no warehouse access
      }
    }

    // Filter by specific warehouse if provided
    if (warehouseId) {
      whereConditions.push(`d.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // Date filtering with moment-timezone
    let start, end;

    if (filterType) {
      switch (filterType) {
        case 'today':
          start = moment.tz(timezone).startOf('day').format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).endOf('day').format('YYYY-MM-DD HH:mm:ss');
          break;
        case 'yesterday':
          start = moment.tz(timezone).subtract(1, 'day').startOf('day').format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'day').endOf('day').format('YYYY-MM-DD HH:mm:ss');
          break;
        case 'week':
          start = moment.tz(timezone).startOf('week').format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).endOf('week').format('YYYY-MM-DD HH:mm:ss');
          break;
        case 'last_week':
          start = moment.tz(timezone).subtract(1, 'week').startOf('week').format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'week').endOf('week').format('YYYY-MM-DD HH:mm:ss');
          break;
        case 'month':
          start = moment.tz(timezone).startOf('month').format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).endOf('month').format('YYYY-MM-DD HH:mm:ss');
          break;
        case 'year':
          start = moment.tz(timezone).startOf('year').format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).endOf('year').format('YYYY-MM-DD HH:mm:ss');
          break;
        case 'last_year':
          start = moment.tz(timezone).subtract(1, 'year').startOf('year').format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'year').endOf('year').format('YYYY-MM-DD HH:mm:ss');
          break;
        case 'last_2_years':
          start = moment.tz(timezone).subtract(2, 'years').startOf('year').format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'year').endOf('year').format('YYYY-MM-DD HH:mm:ss');
          break;
        case 'last_3_years':
          start = moment.tz(timezone).subtract(3, 'years').startOf('year').format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'year').endOf('year').format('YYYY-MM-DD HH:mm:ss');
          break;
        case 'last_4_years':
          start = moment.tz(timezone).subtract(4, 'years').startOf('year').format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'year').endOf('year').format('YYYY-MM-DD HH:mm:ss');
          break;
      }
    } else if (startDate && endDate) {
      start = moment.tz(startDate, timezone).startOf('day').format('YYYY-MM-DD HH:mm:ss');
      end = moment.tz(endDate, timezone).endOf('day').format('YYYY-MM-DD HH:mm:ss');
    }

    if (start && end) {
      whereConditions.push(`d.deposited_at BETWEEN ? AND ?`);
      params.push(start, end);
    }

    // Finalize query
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    query += ` GROUP BY DATE(d.deposited_at), d.warehouse_id`;

    const columnMap = {
      created_at: 'd.deposited_at'
    };
    const orderBy = columnMap[sortBy] || 'd.deposited_at';
    const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    query += ` ORDER BY ${orderBy} ${orderDir}`;

    const [result] = await connection.query(query, params);

    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching approved deposits:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});


// PENDING SALES DEPOSIT GROUP BY DATE ======================

router.get('/pending-deposits/grouped-by-date', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    warehouseId,
    filterType,
    startDate,
    endDate,
    sortBy = 'created_at',
    sortOrder = 'DESC',
  } = req.query;

  const timezone = 'Africa/Nairobi';
  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT
        pd.created_at AS deposit_date,
        pd.warehouse_id, pd.status,
        COUNT(*) AS total_pending,
        SUM(pd.amount) AS total_amount,
        w.id AS warehouse_id,
        w.name AS warehousename
      FROM pending_deposits pd
      JOIN warehouses w ON pd.warehouse_id = w.id
    `;

    const params = [];
    const whereConditions = [`pd.status = 'pending'`];

    // Role-based filtering
    if (!(roleId === 1 || roleId === '1')) {
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (warehouseIds.length > 0) {
        whereConditions.push(`pd.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      } else {
        return res.json({ array: [] }); // no access
      }
    }

    if (warehouseId) {
      whereConditions.push(`pd.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // Date filtering using moment-timezone
    let start, end;

    if (filterType) {
      switch (filterType) {
        case 'today':
          start = moment.tz(timezone).startOf('day').format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).endOf('day').format('YYYY-MM-DD HH:mm:ss');
          break;
        case 'yesterday':
          start = moment.tz(timezone).subtract(1, 'day').startOf('day').format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'day').endOf('day').format('YYYY-MM-DD HH:mm:ss');
          break;
        case 'week':
          start = moment.tz(timezone).startOf('week').format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).endOf('week').format('YYYY-MM-DD HH:mm:ss');
          break;
        case 'last_week':
          start = moment.tz(timezone).subtract(1, 'week').startOf('week').format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'week').endOf('week').format('YYYY-MM-DD HH:mm:ss');
          break;
        case 'month':
          start = moment.tz(timezone).startOf('month').format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).endOf('month').format('YYYY-MM-DD HH:mm:ss');
          break;
        case 'year':
          start = moment.tz(timezone).startOf('year').format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).endOf('year').format('YYYY-MM-DD HH:mm:ss');
          break;
        case 'last_year':
          start = moment.tz(timezone).subtract(1, 'year').startOf('year').format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'year').endOf('year').format('YYYY-MM-DD HH:mm:ss');
          break;
        case 'last_2_years':
          start = moment.tz(timezone).subtract(2, 'years').startOf('year').format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'year').endOf('year').format('YYYY-MM-DD HH:mm:ss');
          break;
        case 'last_3_years':
          start = moment.tz(timezone).subtract(3, 'years').startOf('year').format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'year').endOf('year').format('YYYY-MM-DD HH:mm:ss');
          break;
        case 'last_4_years':
          start = moment.tz(timezone).subtract(4, 'years').startOf('year').format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'year').endOf('year').format('YYYY-MM-DD HH:mm:ss');
          break;
      }
    } else if (startDate && endDate) {
      start = moment.tz(startDate, timezone).startOf('day').format('YYYY-MM-DD HH:mm:ss');
      end = moment.tz(endDate, timezone).endOf('day').format('YYYY-MM-DD HH:mm:ss');
    }

    if (start && end) {
      whereConditions.push(`pd.created_at BETWEEN ? AND ?`);
      params.push(start, end);
    }

    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Grouping and sorting
    query += ` GROUP BY DATE(pd.created_at), pd.warehouse_id`;

    const columnMap = {
      created_at: 'pd.created_at'
    };
    const orderBy = columnMap[sortBy] || 'pd.created_at';
    const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    query += ` ORDER BY ${orderBy} ${orderDir}`;

    const [result] = await connection.query(query, params);

    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching pending deposits:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});


// APPROVE PENDING SALES ==================================

router.post('/sales/deposits/approve', auth.authenticateToken, async (req, res) => {
  const { deposit_date, warehouse_id, deposit_account_id } = req.body;
  const approverId = res.locals.id;
  const approverName = res.locals.name;

  let connection;

  try {
    await new Promise(resolve => setTimeout(resolve, 3000)); // Optional delay
    connection = await getConnection();

    // Get role
    const [[{ name: roleName } = {}]] = await connection.query(
      'SELECT name FROM roles WHERE id = (SELECT role FROM users WHERE id = ?)',
      [approverId]
    );

    if (!['ADMIN', 'MANAGER'].includes(roleName)) {
      return res.json({ message: ' Access denied. Only MANAGER or ADMIN allowed.' });
    }

    // Get store from warehouse
    const [warehouseRows] = await connection.execute(
      'SELECT * FROM warehouses WHERE id = ?', [warehouse_id]
    );
    const store_id = warehouseRows[0]?.storeId;
    if (!store_id) return res.status(400).json({ message: ' Warehouse not found' });

    // Active FY
    const [fyRows] = await connection.execute(
      'SELECT id FROM fy_cycle WHERE store_id = ? AND isActive = 1 LIMIT 1',
      [store_id]
    );
    const fy_id = fyRows[0]?.id;
    if (!fy_id) return res.status(400).json({ message: ' Active financial year not found' });

    // Normalize date range (00:00 to 23:59 for the selected day in Nairobi)
    const start = moment.tz(deposit_date, 'Africa/Nairobi').startOf('day').format('YYYY-MM-DD HH:mm:ss');
    const end = moment.tz(deposit_date, 'Africa/Nairobi').endOf('day').format('YYYY-MM-DD HH:mm:ss');

    // Fetch pending deposits
    const [pendingRows] = await connection.query(
      `SELECT * FROM pending_deposits 
       WHERE warehouse_id = ? 
       AND created_at BETWEEN ? AND ?
      AND status = 'pending'`,
      [warehouse_id, start, end]
    );

    if (pendingRows.length === 0) {
      return res.status(404).json({ message: ' No pending deposits found for given filters' });
    }

    const saleIds = [...new Set(pendingRows.map(r => r.sale_id))];

    // Block self-approvals
    const [sales] = await connection.query(
      `SELECT id, user_id FROM sales WHERE id IN (${saleIds.map(() => '?').join(',')})`,
      saleIds
    );

    const selfApproved = sales.filter(s => s.user_id === approverId);
    if (selfApproved.length > 0) {
      return res.json({
        message: ' You cannot approve sales you created.',
        blocked_sales: selfApproved.map(s => s.id)
      });
    }

    // Transaction starts
    await connection.beginTransaction();

    // Insert into `deposits`
    const depositInsertValues = pendingRows.map(r => [
      fy_id,
      deposit_account_id,
      warehouse_id,
      r.sale_id,
      r.amount,
      0, // payment_sent
      approverName,
      'approved',
      moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss') // deposited_at
    ]);

    await connection.query(
      `INSERT INTO deposits 
       (fy_id, deposit_account_id, warehouse_id, sale_id, payment_received, payment_sent, deposited_by, status, deposited_at) 
       VALUES ?`,
      [depositInsertValues]
    );

    // Delete from `pending_deposits`
    const pendingIds = pendingRows.map(r => r.id);
    await connection.query(
      `DELETE FROM pending_deposits WHERE id IN (${pendingIds.map(() => '?').join(',')})`,
      pendingIds
    );

    // Update sale status to APPROVED
    await connection.query(
      `UPDATE sales SET sale_status = 'APPROVED' WHERE id IN (${saleIds.map(() => '?').join(',')})`,
      saleIds
    );

    await connection.commit();

    res.json({
      message: ` Approved ${pendingRows.length} deposit(s) for warehouse ID ${warehouse_id}.`
    });

  } catch (err) {
    if (connection) await connection.rollback();
    console.error(' Error during approval:', err);
    res.status(500).json({ message: ' Approval failed', error: err.message });
  }
});



// GET DEPOSIT ACCOUNT BY WAREHOUSE ID ===============================

router.get('/deposit_accounts/:warehouse_id', auth.authenticateToken, async (req, res) => {
  const { warehouse_id } = req.params;
  try {
    const connection = await getConnection();
    const [rows] = await connection.query(
      `SELECT id, warehouse_id, type, banks, bankNo, mobile, pay_number FROM deposit_accounts WHERE warehouse_id = ?`,
      [warehouse_id]
    );
    res.json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Failed to fetch deposit accounts ❌' });
  }
});




// PURCHASES MODULE API ==============================

// Add Purchases Carts
router.post('/purchase/cart/add', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const {
    productId,
    quantity,
    cost
  } = req.body;

  // Convert all inputs to numbers to avoid type issues
  const purchase_qty = parseFloat(quantity) || 0;
  const unitCost = parseFloat(cost) || 0;
 
  const connection = await getConnection();
  const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

  try {
    await connection.beginTransaction();

    // Check if item already exists in cart
    const [existing] = await connection.execute(
      `SELECT quantity FROM purchase_carts WHERE user_id = ? AND product_id = ?`,
      [userId, productId]
    );

    if (existing.length) {
      const oldQty = parseFloat(existing[0].quantity);
      const newQty = oldQty + purchase_qty;

      // Calculate subtotal using the updated quantity
      const updatedSubtotal = unitCost * newQty;

      await connection.execute(
        `UPDATE purchase_carts 
         SET quantity = ?, cost = ?, subtotal = ?, updated_at = ?
         WHERE user_id = ? AND product_id = ?`,
        [newQty, unitCost, updatedSubtotal, now, userId, productId]
      );
    } else {
      // Subtotal for new entry
      const subtotal = unitCost * purchase_qty;

      await connection.execute(
        `INSERT INTO purchase_carts 
         (user_id, product_id, quantity, created_at, updated_at, cost, subtotal)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [userId, productId, purchase_qty, now, null, unitCost, subtotal]
      );
    }

    await connection.commit();
    res.json({ message: 'Cart updated successfully ' });

  } catch (err) {
    console.error('Error adding to cart:', err);
    await connection.rollback();
    res.status(500).json({ message: 'Failed to update cart', error: err.message });
  }
});


// Get Purchase Carts

router.get('/get/purchase/cart', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;

  const connection = await getConnection();
  try {
    const [rows] = await connection.execute(
      `SELECT c.id, c.product_id, c.quantity, p.name, p.qty as qty_available,
      c.cost, c.subtotal, c.created_at
       FROM purchase_carts c
       JOIN products p ON c.product_id = p.id
       WHERE c.user_id = ? ORDER BY c.created_at DESC`,
      [userId]
    );

    res.json(rows);
  } catch (err) {
    res.status(500).json({ message: 'Failed to fetch cart' });
  }
});


// Clear All Purchases Cart
router.delete('/purchase-cart/clear', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const connection = await getConnection();

  try {
    await connection.query(`DELETE FROM purchase_carts WHERE user_id = ?`, [userId]);
    res.json({ message: 'Cart cleared successfully' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Failed to clear cart' });
  }
});

// Delete Purchase Cart Row Items by id
router.delete('/purchase/cart/:id', auth.authenticateToken, async (req, res) => {
  const { id } = req.params;
  // Perform deletion logic
  const userId = res.locals.id;
  const connection = await getConnection();

  try {
    await connection.query(`DELETE FROM purchase_carts WHERE user_id = ? AND id = ? `, [userId, id]);
    res.json({ message: 'Removed successfully' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Failed to delete' });
  }
});


// Remove Purchase Carts From the Lists
router.delete('/purchase/cart/remove', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const { productId } = req.body;

  try {
    const connection = await getConnection();
    await connection.execute(
      `DELETE FROM purchase_carts WHERE user_id = ? AND product_id = ?`,
      [userId, productId]
    );

    res.json({ message: 'Item removed from cart' });
  } catch (err) {
    res.status(500).json({ message: 'Error removing item', error: err.message });
  }
});

// CREATE NEW PURCHASE ORDER ===============================

router.post('/purchases/new/items/create', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const userName = res.locals.name;

  const {
    warehouse,
    supplier,
    items,
    orderDiscount = 0,
    orderTax = 0,
    shipping = 0,
    grandTotal = 0,
    modalData = {}
  } = req.body;

  const {
    mobile_phone,
    bank_no,
    pay_name,
    mobile_txn_id,
    bank_txn_id
  } = modalData;

  const connection = await getConnection();

  try {
    function formatDateOnly(dateInput) {
      if (!dateInput) return null;
      const d = new Date(dateInput);
      return d.toISOString().split('T')[0]; // 'YYYY-MM-DD'
    }

    const currentTimestamp = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    await connection.beginTransaction();

    const datePart = moment().tz('Africa/Nairobi').format('YYYYMMDD');

    const [[{ count }]] = await connection.execute(
      `SELECT COUNT(*) as count FROM products WHERE DATE(product_create_date) = CURDATE() AND warehouse_id = ?`,
      [warehouse]
    );
    const refNumber = `PRO-${datePart}-${String(count + 1).padStart(4, '0')}`;

    const [warehouseData] = await connection.execute(
      `SELECT * FROM warehouses WHERE id = ? LIMIT 1`, [warehouse]
    );
    const store_id = warehouseData[0].storeId;

    const [fyCycle] = await connection.execute(
      `SELECT id FROM fy_cycle WHERE store_id = ? AND isActive = 1 LIMIT 1`, [store_id]
    );
    const fy_id = fyCycle[0].id;

    const prefNumber = Math.floor(100000000 + Math.random() * 900000);
    const productIds = [];

    for (const item of items) {
      let whereClause = 'warehouse_id = ?';
      const values = [warehouse];

      if (item.name) {
        whereClause += ' AND name = ?';
        values.push(item.name);
      }
      if (item.barcode_no) {
        whereClause += ' AND barcode_no = ?';
        values.push(item.barcode_no);
      }
      if (item.batch_no) {
        whereClause += ' AND batch_no = ?';
        values.push(item.batch_no);
      }

      const [existing] = await connection.execute(
        `SELECT id FROM products WHERE ${whereClause} LIMIT 1`, values
      );

      let productId;

      if (existing.length === 0) {
        const [insertRes] = await connection.execute(`
          INSERT INTO products (
            store_id, warehouse_id, category_id, brand_id, unit_id, refNumber,
            name, batch_no, barcode_no, qty, cost, price, imei_serial,
            expire_date, vat, discount,
            product_create_date, product_create_by,
            product_update_date, product_update_by,
            product_status, product_qty_alert
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'true', ?)
        `, [
          store_id, warehouse, item.category_id || 0, item.brand_id || 0, item.unit_id || 0, prefNumber,
          item.name, item.batch_no || 'null', item.barcode || 'null',
          item.qty, item.cost, item.price || 0, item.imei_serial || 'null',
          item.expire_date || 'null',
          item.vat || 0, item.discount || 0,
          currentTimestamp, userName,
          currentTimestamp, userName, 0
        ]);

        productId = insertRes.insertId;

      } else {

        productId = existing[0].id;

        await connection.execute(`
          UPDATE products
          SET  batch_no = ?, barcode_no = ?, imei_serial = ?, expire_date = ?, vat = ?, discount = ?,  qty = qty + ?, cost = ?, product_update_date = ?, product_update_by = ?
          WHERE id = ? AND product_status = 'true'
        `, [
          item.batch_no || 'null', 
          item.barcode_no || 'null',
          item.imei_serial || 'null',
          item.expire_date || 'null',
          item.vat || 0, 
          item.discount || 0,
          item.qty, item.cost, 
          currentTimestamp, 
          userName, 
          productId
        ]);

      }

      productIds.push({ ...item, productId });
    }

    const invoiceNo = Math.floor(100000000 + Math.random() * 900000);
    const total = items.reduce((sum, i) => sum + (i.subtotal || 0), 0);

    const [purchaseResult] = await connection.execute(`
      INSERT INTO purchases 
        (mobile_phone, payment_method, mobile_txn_id, bank_no, bank_txn_id, invoiceNo,
         user_id, store_id, warehouse_id, supplier_id, fy_id, refNumber,
         total, order_discount, order_tax, shipping, grand_total,
         created_at, created_by, updated_at, updated_by, purchase_status)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'APPROVED')
    `, [
      mobile_phone || 'null', pay_name || 'null', mobile_txn_id || 'null',
      bank_no || 'null', bank_txn_id || 'null', invoiceNo,
      userId, store_id, warehouse, supplier, fy_id, refNumber,
      total, orderDiscount, orderTax, shipping, grandTotal,
      currentTimestamp, userName, currentTimestamp, userName
    ]);

    const purchaseId = purchaseResult.insertId;

    for (const item of productIds) {
      await connection.execute(`
        INSERT INTO purchase_items (created_at, purchase_id, product_id, cost, quantity, subtotal)
        VALUES (?, ?, ?, ?, ?, ?)`,
        [currentTimestamp, purchaseId, item.productId, item.cost, item.qty, item.subtotal]
      );

      await connection.execute(`
        INSERT INTO logs (user_id, store_id, action, description, createdAt, createdBy)
        VALUES (?, ?, 'CREATE PURCHASE', ?, ?, ?)`,
        [userId, store_id, `Purchased product #${item.productId}, qty: ${item.qty}, subtotal: ${item.subtotal}`, currentTimestamp, userName]
      );
    }

    const [smsConfig] = await connection.execute(
      `SELECT * FROM sms_configuration WHERE store_id = ? LIMIT 1`, [store_id]
    );
    const { api_url, sender_name, username, password } = smsConfig[0] || {};

    const [templateRows] = await connection.execute(
      `SELECT * FROM sms_templates WHERE type = 'PURCHASES' AND store_id = ? LIMIT 1`, [store_id]
    );
    const template = templateRows[0]?.message;

    const [storeInfoRows] = await connection.execute(
      `SELECT s.id as storeId, s.email, w.name as warehousename, s.name as storename, s.phone as ownerPhone
       FROM warehouses w JOIN stores s ON s.id = w.storeId WHERE w.id = ?`, [warehouse]
    );
    const storeInfo = storeInfoRows[0];

    if (warehouseData[0].send_purchase_sms === 1 && storeInfo?.ownerPhone && template) {
      const smsText = template
        .replace('{{store}}', storeInfo.storename)
        .replace('{{warehouse}}', storeInfo.warehousename)
        .replace('{{total}}', grandTotal.toFixed(2))
        .replace('{{username}}', userName)
        .replace('{{date}}', currentTimestamp);

      try {
        const encodedAuth = Buffer.from(`${username}:${password}`).toString('base64');

        await axios.post(api_url, {
          from: sender_name,
          text: smsText,
          to: storeInfo.ownerPhone
        }, {
          headers: {
            'Authorization': `Basic ${encodedAuth}`,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
          }
        });

        await connection.execute(`INSERT INTO sms (store_id, phone, message, date, status) VALUES (?, ?, ?, ?, 'true')`,
          [store_id, storeInfo.ownerPhone, smsText, currentTimestamp]);

      } catch (smsErr) {
        await connection.execute(`INSERT INTO sms (store_id, phone, message, date, status) VALUES (?, ?, ?, ?, 'false')`,
          [store_id, storeInfo.ownerPhone, smsText, currentTimestamp]);
      }
    }

    const [supplierInfoRows] = await connection.execute('SELECT * FROM suppliers WHERE id = ? LIMIT 1', [supplier]);
    const supplierInfo = supplierInfoRows[0];

    const [emailConfig] = await connection.execute('SELECT * FROM mail_configuration WHERE store_id = ? LIMIT 1', [store_id]);
    if (emailConfig.length === 0) throw new Error('Email configuration not found');

    const transporter = nodemailer.createTransport({
      host: emailConfig[0].host,
      port: parseInt(emailConfig[0].port),
      secure: parseInt(emailConfig[0].port) === 465,
      auth: {
        user: emailConfig[0].username,
        pass: emailConfig[0].password
      }
    });

    const storeEmailText = template
      .replace('{{store}}', storeInfo.storename)
      .replace('{{warehouse}}', storeInfo.warehousename)
      .replace('{{total}}', grandTotal.toFixed(2))
      .replace('{{username}}', userName)
      .replace('{{date}}', currentTimestamp);

    const supplierEmailText = `Ndg ${supplierInfo.name},\n\nTumepokea mzigo wa TZS: ${grandTotal.toFixed(2)} leo tarehe ${currentTimestamp}\n\nAsante.`;

    try {
      await transporter.sendMail({
        from: emailConfig[0].username,
        to: storeInfo.email,
        subject: 'Manunuzi ya Bidhaa',
        text: storeEmailText
      });
      await connection.execute(`INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, 'true')`, [storeInfo.email, storeEmailText, currentTimestamp]);
    } catch {
      await connection.execute(`INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, 'false')`, [storeInfo.email, storeEmailText, currentTimestamp]);
    }

    try {
      await transporter.sendMail({
        from: emailConfig[0].username,
        to: supplierInfo.email,
        subject: `Mrejesho wa Manunuzi kutoka ${storeInfo.storename}`,
        text: supplierEmailText
      });
      await connection.execute(`INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, 'true')`, [supplierInfo.email, supplierEmailText, currentTimestamp]);
    } catch {
      await connection.execute(`INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, 'false')`, [supplierInfo.email, supplierEmailText, currentTimestamp]);
    }

    await connection.commit();
    return res.status(200).json({ success: true, message: 'Purchase and items recorded successfully' });

  } catch (err) {
    await connection.rollback();
    return res.status(500).json({ success: false, message: 'Purchase insert failed', error: err.message });
  }
});


// Save/Create Purchases ==================================

router.post('/purchases/create/purchase', auth.authenticateToken, async (req, res) => {
  
  const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');
  const datePart = moment(now).format('YYYYMMDD');
  const currentTimestamp = now;

  const userId = res.locals.id;
  const userName = res.locals.name;

  const {
    items,
    warehouse,
    supplier,
    orderTax = 0,
    orderDiscount = 0,
    shipping = 0,
    grandTotal = 0,
    modalData = {}
  } = req.body;

  const { mobile_phone, bank_no, pay_name, mobile_txn_id, bank_txn_id } = modalData;
  const supplier_id = Number.isInteger(parseInt(supplier)) ? parseInt(supplier) : 0;

  const connection = await getConnection();

  try {
    await connection.beginTransaction();

    const invoiceNo = Math.floor(100000000 + Math.random() * 900000);
    const total = items.reduce((sum, item) => sum + parseFloat(item.subtotal || 0), 0);

    // Get warehouse rows (do NOT throw if none found)
    const [warehouseRows] = await connection.execute(
      `SELECT * FROM warehouses WHERE id = ?`, [warehouse]
    );
    const store_id = warehouseRows[0].storeId;

    // Proceed only if store_id is available; else, fallback to 0 or handle as needed
    if (!store_id) {
      console.warn('Warning: Warehouse not found or missing store_id. Proceeding with store_id=0');
    }

    // Get fiscal year id; fallback safely
    const [fyRows] = await connection.execute(
      `SELECT id FROM fy_cycle WHERE store_id = ? AND isActive = 1 LIMIT 1`, [store_id || 0]
    );
    const fy_id = fyRows[0].id;

    // Count today's purchases to generate ref number
    const [[{ count }]] = await connection.execute(
      `SELECT COUNT(*) as count FROM purchases WHERE DATE(created_at) = CURDATE() AND warehouse_id = ?`,
      [warehouse]
    );
    const refNumber = `PUR-${datePart}-${String(count + 1).padStart(4, '0')}`;

    // Insert into purchases table
    const [purchaseResult] = await connection.execute(
      `INSERT INTO purchases 
      (mobile_phone, payment_method, mobile_txn_id, bank_no, bank_txn_id, invoiceNo, refNumber, user_id, supplier_id, store_id, warehouse_id, fy_id, total, order_discount, order_tax, shipping, grand_total, created_at, created_by, updated_at, updated_by, purchase_status)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        mobile_phone || 'null',
        pay_name || 'null',
        mobile_txn_id || 'null',
        bank_no || 'null',
        bank_txn_id || 'null',
        invoiceNo,
        refNumber,
        userId,
        supplier_id,
        store_id,
        warehouse,
        fy_id,
        total,
        orderDiscount,
        orderTax,
        shipping,
        grandTotal,
        now,
        userName,
        now,
        userName,
        'APPROVED'
      ]
    );

    const purchaseId = purchaseResult.insertId;

    // Process each item
    for (const item of items) {
      const [existingProduct] = await connection.execute(
        `SELECT id, qty FROM products WHERE name = ? AND warehouse_id = ? AND store_id = ? LIMIT 1`,
        [item.name, warehouse, store_id]
      );

      let productId;

      if (existingProduct.length > 0) {
        productId = existingProduct[0].id;
        const newQty = Number(existingProduct[0].qty || 0) + Number(item.quantity || 0);

        await connection.execute(
          `UPDATE products SET qty = ?, cost = ?, product_update_date = ?, product_update_by = ? WHERE id = ?`,
          [newQty, item.cost, now, userName, productId]
        );
      } else {
        
        productId = insertRes.insertId;
      }

      await connection.execute(
        `INSERT INTO purchase_items (purchase_id, product_id, cost, quantity, subtotal, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
        [purchaseId, productId, item.cost, item.quantity, item.subtotal, now]
      );

      await connection.execute(
        `INSERT INTO logs (user_id, store_id, action, description, createdAt, createdBy) VALUES (?, ?, ?, ?, ?, ?)`,
        [userId, store_id, 'CREATE PURCHASE', `Purchased product #${productId}, qty: ${item.quantity}, subtotal: ${item.subtotal}`, now, userName]
      );
    }

    // SMS config
    const [smsConfigRows] = await connection.execute(
      `SELECT * FROM sms_configuration WHERE store_id = ? LIMIT 1`, [store_id || 0]
    );
    const smsConfig = smsConfigRows[0] || {};
    const { api_url, sender_name, username, password } = smsConfig;

    // SMS template
    const [templateRows] = await connection.execute(
      `SELECT * FROM sms_templates WHERE type = 'PURCHASES' AND store_id = ? LIMIT 1`, [store_id || 0]
    );
    const template = templateRows[0]?.message;

    // Store info
    const [storeInfoRows] = await connection.execute(
      `SELECT s.id as storeId, s.email, w.name as warehousename, s.name as storename, s.phone as ownerPhone
       FROM warehouses w JOIN stores s ON s.id = w.storeId WHERE w.id = ?`, [warehouse]
    );
    const storeInfo = storeInfoRows[0] || {};

    if (warehouseRows.length && warehouseRows[0].send_purchase_sms === 1 && storeInfo?.ownerPhone && template) {
      const smsText = template
        .replace('{{store}}', storeInfo.storename || '')
        .replace('{{warehouse}}', storeInfo.warehousename || '')
        .replace('{{total}}', grandTotal.toFixed(2))
        .replace('{{username}}', userName)
        .replace('{{date}}', currentTimestamp);

      try {
        const encodedAuth = Buffer.from(`${username}:${password}`).toString('base64');

        await axios.post(api_url, {
          from: sender_name,
          text: smsText,
          to: storeInfo.ownerPhone
        }, {
          headers: {
            'Authorization': `Basic ${encodedAuth}`,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
          }
        });

        await connection.execute(
          `INSERT INTO sms (store_id, phone, message, date, status) VALUES (?, ?, ?, ?, 'true')`,
          [store_id || 0, storeInfo.ownerPhone, smsText, currentTimestamp]
        );

      } catch (smsErr) {
        await connection.execute(
          `INSERT INTO sms (store_id, phone, message, date, status) VALUES (?, ?, ?, ?, 'false')`,
          [store_id || 0, storeInfo.ownerPhone, smsText, currentTimestamp]
        );
        console.error('SMS sending failed:', smsErr.message);
      }
    }

    // Supplier info
    const [supplierInfoRows] = await connection.execute(
      'SELECT * FROM suppliers WHERE id = ? LIMIT 1', [supplier]
    );
    const supplierInfo = supplierInfoRows[0] || {};

    // Email config
    const [emailConfigRows] = await connection.execute(
      'SELECT * FROM mail_configuration WHERE store_id = ? LIMIT 1', [store_id || 0]
    );
    if (!emailConfigRows.length) console.warn('Email configuration not found');
    const emailConfig = emailConfigRows[0] || {};

    if (emailConfig.host && emailConfig.username && emailConfig.password) {
      const transporter = nodemailer.createTransport({
        host: emailConfig.host,
        port: parseInt(emailConfig.port, 10),
        secure: parseInt(emailConfig.port, 10) === 465,
        auth: {
          user: emailConfig.username,
          pass: emailConfig.password
        }
      });

      const storeEmailText = template
        ? template
            .replace('{{store}}', storeInfo.storename || '')
            .replace('{{warehouse}}', storeInfo.warehousename || '')
            .replace('{{total}}', grandTotal.toFixed(2))
            .replace('{{username}}', userName)
            .replace('{{date}}', currentTimestamp)
        : '';

      const supplierEmailText = `Ndg ${supplierInfo.name || ''},\n\nTumepokea mzigo wa TZS: ${grandTotal.toFixed(2)} leo tarehe ${currentTimestamp}\n\nAsante.`;

      try {
        if (storeInfo.email) {
          await transporter.sendMail({
            from: emailConfig.username,
            to: storeInfo.email,
            subject: 'Manunuzi ya Bidhaa',
            text: storeEmailText
          });
          await connection.execute(
            `INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, 'true')`,
            [storeInfo.email, storeEmailText, currentTimestamp]
          );
        }
      } catch (emailErr) {
        if (storeInfo.email) {
          await connection.execute(
            `INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, 'false')`,
            [storeInfo.email, storeEmailText, currentTimestamp]
          );
        }
        console.error('Store email sending failed:', emailErr.message);
      }

      try {
        if (supplierInfo.email) {
          await transporter.sendMail({
            from: emailConfig.username,
            to: supplierInfo.email,
            subject: `Mrejesho wa Manunuzi kutoka ${storeInfo.storename || ''}`,
            text: supplierEmailText
          });
          await connection.execute(
            `INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, 'true')`,
            [supplierInfo.email, supplierEmailText, currentTimestamp]
          );
        }
      } catch (emailErr) {
        if (supplierInfo.email) {
          await connection.execute(
            `INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, 'false')`,
            [supplierInfo.email, supplierEmailText, currentTimestamp]
          );
        }
        console.error('Supplier email sending failed:', emailErr.message);
      }
    } else {
      console.warn('Email configuration incomplete or missing; skipping email sending');
    }

    // Clear user's purchase cart
    await connection.execute(`DELETE FROM purchase_carts WHERE user_id = ?`, [userId]);

    await connection.commit();
    res.status(200).json({ message: 'Purchase saved successfully' });

  } catch (error) {
    await connection.rollback();
    console.error('Transaction failed:', error.message);
    res.status(500).json({ message: 'Error inserting purchase', error: error.message });
  }
});





// INTEGRATE WITH TEXT EDITOR ====================

router.post('/sql/execute', auth.authenticateToken, async (req, res) => {
  const { sql } = req.body;
  let connection;

  try {
    connection = await getConnection();

    const roleId = res.locals.role;
    const isSuperAdmin = (roleId === 1 || roleId === '1');

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ 
        success: false, 
        error: 'Access denied. OOpps only super admin can access this !!' 
      });
    }

    const [rows] = await connection.query(sql);

    res.json({ 
      success: true, 
      message: 'SQL executed successfully.', 
      data: rows 
    });
  } catch (error) {
    res.status(400).json({ 
      success: false, 
      error: `Execution failed: ${error.message}` 
    });
  }
});




// Get product list for dropdown ==================


router.get('/products/lists/by-warehouse/:warehouse', auth.authenticateToken, async (req, res) => {
  try {
    const conn = await getConnection();
    const [rows] = await conn.query(
      `SELECT id, name FROM products WHERE warehouse_id = ?`,
      [req.params.warehouse]
    );
    res.json(rows);
  } catch (err) {
    res.status(500).json({ message: 'Failed to load products' });
  }
});

// GET EXISTING PRODUCTS LIST DATA FOR REPURCHASE ============

// Get existing products for repurchase
router.get('/existing/:storeId/:warehouseId', async (req, res) => {
  try {
    const { storeId, warehouseId } = req.params;
    const conn = await getConnection();

    const [rows] = await conn.query(
      `SELECT id, name, qty, cost, price FROM products 
       WHERE store_id = ? AND warehouse_id = ? ORDER BY name ASC`,
      [storeId, warehouseId]
    );

    res.json(rows);
  } catch (err) {
    res.status(500).json({ message: 'Failed to fetch existing purchases' });
  }
});


// IMPORT EXCEL FILE FOR PURCHASES ================================


// Middleware for uploading Excel data
var storage = multer.diskStorage({
    destination: function (req, file, cb) {
        return cb(null, './UPLOADS');
    },

    filename: function (req, file, cb) {
        const id = Math.floor(100000 + Math.random() * 900000);
        return cb(null, `${id}_${file.originalname}`) //Appending extension
    }

})

var upload = multer({ storage: storage });


router.post('/create/purchases/import-excel/data', auth.authenticateToken, upload.single('file'), async (req, res) => {
  const conn = await getConnection();
  const filePath = req.file?.path;

  const { warehouse, supplier, store } = req.body;

  try {
    if (!filePath || !fs.existsSync(filePath)) {
      return res.json({ message: 'No file uploaded.' });
    }

    const workbook = XLSX.readFile(filePath);
    const worksheet = workbook.Sheets[workbook.SheetNames[0]];
    const jsonData = XLSX.utils.sheet_to_json(worksheet);

    if (!jsonData.length) {
      fs.unlinkSync(filePath);
      return res.json({ message: 'Uploaded Excel file is empty.' });
    }

    
      // INSERT VALIDATION HERE
      const requiredColumns = [
        'Product Name',
        'Barcode Number',
        'Batch Number',
        'Quantity',
        'Unit Cost',
        'Tax',
        'Discount',
        'Expire Date'
      ];

      const actualColumns = Object.keys(jsonData[0]);
      const missingColumns = requiredColumns.filter(col => !actualColumns.includes(col));

      if (missingColumns.length > 0) {
        fs.unlinkSync(filePath);
        return res.json({
          message: `Invalid Excel format. Missing column(s): ${missingColumns.join(', ')}`
        });
      }

    // Get active financial year
    const [fyRows] = await conn.execute(
      `SELECT id FROM fy_cycle WHERE store_id = ? AND isActive = 1 LIMIT 1`,
      [store]
    );

    if (!fyRows.length) {
      return res.json({ success: false, message: 'No active financial year found' });
    }
    const fy_id = fyRows[0].id;

    // Get Warehouse Information =======================

    const [warehouseRows] = await conn.execute(
      `SELECT * FROM warehouses WHERE id = ?`, [warehouse]
    );

    if (!warehouseRows.length) {
      return res.json({ success: false, message: 'No warehouse found' });
    }

    // Get Sms configurations ===================

    const [smsConfig] = await conn.execute(
      `SELECT * FROM sms_configuration WHERE store_id = ? LIMIT 1`, [store]
    );
    const { api_url, sender_name, username, password } = smsConfig[0];

    // Get SMS template
    const [templateRows] = await conn.execute(
      `SELECT * FROM sms_templates WHERE type = 'PURCHASES' AND store_id = ? LIMIT 1`,
      [store]
    );

    const template = templateRows[0]?.message;

    // Get Store Owner Info =======================

    const [storeInfoRows] = await conn.execute(
      `SELECT s.id as storeId, s.email as email, w.name as warehousename, s.name as storename, s.phone as ownerPhone
       FROM warehouses w JOIN stores s ON s.id = w.storeId WHERE w.id = ?`,
      [warehouse]
    );
    const storeInfo = storeInfoRows[0];
  
    const invoiceNo = Math.floor(100000000 + Math.random() * 900000);
    const refNumber = Math.floor(100000000 + Math.random() * 900000);
    const date_created = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    let purchaseTotal = 0;
    let orderTax = 0;
    let orderDiscount = 0;
    const items = [];

    for (const item of jsonData) {
      const product_name = item['Product Name']?.trim() || '';
      const quantity = parseFloat(item['Quantity']) || 0;
      const unit_cost = parseFloat(item['Unit Cost']) || 0;
      const tax = parseFloat(item['Tax']) || 0;
      const discount = parseFloat(item['Discount']) || 0;
      const barcode_no = item['Barcode Number'] || 'null';
      const batch_no = item['Batch Number'] || 'null';
      
      let expire_date = 'null';

      if (item['Expire Date']) {
        const rawDate = item['Expire Date'];

        if (typeof rawDate === 'number') {
          // Excel stores date as serial number, convert it
          expire_date = moment(new Date((rawDate - 25569) * 86400 * 1000)).format('YYYY-MM-DD');
        } else {
          const parsed = moment(new Date(rawDate));
          expire_date = parsed.isValid() ? parsed.format('YYYY-MM-DD') : null;
        }
      }


      if (!product_name || quantity <= 0 || unit_cost < 0) {
        continue; // skip invalid rows
      }

      // Check if product exists
      const [productRows] = await conn.execute(
        `SELECT id, qty as existing_qty FROM products WHERE name = ? AND warehouse_id = ? LIMIT 1`,
        [product_name, warehouse]
      );

      let product_id;

      if (productRows.length > 0) {
        const existingQty = parseFloat(productRows[0].existing_qty || 0);
        const updatedQty = existingQty + quantity;
        product_id = productRows[0].id;

        await conn.execute(
          'UPDATE products SET vat = ?, discount = ?, cost = ?,  qty = ? WHERE id = ?',
          [tax, discount, unit_cost, updatedQty, product_id]
        );
      } else {
        const [newProductResult] = await conn.execute(
          `INSERT INTO products (
            expire_date, imei_serial, refNumber, vat, discount, product_qty_alert, store_id, warehouse_id, category_id, brand_id, unit_id, name,
            cost, price, qty, product_status, product_create_date, product_create_by,
            product_update_date, product_update_by, barcode_no, batch_no
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            expire_date || 'null',
            'null',
            Math.floor(100000000 + Math.random() * 900000),
            tax, 
            discount,
            0,
            store,
            warehouse,
            0,
            0,
            0,
            product_name,
            unit_cost,
            0,
            quantity,
            'true',
            date_created,
            res.locals.name,
            'null',
            'null',
            barcode_no,
            batch_no
          ]
        );
        product_id = newProductResult.insertId;
      }

      const subtotal = unit_cost * quantity;
      const total = subtotal + tax - discount;

      purchaseTotal += total;
      orderTax += tax;
      orderDiscount += discount;

      items.push({
        product_id,
        quantity,
        unit_cost,
        tax,
        discount,
        total,
        subtotal
      });
    }

    const shipping = 0;
    const grand_total = purchaseTotal;

    // Insert purchase record
    const [purchaseResult] = await conn.query(
      `INSERT INTO purchases (
        invoiceNo, refNumber, user_id, supplier_id, store_id, warehouse_id, fy_id,
        total, order_discount, order_tax, shipping, grand_total,
        created_at, created_by, updated_at, updated_by, purchase_status
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'null', 'null', 'APPROVED')`,
      [
        invoiceNo,
        refNumber,
        res.locals.id,
        supplier,
        store,
        warehouse,
        fy_id,
        purchaseTotal,
        orderDiscount,
        orderTax,
        shipping,
        grand_total,
        date_created,
        res.locals.name,
        // updated_at, updated_by are null
      ]
    );

    const purchase_id = purchaseResult.insertId;

    // Insert purchase items and logs
    for (const item of items) {

      await conn.query(
        `INSERT INTO purchase_items (
          purchase_id, product_id, quantity, cost, subtotal, created_at
        ) VALUES (?, ?, ?, ?, ?, ?)`,
        [
          purchase_id,
          item.product_id,
          item.quantity,
          item.unit_cost,
          item.subtotal,
          date_created
        ]
      );

      await conn.execute(
        `INSERT INTO logs (user_id, store_id, action, description, createdAt, createdBy) VALUES (?, ?, ?, ?, ?, ?)`,
        [
          res.locals.id,
          store,
          'CREATE PURCHASE VIA IMPORT DATA',
          `Purchased product #${item.product_id}, qty: ${item.quantity}, subtotal: ${item.subtotal}`,
          date_created,
          res.locals.name
        ]
      );
    }

    // SEND SMS AND MAIL NOTIFICATIONS ======================

     if (warehouseRows[0].send_purchase_sms === 1 && storeInfo?.ownerPhone) {
         
          const smsText = template
          .replace('{{store}}', storeInfo.storename)
          .replace('{{warehouse}}', storeInfo.warehousename)
          .replace('{{total}}', grand_total.toFixed(2))
          .replace('{{username}}', res.locals.name)
          .replace('{{date}}', date_created);
    
          try {
            const encodedAuth = Buffer.from(`${username}:${password}`).toString('base64');
    
            const payload = {
              from: sender_name,
              text: smsText,
              to: storeInfo.ownerPhone
            };
    
            await axios.post(api_url, payload, {
              headers: {
                'Authorization': `Basic ${encodedAuth}`,
                'Content-Type': 'application/json',
                'Accept': 'application/json'
              }
            });
    
            await conn.execute(
              `INSERT INTO sms (store_id, phone, message, date, status) VALUES (?, ?, ?, ?, ?)`,
              [store, storeInfo.ownerPhone, smsText, date_created, 'true']
            );
    
          } catch (err) {
            await conn.execute(
              `INSERT INTO sms (store_id, phone, message, date, status) VALUES (?, ?, ?, ?, ?)`,
              [store, storeInfo.ownerPhone, smsText, date_created, 'false']
            );
            console.error('Error sending SMS:', err.message);
          }
    
          // GET SUPPLIER INFOS ============================
    
          const [supplierInfoRows] = await conn.execute(
            'SELECT * FROM suppliers WHERE id = ? LIMIT 1',
            [supplier]
          );
    
          const supplierInfo = supplierInfoRows[0];
    
          // SENDING EMAIL TO STORE OWNER ===================
    
              const [emailConfig] = await conn.execute(
                'SELECT * FROM mail_configuration WHERE store_id = ? LIMIT 1',
                [store]
              );
    
              if (emailConfig.length === 0) {
                return res.status(500).json({ error: 'Email configuration not found.' });
              }
    
              const transporter = nodemailer.createTransport({
                host: emailConfig[0].host,
                port: parseInt(emailConfig[0].port),
                secure: parseInt(emailConfig[0].port) === 465,
                auth: {
                  user: emailConfig[0].username,
                  pass: emailConfig[0].password
                }
              });
    
    
              // SENDING SMS TO STORE OWNER =====================
              try {
                await transporter.sendMail({
                  from: emailConfig[0].username,
                  to: storeInfo.email,
                  subject: 'Manunuzi ya Bidhaa',
                  text: smsText
                });
    
                await conn.execute(
                  `INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, ?)`,
                  [storeInfo.email || '', smsText, date_created, 'true']
                );
    
              } catch (mailError) {
              
                await conn.execute(
                  `INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, ?)`,
                  [storeInfo.email || '', smsText, date_created, 'false']
                );
              }
    
              // SENDING SMS TO SUPPLIER =====================
              const mailText = `Ndg ${supplierInfo.name},\n\nTunashukuru sana tumepokea mzigo wa TZS: ${grand_total.toFixed(2)} leo tarehe ${date_created}\n\nAsante.`;
      
              try {
                await transporter.sendMail({
                  from: emailConfig[0].username,
                  to: supplierInfo.email,
                  subject: `Mrejesho wa Manunuzi kutoka ${storeInfo.storename}`,
                  text: mailText
                });
    
                await conn.execute(
                  `INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, ?)`,
                  [supplierInfo.email || '', mailText, date_created, 'true']
                );
    
              } catch (mailError) {
              
                await conn.execute(
                  `INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, ?)`,
                  [supplierInfo.email || '', mailText, date_created, 'false']
                );
              }
    
            // Looping end here =========================================
    
        }



    // Delete the uploaded file after processing
    if (filePath && fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }

    return res.status(200).json({
      success: true,
      message: 'Purchase uploaded successfully',
      purchase_id
    });

  } catch (error) {
    // Delete the uploaded file if error
    if (filePath && fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }

    return res.status(500).json({ message: 'Internal Server Error', error: error.message });
  }
});


// Get Purchases Data by id
router.get('/get/purchase/by/id/:id', auth.authenticateToken, async (req, res) => {
  const id = req.params.id;
  const connection = await getConnection();
  
  try {
    const [purchase] = await connection.execute(
      `SELECT 
        w.id AS warehouse_id,
        w.name AS warehouse_name,
        pu.id AS id,
        pu.created_at,
        pu.total,
        pu.order_discount,
        pu.order_tax,
        pu.shipping,
        pu.grand_total,
        pu.created_by,
        st.id AS store_id,
        st.name AS store_name,
        st.phone AS store_phone
      FROM purchases pu 
      JOIN warehouses w ON pu.warehouse_id = w.id 
      JOIN stores st ON pu.store_id = st.id
      WHERE pu.id = ?`, [id]
    );

    const [items] = await connection.execute(
      `SELECT p.name as product_name, pi.quantity as qty, 
      pi.cost as cost, pi.discount as discount, pi.vat as vat,
      pi.subtotal as subtotal, pi.purchase_id
      FROM purchase_items pi 
      JOIN products p ON pi.product_id = p.id
      WHERE pi.purchase_id = ?`, [id]
    );

    if (!purchase[0]) {
      return res.json({ message: 'Purchase not found' });
    }

    // HTML template for PDF
    const html = `
      <div class="invoice-container">
        <header>
          <h1>${purchase[0].store_name}</h1>
          <hr style="border:2px solid black">
          <p>${purchase[0].warehouse_name}, ${purchase[0].warehouse_location}, ${purchase[0].store_phone}</p>
          <hr>
        </header>
        <section class="details">
          <p><strong>Invoice No:</strong> #${purchase[0].id}</p>
          <p><strong>Date:</strong> ${new Date(purchase[0].created_at).toLocaleDateString()}</p>
          <p><strong>Supplier Name:</strong> ${purchase[0].supplier_name}</p>
          <p><strong>Supplier Number:</strong> ${purchase[0].supplier_phone}</p>
        </section>
        <table>
          <thead>
            <tr>
              <th>No</th>
              <th>Product</th>
              <th>Qty</th>
              <th>Cost</th>
              <th>Discount</th>
              <th>VAT</th>
              <th>Amount</th>
            </tr>
          </thead>
          <tbody>
            ${items.map((item, index) => `
              <tr>
                <td>${index + 1}</td>
                <td>${item.product_name}</td>
                <td>${item.qty}</td>
                <td>${item.cost}</td>
                <td>${item.discount}</td>
                <td>${item.vat}</td>
                <td>${item.subtotal}</td>
              </tr>
            `).join('')}
          </tbody>
          <tfoot>
            <tr>
              <td colspan="5"></td>
              <td><strong>SUBTOTAL</strong></td>
              <td><strong>${purchase[0].total}</strong></td>
            </tr>
            <tr>
              <td colspan="5"></td>
              <td><strong>DISCOUNT</strong></td>
              <td><strong>${purchase[0].order_discount || 0}</strong></td>
            </tr>
            <tr>
              <td colspan="5"></td>
              <td><strong>VAT</strong></td>
              <td><strong>${purchase[0].order_tax || 0}</strong></td>
            </tr>
            <tr>
              <td colspan="5"></td>
              <td><strong>TOTAL</strong></td>
              <td><strong>${purchase[0].grand_total}</strong></td>
            </tr>
          </tfoot>
        </table>
        <footer>
          <p>Authorized Signature: ______________________</p>
          <p>Authorized By: ${purchase[0].created_by}</p>
          <p>Authorized Date: ${new Date(purchase[0].created_at).toLocaleDateString()}</p>
          <p>GENERATED BY DUKA ENTERPRISES PORTAL</p>
        </footer>
      </div>
    `;

    // Generate PDF
    const pdfPath = path.join(pdfDirectory, `purchase_${purchase[0].id}.pdf`);
    pdf.create(html, { format: 'A4' }).toFile(pdfPath, (err, result) => {
      if (err) {
        return res.status(500).json({ message: 'Failed to generate PDF', error: err.message });
      }

      // Return PDF URL
      const pdfUrl = `/UPLOADS/INVOICE/${path.basename(pdfPath)}`;
      res.json({ message: 'Purchase created successfully', pdfUrl });
    });

  } catch (err) {
    res.status(500).json({ message: 'Failed to fetch purchase', error: err.message });
  }

});


// Financial Year Module

// Start Cycle for Financial Year

router.post('/fy/create/fy', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    conn = await getConnection();
    const userId = res.locals.id;

    // ================= TIME =================
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');
    const year = moment().tz('Africa/Nairobi').format('YYYY');
    const expireAt = moment.tz(`${year}-12-31 23:59:59`, 'Africa/Nairobi')
      .format('YYYY-MM-DD HH:mm:ss');

    // ================= FETCH STORE =================
    const [stores] = await conn.query(
      `SELECT store_id FROM user_stores WHERE user_id = ? LIMIT 1`,
      [userId]
    );

    if (stores.length === 0) {
      return res.status(400).json({
        message: 'No store assigned to this user'
      });
    }

    const storeId = stores[0].store_id;

    // ================= CHECK DUPLICATE FY =================
    const [[existing]] = await conn.query(
      `SELECT id FROM fy_cycle WHERE name = ? AND store_id = ? LIMIT 1`,
      [year, storeId]
    );

    if (existing) {
      return res.status(409).json({
        message: `FY ${year} already exists`
      });
    }

    // ================= INSERT FY =================
    await conn.query(
      `INSERT INTO fy_cycle 
        (store_id, name, isActive, startedAt, closedAt, expireAt)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [
        storeId,
        year,
        1,
        now,
        'null', 
        expireAt
      ]
    );

    return res.status(201).json({
      message: `FY ${year} created successfully`
    });

  } catch (err) {
    console.error(' Error creating FY:', err);
    return res.status(500).json({
      message: 'Failed to create fiscal year',
      error: err.message
    });
  }
});



// Suspend Cycle for Financial Year

router.post('/fy/suspend/fy', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    conn = await getConnection();

    const userId = res.locals.id;
    const now = moment().tz('Africa/Nairobi');
    const yearName = now.format('YYYY');
    const closedAt = now.format('YYYY-MM-DD HH:mm:ss');

    // ================= FETCH STORE =================
    const [stores] = await conn.query(
      `SELECT store_id FROM user_stores WHERE user_id = ? LIMIT 1`,
      [userId]
    );

    if (stores.length === 0) {
      return res.status(400).json({
        message: 'No store assigned to this user'
      });
    }

    const storeId = stores[0].store_id;

    // ================= SUSPEND ACTIVE FY =================
    const [result] = await conn.query(
      `UPDATE fy_cycle
       SET isActive = 0,
           closedAt = ?
       WHERE store_id = ?
         AND isActive = 1`,
      [closedAt, storeId]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({
        message: 'No active fiscal year found for this store'
      });
    }

    return res.status(200).json({
      message: `Fiscal year ${yearName} suspended successfully`
    });

  } catch (err) {
    console.error(' FY Suspend Error:', err);
    return res.status(500).json({
      message: 'Failed to suspend fiscal year',
      error: err.message
    });
  } 
});


// Reopen Suspend Cycle for Financial Year

router.post('/fy/reopensuspend/fy', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    conn = await getConnection();

    const userId = res.locals.id;
    const now = moment().tz('Africa/Nairobi');
    const yearName = now.format('YYYY');
    const reopenedAt = now.format('YYYY-MM-DD HH:mm:ss');

    // ================= FETCH STORE =================
    const [stores] = await conn.query(
      `SELECT store_id FROM user_stores WHERE user_id = ? LIMIT 1`,
      [userId]
    );

    if (stores.length === 0) {
      return res.status(400).json({
        message: 'No store assigned to this user'
      });
    }

    const storeId = stores[0].store_id;

    // ================= ENSURE NO ACTIVE FY =================
    const [[activeFY]] = await conn.query(
      `SELECT id FROM fy_cycle 
       WHERE store_id = ? AND isActive = 1
       LIMIT 1`,
      [storeId]
    );

    if (activeFY) {
      return res.status(409).json({
        message: 'Another fiscal year is already active. Suspend it first.'
      });
    }

    // ================= REOPEN SUSPENDED FY =================
    const [result] = await conn.query(
      `UPDATE fy_cycle
       SET isActive = 1,
           closedAt = NULL
       WHERE store_id = ?
         AND isActive = 0
         AND name = ?`,
      [storeId, yearName]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({
        message: 'No suspended fiscal year found to reopen'
      });
    }

    return res.status(200).json({
      message: `Fiscal year ${yearName} reopened successfully`
    });

  } catch (err) {
    console.error(' FY Reopen Error:', err);
    return res.status(500).json({
      message: 'Failed to reopen fiscal year',
      error: err.message
    });
  } 
});


// Get Active Financial Year ================

router.get('/fy/active/fy', auth.authenticateToken, async (req, res) => {
  const conn = await getConnection();
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const now = moment().tz('Africa/Nairobi');
  const yearName = now.format('YYYY');

  let activeSession = {
    isActive: false,
    startedAt: null
  };

  try {
    let storeIds = [];

    // If not admin (role ID 1), get assigned store IDs
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await conn.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      storeIds = storeRows.map(row => row.store_id);
    }

    // Base query
    let query = `SELECT * FROM fy_cycle WHERE isActive = 1 AND name = ?`;
    const params = [yearName];

    // Restrict to user-assigned stores if not admin
    if (storeIds.length > 0) {
      const placeholders = storeIds.map(() => '?').join(',');
      query += ` AND store_id IN (${placeholders})`;
      params.push(...storeIds);
    }

    query += ` LIMIT 1`;

    const [result] = await conn.query(query, params);

    if (result.length > 0) {
      activeSession = {
        isActive: true,
        startedAt: result[0].startedAt
      };
    }

    res.json(activeSession);

  } catch (err) {
    console.error('[FY Active Fetch Error]', err);
    res.status(500).json({ message: 'Database error', error: err.message });
  }
});



// GET ALL FINANCIAL YEAR LISTS AND THEIR CORRESPONDENCE SALES
// PURCHASES, SALES RETURN AND PURCHASES RETURN ===============================

router.get('/fy/list/fy', auth.authenticateToken, async (req, res) => {
  const conn = await getConnection();
  const userId = res.locals.id;
  const roleId = res.locals.role;

  try {
    let storeIds = [];

    // Admin can view all stores
    if (roleId === 1 || roleId === '1') {
      const [stores] = await conn.query('SELECT id FROM stores');
      storeIds = stores.map(row => row.id);
    } else {
      const [userStores] = await conn.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?',
        [userId]
      );
      storeIds = userStores.map(row => row.store_id);
    }

    if (!storeIds.length) return res.status(200).json([]);

    const placeholders = storeIds.map(() => '?').join(',');

    const [result] = await conn.query(
      `
      SELECT 
      
        st.name AS storename,
      
        -- Sales Summary
        (SELECT SUM(s.total) FROM sales s WHERE s.fy_id = fy.id) AS total_sales,
        (SELECT SUM(s.order_discount) FROM sales s WHERE s.fy_id = fy.id) AS sales_order_discount,
        (SELECT SUM(s.order_tax) FROM sales s WHERE s.fy_id = fy.id) AS sales_order_tax,
        (SELECT SUM(s.grand_total) FROM sales s WHERE s.fy_id = fy.id) AS sales_grand_total,

        -- Sales Items Summary
        (SELECT SUM(si.quantity) FROM sales s 
         JOIN sale_items si ON si.sale_id = s.id 
         WHERE s.fy_id = fy.id) AS total_sales_items_qty,
        (SELECT SUM(si.price * si.quantity) FROM sales s 
         JOIN sale_items si ON si.sale_id = s.id 
         WHERE s.fy_id = fy.id) AS total_sales_items_amount,

        -- Purchases Summary
        (SELECT SUM(p.total) FROM purchases p WHERE p.fy_id = fy.id) AS total_purchases,
        (SELECT SUM(p.order_discount) FROM purchases p WHERE p.fy_id = fy.id) AS purchases_order_discount,
        (SELECT SUM(p.order_tax) FROM purchases p WHERE p.fy_id = fy.id) AS purchases_order_tax,
        (SELECT SUM(p.shipping) FROM purchases p WHERE p.fy_id = fy.id) AS purchases_shipping,
        (SELECT SUM(p.grand_total) FROM purchases p WHERE p.fy_id = fy.id) AS purchases_grand_total,

        -- Purchase Items Summary
        (SELECT SUM(pi.quantity) FROM purchases p 
         JOIN purchase_items pi ON pi.purchase_id = p.id 
         WHERE p.fy_id = fy.id) AS total_purchase_items_qty,
        (SELECT SUM(pi.cost * pi.quantity) FROM purchases p 
         JOIN purchase_items pi ON pi.purchase_id = p.id 
         WHERE p.fy_id = fy.id) AS total_purchase_items_amount,

        -- Adjusted Purchase Value
        (
          SELECT 
            SUM(
              (SELECT SUM(pi.cost * pi.quantity) FROM purchase_items pi WHERE pi.purchase_id = p.id) 
              - p.order_discount + p.order_tax + p.shipping
            )
          FROM purchases p 
          WHERE p.fy_id = fy.id
        ) AS total_purchase_value,

        -- Fiscal Year Info
        fy.name, fy.startedAt, fy.isActive, fy.expireAt, fy.closedAt

      FROM fy_cycle fy
      JOIN stores st ON st.id = fy.store_id
      WHERE fy.store_id IN (${placeholders})
      ORDER BY fy.startedAt DESC
    `,
      storeIds
    );

    res.status(200).json(result);

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Database error ', error: err.message });
  }
});





// ADD FORM ================================

// Middleware for uploading an image with data
var storage = multer.diskStorage({
    destination: function (req, file, cb) {
        return cb(null, './UPLOADS/PDF');
    },

    filename: function (req, file, cb) {
        const id = Math.floor(100000 + Math.random() * 900000);
        return cb(null, `${id}_${file.originalname}`) //Appending extension
    }
})

var add_form = multer({ storage: storage });

router.post('/addForm', add_form.single('file'), auth.authenticateToken, async (req, res) => {
  let conn;
  try {

    let f = req.body;
    const img = [req.file.filename];
    const pdfPath = `/PDF/${req.file.filename}`;
    const uuid_id = uuid.v1();

  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');


    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Optional: Simulate delay
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.json({ message: 'Access denied. Super admin only.' });
    }

    // Check for existing menu
    const [existing] = await conn.query(
      `SELECT * FROM forms WHERE titleName = ? `,
      [f.titleName]
    );

    if (existing.length > 0) {
      return res.json({
        message: `Name "${f.titleName}" already exists.`
      });
    }

    // Insert new 
    await conn.query(
      `INSERT INTO forms (uuid_id, titleName, docFile, pdfPath)
       VALUES (?, ?, ?, ?)`,
      [uuid_id, f.titleName, img, pdfPath]
    );

    return res.status(201).json({
      message: `Created successfully!`
    });

  } catch (err) {
    res.status(500).json({ message: 'Internal server error' });
  } 
});




// Get Form Lists
router.get('/get/form/list', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Use async/await with MySQL query
    const [results] = await conn.query('SELECT * FROM forms ORDER BY titleName ASC');

    // Return the list 
    res.json(results);

  } catch (err) {
    return res.json({ message: 'Internal server error' });
  } 
});


// Get pdf file =============================

router.get('/get/pdf/file/:uuid_id', async (req, res) => {
  const uuid_id = req.params.uuid_id;
  const query = `
    SELECT * 
    FROM 
    forms
    WHERE uuid_id = ?
  `;

  let conn;
  try {
    conn = await getConnection(); 
    const [result] = await conn.query(query, [uuid_id]);

    return res.status(200).json(result);
  } catch (err) {
    console.error('Error fetching PDF:', err);
    return res.status(500).json({ message: 'Internal server error', error: err });
  } finally {
    if (conn) conn.release(); // release connection back to pool
  }
});


// Get Form Lists to web

router.get('/get/documents/lists', async (req, res) => {
  let conn;
  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Use async/await with MySQL query
    const [results] = await conn.query('SELECT * FROM forms ORDER BY titleName ASC');

    // Return the list 
    res.json(results);

  } catch (err) {
    return res.json({ message: 'Internal server error' });
  } 
});


// Update Form
router.put('/updateForm/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

    const { titleName } = req.body;
    const id = req.params.id;

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.json({ message: 'Access denied. Super admin only.' });
    }

    // Update 
    const [result] = await conn.query(
      `UPDATE forms SET titleName = ? WHERE id = ?`,
      [titleName, id]
    );

    if (result.affectedRows === 0) {
      return res.json({ message: "Not found" });
    }

    return res.status(200).json({
      message: `"${titleName}" updated successfully!`
    });

  } catch (err) {
    res.status(500).json({ message: 'Internal server error' });
  } 
});



// Delete Form ======================

router.delete('/deleteForm/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const roleId = res.locals.role;
    const isSuperAdmin = (roleId === 1 || roleId === '1');
    const id = req.params.id;

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ message: 'Access denied. Super admin only.' });
    }

    conn = await getConnection();

    // Get form details
    const [formRow] = await conn.query(
      "SELECT * FROM forms WHERE id = ?",
      [id]
    );

    if (formRow.length === 0) {
      return res.status(404).json({ message: `Form with id ${id} not found` });
    }

    const pdfPath = formRow[0].pdfPath;
    if (pdfPath) {
        
      // Ensure the path matches your uploads folder
      
      const filePath = path.join(__dirname, '..', 'UPLOADS', 'PDF', path.basename(pdfPath));

      try {
        if (fs.existsSync(filePath)) {
          fs.unlinkSync(filePath);
        } else {
          console.warn(`PDF file not found: ${filePath}`);
        }
      } catch (err) {
        console.error('Error deleting PDF file:', err);
        return res.status(500).json({ message: 'Failed to delete PDF file', error: err.message });
      }
    }

    // Delete from forms table
    const [result] = await conn.query(
      "DELETE FROM forms WHERE id = ?",
      [id]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ message: `Form with id ${id} not found` });
    }

    return res.status(200).json({ message: 'Form deleted successfully' });

  } catch (err) {
    console.error(err);
    return res.status(500).json({ message: 'Internal server error', error: err.message });
  } 
});

// Update Form File ==================


router.post('/updateFormFile', add_form.single('file'), auth.authenticateToken, async (req, res) => {
    let conn;
    try {
        const f = req.body;
        const img = req.file.filename; 
        const pdfPaths = `/PDF/${req.file.filename}`;

        const roleId = res.locals.role;
        const isSuperAdmin = (roleId === 1 || roleId === '1');

        // Only allow super admins
        if (!isSuperAdmin) {
            return res.json({ message: 'Access denied. Super admin only.' });
        }

        conn = await getConnection();

        // Optional: Simulate delay
        await new Promise(resolve => setTimeout(resolve, 3000));

        // Get form details
        const [formRow] = await conn.query(
            "SELECT * FROM forms WHERE id = ?",
            [f.id]
        );

        if (formRow.length === 0) {
            return res.json({ message: `Form with id ${f.id} not found` });
        }

        const oldPdfPath = formRow[0].pdfPath;

        if (oldPdfPath) {
            const filePath = path.join(__dirname, '..', 'UPLOADS', 'PDF', path.basename(oldPdfPath));

            try {
                await fs.unlink(filePath);
            } catch (err) {
                console.warn(`Could not delete old file: ${filePath}`);
            }
        }

        // Update forms table
        const [result] = await conn.query(
            "UPDATE forms SET pdfPath = ?, docFile = ? WHERE id = ?",
            [pdfPaths, img, f.id]
        );

        if (result.affectedRows === 0) {
            return res.json({ message: `Form with id ${f.id} not found` });
        }

        return res.status(201).json({
            message: `Updated successfully!`
        });

    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Internal server error', details: err.message });
    }
});


// Get System User Manual ===========

router.get('/get/system/user/manual', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Use async/await with MySQL query
    const [results] = await conn.query(`SELECT * FROM forms WHERE titleName = 'MANUAL' `);

    // Return the list 
    res.json(results);

  } catch (err) {
    return res.json({ message: 'Internal server error' });
  } 
});


// GET OWNERSHIP PLAN. USER CAN ABLE TO SELECT OWNERSHIP PLAN ON STORE
// STORE REGISTRATION ==================================

router.get('/get/ownership/plans/two', auth.authenticateToken, async (req, res) => {
  const conn = await getConnection();
    try {
      // Backend Response
      const [result] = await conn.query(`
        SELECT * FROM ownership_plans `);
      return res.status(200).json(result);
    } catch (err) {
      res.status(500).json({ message: 'Database error ' });
    }
  });


// GET OWNERSHIP PLAN. USER CAN ABLE TO SELECT OWNERSHIP PLAN ON STORE
// STORE REGISTRATION ==================================

router.get('/get/ownership/plans', async (req, res) => {
  const conn = await getConnection();
    try {
      // Backend Response
      const [result] = await conn.query(`
        SELECT * FROM ownership_plans `);
      return res.status(200).json(result);
    } catch (err) {
      res.status(500).json({ message: 'Database error ' });
    }
  });

// GET BOTH TANZANIA MAINLAND AND ISLAND REGIONS ==================

router.get('/get/tanzania/region', async (req, res) => {
const conn = await getConnection();
  try {
    // Backend Response
    const [result] = await conn.query(`
      SELECT * FROM regions `);
    return res.status(200).json(result);
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Database error ' });
  }
});

// GET DISTRICT LISTS BASED ON THE REGION SELECTIONS
router.get('/get/district/region/:id', async (req, res) => {
  
  const id = req.params.id;

  const conn = await getConnection();
  try {
    // Backend Response
    const [result] = await conn.query(`
      SELECT * FROM districts WHERE region_id = ? `, [id]);
    return res.status(200).json(result);
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Database error ' });
  }

});


// ACTIVATE ACCOUNT =============================

router.get('/activate-account/:token', async (req, res) => {
  const { token } = req.params;
  
  let conn;
  try {
    const conn = await getConnection();

    const [user] = await conn.query(
      `SELECT * FROM users WHERE activation_token = ?`,
      [token]
    );

    if (user.length === 0) {
        return res.json({ message: 'Invalid or expired activation token' });
    
    }

    await conn.query(
      `UPDATE users SET userStatus = 'true', activation_token = 'null' WHERE id = ?`,
      [user[0].id]
    );

      return res.status(200).json({ message: 'Account activated successfully. You can now log in.' });
    
  } catch (err) {
    console.error(err);
    return res.status(500).json({ message: 'Internal server error' });
  }
});


// USER SIGNUP AND CREATE STORE =================================

router.post('/signup/create/self/account', async (req, res) => {
  const conn = await getConnection();
  
  const {
    phoneNumber,
    password
  } = req.body;

  try {
    const hashedPassword = await bcrypt.hash(password.toString(), 12);
    const uuidtoken = uuid.v1();

    // Check if phone already exists
    const [existing] = await conn.query(`SELECT * FROM users WHERE  phone = ?`, [phoneNumber]);
    if (existing.length > 0) {
      return res.json({ message: `"${phoneNumber}" already exists .` });
    }

    // Check Manager Role
    const [role] = await conn.query(`SELECT * FROM roles WHERE name = 'MANAGER' `);
    
    // Insert User
    const currentDate = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

const [userResult] = await conn.query(
  `INSERT INTO users (
    profileCompleted,is_2fa_enabled, last_password_change, activation_token, name, email, phone, password,
    createDate, updateDate, createBy, updateBy,
    userStatus, loggedIn, lastActive, accountDisabled, loginAttempts, attemptStatus,
    expiresAt, accountExpDate, accountExpireStatus,
    userDeletable, digitalSignature, isFirstLogin, mustChangePassword,
    role, accessAllStores, accessAllWarehouses
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,

  [
    0,
    0,
    currentDate,
    uuidtoken,
    'null',
    'null',
    phoneNumber,
    hashedPassword,
    currentDate,
    currentDate,
    'null',
    'null',
    'true',
    'null',
    'null',
    'false',
    0,
    0,
    'null',
    'null',
    'false',
    'false',
    'false',
    'false',
    'false',
    role[0].id,
    0,
    0
  ]
);

    const userId = userResult.insertId;

    await conn.query(
  `INSERT INTO password_history (user_id, password_hash, changed_at) VALUES (?, ?, ?)`,
  [userId, hashedPassword, currentDate]
);

    return res.json({
      message: `Registered successfully.`
    });

  } catch (err) {
    console.error('Insert failed:', err);
    return res.status(500).json({ error: 'Internal server error ' });
  }
});


// Marquee Notifications ==================

router.get('/general/notifications/expiring', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;

  try {
    const connection = await getConnection();

    const [userRows] = await connection.query(
      `SELECT accountExpDate, last_password_change FROM users WHERE id = ?`,
      [userId]
    );

    if (!userRows.length) return res.json({ message: 'User not found' });

    const user = userRows[0];
    const messages = [];

    // Current time in Nairobi timezone as formatted string
    const nowStr = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');
    const now = moment.tz(nowStr, 'Africa/Nairobi');

    // Password expiry notification
    if (user.last_password_change) {
      const lastChange = moment.tz(user.last_password_change, 'Africa/Nairobi');
      const passExpiry = lastChange.clone().add(90, 'days');
      const passDaysLeft = passExpiry.diff(now, 'days');

      if (passDaysLeft <= 10) {
        messages.push(`🔐 Your password will expire in ${passDaysLeft} day(s). Last changed on ${lastChange.format('YYYY-MM-DD')}`);
      }
    }

    // Subscription expiry notification
    if (user.accountExpDate) {
      const expDate = moment.tz(user.accountExpDate, 'Africa/Nairobi');
      const subDaysLeft = expDate.diff(now, 'days');

      if (subDaysLeft <= 5) {
        messages.push(`📅 Your subscription will expire in ${subDaysLeft} day(s).`);
      }
    }

    return res.json({ messages });

  } catch (err) {
    console.error('Notification fetch error:', err);
    res.status(500).json({ error: 'Server error' });
  }
});



// Create Stores

router.post('/stores/create/store', auth.authenticateToken, async (req, res) => {
  const conn = await getConnection();
  const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

  const {
    two_factor_auth,
    enable_password_change,
    auto_check_subscriber,
    auto_daily_report,
    auto_monthly_report,
    name,
    phone,
    email,
    region,
    district,
    ownership,
    duration_type, // 'Month' or 'Year'
    duration_value, // number of months or years
    amount,
  } = req.body;

  try {
    await sleep(3000);

    // Check if store with same name, phone, or email exists
    const [existing] = await conn.query(
      `SELECT * FROM stores WHERE name = ? OR phone = ? OR email = ?`,
      [name, phone, email]
    );

    if (existing.length > 0) {
      return res.status(401).json({ message: `Store with name "${name}", phone "${phone}" or email "${email}" already exists ` });
    }

    // Current time in Africa/Nairobi timezone
    const now = moment().tz('Africa/Nairobi');
    const today = now.format('YYYY-MM-DD HH:mm:ss');

    let endDate = 'null';
    let formattedEndDate = 'null';
    let status = 'true';

    if (ownership === 'Fully') {
      // Insert fully owned store (no end date)
      const [insertResult] = await conn.query(
        `INSERT INTO stores (
          two_factor_auth,
          enable_password_change,
          auto_check_subscriber,
          auto_daily_report,
          auto_monthly_report,
          name,
          phone,
          email,
          region_id,
          district_id,
          ownership,
          duration_type,
          duration_value,
          amount,
          start_date,
          end_date,
          status,
          last_reminder_sent,
          created_at,
          updated_at,
          created_by,
          updated_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          two_factor_auth || 0,
          enable_password_change || 1,
          auto_check_subscriber || 1,
          auto_daily_report || 0,
          auto_monthly_report || 0,
          name,
          phone,
          email,
          region,
          district,
          'Fully',
          0,
          0,
          amount,
          today,
          'null',
          status,
          0,
          today,
          today,
          res.locals.name,
          0,
        ]
      );

      const storeId = insertResult.insertId;

      // Record payment for fully owned store
      await conn.query(
        `INSERT INTO store_payments (
          store_id,
          payment_amount,
          duration_type,
          duration_value,
          paid_at,
          paid_by,
          pay_status,
          start_date,
          paid,
          pay_reference_no
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          storeId,
          amount,
          'Fully',
          0,
          today,
          res.locals.name,
          'true',
          today,
          amount,
          Math.floor(100000 + Math.random() * 900000),
        ]
      );

      return res.status(200).json({ message: 'Fully owned store registered.' });
    }

    if (ownership === 'Partially') {
      // Validate duration_value
      if (!Number.isInteger(duration_value) || duration_value < 1) {
        return res.status(400).json({ error: `Number of ${duration_type.toLowerCase()}s must be a positive integer ` });
      }

      // Calculate endDate based on duration_type and duration_value
      if (duration_type === 'Month') {
        endDate = now.clone().add(duration_value, 'months');
      } else if (duration_type === 'Year') {
        endDate = now.clone().add(duration_value, 'years');
      } else {
        return res.status(400).json({ error: 'Invalid duration_type for partially owned store ' });
      }

      formattedEndDate = endDate.format('YYYY-MM-DD HH:mm:ss');
      
      // Insert partially owned store
      const [insertResult] = await conn.query(
        `INSERT INTO stores (
          two_factor_auth,
          enable_password_change,
          auto_check_subscriber,
          auto_daily_report,
          auto_monthly_report,
          name,
          phone,
          email,
          region_id,
          district_id,
          ownership,
          duration_type,
          duration_value,
          amount,
          start_date,
          end_date,
          status,
          last_reminder_sent,
          created_at,
          updated_at,
          created_by,
          updated_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          two_factor_auth || 0,
          enable_password_change || 1,
          auto_check_subscriber || 1,
          auto_daily_report || 0,
          auto_monthly_report || 0,
          name,
          phone,
          email,
          region,
          district,
          'Partially',
          duration_type,
          duration_value,
          amount,
          today,
          formattedEndDate,
          'false',
          0,
          today,
          today,
          res.locals.name,
          0,
        ]
      );

      const storeId = insertResult.insertId;

      // Record payment for partially owned store
      await conn.query(
        `INSERT INTO store_payments (
          store_id,
          payment_amount,
          duration_type,
          duration_value,
          paid_at,
          paid_by,
          pay_status,
          start_date,
          paid,
          pay_reference_no
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          storeId,
          amount,
          duration_type,
          duration_value,
          'null',
          'null',
          'false',
          today,
          0,
          'null',
        ]
      );

      return res.status(201).json({ message: `Partially owned store registered for ${duration_value} ${duration_type.toLowerCase()}(s). ` });
    }

    return res.status(400).json({ error: 'Invalid ownership type ' });
  } catch (err) {
    console.error('Insert failed:', err);
    res.status(500).json({ error: 'Internal server error ' });
  }
});


// Get Store Lists
router.get('/get/stores/lists', auth.authenticateToken, async (req, res) => {
  const conn = await getConnection();
  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

  try {

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ message: 'Access denied. Super admin only.' });
    }

    const [result] = await conn.query(`
      SELECT 
        s.id,
        s.name,
        s.phone,
        s.email,
        s.ownership,
        s.duration_type,
        s.duration_value,
        s.amount,
        s.start_date,
        s.end_date,
        s.status,
        s.last_reminder_sent,
        s.created_at,
        s.updated_at,
        s.created_by,
        s.updated_by,
        s.two_factor_auth,
        s.enable_password_change,
        s.auto_check_subscriber,
        s.auto_daily_report,
        s.auto_monthly_report,
        r.name AS region_name,
        d.name AS district_name
      FROM stores s
      LEFT JOIN regions r ON s.region_id = r.id
      LEFT JOIN districts d ON s.district_id = d.id
      ORDER BY s.created_at DESC
    `);

    res.status(200).json(result);
  } catch (err) {
    console.error('Failed to fetch stores with location info:', err);
    res.status(500).json({ message: 'Internal server error' });
  }
});


// Get Stores By Id
router.get('/get/store/by/:id', auth.authenticateToken, async (req, res) => {
  const conn = await getConnection();
  const id = req.params.id;
  
  try {
    const [store] = await conn.query(`
      SELECT 
        s.id,
        s.name,
        s.phone,
        s.email,
        s.ownership,
        s.duration_type,
        s.duration_value,
        s.amount,
        s.start_date,
        s.end_date,
        s.status,
        s.last_reminder_sent,
        s.created_at,
        s.updated_at,
        s.created_by,
        s.updated_by,
        s.two_factor_auth,
        s.enable_password_change,
        s.auto_check_subscriber,
        s.auto_daily_report,
        s.auto_monthly_report,
        r.name AS region_name,
        d.name AS district_name
      FROM stores s
      LEFT JOIN regions r ON s.region_id = r.id
      LEFT JOIN districts d ON s.district_id = d.id
      WHERE s.id = ?
    `,[id]);

    res.status(200).json(store[0]);
  } catch (err) {
    console.error('Failed to fetch stores with location info', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});


// Forward Mail Data

router.put('/foward/mail/data/:id', auth.authenticateToken, async (req, res) => {
  const { id } = req.params;
  const {
    email,
    message
  } = req.body;

const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');
  const conn = await getConnection();
  const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
  
  try {
    await sleep(3000); // Simulated delay


    // Get role name
  const [[{ name: roleName } = {}]] = await conn.query(
    'SELECT name FROM roles WHERE id = (SELECT role FROM users WHERE id = ?)',
    [res.locals.id]
  );

  if (!roleName) {
    return res.status(403).json({ message: 'Invalid role access.' });
  }

  const isAdmin = roleName === 'ADMIN';
  const isManager = roleName === 'MANAGER';

  if (!isAdmin && !isManager) {
  return res.status(403).json({ message: 'Access denied. Only MANAGER or ADMIN allowed.' });
  }

      // Get times
    const [countRows] = await conn.query(
      `SELECT times FROM mails WHERE id = ?`,
      [id]
    );
    const times = countRows[0].times + 1;

    const [emailConfig] = await conn.execute(
      'SELECT * FROM system_mail_configuration LIMIT 1'
    );

    if (emailConfig.length === 0) {
      return res.status(500).json({ message: 'Email configuration not found.' });
    }

    const { host, port, username, password } = emailConfig[0];

    const transporter = nodemailer.createTransport({
      host: host,
      port: parseInt(port),
      secure: parseInt(port) === 465,
      auth: {
        user: username,
        pass: password
      }
    });

    try {
      await transporter.sendMail({
        from: username,
        to: email,
        subject: 'Mail Foward',
        text: message
      });

      // INSERT MAILS
      await conn.execute(
        `INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, ?)`,
        [email || '', message, now, 'true']
      );

      // UPDATE TIMES ================
      await conn.execute(
        `UPDATE mails SET times = ? WHERE id = ?`,
        [times, id]
      );

    } catch (mailError) {
      
      // INSERT MAILS
      await conn.execute(
        `INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, ?)`,
        [email || '', message, now, 'false']
      );
     }
    
    return res.status(200).json({ message: 'Mail Forward successfully' });
  

  } catch (err) {
    console.error('Mail Forward failed:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});


// Forward SMS Data

router.put('/foward/sms/data/:id', auth.authenticateToken, async (req, res) => {
  const { id } = req.params;
  const {
    newPhone,
    message
  } = req.body;

  const conn = await getConnection();
  const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
  
  try {
    await sleep(3000); // Simulated delay

    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    // Get SMS by Id
    const[smsInfo] = await conn.query(
      `SELECT * FROM sms WHERE id = ?`,
      [id] );

      const { store_id } = smsInfo[0];

      // Get times
    const [countRows] = await conn.query(
      `SELECT times FROM sms WHERE id = ?`,
      [id]
    );
    const times = countRows[0].times + 1;


      const [smsConfig] = await conn.execute(`SELECT * FROM sms_configuration WHERE store_id = ? LIMIT 1`, [store_id]);
    const { api_url, sender_name, username, password } = smsConfig[0];

    const encodedAuth = Buffer.from(`${username}:${password}`).toString('base64');
    const smsText = `${message}`;
          
          const payload = {
            from: sender_name,
            text: smsText,
            to: newPhone
          };
    
          try {
            await axios.post(api_url, payload, {
              headers: {
                'Authorization': `Basic ${encodedAuth}`,
                'Content-Type': 'application/json',
                'Accept': 'application/json'
              },
              timeout: 10000
            });
    
            // UPDATE TIMES ================
            await conn.execute(
              `UPDATE sms SET times = ? WHERE id = ?`,
              [times, id]
            );

            // RE INSERT NEW SMS =====================
            await conn.execute(
              `INSERT INTO sms (store_id, phone, message, date, status) VALUES (?, ?, ?, ?, ?)`,
              [store_id, newPhone, smsText, now, 'true']
            );

          } catch (err) {
            console.error(' Resend SMS failed:', err.message);

            // RE INSERT ALREADY EXIST SMS
            await conn.execute(
              `INSERT INTO sms (store_id, phone, message, date, status) VALUES (?, ?, ?, ?, ?)`,
              [store_id, newPhone, smsText, now, 'false']
            );
          }

      return res.status(200).json({ message: 'SMS Forward successfully' });
  

  } catch (err) {
    console.error('SMS Forward failed:', err);
    res.status(500).json({ error: 'Internal server error ' });
  }
});


// Resend Failed Mail Data

router.put('/resend/failed/mail/data/:id', auth.authenticateToken, async (req, res) => {
  const { id } = req.params;
  const { email, message } = req.body;
  const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

  const conn = await getConnection();
  const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
  
  try {
    await sleep(3000); // Simulated delay

    // Get user role
    const [[roleRow] = [{}]] = await conn.query(
      'SELECT name FROM roles WHERE id = (SELECT role FROM users WHERE id = ?)',
      [res.locals.id]
    );
    const roleName = roleRow?.name;

    if (!roleName || !['ADMIN', 'MANAGER'].includes(roleName)) {
      return res.status(403).json({ message: 'Access denied. Only MANAGER or ADMIN allowed.' });
    }

    // Get resend count
    const [countRows] = await conn.query('SELECT times FROM mails WHERE id = ?', [id]);
    const times = (countRows[0]?.times ?? 0) + 1;

    // Get email config
    const [emailConfig] = await conn.execute(
      'SELECT * FROM system_mail_configuration LIMIT 1'
    );
    if (!emailConfig.length) {
      return res.status(500).json({ error: 'Email configuration not found.' });
    }

    const { host, port, username, password } = emailConfig[0];

    const transporter = nodemailer.createTransport({
      host: host,
      port: parseInt(port),
      secure: parseInt(port) === 465,
      auth: { user: username, pass: password }
    });

    try {
      await transporter.sendMail({
        from: username,
        to: email || '', // fallback to avoid undefined
        subject: 'Mail Resend',
        text: message || ''
      });

      await conn.execute(
        `UPDATE mails SET times = ?, status = 'true' WHERE id = ?`,
        [times, id]
      );

    } catch (mailError) {
      console.error('Email send error:', mailError);

      await conn.execute(
        `UPDATE mails SET times = ?, status = 'false' WHERE id = ?`,
        [times, id]
      );
    }

    return res.status(200).json({ message: 'Resend completed.' });

  } catch (err) {
    console.error('Resend failed:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});



// Resend Failed SMS Data

router.put('/resend/failed/sms/data/:id', auth.authenticateToken, async (req, res) => {
  const { id } = req.params;
  const {
    phone,
    message
  } = req.body;

  const conn = await getConnection();
  const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
  
  try {
    await sleep(3000); // Simulated delay

    // Get SMS by Id
    const[smsInfo] = await conn.query(
      `SELECT * FROM sms WHERE id = ?`,
      [id] 
    );

      const { store_id } = smsInfo[0];

      // Get times
    const [countRows] = await conn.query(
      `SELECT times FROM sms WHERE id = ?`,
      [id]
    );
    const times = countRows[0].times + 1;


      const [smsConfig] = await conn.execute(`SELECT * FROM sms_configuration WHERE store_id = ? LIMIT 1`, [store_id]);
    const { api_url, sender_name, username, password } = smsConfig[0];

    const encodedAuth = Buffer.from(`${username}:${password}`).toString('base64');
    const smsText = `${message}`;
          
          const payload = {
            from: sender_name,
            text: smsText,
            to: phone
          };
    
          try {
            await axios.post(api_url, payload, {
              headers: {
                'Authorization': `Basic ${encodedAuth}`,
                'Content-Type': 'application/json',
                'Accept': 'application/json'
              },
              timeout: 10000
            });
    
            await conn.execute(
              `UPDATE sms SET status = 'true', times = ? WHERE id = ?`,
              [times, id]
            );
          } catch (err) {
            console.error('Send SMS failed:', err.message);
            await conn.execute(
              `UPDATE sms SET status = 'false', times = ? WHERE id = ?`,
              [times, id]
            );
          }

      return res.status(200).json({ message: 'Resend successfully' });
  

  } catch (err) {
    console.error(' Resend failed:', err);
    res.status(500).json({ error: 'Internal server error ' });
  }
});


// Update Products Data
router.put('/update/products/data/:id', auth.authenticateToken, async (req, res) => {
  const { id } = req.params;
  approverId = res.locals.id;
  const {
    name,
    imei_serial,
    expire_date,
    product_qty_alert
  } = req.body;

  const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

  const conn = await getConnection();
  const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
  
  try {
    await sleep(3000); // Simulated delay

    // Get role name
    const [[{ name: roleName } = {}]] = await conn.query(
      'SELECT name FROM roles WHERE id = (SELECT role FROM users WHERE id = ?)',
      [approverId]
    );

    if (!roleName) {
      return res.status(403).json({ message: 'Invalid role access.' });
    }

    const isAdmin = roleName === 'ADMIN';
    const isManager = roleName === 'MANAGER';

    if (!isAdmin && !isManager) {
    return res.status(403).json({ message: 'Access denied. Only MANAGER or ADMIN allowed.' });
    }

      // Update bulk
      await conn.query(
        `UPDATE products 
         SET name = ?, imei_serial = ?, expire_date = ?, product_qty_alert = ?
         WHERE id = ?`,
        [
          name, imei_serial, expire_date, product_qty_alert, id
        ]
      );

      return res.status(200).json({ message: 'Updated successfully' });
  

  } catch (err) {
    console.error(' Update failed:', err);
    res.status(500).json({ error: 'Internal server error ' });
  }
});


// Update Products Price
router.put('/update/products/price/:id', auth.authenticateToken, async (req, res) => {
  const { id } = req.params;
  const approverId = res.locals.id;
  const {
    price, vat, discount
  } = req.body;

  const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

  const newprice = price;

  const conn = await getConnection();
  const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
  
  try {
    await sleep(3000); // Simulated delay

    // Get role name
    const [[{ name: roleName } = {}]] = await conn.query(
      'SELECT name FROM roles WHERE id = (SELECT role FROM users WHERE id = ?)',
      [approverId]
    );

    if (!roleName) {
      return res.status(403).json({ message: 'Invalid role access.' });
    }

    const isAdmin = roleName === 'ADMIN';
    const isManager = roleName === 'MANAGER';

    if (!isAdmin && !isManager) {
    return res.status(403).json({ message: 'Access denied. Only MANAGER or ADMIN allowed.' });
    }

    // Get First Product Details Data

    const [productInfo] = await conn.query(
      `SELECT * FROM products WHERE id = ?`,
      [ id ]
    );

    const { store_id, warehouse_id, name, price } = productInfo[0];

      // Update bulk
      await conn.query(
        `UPDATE products 
         SET price = ?, vat = ?, discount = ?
         WHERE id = ?`,
        [
          newprice, vat, discount, id
        ]
      );

      // Fetch SMS template
    const [templateRows] = await conn.execute(
      `SELECT * FROM sms_templates WHERE type = 'PRODUCTPRICE' AND store_id = ? LIMIT 1`,
      [store_id]
    );
    const template = templateRows[0]?.message;
   
      // Send SMS notification

    // Get Store Owner Informations
    const [storeInfoRows] = await conn.execute(
      `SELECT s.id as storeId, w.name as warehousename, s.name as storename, s.phone as ownerPhone, s.email as email
       FROM warehouses w JOIN stores s ON s.id = w.storeId WHERE w.id = ?`,
      [warehouse_id]
    );
    const storeInfo = storeInfoRows[0];

    const [smsConfig] = await conn.execute(`SELECT * FROM sms_configuration WHERE store_id = ? LIMIT 1`, [store_id]);
    const { api_url, sender_name, username, password } = smsConfig[0];
    const encodedAuth = Buffer.from(`${username}:${password}`).toString('base64');
    
    const smsText = template
      .replace('{{store}}', storeInfo.storename)
      .replace('{{warehouse}}', storeInfo.warehousename)
      .replace('{{product}}', name)
      .replace('{{oldprice}}', price)
      .replace('{{currentprice}}', newprice)
      .replace('{{username}}', res.locals.name)
      .replace('{{date}}', now);

          const payload = {
            from: sender_name,
            text: smsText,
            to: storeInfo.ownerPhone
          };
    
          try {
            await axios.post(api_url, payload, {
              headers: {
                'Authorization': `Basic ${encodedAuth}`,
                'Content-Type': 'application/json',
                'Accept': 'application/json'
              },
              timeout: 10000
            });
    
            await conn.execute(
              `INSERT INTO sms (store_id, phone, message, date, status) VALUES (?, ?, ?, ?, ?)`,
              [store_id, storeInfo.ownerPhone, smsText, now, 'true']
            );
          } catch (err) {
            console.error(' OWNER SMS failed:', err.message);
            await conn.execute(
              `INSERT INTO sms (store_id, phone, message, status, date) VALUES (?, ?, ?, ?, ?)`,
              [store_id, storeInfo.ownerPhone, smsText, 'false', now]
            );
          }

          // SENDING EMAIL TO STORE OWNER ---------------------

          const [emailConfig] = await conn.execute(
            'SELECT * FROM mail_configuration WHERE store_id = ? LIMIT 1',
            [store_id]
          );
    
          if (emailConfig.length === 0) {
            return res.status(500).json({ error: 'Email configuration not found.' });
          }
    
          const transporter = nodemailer.createTransport({
            host: emailConfig[0].host,
            port: parseInt(emailConfig[0].port),
            secure: parseInt(emailConfig[0].port) === 465,
            auth: {
              user: emailConfig[0].username,
              pass: emailConfig[0].password
            }
          });


          try {
            await transporter.sendMail({
              from: emailConfig[0].username,
              to: storeInfo.email,
              subject: 'Badiliko la Bei ya Bidhaa',
              text: smsText
            });
    
            await conn.execute(
              `INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, ?)`,
              [storeInfo.email || '', smsText, now, 'true']
            );
    
          } catch (mailError) {
           
            await conn.execute(
              `INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, ?)`,
              [storeInfo.email || '', smsText, now, 'false']
            );
          }

      return res.status(200).json({ message: 'Updated successfully' });
  
  } catch (err) {
    console.error('Update failed:', err);
    res.status(500).json({ error: 'Internal server error ' });
  }
});


// Update Stores Data

router.put('/stores/update/:id', auth.authenticateToken, async (req, res) => {
  const { id } = req.params;
  const {
    two_factor_auth,
    enable_password_change = 1,
    auto_check_subscriber = 1,
    auto_daily_report,
    auto_monthly_report,
    name,
    phone,
    email,
    region,
    district,
    ownership,
    duration_type,
    duration_value,
    amount
  } = req.body;

  const conn = await getConnection();
  const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
  const now = moment().tz('Africa/Nairobi');
  const today = now.format('YYYY-MM-DD HH:mm:ss');

  try {
    await sleep(3000); // Simulated delay

    // Check if store name or phone already exist for a different store
    const [existing] = await conn.query(
      `SELECT * FROM stores WHERE (name = ? OR phone = ?) AND id != ?`,
      [name, phone, id]
    );

    if (existing.length > 0) {
      return res.status(400).json({ message: `Store with name "${name}" or phone "${phone}" already exists.` });
    }

    let endDate = 'null';
    let formattedEndDate = 'null';
    let status = 'true';

    if (ownership === 'Fully') {
      formattedEndDate = 'null';

      await conn.query(
        `UPDATE stores 
         SET two_factor_auth = ?,
         enable_password_change =?,
         auto_check_subscriber = ?,
         auto_daily_report = ?,
         auto_monthly_report = ?,
         name = ?, phone = ?, email = ?, region_id = ?, district_id = ?, ownership = ?, 
         duration_type = ?, duration_value = ?, amount = ?, start_date = ?, end_date = ?, 
         status = ?, updated_at = ?, updated_by = ?
         WHERE id = ?`,
        [
          two_factor_auth,
          enable_password_change,
          auto_check_subscriber,
          auto_daily_report,
          auto_monthly_report,
          name, phone, email, region, district, 'Fully',
          0, 0, amount, today, formattedEndDate,
          status, today, res.locals.name, id
        ]
      );

      await conn.query(
        `UPDATE users u
         JOIN user_stores us ON u.id = us.user_id
         SET u.is_2fa_enabled = ?
         WHERE us.store_id = ?`,
        [two_factor_auth, id]
      );

      return res.status(200).json({ message: 'Fully owned store updated ' });
    }

    // Handle Partially owned
    if (ownership === 'Partially') {
      if (duration_type === 'Month') {
        if (!Number.isInteger(duration_value) || duration_value < 1) {
          return res.json({ error: 'Number of months must be a positive integer' });
        }
        endDate = now.clone().add(duration_value, 'months');
      } else if (duration_type === 'Year') {
        if (!Number.isInteger(duration_value) || duration_value < 1) {
          return res.json({ error: 'Number of years must be a positive integer' });
        }
        endDate = now.clone().add(duration_value, 'years');
      } else {
        return res.json({ error: 'Invalid duration_type'});
      }

      formattedEndDate = endDate.format('YYYY-MM-DD HH:mm:ss');

      await conn.query(
        `UPDATE stores 
         SET two_factor_auth = ?, 
             enable_password_change = ?,
             auto_check_subscriber = ?,
             auto_daily_report = ?,
             auto_monthly_report = ?,
             name = ?, 
             phone = ?, email = ?, region_id = ?, district_id = ?, ownership = ?, 
             duration_type = ?, duration_value = ?, amount = ?, start_date = ?, end_date = ?, 
             status = ?, updated_at = ?, updated_by = ?
         WHERE id = ?`,
        [
          two_factor_auth,
          enable_password_change,
          auto_check_subscriber,
          auto_daily_report,
          auto_monthly_report,
          name, phone, email, region, district, 'Partially',
          duration_type, duration_value, amount, today, formattedEndDate,
          status, today, res.locals.name, id
        ]
      );

      // Update user expiration date
      await conn.query(
        `UPDATE users u
         JOIN user_stores us ON u.id = us.user_id
         SET u.accountExpDate = ?
         WHERE us.store_id = ?`,
        [formattedEndDate, id]
      );

      // Update 2FA for users in this store
      await conn.query(
        `UPDATE users u
         JOIN user_stores us ON u.id = us.user_id
         SET u.is_2fa_enabled = ?
         WHERE us.store_id = ?`,
        [two_factor_auth, id]
      );

      // Get last store payment
      const [lastPayments] = await conn.query(
        `SELECT payment_amount FROM store_payments 
         WHERE store_id = ? 
         ORDER BY paid_at DESC 
         LIMIT 1`,
        [id]
      );

      const lastAmount = lastPayments.length > 0 ? parseFloat(lastPayments[0].payment_amount) : null;

      if (lastAmount === 'null' || lastAmount !== parseFloat(amount)) {
        await conn.query(
          `INSERT INTO store_payments (
             store_id, payment_amount, duration_type, duration_value,
             paid_at, paid_by, pay_status, paid, start_date, pay_reference_no
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            id,
            amount,
            duration_type,
            duration_value,
            today,
            res.locals.name,
            'true',
            amount,
            today,
            Math.floor(100000 + Math.random() * 900000),
          ]
        );
      }

      return res.status(200).json({
        message: `Partially owned store updated for ${duration_value} ${duration_type.toLowerCase()}(s)`
      });
    }

    return res.status(400).json({ error: 'Invalid ownership type ' });

  } catch (err) {
    console.error('Store update failed:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});


// Extend Expire date for stores

router.post('/extend/stores/expire/date', auth.authenticateToken, async (req, res) => {
  
  const conn = await getConnection();
  const { id, duration_type, duration_value, amount } = req.body;
  const now = moment().tz('Africa/Nairobi');
  const today = now.format('YYYY-MM-DD HH:mm:ss');

  try {
    // Get current end_date
    const [storeResult] = await conn.query(
      `SELECT end_date FROM stores WHERE id = ?`,
      [id]
    );

    if (storeResult.length === 0) {
      return res.status(404).json({ message: 'Store not found ' });
    }

    let currentEndDate = storeResult[0].end_date
      ? moment(storeResult[0].end_date).tz('Africa/Nairobi')
      : now.clone();

    // 2. Extend the end date
    if (duration_type === 'Month') {
      if (!Number.isInteger(duration_value) || duration_value < 1) {
        return res.json({ message: 'Number of months must be a positive integer ' });
      }
      currentEndDate.add(duration_value, 'months');
    } else if (duration_type === 'Year') {
      if (!Number.isInteger(duration_value) || duration_value < 1) {
        return res.status(400).json({ message: 'Number of years must be a positive integer ' });
      }
      currentEndDate.add(duration_value, 'years');
    } else {
      return res.json({ message: 'Invalid duration_type ' });
    }

    const formattedEndDate = currentEndDate.format('YYYY-MM-DD HH:mm:ss');

    // Update store table
    await conn.query(
      `UPDATE stores 
       SET duration_type = ?, duration_value = ?, amount = ?, start_date = ?, end_date = ?, updated_at = ?, updated_by = ?
       WHERE id = ?`,
      [
        duration_type,
        duration_value,
        amount,
        today,
        formattedEndDate,
        today,
        res.locals.name,
        id
      ]
    );

    // Update users' account expiration
    await conn.query(
      `UPDATE users u
       JOIN user_stores us ON u.id = us.user_id
       SET u.accountExpDate = ?, u.accountExpireStatus = 'false'
       WHERE us.store_id = ?`,
      [formattedEndDate, id]
    );

    // Insert store payment record
    const payRef = Math.floor(100000 + Math.random() * 900000);
    await conn.query(
      `INSERT INTO store_payments 
       (store_id, payment_amount, duration_type, duration_value, paid_at, paid_by, pay_status, paid, start_date, pay_reference_no)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        id,
        amount,
        duration_type,
        duration_value,
        today,
        res.locals.name,
        'true',
        amount,
        today,
        payRef
      ]
    );

    return res.status(200).json({
      message: `Store extended for ${duration_value} more ${duration_type.toLowerCase()}(s)`
    });
  } catch (error) {
    console.error('Error extending store:', error);
    return res.status(500).json({ message: 'Internal server error' });
  }
});


// Delete Store Data
router.post('/stores/delete', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;

  try {
    await new Promise(resolve => setTimeout(resolve, 3000));

    const placeholders = ids.map(() => '?').join(',');
    const connection = await getConnection();

    // Disable the stores
    const [storeResult] = await connection.query(
      `DELETE FROM stores WHERE id IN (${placeholders})`,
      ids
    );

    // Delete users related to the deleted stores
    const [userResult] = await connection.query(
      `
      DELETE FROM users
      WHERE id IN (
        SELECT DISTINCT user_id
        FROM user_stores
        WHERE store_id IN (${placeholders})
      )
      `,
      ids
    );

    res.json({
      message: `${storeResult.affectedRows} store(s) and ${userResult.affectedRows} user(s) deleted successfully`
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Delete failed', error: err.message });
  }
});

// Enable Store Data
router.post('/stores/enable', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;

  try {
    await new Promise(resolve => setTimeout(resolve, 3000));

    const placeholders = ids.map(() => '?').join(',');
    const connection = await getConnection();

    // Enable the stores
    const [storeResult] = await connection.query(
      `UPDATE stores SET status = "true" WHERE id IN (${placeholders})`,
      ids
    );

    // Enable users related to the enabled stores
    const [userResult] = await connection.query(
      `
      UPDATE users
      SET accountDisabled = "false"
      WHERE id IN (
        SELECT DISTINCT user_id
        FROM user_stores
        WHERE store_id IN (${placeholders})
      )
      `,
      ids
    );

    res.json({
      message: `${storeResult.affectedRows} store(s) and ${userResult.affectedRows} user(s) enabled successfully `
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Enable failed ', error: err.message });
  }
});


// Disable Store Data
router.post('/stores/disable', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;

  try {
    await new Promise(resolve => setTimeout(resolve, 3000));

    const placeholders = ids.map(() => '?').join(',');
    const connection = await getConnection();

    // Disable the stores
    const [storeResult] = await connection.query(
      `UPDATE stores SET status = "false" WHERE id IN (${placeholders})`,
      ids
    );

    // Disable users related to the disabled stores
    const [userResult] = await connection.query(
      `
      UPDATE users
      SET accountDisabled = "true"
      WHERE id IN (
        SELECT DISTINCT user_id
        FROM user_stores
        WHERE store_id IN (${placeholders})
      )
      `,
      ids
    );

    res.json({
      message: `${storeResult.affectedRows} store(s) and ${userResult.affectedRows} user(s) disabled successfully`
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Disable failed ', error: err.message });
  }
});

// Add a new expenses category
router.post('/create/expenses/category', auth.authenticateToken, async (req, res) => {
  let conn;

  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

  try {

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ message: 'Access denied. Super admin only.' });
    }


    const { name } = req.body;
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Optional: Simulate delay
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Check for existing menu
    const [existing] = await conn.query(
      `SELECT * FROM expenses_category WHERE name = ? `,
      [name]
    );

    if (existing.length > 0) {
      return res.json({
        message: `Name "${name}" already exists`
      });
    }

    // Optional: Another delay before insert
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Insert new 
    const [result] = await conn.query(
      `INSERT INTO expenses_category (name)
       VALUES (?)`,
      [name]
    );

    return res.status(201).json({
      message: `Name "${name}" created successfully! `
    });

  } catch (err) {
    console.error('Error:', err);
    res.status(500).json({ message: 'Internal server error' });
  } 
});





// Download Excel File that contain product lists ======

router.get('/warehouse/:id/product-template-download', auth.authenticateToken, async (req, res) => {
  const warehouseId = req.params.id;

  try {
    const conn = await getConnection();
    const [products] = await conn.execute(
      'SELECT id, name FROM products WHERE warehouse_id = ?',
      [warehouseId]
    );

    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Purchase Template');

    // Define main columns
    worksheet.columns = [
      { header: 'Product Name', key: 'product_id', width: 30 },
      { header: 'Barcode Number', key: 'barcode_no', width: 15 },
      { header: 'Batch Number', key: 'batch_no', width: 15 },
      { header: 'Quantity', key: 'quantity', width: 15 },
      { header: 'Unit Cost', key: 'unit_cost', width: 15 },
      { header: 'Tax', key: 'tax', width: 15 },
      { header: 'Discount', key: 'discount', width: 15 },
      { header: 'Expire Date', key: 'expire_date', width: 15 },
    ];

    // Style header row
    const headerRow = worksheet.getRow(1);
    headerRow.font = { bold: true };
    worksheet.views = [{ state: 'frozen', ySplit: 1 }];

    // Add 1000 empty rows
    for (let i = 0; i < 1000; i++) {
      worksheet.addRow({});
    }

    // Create hidden sheet for dropdown
    const productSheet = workbook.addWorksheet('Products');
    productSheet.state = 'veryHidden';
    products.forEach((product, index) => {
      productSheet.getCell(`A${index + 1}`).value = `${product.name}`;
    });

    // Apply dropdown list to product_id column (A2 to A1001)
    const dropdownRange = `=Products!$A$1:$A$${products.length}`;
    for (let row = 2; row <= 1001; row++) {
      worksheet.getCell(`A${row}`).dataValidation = {
        type: 'list',
        allowBlank: false,
        formulae: [dropdownRange],
        showErrorMessage: true,
        errorTitle: 'Invalid Selection',
        error: 'Choose a valid product from the dropdown list.',
      };
    }

    // Prepare and send response
    res.setHeader(
      'Content-Disposition',
      `attachment; filename=Purchase_Template_Warehouse_${warehouseId}.xlsx`
    );
    res.setHeader(
      'Content-Type',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    );

    await workbook.xlsx.write(res);
    res.end();
  } catch (err) {
    console.error('Excel generation error:', err);
    res.status(500).json({ message: 'Failed to generate Excel template' });
  }
});



// Add a new deposit category
router.post('/create/deposit/category', auth.authenticateToken, async (req, res) => {
  let conn;

  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

  try {

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ message: 'Access denied. Super admin only.' });
    }


    const { name } = req.body;
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Optional: Simulate delay
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Check for existing menu
    const [existing] = await conn.query(
      `SELECT * FROM deposit_category WHERE name = ? `,
      [name]
    );

    if (existing.length > 0) {
      return res.json({
        message: `Name "${name}" already exists `
      });
    }

    // Optional: Another delay before insert
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Insert new 
    const [result] = await conn.query(
      `INSERT INTO deposit_category (name)
       VALUES (?)`,
      [name]
    );

    return res.status(201).json({
      message: `Name "${name}" created successfully! `
    });

  } catch (err) {
    console.error('Error:', err);
    res.status(500).json({ message: 'Internal server error' });
  } 
});



// Update Deposit Category
router.put('/update/deposit/category/:id', auth.authenticateToken, async (req, res) => {
  let conn;

  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

  try {

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ message: 'Access denied. Super admin only.' });
    }

    const { name } = req.body;
    const id = req.params.id;

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    // Update 
    const [result] = await conn.query(
      `UPDATE deposit_category SET name = ? WHERE id = ?`,
      [name, id]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ message: "Name not found" });
    }

    return res.status(200).json({
      message: `Name "${name}" updated successfully! `
    });

  } catch (err) {
    console.error('Update Error:', err);
    res.status(500).json({ message: 'Internal server error ' });
  } 
});



// Update Expenses Category
router.put('/update/expenses/category/:id', auth.authenticateToken, async (req, res) => {
  let conn;

  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

  try {

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ message: 'Access denied. Super admin only.' });
    }

    const { name } = req.body;
    const id = req.params.id;

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    // Update 
    const [result] = await conn.query(
      `UPDATE expenses_category SET name = ? WHERE id = ?`,
      [name, id]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ message: "Name not found" });
    }

    return res.status(200).json({
      message: `Name "${name}" updated successfully! `
    });

  } catch (err) {
    console.error('Update Error:', err);
    res.status(500).json({ error: 'Internal server error ' });
  } 
});


// Get Deposit Category
router.get('/get/deposit/category', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Get
    const [result] = await conn.query(
      `SELECT * FROM deposit_category ORDER BY name ASC`
    );
    return res.status(200).json(result);

  } catch (err) {
    res.status(500).json({ message: 'Internal server error ' });
  } 
});


// Get Expenses Category
router.get('/get/expenses/category', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Get
    const [result] = await conn.query(
      `SELECT * FROM expenses_category ORDER BY name ASC`
    );
    return res.status(200).json(result);

  } catch (err) {
    res.status(500).json({ message: 'Internal server error ' });
  } 
});


// Delete Deposit Category
router.delete('/delete/deposit/category/:id', auth.authenticateToken, async (req, res) => {
  let conn;

  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

  try {

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({message: 'Access denied. Super admin only.' });
    }

    const id = req.params.id;

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    // Delete
    const [result] = await conn.query(
      `DELETE FROM deposit_category WHERE id = ?`,
      [id]
    );

    if (result.affectedRows === 0) {
      return res.json({ message: "Name not found " });
    }

    return res.status(200).json({
      message: `Deleted successfully!`
    });

  } catch (err) {
    console.error('Error:', err);
    res.status(500).json({ message: 'Internal server error' });
  } 
});


// Delete Expenses Category
router.delete('/delete/expenses/category/:id', auth.authenticateToken, async (req, res) => {
  let conn;

  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

  try {

    // Only allow super admins
    if (!isSuperAdmin) {
      return res.status(403).json({ message: 'Access denied. Super admin only.' });
    }

    const id = req.params.id;

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    // Delete
    const [result] = await conn.query(
      `DELETE FROM expenses_category WHERE id = ?`,
      [id]
    );

    if (result.affectedRows === 0) {
      return res.json({ message: "Name not found " });
    }

    return res.status(200).json({
      message: `Deleted successfully!`
    });

  } catch (err) {
    console.error('Error:', err);
    res.status(500).json({ message: 'Internal server error' });
  } 
});


// Create Expenses
router.post('/create/expenses/add', auth.authenticateToken, async (req, res) => {
  const { name, amount, category, store, warehouse, notes } = req.body;
  
  try {
    const connection = await getConnection();
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds


    // Get Active Financial Year
    const [fyRows] = await connection.execute(
      `SELECT id FROM fy_cycle WHERE store_id = ? AND isActive = 1 LIMIT 1`, [store]
    );
    const fy_id = fyRows[0].id;

    const [result] = await connection.query(
      `INSERT INTO expenses (fy_id, title, amount, category, store_id, warehouse_id, notes, expense_date, created_by) 
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [fy_id, name, amount, category, store, warehouse, notes, now, res.locals.name]
    );
    res.status(201).json({ message: 'Expense added successfully' });
  } catch (err) {
    console.error('Insert Expense Error:', err);
    res.status(500).json({ message: 'Server Error ', error: err.message });
  }
});

// Get User Expenses Lists

router.get('/get/users/expenses/list', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    username,
    storeId,
    warehouseId,
    filterType,
    startDate,
    endDate,
    sortBy = 'created_at',
    sortOrder = 'DESC',
  } = req.query;

  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT
        e.id, e.title, e.amount, e.category, e.store_id, e.warehouse_id,
        e.notes, e.expense_date, e.created_by, e.created_at,
        w.id AS warehouse_id, w.name AS warehousename,
        s.id AS store_id, s.name AS storename,
        ec.id AS category_id, ec.name AS category
      FROM expenses e
      JOIN warehouses w ON e.warehouse_id = w.id
      JOIN stores s ON e.store_id = s.id
      JOIN expenses_category ec ON e.category = ec.id
    `;

    const params = [];
    const whereConditions = [];

    // Role-based filtering
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      if (storeIds.length > 0 && warehouseIds.length > 0) {
        whereConditions.push(`(e.store_id IN (${storeIds.map(() => '?').join(',')}) AND e.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
        params.push(...storeIds, ...warehouseIds);
      } else if (storeIds.length > 0) {
        whereConditions.push(`e.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } else if (warehouseIds.length > 0) {
        whereConditions.push(`e.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    // Optional override filters
    if (storeId) {
      whereConditions.push(`e.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`e.warehouse_id = ?`);
      params.push(warehouseId);
    }

    if (username) {
      whereConditions.push(`e.created_by = ?`);
      params.push(username);
    }

    // Date Filtering with moment-timezone
    const tz = 'Africa/Nairobi';
    let start, end;

    if (filterType) {
      const now = moment().tz(tz);

      switch (filterType) {
        case 'today':
          start = now.clone().startOf('day');
          end = now.clone().endOf('day');
          break;
        case 'yesterday':
          start = now.clone().subtract(1, 'day').startOf('day');
          end = now.clone().subtract(1, 'day').endOf('day');
          break;
        case 'week':
          start = now.clone().startOf('week');
          end = now.clone().endOf('week');
          break;
        case 'last_week':
          start = now.clone().subtract(1, 'week').startOf('week');
          end = now.clone().subtract(1, 'week').endOf('week');
          break;
        case 'month':
          start = now.clone().startOf('month');
          end = now.clone().endOf('month');
          break;
        case 'year':
          start = now.clone().startOf('year');
          end = now.clone().endOf('year');
          break;
        case 'last_year':
          start = now.clone().subtract(1, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_2_years':
          start = now.clone().subtract(2, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_3_years':
          start = now.clone().subtract(3, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_4_years':
          start = now.clone().subtract(4, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
      }
    } else if (startDate && endDate) {
      start = moment.tz(startDate, tz).startOf('day');
      end = moment.tz(endDate, tz).endOf('day');
    }

    if (start && end) {
      whereConditions.push(`e.created_at BETWEEN ? AND ?`);
      params.push(start.format('YYYY-MM-DD HH:mm:ss'), end.format('YYYY-MM-DD HH:mm:ss'));
    }

    // Add WHERE conditions
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sorting
    const columnMap = {
      created_at: 'e.created_at'
    };

    const orderBy = columnMap[sortBy] || 'e.created_at';
    const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    query += ` ORDER BY ${orderBy} ${orderDir}`;

    // Run final query
    const [result] = await connection.query(query, params);

    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching expenses list', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});

// Get Expenses Lists

router.get('/get/expenses/list', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    storeId,
    warehouseId,
    filterType,
    startDate,
    endDate,
    sortBy = 'created_at',
    sortOrder = 'DESC',
  } = req.query;

  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT
        e.id, e.title, e.amount, e.category, e.store_id, e.warehouse_id,
        e.notes, e.expense_date, e.created_by, e.created_at,
        w.id AS warehouse_id, w.name AS warehousename,
        s.id AS store_id, s.name AS storename,
        ec.id AS category_id, ec.name AS category
      FROM expenses e
      JOIN warehouses w ON e.warehouse_id = w.id
      JOIN stores s ON e.store_id = s.id
      JOIN expenses_category ec ON e.category = ec.id
    `;

    const whereConditions = [];
    const params = [];

    // Role-based filtering
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [], totalCount: 0, totalAmount: 0 });
      }

      if (storeIds.length > 0 && warehouseIds.length > 0) {
        whereConditions.push(`(e.store_id IN (${storeIds.map(() => '?').join(',')}) AND e.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
        params.push(...storeIds, ...warehouseIds);
      } else if (storeIds.length > 0) {
        whereConditions.push(`e.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } else if (warehouseIds.length > 0) {
        whereConditions.push(`e.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    // Manual filters
    if (storeId) {
      whereConditions.push(`e.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`e.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // Timezone-aware date filtering
    const tz = 'Africa/Nairobi';
    let start, end;

    if (filterType) {
      const now = moment().tz(tz);

      switch (filterType) {
        case 'today':
          start = now.clone().startOf('day');
          end = now.clone().endOf('day');
          break;
        case 'yesterday':
          start = now.clone().subtract(1, 'day').startOf('day');
          end = now.clone().subtract(1, 'day').endOf('day');
          break;
        case 'week':
          start = now.clone().startOf('week');
          end = now.clone().endOf('week');
          break;
        case 'last_week':
          start = now.clone().subtract(1, 'week').startOf('week');
          end = now.clone().subtract(1, 'week').endOf('week');
          break;
        case 'month':
          start = now.clone().startOf('month');
          end = now.clone().endOf('month');
          break;
        case 'year':
          start = now.clone().startOf('year');
          end = now.clone().endOf('year');
          break;
        case 'last_year':
          start = now.clone().subtract(1, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_2_years':
          start = now.clone().subtract(2, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_3_years':
          start = now.clone().subtract(3, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_4_years':
          start = now.clone().subtract(4, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
      }
    } else if (startDate && endDate) {
      start = moment.tz(startDate, tz).startOf('day');
      end = moment.tz(endDate, tz).endOf('day');
    }

    if (start && end) {
      whereConditions.push(`e.created_at BETWEEN ? AND ?`);
      params.push(start.format('YYYY-MM-DD HH:mm:ss'), end.format('YYYY-MM-DD HH:mm:ss'));
    }

    // Apply WHERE
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sorting
    const validColumns = ['created_at', 'amount'];
    const orderBy = validColumns.includes(sortBy) ? `e.${sortBy}` : 'e.created_at';
    const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    query += ` ORDER BY ${orderBy} ${orderDir}`;

    // Execute
    const [result] = await connection.query(query, params);

    // Total Amount
    const totalAmount = result.reduce((sum, row) => sum + parseFloat(row.amount || 0), 0);

    res.json({
      array: result,
      totalCount: result.length,
      totalAmount
    });

  } catch (err) {
    console.error('Error fetching expenses list', err);
    res.status(500).json({ message: 'Something went wrong ', error: err.message });
  }
});



// Get Sold Items By sales Id
router.get('/get/sales/item/byId/referenceno/:id', auth.authenticateToken, async (req, res) => {
  const conn = await getConnection();
  const { id } = req.params;

  try {
    // Fetch sales with customer and warehouse info
// Wait for 3 seconds before proceeding
await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    const [sales] = await conn.query(`
      SELECT 
        sa.*, 
        c.name AS customer_name, 
        c.phone AS customer_phone, 
        wh.name AS warehouse_name, 
        s.name AS store_name 
      FROM sales sa
      LEFT JOIN customers c ON sa.customer_id = c.id
      LEFT JOIN warehouses wh ON sa.warehouse_id = wh.id
      LEFT JOIN stores s ON sa.store_id = s.id
      WHERE sa.id = ?`, [id]);

    // Fetch sales items
    const [items] = await conn.query(
      `SELECT 
         si.product_id, 
         si.quantity, 
         si.price, 
         si.discount, 
         si.vat, 
         si.subtotal, 
         p.name AS product_name 
       FROM sale_items si
       JOIN products p ON si.product_id = p.id
       WHERE si.sale_id = ?`, [id]
    );

    res.json({ ...sales[0], items });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Failed to load sales details ' });
  }
});



// Get Purchased Items By Purchase Id
router.get('/get/purchased/item/byId/:id', auth.authenticateToken, async (req, res) => {
  const conn = await getConnection();
  const { id } = req.params;

  try {
    // Fetch purchase with supplier and warehouse info
// Wait for 3 seconds before proceeding
await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    const [purchase] = await conn.query(`
      SELECT 
        pur.*, 
        sup.name AS supplier_name, 
        sup.phone AS supplier_phone, 
        wh.name AS warehouse_name, 
        s.name AS store_name 
      FROM purchases pur
      LEFT JOIN suppliers sup ON pur.supplier_id = sup.id
      LEFT JOIN warehouses wh ON pur.warehouse_id = wh.id
      LEFT JOIN stores s ON pur.store_id = s.id
      WHERE pur.id = ?`, [id]);

    // Fetch purchase items
    const [items] = await conn.query(
      `SELECT 
         pi.product_id, 
         pi.quantity, 
         pi.cost, 
         pi.subtotal, 
         p.name AS product_name 
       FROM purchase_items pi
       JOIN products p ON pi.product_id = p.id
       WHERE pi.purchase_id = ?`, [id]
    );

    res.json({ ...purchase[0], items });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Failed to load purchase details' });
  }
});



// Get Purchase Reference No
router.get('/get/purchases/reference/number', auth.authenticateToken, async (req, res) => {
  
const userId = res.locals.id;
  const roleId = res.locals.role;
  const { storeId, warehouseId } = req.query;


  let connection;

  try {
    connection = await getConnection();

    // Base query
    let query = `
      SELECT p.*, 
        s.name AS storename, s.id AS store_id, 
        w.name AS warehousename, w.id AS warehouse_id
      FROM purchases p
      JOIN stores s ON s.id = p.store_id
      JOIN warehouses w ON w.id = p.warehouse_id
    `;

    const params = [];
    const whereConditions = [];

    // If the user is not an admin, apply restrictions based on their assigned stores and warehouses
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      // If user has no stores or warehouses assigned, return an empty response
      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      // Conditions for stores and warehouses assigned to the user
      const accessConditions = [];

      if (storeIds.length > 0) {
        accessConditions.push(`p.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (warehouseIds.length > 0) {
        accessConditions.push(`p.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }

      if (storeIds.length > 0 && warehouseIds.length > 0) {
        whereConditions.push(`(p.store_id IN (${storeIds.map(() => '?').join(',')}) AND p.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
        params.push(...storeIds, ...warehouseIds);
      } else if (storeIds.length > 0) {
        whereConditions.push(`p.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } else if (warehouseIds.length > 0) {
        whereConditions.push(`p.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
      


    }

    // Admin doesn't need store/warehouse filters, apply optional filters if passed
    if (storeId) {
      whereConditions.push(`p.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`p.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // If there are any where conditions, add them to the query
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sort the result 
    query += ` ORDER BY p.refNumber ASC`;

    const [result] = await connection.query(query, params);
    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching:', err);
    res.status(500).json({ message: 'Something went wrong' });
  }

});


// Get Sales Reference No
router.get('/get/sales/reference/number', auth.authenticateToken, async (req, res) => {
  
 const userId = res.locals.id;
  const roleId = res.locals.role;
  const { storeId, warehouseId } = req.query;

  let connection;

  try {
    connection = await getConnection();

    // Base query
    let query = `
      SELECT sa.*, 
        s.name AS storename, s.id AS store_id, 
        w.name AS warehousename, w.id AS warehouse_id
      FROM sales sa
      JOIN stores s ON s.id = sa.store_id
      JOIN warehouses w ON w.id = sa.warehouse_id
    `;

    const params = [];
    const whereConditions = [];

    // If the user is not an admin, apply restrictions based on their assigned stores and warehouses
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      // If user has no stores or warehouses assigned, return an empty response
      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      // Conditions for stores and warehouses assigned to the user
      const accessConditions = [];

      if (storeIds.length > 0) {
        accessConditions.push(`sa.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (warehouseIds.length > 0) {
        accessConditions.push(`sa.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }

      if (storeIds.length > 0 && warehouseIds.length > 0) {
        whereConditions.push(`(sa.store_id IN (${storeIds.map(() => '?').join(',')}) AND sa.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
        params.push(...storeIds, ...warehouseIds);
      } else if (storeIds.length > 0) {
        whereConditions.push(`sa.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } else if (warehouseIds.length > 0) {
        whereConditions.push(`sa.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
      


    }

    // Admin doesn't need store/warehouse filters, apply optional filters if passed
    if (storeId) {
      whereConditions.push(`sa.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`sa.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // If there are any where conditions, add them to the query
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sort the result 
    query += ` ORDER BY sa.refNumber ASC`;

    const [result] = await connection.query(query, params);
    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching:', err);
    res.status(500).json({ message: 'Something went wrong' });
  }

});


// Get Product Lists Data
router.get('/get/product/lists', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const { storeId, warehouseId } = req.query;


  let connection;

  try {
    connection = await getConnection();

    // Base query
    let query = `
      SELECT 
  p.id AS id,
  p.store_id AS store_id,
  p.warehouse_id AS warehouse_id,
  p.category_id AS category_id,
  p.brand_id AS brand_id,
  p.unit_id AS unit_id,
  p.refNumber AS refNumber,
  p.name AS name,
  p.qty AS qty,
  p.cost AS cost,
  p.price AS price,
  p.imei_serial AS imei_serial,
  p.expire_date AS expire_date,
  p.vat AS vat,
  p.discount AS discount,
  p.product_create_date AS product_create_date,
  p.product_create_by AS product_create_by,
  p.product_update_date AS product_update_date,
  p.product_update_by AS product_update_by,
  p.product_status AS product_status,
  p.product_qty_alert AS product_qty_alert,

  s.name AS storename,
  s.id AS store_id,
  w.name AS warehousename,
  w.id AS warehouse_id

      FROM products p
      JOIN stores s ON s.id = p.store_id
      JOIN warehouses w ON w.id = p.warehouse_id
    `;

    const params = [];
    const whereConditions = [];

    // If the user is not an admin, apply restrictions based on their assigned stores and warehouses
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      // If user has no stores or warehouses assigned, return an empty response
      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      // Conditions for stores and warehouses assigned to the user
      const accessConditions = [];

      if (storeIds.length > 0) {
        accessConditions.push(`p.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (warehouseIds.length > 0) {
        accessConditions.push(`p.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }

      if (storeIds.length > 0 && warehouseIds.length > 0) {
        whereConditions.push(`(p.store_id IN (${storeIds.map(() => '?').join(',')}) AND p.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
        params.push(...storeIds, ...warehouseIds);
      } else if (storeIds.length > 0) {
        whereConditions.push(`p.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } else if (warehouseIds.length > 0) {
        whereConditions.push(`p.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
      


    }

    // Admin doesn't need store/warehouse filters, apply optional filters if passed
    if (storeId) {
      whereConditions.push(`p.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`p.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // If there are any where conditions, add them to the query
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sort the result 
    query += ` ORDER BY p.name ASC`;

    const [result] = await connection.query(query, params);
    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching:', err);
    res.status(500).json({ message: 'Something went wrong' });
  }
});


// Delete Products Data

router.post('/products/delete', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;

  try {
    await new Promise(resolve => setTimeout(resolve, 3000));

    const placeholders = ids.map(() => '?').join(',');
    const connection = await getConnection();

    // Disable the stores
    const [result] = await connection.query(
      `DELETE FROM products WHERE id IN (${placeholders})`,
      ids
    );

    res.json({
      message: `${result.affectedRows} product(s) deleted successfully`
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Delete failed ', error: err.message });
  }
});


// Disable Products Data
router.post('/products/disable', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;

  try {
    await new Promise(resolve => setTimeout(resolve, 3000));

    const placeholders = ids.map(() => '?').join(',');
    const connection = await getConnection();

    // Disable 
    const [result] = await connection.query(
      `UPDATE products SET product_status = "false" WHERE product_status !='pending' AND id IN (${placeholders})`,
      ids
    );

    res.json({
      message: `${result.affectedRows} product(s) locked successfully`
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Disable failed', error: err.message });
  }
});


// Enable Products Data
router.post('/products/enable', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;

  try {
    await new Promise(resolve => setTimeout(resolve, 3000));

    const placeholders = ids.map(() => '?').join(',');
    const connection = await getConnection();

    // Enable
    const [result] = await connection.query(
      `UPDATE products SET product_status = "true" WHERE product_status !='pending' AND id IN (${placeholders})`,
      ids
    );

    res.json({
      message: `${result.affectedRows} product(s) enabled successfully`
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Enable failed ' });
  }
});


// Create Warehouse
router.post('/create/warehouse', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const { 
    name, 
    store, 
    region, 
    district,
    tra_enabled,
    lipa_namba,
    akaunti_namba,
    supports_barcode,
    supports_beep,
    customer_field,
    supplier_field,
    send_sale_sms,
    send_purchase_sms,
    send_low_qty_sms,
    send_sms_every_week_sale,
    batch_number,
    show_discount_field,
    show_vat_field,
    show_transport_field,
    expire_date_field,
    auto_delete_logs,
    auto_delete_expired_product,
    auto_send_summary_report
     } = req.body;

     const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Optional: Simulate delay
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Check for existing
    const [existing] = await conn.query(
      `SELECT * FROM warehouses WHERE name = ? AND storeId = ? `,
      [name, store]
    );

    if (existing.length > 0) {
      return res.json({
        message: `Name "${name}" already exists`
      });
    }

    // Optional: Another delay before insert
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Insert new 
    await conn.query(
      `INSERT INTO warehouses (
    tra_enabled,
    auto_delete_logs,
    auto_delete_expired_product,
    auto_send_summary_report,
    lipa_namba,
    akaunti_namba, 
    supports_barcode,
    supports_beep,
    customer_field,
    supplier_field,
    send_sale_sms,
    send_purchase_sms,
    send_low_qty_sms,
    send_sms_every_week_sale,
    batch_number,
    show_discount_field,
    show_vat_field,
    show_transport_field,
    expire_date_field,
    name, storeId, region_id, district_id, created_at, created_by, status)
       VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
    tra_enabled || 0,
    auto_delete_logs || 0,
    auto_delete_expired_product || 0,
    auto_send_summary_report || 0,
    lipa_namba,
    akaunti_namba,
    supports_barcode || 0,
    supports_beep || 0,
    customer_field || 0,
    supplier_field || 0,
    send_sale_sms || 0,
    send_purchase_sms || 0,
    send_low_qty_sms || 0,
    send_sms_every_week_sale || 0,
    batch_number || 0,
    show_discount_field || 0,
    show_vat_field || 0,
    show_transport_field || 0,
    expire_date_field || 0,
     name, store, region, district, now, res.locals.name, 'true']
    );

    // Insert into logs
    await conn.query(
      `INSERT INTO logs (
          user_id, store_id, action, description, createdAt, createdBy
      ) VALUES (?, ?, ?, ?, ?, ?)`,
      [
        res.locals.id,             // ID of the user performing the action
        store,                    // Store IDs
        'CREATE WAREHOUSE',             // Action type
        `Warehouse ${name} was created`, // Description
        now,                // Timestamp
        res.locals.name           // Name of the user who did the action
      ]
    );

    return res.status(201).json({
      message: `Warehouse of "${name}" created successfully! `
    });

  } catch (err) {
    res.status(500).json({ error: 'Internal server error!' });
  } 
});

// Update Warehouse
router.put('/update/warehouse/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const { 
    tra_enabled,
    auto_delete_logs,
    auto_delete_expired_product,
    auto_send_summary_report,
    lipa_namba,
    akaunti_namba,
    name, store, region, district,
    supports_barcode,
    supports_beep,
    customer_field,
    supplier_field,
    send_sale_sms,
    send_purchase_sms,
    send_low_qty_sms,
    send_sms_every_week_sale,
    batch_number,
    show_discount_field,
    show_vat_field,
    show_transport_field,
    expire_date_field
     } = req.body;
    const id = req.params.id;

    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    // Wait for 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000)); // 3000ms = 3 seconds

    //  Update 
    const [result] = await conn.query(
    `UPDATE warehouses SET
    tra_enabled = ?,
    auto_delete_logs = ?,
    auto_delete_expired_product = ?,
    auto_send_summary_report = ?,
    lipa_namba = ?,
    akaunti_namba = ?,
    show_discount_field = ?,
    show_vat_field = ?,
    show_transport_field = ?,
    batch_number = ?,
    supports_barcode = ?,
    supports_beep = ?,
    customer_field = ?,
    supplier_field = ?,
    send_sale_sms = ?,
    send_purchase_sms = ?,
    send_low_qty_sms =?,
    send_sms_every_week_sale =?,
    expire_date_field = ?,
    name = ?, 
    storeId = ?, 
    region_id = ?, 
    district_id = ? 
    WHERE id = ?`,
     [
    tra_enabled,
    auto_delete_logs,
    auto_delete_expired_product,
    auto_send_summary_report,
    lipa_namba,
    akaunti_namba,
    show_discount_field,
    show_vat_field,
    show_transport_field,
    batch_number,
    supports_barcode,
    supports_beep,
    customer_field,
    supplier_field,
    send_sale_sms,
    send_purchase_sms,
    send_low_qty_sms,
    send_sms_every_week_sale,
    expire_date_field,
    name, store, region, district, id]
    );

    // Insert into logs
    await conn.query(
      `INSERT INTO logs (
          user_id, store_id, action, description, createdAt, createdBy
      ) VALUES (?, ?, ?, ?, ?, ?)`,
      [
        res.locals.id,             // ID of the user performing the action
        store,                    // Store IDs
        'UPDATE WAREHOUSE',             // Action type
        `Warehouse ${name} (${id}) was updated`, // Description
        now,                // Timestamp
        res.locals.name           // Name of the user who did the action
      ]
    );

    if (result.affectedRows === 0) {
      return res.json({ message: "Id not found " });
    }

    return res.status(200).json({
      message: `Warehouse "${name}" updated successfully! `
    });

  } catch (err) {
    console.error('Update Error:', err);
    res.status(500).json({ message: 'Internal server error ' });
  } 
});

// Get Warehouse Lists
router.get('/get/warehouse/list', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const { storeId } = req.query;


  let connection;

  try {
    connection = await getConnection();

    // Base query
  let query = `
  SELECT 
  w.id AS id,
  w.storeId AS storeId,
  w.name AS name,
  w.region_id AS region_id,
  w.district_id AS district_id,
  w.created_at AS created_at,
  w.created_by AS created_by,
  w.status AS status,
  w.supports_barcode,
  w.supports_beep,
  w.customer_field,
  w.supplier_field,
  w.expire_date_field,
  w.send_sale_sms,
  w.send_purchase_sms,
  w.send_low_qty_sms,
  w.send_sms_every_week_sale,
  w.batch_number,
  w.show_discount_field,
  w.show_vat_field,
  w.show_transport_field,
  w.lipa_namba,
  w.akaunti_namba,
  w.auto_delete_logs,
  w.auto_delete_expired_product,
  w.auto_send_summary_report,
  w.tra_enabled,

  s.id AS store_id,               
  s.name AS storename,
  r.name AS region_name,
  d.name AS district_name

      FROM warehouses w
      LEFT JOIN regions r ON w.region_id = r.id
      LEFT JOIN districts d ON w.district_id = d.id
      LEFT JOIN stores s ON s.id = w.storeId
    `;

    const params = [];
    const whereConditions = [];

    // If the user is not an admin, apply restrictions based on their assigned stores
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
    
      const storeIds = storeRows.map(r => r.store_id);
      
      // If user has no stores or warehouses assigned, return an empty response
      if (storeIds.length === 0 ) {
        return res.json({ array: [] });
      }

      // Conditions for stores and warehouses assigned to the user
      const accessConditions = [];

      if (storeIds.length > 0) {
        accessConditions.push(`w.storeId IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (storeIds.length > 0 ) {
        whereConditions.push(`(w.storeId IN (${storeIds.map(() => '?').join(',')}))`);
        params.push(...storeIds);
      } else if (storeIds.length > 0) {
        whereConditions.push(`w.storeId IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } 

    }

    // Admin doesn't need store/warehouse filters, apply optional filters if passed
    if (storeId) {
      whereConditions.push(`w.storeId = ?`);
      params.push(storeId);
    }

    // If there are any where conditions, add them to the query
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sort the result 
    query += ` ORDER BY w.name ASC`;

    const [result] = await connection.query(query, params);
    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching:', err);
    res.status(500).json({ message: 'Something went wrong' });
  }
});

// Disable Warehouse Data
router.post('/warehouses/disable', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;

  try {
    await new Promise(resolve => setTimeout(resolve, 3000));

    const placeholders = ids.map(() => '?').join(',');
    const connection = await getConnection();

    // Enable
    const [result] = await connection.query(
      `UPDATE warehouses SET status = "false" WHERE id IN (${placeholders})`,
      ids
    );

    res.json({
      message: `${result.affectedRows} warehouses(s) disabled successfully `
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Disable failed' });
  }
});


// Enable Warehouse Data
router.post('/warehouses/enable', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;

  try {
    await new Promise(resolve => setTimeout(resolve, 3000));

    const placeholders = ids.map(() => '?').join(',');
    const connection = await getConnection();

    // Enable
    const [result] = await connection.query(
      `UPDATE warehouses SET status = "true" WHERE id IN (${placeholders})`,
      ids
    );

    res.json({
      message: `${result.affectedRows} warehouses(s) enabled successfully `
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Enable failed ' });
  }
});


// Delete Warehouse Data
router.post('/warehouses/delete', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;

  try {
    await new Promise(resolve => setTimeout(resolve, 3000));

    const placeholders = ids.map(() => '?').join(',');
    const connection = await getConnection();

    // Enable
    const [result] = await connection.query(
      `DELETE FROM warehouses WHERE id IN (${placeholders})`,
      ids
    );

    res.json({
      message: `${result.affectedRows} warehouses(s) deleted successfully `
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Delete failed' });
  }
});



// Get Tra Configuration Parameters

// Load one Tra configuration by ID
router.get('/get/tra/configuration', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;

  let connection;

  try {
    connection = await getConnection();

    // Get stores assigned to the user
    const [storeRows] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?',
      [userId]
    );

    const storeIds = storeRows.map(r => r.store_id);

    if (storeIds.length === 0) {
      // User has no stores assigned → no config available
      return res.status(404).json({ success: false, message: 'No stores assigned to this user' });
    }

    // Fetch SMS config for these stores
    const placeholders = storeIds.map(() => '?').join(',');

    const [configs] = await connection.query(
      `SELECT id, store_id, tin, api_url, username, password
       FROM tra_configuration
       WHERE store_id IN (${placeholders})
       ORDER BY id DESC
       LIMIT 1`,
      storeIds
    );

    if (configs.length === 0) {
      return res.status(404).json({ success: false, message: 'No Tra configuration found for your assigned stores' });
    }

    // Return the found config (first one)
    return res.json({ success: true, data: configs[0] });

  } catch (err) {
    console.error('Error fetching Tra configuration:', err);
    return res.status(500).json({ success: false, message: 'Internal server error', error: err.message });
  }
});


// Get Backend Call Api

// Load one System Backend configuration by ID

router.get('/get/backend/base/url', auth.authenticateToken, async (req, res) => {
  
  let connection;

  try {
    connection = await getConnection();

    const [configs] = await connection.query(
      `SELECT id, backendUrl
       FROM backend_call_api
       ORDER BY id DESC
       LIMIT 1`
    );

    if (configs.length === 0) {
      return res.status(404).json({ success: false, message: 'No Frontend Base URL configuration found for your assigned stores' });
    }

    // Return the found config (first one)
    return res.json({ success: true, data: configs[0] });

  } catch (err) {
    console.error('Error fetching System backend url base configuration:', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});


// Get FrontEnd Base Url

// Load one System Mail configuration by ID
router.get('/get/frontend/base/url', auth.authenticateToken, async (req, res) => {
  
  let connection;

  try {
    connection = await getConnection();

    const [configs] = await connection.query(
      `SELECT id, baseUrl
       FROM front_end_base_url
       ORDER BY id DESC
       LIMIT 1`
    );

    if (configs.length === 0) {
      return res.status(404).json({ success: false, message: 'No Frontend Base URL configuration found for your assigned stores' });
    }

    // Return the found config (first one)
    return res.json({ success: true, data: configs[0] });

  } catch (err) {
    console.error('Error fetching System Front end url base configuration:', err);
    return res.status(500).json({ success: false, message: 'Internal server error'});
  }
});


// Get System Mail Configuration Parameters

// Load one System Mail configuration by ID
router.get('/get/system/mail/configuration', auth.authenticateToken, async (req, res) => {
  
  let connection;

  try {
    connection = await getConnection();

    const [configs] = await connection.query(
      `SELECT id, host, port, secure, username, password
       FROM system_mail_configuration
       ORDER BY id DESC
       LIMIT 1`
    );

    if (configs.length === 0) {
      return res.status(404).json({ success: false, message: 'No Mail configuration found for your assigned stores' });
    }

    // Return the found config (first one)
    return res.json({ success: true, data: configs[0] });

  } catch (err) {
    console.error('Error fetching System Mail configuration:', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});


// Get Mail Configuration Parameters

// Load one Mail configuration by ID
router.get('/get/mail/configuration', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;

  let connection;

  try {
    connection = await getConnection();

    // Get stores assigned to the user
    const [storeRows] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?',
      [userId]
    );

    const storeIds = storeRows.map(r => r.store_id);

    if (storeIds.length === 0) {
      // User has no stores assigned → no config available
      return res.status(404).json({ success: false, message: 'No stores assigned to this user' });
    }

    // Fetch SMS config for these stores
    const placeholders = storeIds.map(() => '?').join(',');

    const [configs] = await connection.query(
      `SELECT id, store_id, host, port, secure, username, password
       FROM mail_configuration
       WHERE store_id IN (${placeholders})
       ORDER BY id DESC
       LIMIT 1`,
      storeIds
    );

    if (configs.length === 0) {
      return res.status(404).json({ success: false, message: 'No Mail configuration found for your assigned stores' });
    }

    // Return the found config (first one)
    return res.json({ success: true, data: configs[0] });

  } catch (err) {
    console.error('Error fetching Mail configuration:', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});


// Get Mobile Configuration Parameters

// Load one Mobile configuration by ID
router.get('/get/mobile/configuration', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;

  let connection;

  try {
    connection = await getConnection();

    // Get stores assigned to the user
    const [storeRows] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?',
      [userId]
    );

    const storeIds = storeRows.map(r => r.store_id);

    if (storeIds.length === 0) {
      // User has no stores assigned → no config available
      return res.status(404).json({ success: false, message: 'No stores assigned to this user' });
    }

    // Fetch Mobile config for these stores
    const placeholders = storeIds.map(() => '?').join(',');

    const [configs] = await connection.query(
      `SELECT id, store_id, provider, api_url, client_id, client_secret, shortcode, passkey
       FROM mobile_configuration
       WHERE store_id IN (${placeholders})
       ORDER BY id DESC
       LIMIT 1`,
      storeIds
    );

    if (configs.length === 0) {
      return res.status(404).json({ success: false, message: 'No Mobile configuration found for your assigned stores' });
    }

    // Return the found config (first one)
    return res.json({ success: true, data: configs[0] });

  } catch (err) {
    console.error('Error fetching Mobile configuration:', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});


// Load Reset Password Template =====================

router.get('/get/reset/template', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;

  let connection;

  try {
    connection = await getConnection();

    // Get stores assigned to the user
    const [storeRows] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?',
      [userId]
    );

    const storeIds = storeRows.map(r => r.store_id);

    if (storeIds.length === 0) {
      // User has no stores assigned → no config available
      return res.status(404).json({ success: false, message: 'No stores assigned to this user' });
    }

    // stores
    const placeholders = storeIds.map(() => '?').join(',');

    const [configs] = await connection.query(
      `SELECT id, store_id, type, message
       FROM mail_template
       WHERE type = 'RESET' AND store_id IN (${placeholders})
       ORDER BY id DESC
       LIMIT 1`,
      storeIds
    );

    if (configs.length === 0) {
      return res.status(404).json({ success: false, message: 'Not found for your assigned stores' });
    }

    // Return the found config (first one)
    return res.json({ success: true, data: configs[0] });

  } catch (err) {
    console.error('Fetching error:', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});


// Load Purchases Template =====================

router.get('/get/purchases/template', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;

  let connection;

  try {
    connection = await getConnection();

    // Get stores assigned to the user
    const [storeRows] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?',
      [userId]
    );

    const storeIds = storeRows.map(r => r.store_id);

    if (storeIds.length === 0) {
      // User has no stores assigned → no config available
      return res.status(404).json({ success: false, message: 'No stores assigned to this user' });
    }

    // stores
    const placeholders = storeIds.map(() => '?').join(',');

    const [configs] = await connection.query(
      `SELECT id, store_id, type, message
       FROM sms_templates
       WHERE type = 'PURCHASES' AND store_id IN (${placeholders})
       ORDER BY id DESC
       LIMIT 1`,
      storeIds
    );

    if (configs.length === 0) {
      return res.status(404).json({ success: false, message: 'Not found for your assigned stores' });
    }

    // Return the found config (first one)
    return res.json({ success: true, data: configs[0] });

  } catch (err) {
    console.error('Fetching error:', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});


// Load Outstock Template =====================

router.get('/get/outstock/template', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;

  let connection;

  try {
    connection = await getConnection();

    // Get stores assigned to the user
    const [storeRows] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?',
      [userId]
    );

    const storeIds = storeRows.map(r => r.store_id);

    if (storeIds.length === 0) {
      // User has no stores assigned → no config available
      return res.status(404).json({ success: false, message: 'No stores assigned to this user' });
    }

    // stores
    const placeholders = storeIds.map(() => '?').join(',');

    const [configs] = await connection.query(
      `SELECT id, store_id, type, message
       FROM sms_templates
       WHERE type = 'OUTSTOCK' AND store_id IN (${placeholders})
       ORDER BY id DESC
       LIMIT 1`,
      storeIds
    );

    if (configs.length === 0) {
      return res.status(404).json({ success: false, message: 'Not found for your assigned stores' });
    }

    // Return the found config (first one)
    return res.json({ success: true, data: configs[0] });

  } catch (err) {
    console.error('Fetching error:', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});


// Load Sale Summary Template =====================

router.get('/get/salesummary/template', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;

  let connection;

  try {
    connection = await getConnection();

    // Get stores assigned to the user
    const [storeRows] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?',
      [userId]
    );

    const storeIds = storeRows.map(r => r.store_id);

    if (storeIds.length === 0) {
      // User has no stores assigned → no config available
      return res.status(404).json({ success: false, message: 'No stores assigned to this user' });
    }

    // stores
    const placeholders = storeIds.map(() => '?').join(',');

    const [configs] = await connection.query(
      `SELECT id, store_id, type, message
       FROM sms_templates
       WHERE type = 'SALESUMMARY' AND store_id IN (${placeholders})
       ORDER BY id DESC
       LIMIT 1`,
      storeIds
    );

    if (configs.length === 0) {
      return res.status(404).json({ success: false, message: 'Not found for your assigned stores' });
    }

    // Return the found config (first one)
    return res.json({ success: true, data: configs[0] });

  } catch (err) {
    console.error('Fetching error:', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});


// Load Product Price Template =====================

router.get('/get/productprice/template', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;

  let connection;

  try {
    connection = await getConnection();

    // Get stores assigned to the user
    const [storeRows] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?',
      [userId]
    );

    const storeIds = storeRows.map(r => r.store_id);

    if (storeIds.length === 0) {
      // User has no stores assigned → no config available
      return res.status(404).json({ success: false, message: 'No stores assigned to this user' });
    }

    // stores
    const placeholders = storeIds.map(() => '?').join(',');

    const [configs] = await connection.query(
      `SELECT id, store_id, type, message
       FROM sms_templates
       WHERE type = 'PRODUCTPRICE' AND store_id IN (${placeholders})
       ORDER BY id DESC
       LIMIT 1`,
      storeIds
    );

    if (configs.length === 0) {
      return res.status(404).json({ success: false, message: 'Not found for your assigned stores' });
    }

    // Return the found config (first one)
    return res.json({ success: true, data: configs[0] });

  } catch (err) {
    console.error('Fetching error:', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});



// Load Announcement Template =====================

router.get('/get/announcement/template', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;

  let connection;

  try {
    connection = await getConnection();

    // Get stores assigned to the user
    const [storeRows] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?',
      [userId]
    );

    const storeIds = storeRows.map(r => r.store_id);

    if (storeIds.length === 0) {
      // User has no stores assigned → no config available
      return res.status(404).json({ success: false, message: 'No stores assigned to this user' });
    }

    // stores
    const placeholders = storeIds.map(() => '?').join(',');

    const [configs] = await connection.query(
      `SELECT id, store_id, type, message
       FROM mail_template
       WHERE type = 'ANNOUNCEMENT' AND store_id IN (${placeholders})
       ORDER BY id DESC
       LIMIT 1`,
      storeIds
    );

    if (configs.length === 0) {
      return res.status(404).json({ success: false, message: 'Not found for your assigned stores' });
    }

    // Return the found config (first one)
    return res.json({ success: true, data: configs[0] });

  } catch (err) {
    console.error('Fetching error:', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});


// Load Adjustment Template =====================

router.get('/get/adjustments/template', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;

  let connection;

  try {
    connection = await getConnection();

    // Get stores assigned to the user
    const [storeRows] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?',
      [userId]
    );

    const storeIds = storeRows.map(r => r.store_id);

    if (storeIds.length === 0) {
      // User has no stores assigned → no config available
      return res.status(404).json({ success: false, message: 'No stores assigned to this user' });
    }

    // stores
    const placeholders = storeIds.map(() => '?').join(',');

    const [configs] = await connection.query(
      `SELECT id, store_id, type, message
       FROM sms_templates
       WHERE type = 'ADJUSTMENTS' AND store_id IN (${placeholders})
       ORDER BY id DESC
       LIMIT 1`,
      storeIds
    );

    if (configs.length === 0) {
      return res.status(404).json({ success: false, message: 'Not found for your assigned stores' });
    }

    // Return the found config (first one)
    return res.json({ success: true, data: configs[0] });

  } catch (err) {
    console.error('Fetching error:', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});

// M PESA INTEGRATION ==========================

router.post('/registration/new/account/mpesa/stkpush', async (req, res) => {
  const {
    phone,
    amount,
    accountReference = 'Subscription Fee',
    transactionDesc = 'Subscription Payment'
  } = req.body;

  if (!phone || !amount) {
    return res.json({ message: 'Phone number and amount are required' });
  }

  let connection;

  try {
    connection = await getConnection();

    // 1. Get MPESA Config
    const [configRows] = await connection.query(`
      SELECT * FROM mpesa_system_configuration 
      WHERE type = 'MPESA' 
      ORDER BY id DESC LIMIT 1
    `);
    if (!configRows.length) {
      return res.json({ message: 'MPESA configuration not found' });
    }

    const {
      mpesa_base_url,
      username,
      password,
      mpesa_shortcode,
      subscription_key,
      environment_url 
    } = configRows[0];

    // Get Backend Callback URL
    const [backendRows] = await connection.query(`
      SELECT * FROM backend_call_api 
      ORDER BY id DESC LIMIT 1
    `);
    if (!backendRows.length) {
      return res.json({ message: 'Backend URL not found' });
    }

    const { backendUrl } = backendRows[0];
    const stkUrl = `${environment_url}`; // endpoint

    // Generate Access Token
    const auth = Buffer.from(`${username}:${password}`).toString('base64');
    const tokenRes = await axios.get(`${mpesa_base_url}`, {
      headers: {
        Authorization: `Basic ${auth}`
      }
    });

    const accessToken = tokenRes.data.access_token;

    // 4. Prepare STK payload
    const timestamp = moment().format('YYYYMMDDHHmmss');
    const stkPassword = Buffer.from(`${mpesa_shortcode}${subscription_key}${timestamp}`).toString('base64');

    const stkPayload = {
      BusinessShortCode: mpesa_shortcode,
      Password: stkPassword,
      Timestamp: timestamp,
      TransactionType: 'CustomerPayBillOnline',
      Amount: amount,
      PartyA: phone,
      PartyB: mpesa_shortcode,
      PhoneNumber: phone,
      CallBackURL: `${backendUrl}/user/self/signup/mpesa/callback`,
      AccountReference: accountReference,
      TransactionDesc: transactionDesc
    };

    // Send STK Push
    let stkRes;
    try {
      stkRes = await axios.post(stkUrl, stkPayload, {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          'Content-Type': 'application/json'
        }
      });
      console.log('[STK RESPONSE]', stkRes.data);
    } catch (error) {
      console.error('[STK PUSH ERROR]', error.response?.data || error.message);
      return res.status(500).json({
        error: 'STK Push failed',
        details: error.response?.data || error.message
      });
    }

    const {
      MerchantRequestID,
      CheckoutRequestID,
      CustomerMessage,
      ResponseDescription
    } = stkRes.data;

    // Insert transaction to DB
    await connection.query(
      `INSERT INTO subscription_payments 
        (amount, phone, status, checkout_id, merchant_id, created_at) 
       VALUES (?, ?, 'PENDING', ?, ?, NOW())`,
      [amount, phone, CheckoutRequestID, MerchantRequestID]
    );

    // Send success response
    return res.status(200).json({
      message: 'STK Push initiated',
      CustomerMessage,
      ResponseDescription,
      checkout_id: CheckoutRequestID,
      merchant_id: MerchantRequestID
    });

  } catch (err) {
    console.error('[MPESA STK ERROR]', err.response?.data || err.message);
    res.status(500).json({ error: 'Failed to initiate STK Push', details: err.message });
  }
});


// MPESA SIGNUP CALLBACK ==========================

router.post('/user/self/signup/mpesa/callback', async (req, res) => {
  const callbackData = req.body.Body?.stkCallback;

  if (!callbackData) {
    return res.json({ message: 'Invalid callback format' });
  }

  const CheckoutRequestID = callbackData.CheckoutRequestID;
  const ResultCode = callbackData.ResultCode;

  try {
    const conn = await getConnection();

    if (ResultCode === 0) {
      const amount = callbackData.CallbackMetadata?.Item?.find(i => i.Name === 'Amount')?.Value || 0;
      const phone = callbackData.CallbackMetadata?.Item?.find(i => i.Name === 'PhoneNumber')?.Value || '';

      await conn.query(
        `UPDATE subscription_payments 
         SET status = 'SUCCESS', amount = ?, phone = ? 
         WHERE checkout_id = ?`,
        [amount, phone, CheckoutRequestID]
      );
    } else {
      await conn.query(
        `UPDATE subscription_payments 
         SET status = 'FAILED' 
         WHERE checkout_id = ?`,
        [CheckoutRequestID]
      );
    }

    res.status(200).json({ message: 'Callback processed successfully' });
  } catch (err) {
    console.error('Callback Error:', err);
    res.status(500).json({ message: 'Internal server error' });
  }
});


router.get('/self/signup/payment/status/verification/:checkoutId', async (req, res) => {
  const { checkoutId } = req.params;

  try {
    const conn = await getConnection();

    const [rows] = await conn.query(
      `SELECT status FROM subscription_payments WHERE checkout_id = ?`,
      [checkoutId]
    );
    conn.release();

    if (rows.length === 0) {
      return res.status(404).json({ status: 'NOT_FOUND', message: 'Hakuna taarifa ya malipo' });
    }

    res.json({ status: rows[0].status });
  } catch (err) {
    console.error('[MPESA] Error:', err);
    res.status(500).json({ status: 'ERROR', message: 'Tatizo la mfumo' });
  }
});



// Load Signup Template =====================

router.get('/get/signup/template', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;

  let connection;

  try {
    connection = await getConnection();

    // Get stores assigned to the user
    const [storeRows] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?',
      [userId]
    );

    const storeIds = storeRows.map(r => r.store_id);

    if (storeIds.length === 0) {
      // User has no stores assigned → no config available
      return res.status(404).json({ success: false, message: 'No stores assigned to this user' });
    }

    // stores
    const placeholders = storeIds.map(() => '?').join(',');

    const [configs] = await connection.query(
      `SELECT id, store_id, type, message
       FROM mail_template
       WHERE type = 'SIGNUP' AND store_id IN (${placeholders})
       ORDER BY id DESC
       LIMIT 1`,
      storeIds
    );

    if (configs.length === 0) {
      return res.status(404).json({ success: false, message: 'Not found for your assigned stores' });
    }

    // Return the found config (first one)
    return res.json({ success: true, data: configs[0] });

  } catch (err) {
    console.error('Fetching error:', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});


// Load Sales Template =====================

router.get('/get/sales/template', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;

  let connection;

  try {
    connection = await getConnection();

    // Get stores assigned to the user
    const [storeRows] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?',
      [userId]
    );

    const storeIds = storeRows.map(r => r.store_id);

    if (storeIds.length === 0) {
      // User has no stores assigned → no config available
      return res.status(404).json({ success: false, message: 'No stores assigned to this user' });
    }

    // stores
    const placeholders = storeIds.map(() => '?').join(',');

    const [configs] = await connection.query(
      `SELECT id, store_id, type, message
       FROM sms_templates
       WHERE type = 'SALES' AND store_id IN (${placeholders})
       ORDER BY id DESC
       LIMIT 1`,
      storeIds
    );

    if (configs.length === 0) {
      return res.status(404).json({ success: false, message: 'Not found for your assigned stores' });
    }

    // Return the found config (first one)
    return res.json({ success: true, data: configs[0] });

  } catch (err) {
    console.error('Fetching error:', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});



// GET MPESA SYSTEM CONFIGURATION ======================

router.get('/get/system/mpesa/configuration', auth.authenticateToken, async (req, res) => {
  
  let connection;

  try {
    connection = await getConnection();

    const [configs] = await connection.query(
      `SELECT environment_url, environment, id, type, username, password, mpesa_base_url, subscription_key, mpesa_shortcode
       FROM mpesa_system_configuration
       WHERE type = 'MPESA'
       ORDER BY id DESC
       LIMIT 1`
    );

    if (configs.length === 0) {
      return res.status(404).json({ success: false, message: 'No Currency setting found for your assigned stores' });
    }

    // Return the found config (first one)
    return res.json({ success: true, data: configs[0] });

  } catch (err) {
    console.error('Error fetching currency setting:', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});


// Get Currency Setting

// Load one currency setting by ID
router.get('/get/currency/setting', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;

  let connection;

  try {
    connection = await getConnection();

    // Get stores assigned to the user
    const [storeRows] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?',
      [userId]
    );

    const storeIds = storeRows.map(r => r.store_id);

    if (storeIds.length === 0) {
      // User has no stores assigned → no config available
      return res.status(404).json({ success: false, message: 'No stores assigned to this user' });
    }

    // Fetch currency config for these stores
    const placeholders = storeIds.map(() => '?').join(',');

    const [configs] = await connection.query(
      `SELECT id, store_id, name
       FROM currency_setting
       WHERE store_id IN (${placeholders})
       ORDER BY id DESC
       LIMIT 1`,
      storeIds
    );

    if (configs.length === 0) {
      return res.status(404).json({ success: false, message: 'No Currency setting found for your assigned stores' });
    }

    // Return the found config (first one)
    return res.json({ success: true, data: configs[0] });

  } catch (err) {
    console.error('Error fetching currency setting:', err);
    return res.status(500).json({ success: false, message: 'Internal server error'});
  }
});


// Get Bank Configuration Parameters

// Load one Bank configuration by ID
router.get('/get/bank/configuration', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;

  let connection;

  try {
    connection = await getConnection();

    // Get stores assigned to the user
    const [storeRows] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?',
      [userId]
    );

    const storeIds = storeRows.map(r => r.store_id);

    if (storeIds.length === 0) {
      // User has no stores assigned → no config available
      return res.status(404).json({ success: false, message: 'No stores assigned to this user' });
    }

    // Fetch Bank config for these stores
    const placeholders = storeIds.map(() => '?').join(',');

    const [configs] = await connection.query(
      `SELECT id, store_id, bank_name, account_number, branch_name, account_holder_name, swift_code
       FROM bank_configuration
       WHERE store_id IN (${placeholders})
       ORDER BY id DESC
       LIMIT 1`,
      storeIds
    );

    if (configs.length === 0) {
      return res.status(404).json({ success: false, message: 'No Bank configuration found for your assigned stores' });
    }

    // Return the found config (first one)
    return res.json({ success: true, data: configs[0] });

  } catch (err) {
    console.error('Error fetching Bank configuration:', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});


// Get SMS Configuration Parameters

// Load one SMS configuration by ID
router.get('/get/sms/configuration', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;

  let connection;

  try {
    connection = await getConnection();

    // Get stores assigned to the user
    const [storeRows] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?',
      [userId]
    );

    const storeIds = storeRows.map(r => r.store_id);

    if (storeIds.length === 0) {
      // User has no stores assigned → no config available
      return res.status(404).json({ success: false, message: 'No stores assigned to this user' });
    }

    // Fetch SMS config for these stores
    const placeholders = storeIds.map(() => '?').join(',');

    const [configs] = await connection.query(
      `SELECT id, store_id, api_url, sender_name, username, password
       FROM sms_configuration
       WHERE store_id IN (${placeholders})
       ORDER BY id DESC
       LIMIT 1`,
      storeIds
    );

    if (configs.length === 0) {
      return res.status(404).json({ success: false, message: 'No SMS configuration found for your assigned stores' });
    }

    // Return the found config (first one)
    return res.json({ success: true, data: configs[0] });

  } catch (err) {
    console.error('Error fetching SMS configuration:', err);
    return res.status(500).json({ success: false, message: 'Internal server error' });
  }
});


// Add Tra Configuration

// Create or Update Configuration
router.post('/create/tra/configuration', auth.authenticateToken, async (req, res) => {
  const { id, tin, api_url, username, password } = req.body;
  const connection = await getConnection();
  const userId = res.locals.id;

  try {

    const [store] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
    );

    store_id = store[0].store_id;

    if (id) {
      // UPDATE
      const [result] = await connection.execute(
        'UPDATE tra_configuration SET store_id = ?,  tin = ?, api_url = ?, username = ?, password = ? WHERE id = ? AND store_id = ? ',
        [store_id, tin, api_url, username, password, id, store_id]
      );
      res.send({ message: 'Updated successfully', result });
    } else {
      // CREATE
      const [result] = await connection.execute(
        'INSERT INTO tra_configuration (store_id, tin, api_url, username, password) VALUES (?, ?, ?, ?, ?)',
        [store_id, tin, api_url, username, password]
      );
      res.send({ message: 'Tra Configuration created' });
    }
  } catch (err) {
    res.status(500).send({ error: err.message });
  }
});

// Add Mobile Configuration

// Create or Update Configuration
router.post('/create/mobile/configuration', auth.authenticateToken, async (req, res) => {
  const { id, provider, api_url, client_id, client_secret, shortcode, passkey } = req.body;
  const connection = await getConnection();
  const userId = res.locals.id;

  try {

    const [store] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
    );

    store_id = store[0].store_id;

    if (id) {
      // UPDATE
      const [result] = await connection.execute(
        'UPDATE mobile_configuration SET store_id = ?,  provider = ?, api_url = ?, client_id = ?, client_secret = ?, shortcode = ?, passkey = ? WHERE id = ? AND store_id = ? ',
        [store_id, provider, api_url, client_id, client_secret, shortcode, passkey, id, store_id]
      );
      res.send({ message: 'Updated successfully', result });
    } else {
      // CREATE
      const [result] = await connection.execute(
        'INSERT INTO mobile_configuration (store_id, provider, api_url, client_id, client_secret, shortcode, passkey) VALUES (?, ?, ?, ?, ?, ?, ?)',
        [store_id, provider, api_url, client_id, client_secret, shortcode, passkey]
      );
      res.send({ message: 'Mobile Configuration created' });
    }
  } catch (err) {
    res.status(500).send({ error: err.message });
  }
});


// CREATE SIGNUP TEMPLATE =======================

router.post('/create/signup/template', auth.authenticateToken, async (req, res) => {
  const { id, message } = req.body;
  const connection = await getConnection();
  const userId = res.locals.id;

  try {

    const [store] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
    );

    store_id = store[0].store_id;

    if (id) {
      // UPDATE
      const [result] = await connection.execute(
        `UPDATE mail_template SET message = ? WHERE type = 'SIGNUP' AND id = ? AND store_id = ? `,
        [message, id, store_id]
      );
      res.send({ message: 'Updated successfully', result });
    } else {
      // CREATE
      const [result] = await connection.execute(
        'INSERT INTO mail_template (store_id, message, type) VALUES (?, ?, ?)',
        [store_id, message, 'SIGNUP']
      );
      res.send({ message: 'Created success' });
    }
  } catch (err) {
    res.status(500).send({ error: err.message });
  }
});


// CREATE ANNOUNCEMENT TEMPLATE =======================

router.post('/create/announcement/template', auth.authenticateToken, async (req, res) => {
  const { id, message } = req.body;
  const connection = await getConnection();
  const userId = res.locals.id;

  try {

    const [store] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
    );

    store_id = store[0].store_id;

    if (id) {
      // UPDATE
      const [result] = await connection.execute(
        `UPDATE mail_template SET message = ? WHERE type = 'ANNOUNCEMENT' AND id = ? AND store_id = ? `,
        [message, id, store_id]
      );
      res.send({ message: 'Updated successfully', result });
    } else {
      // CREATE
      const [result] = await connection.execute(
        'INSERT INTO mail_template (store_id, message, type) VALUES (?, ?, ?)',
        [store_id, message, 'ANNOUNCEMENT']
      );
      res.send({ message: 'Created success' });
    }
  } catch (err) {
    res.status(500).send({ error: err.message });
  }
});


// CREATE SALES TEMPLATE =======================

router.post('/create/sales/template', auth.authenticateToken, async (req, res) => {
  const { id, message } = req.body;
  const connection = await getConnection();
  const userId = res.locals.id;

  try {

    const [store] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
    );

    store_id = store[0].store_id;

    if (id) {
      // UPDATE
      const [result] = await connection.execute(
        `UPDATE sms_templates SET message = ? WHERE type = 'SALES' AND id = ? AND store_id = ? `,
        [message, id, store_id]
      );
      res.send({ message: 'Updated successfully', result });
    } else {
      // CREATE
      const [result] = await connection.execute(
        'INSERT INTO sms_templates (store_id, message, type) VALUES (?, ?, ?)',
        [store_id, message, 'SALES']
      );
      res.send({ message: 'Created success' });
    }
  } catch (err) {
    res.status(500).send({ error: err.message });
  }
});


// CREATE OUTSTOCK TEMPLATE =======================

router.post('/create/outstock/template', auth.authenticateToken, async (req, res) => {
  const { id, message } = req.body;
  const connection = await getConnection();
  const userId = res.locals.id;

  try {

    const [store] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
    );

    store_id = store[0].store_id;

    if (id) {
      // UPDATE
      const [result] = await connection.execute(
        `UPDATE sms_templates SET message = ? WHERE type = 'OUTSTOCK' AND id = ? AND store_id = ? `,
        [message, id, store_id]
      );
      res.send({ message: 'Updated successfully', result });
    } else {
      // CREATE
      const [result] = await connection.execute(
        'INSERT INTO sms_templates (store_id, message, type) VALUES (?, ?, ?)',
        [store_id, message, 'OUTSTOCK']
      );
      res.send({ message: 'Created success', insertId: result.insertId });
    }
  } catch (err) {
    res.status(500).send({ error: err.message });
  }
});


// CREATE SALE SUMMARY TEMPLATE =======================

router.post('/create/salesummary/template', auth.authenticateToken, async (req, res) => {
  const { id, message } = req.body;
  const connection = await getConnection();
  const userId = res.locals.id;

  try {

    const [store] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
    );

    store_id = store[0].store_id;

    if (id) {
      // UPDATE
      const [result] = await connection.execute(
        `UPDATE sms_templates SET message = ? WHERE type = 'SALESUMMARY' AND id = ? AND store_id = ? `,
        [message, id, store_id]
      );
      res.send({ message: 'Updated successfully', result });
    } else {
      // CREATE
      const [result] = await connection.execute(
        'INSERT INTO sms_templates (store_id, message, type) VALUES (?, ?, ?)',
        [store_id, message, 'SALESUMMARY']
      );
      res.send({ message: 'Created success', insertId: result.insertId });
    }
  } catch (err) {
    res.status(500).send({ error: err.message });
  }
});


// CREATE PRODUCT PRICE TEMPLATE =======================

router.post('/create/productprice/template', auth.authenticateToken, async (req, res) => {
  const { id, message } = req.body;
  const connection = await getConnection();
  const userId = res.locals.id;

  try {

    const [store] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
    );

    store_id = store[0].store_id;

    if (id) {
      // UPDATE
      const [result] = await connection.execute(
        `UPDATE sms_templates SET message = ? WHERE type = 'PRODUCTPRICE' AND id = ? AND store_id = ? `,
        [message, id, store_id]
      );
      res.send({ message: 'Updated successfully', result });
    } else {
      // CREATE
      const [result] = await connection.execute(
        'INSERT INTO sms_templates (store_id, message, type) VALUES (?, ?, ?)',
        [store_id, message, 'PRODUCTPRICE']
      );
      res.send({ message: 'Created success', insertId: result.insertId });
    }
  } catch (err) {
    res.status(500).send({ error: err.message });
  }
});


// CREATE ADJUSTMENTS TEMPLATE =======================

router.post('/create/adjustments/template', auth.authenticateToken, async (req, res) => {
  const { id, message } = req.body;
  const connection = await getConnection();
  const userId = res.locals.id;

  try {

    const [store] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
    );

    store_id = store[0].store_id;

    if (id) {
      // UPDATE
      const [result] = await connection.execute(
        `UPDATE sms_templates SET message = ? WHERE type = 'ADJUSTMENTS' AND id = ? AND store_id = ? `,
        [message, id, store_id]
      );
      res.send({ message: 'Updated successfully', result });
    } else {
      // CREATE
      const [result] = await connection.execute(
        'INSERT INTO sms_templates (store_id, message, type) VALUES (?, ?, ?)',
        [store_id, message, 'ADJUSTMENTS']
      );
      res.send({ message: 'Created success', insertId: result.insertId });
    }
  } catch (err) {
    res.status(500).send({ error: err.message });
  }
});



// CREATE PASSWORD RESET TEMPLATE =======================

router.post('/create/reset/template', auth.authenticateToken, async (req, res) => {
  const { id, message } = req.body;
  const connection = await getConnection();
  const userId = res.locals.id;

  try {

    const [store] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
    );

    store_id = store[0].store_id;

    if (id) {
      // UPDATE
      const [result] = await connection.execute(
        `UPDATE mail_template SET message = ? WHERE type = 'RESET' AND id = ? AND store_id = ? `,
        [message, id, store_id]
      );
      res.send({ message: 'Updated successfully', result });
    } else {
      // CREATE
      const [result] = await connection.execute(
        'INSERT INTO mail_template (store_id, message, type) VALUES (?, ?, ?)',
        [store_id, message, 'RESET']
      );
      res.send({ message: 'Created success', insertId: result.insertId });
    }
  } catch (err) {
    res.status(500).send({ error: err.message });
  }
});


// CREATE PURCHASES TEMPLATE =======================

router.post('/create/purchases/template', auth.authenticateToken, async (req, res) => {
  const { id, message } = req.body;
  const connection = await getConnection();
  const userId = res.locals.id;

  try {

    const [store] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
    );

    store_id = store[0].store_id;

    if (id) {
      // UPDATE
      const [result] = await connection.execute(
        `UPDATE sms_templates SET message = ? WHERE type = 'PURCHASES' AND id = ? AND store_id = ? `,
        [message, id, store_id]
      );
      res.send({ message: 'Updated successfully', result });
    } else {
      // CREATE
      const [result] = await connection.execute(
        'INSERT INTO sms_templates (store_id, message, type) VALUES (?, ?, ?)',
        [store_id, message, 'PURCHASES']
      );
      res.send({ message: 'Created success', insertId: result.insertId });
    }
  } catch (err) {
    res.status(500).send({ error: err.message });
  }
});


// Mpesa System Setting Configuration

router.post('/create/mpesa/system/setting', auth.authenticateToken, async (req, res) => {
  const { id, environment_url, environment, mpesa_shortcode, mpesa_base_url, subscription_key, username, password, } = req.body;
  const connection = await getConnection();

  roleId = res.locals.role;
 
  try {

    if (id) {
      // UPDATE
      const [result] = await connection.execute(
        `UPDATE mpesa_system_configuration SET environment_url = ?, environment = ?, mpesa_base_url = ?, subscription_key = ?, mpesa_shortcode = ?, username = ?, password = ? WHERE id = ?  AND type = 'MPESA' `,
        [	environment_url, environment, mpesa_base_url, subscription_key, mpesa_shortcode, username, password, id]
      );
      res.send({ message: 'Mpesa configuration updated successfully', result });
    } else {
      // CREATE
      const [result] = await connection.execute(
        'INSERT INTO mpesa_system_configuration (environment_url, environment, mpesa_base_url, subscription_key, mpesa_shortcode, username, password, type) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        [environment_url, environment, mpesa_base_url, subscription_key, mpesa_shortcode, username, password, 'MPESA']
      );
      res.send({ message: 'Mpesa setting configuration created', insertId: result.insertId });
    }
  } catch (err) {
    res.status(500).send({ error: err.message });
  }
});


// Create or Update Currency Setting

router.post('/create/currency/setting', auth.authenticateToken, async (req, res) => {
  const { id, name } = req.body;

  console.log(req.body);

  const connection = await getConnection();
  const userId = res.locals.id;

  try {

    const [store] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
    );

    store_id = store[0].store_id;

    if (id) {
      // UPDATE
      const [result] = await connection.execute(
        'UPDATE currency_setting SET store_id = ?, name = ? WHERE id = ? AND store_id = ? ',
        [store_id, name, id, store_id]
      );
      res.send({ message: 'Updated successfully', result });
    } else {
      // CREATE
      const [result] = await connection.execute(
        'INSERT INTO currency_setting (store_id, name) VALUES (?, ?)',
        [store_id, name]
      );
      res.send({ message: 'Currency setting created', insertId: result.insertId });
    }
  } catch (err) {
    res.status(500).send({ error: err.message });
  }
});


// Add Bank Configuration

// Create or Update Configuration
router.post('/create/bank/configuration', auth.authenticateToken, async (req, res) => {
  const { id, bank_name, account_number, branch_name, account_holder_name, swift_code } = req.body;
  const connection = await getConnection();
  const userId = res.locals.id;

  try {

    const [store] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
    );

    store_id = store[0].store_id;

    if (id) {
      // UPDATE
      const [result] = await connection.execute(
        'UPDATE bank_configuration SET swift_code = ?, account_holder_name = ?, branch_name = ?, account_number = ?, bank_name = ? WHERE id = ? AND store_id = ? ',
        [swift_code, account_holder_name, branch_name, account_number, bank_name, id, store_id]
      );
      res.send({ message: 'Updated successfully', result });
    } else {
      // CREATE
      const [result] = await connection.execute(
        'INSERT INTO bank_configuration (store_id, swift_code, account_holder_name, branch_name, account_number, bank_name) VALUES (?, ?, ?, ?, ?, ?)',
        [store_id, swift_code, account_holder_name, branch_name, account_number, bank_name]
      );
      res.send({ message: 'Bank Configuration created', insertId: result.insertId });
    }
  } catch (err) {
    res.status(500).send({ error: err.message });
  }
});



// Create or Update backend BASE URL 

router.post('/create/backend/base/url', auth.authenticateToken, async (req, res) => {
  const { id, backendUrl } = req.body;
  const connection = await getConnection();

  try {

  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

  // Only allow super admins
  if (!isSuperAdmin) {
    return res.status(403).json({ error: 'Access denied. Super admin only.' });
  }

    if (id) {
      // UPDATE
      const [result] = await connection.execute(
        'UPDATE backend_call_api SET backendUrl = ? WHERE id = ?  ',
        [backendUrl, id]
      );
      res.send({ message: 'backend BASE URL Updated successfully', result });
    } else {
      // CREATE
      const [result] = await connection.execute(
        'INSERT INTO backend_call_api (backendUrl) VALUES (?)',
        [backendUrl]
      );
      res.send({ message: 'Backend call base url created successfully !! ', insertId: result.insertId });
    }
  } catch (err) {
    res.status(500).send({ error: err.message });
  }
});


// Add Frontend Base URL 

// Create or Update Frontend BASE URL 

router.post('/create/frontend/base/url', auth.authenticateToken, async (req, res) => {
  const { id, baseUrl } = req.body;
  const connection = await getConnection();

  try {

  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

  // Only allow super admins
  if (!isSuperAdmin) {
    return res.status(403).json({ message: 'Access denied. Super admin only.' });
  }

    if (id) {
      // UPDATE
      const [result] = await connection.execute(
        'UPDATE front_end_base_url SET baseUrl = ? WHERE id = ?  ',
        [baseUrl, id]
      );
      res.send({ message: 'Frontend BASE URL Updated successfully', result });
    } else {
      // CREATE
      const [result] = await connection.execute(
        'INSERT INTO front_end_base_url (baseUrl) VALUES (?)',
        [baseUrl]
      );
      res.send({ message: 'Frontend base url created successfully !! ', insertId: result.insertId });
    }
  } catch (err) {
    res.status(500).send({ error: err.message });
  }
});


// Add System Mail Configuration

// Create or Update System Mail Configuration
router.post('/create/system/mail/configuration', auth.authenticateToken, async (req, res) => {
  const { id, host, port, secure, username, password } = req.body;
  const connection = await getConnection();

  try {

  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

  // Only allow super admins
  if (!isSuperAdmin) {
    return res.status(403).json({ message: 'Access denied. Super admin only.' });
  }

    if (id) {
      // UPDATE
      const [result] = await connection.execute(
        'UPDATE system_mail_configuration SET host = ?, port = ?, username = ?, password = ? WHERE id = ?  ',
        [host, port, username, password, id]
      );
      res.send({ message: 'System Mail Updated successfully', result });
    } else {
      // CREATE
      const [result] = await connection.execute(
        'INSERT INTO system_mail_configuration (host, port, secure, username, password) VALUES (?, ?, ?, ?, ?)',
        [host, port, secure, username, password]
      );
      res.send({ message: 'System Mail Configuration created', insertId: result.insertId });
    }
  } catch (err) {
    res.status(500).send({ error: err.message });
  }
});

// Add Mail Configuration

// Create or Update Configuration
router.post('/create/mail/configuration', auth.authenticateToken, async (req, res) => {
  const { id, host, port, secure, username, password } = req.body;
  const connection = await getConnection();
  const userId = res.locals.id;

  try {

    const [store] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
    );

    store_id = store[0].store_id;

    if (id) {
      // UPDATE
      const [result] = await connection.execute(
        'UPDATE mail_configuration SET host = ?, port = ?, username = ?, password = ? WHERE id = ? AND store_id = ? ',
        [host, port, username, password, id, store_id]
      );
      res.send({ message: 'Updated successfully', result });
    } else {
      // CREATE
      const [result] = await connection.execute(
        'INSERT INTO mail_configuration (store_id, host, port, secure, username, password) VALUES (?, ?, ?, ?, ?, ?)',
        [store_id, host, port, secure, username, password]
      );
      res.send({ message: 'Mail Configuration created', insertId: result.insertId });
    }
  } catch (err) {
    res.status(500).send({ error: err.message });
  }
});

// Add SMS Configuration

// Create or Update Configuration
router.post('/create/sms/configuration', auth.authenticateToken, async (req, res) => {
  const { id, api_url, sender_name, username, password } = req.body;
  const connection = await getConnection();
  const userId = res.locals.id;

  try {

    const [store] = await connection.query(
      'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
    );

    store_id = store[0].store_id;

    if (id) {
      // UPDATE
      const [result] = await connection.execute(
        'UPDATE sms_configuration SET api_url = ?, sender_name = ?, username = ?, password = ? WHERE id = ? AND store_id = ? ',
        [api_url, sender_name, username, password, id, store_id]
      );
      res.send({ message: 'Updated successfully', result });
    } else {
      // CREATE
      const [result] = await connection.execute(
        'INSERT INTO sms_configuration (store_id, api_url, sender_name, username, password) VALUES (?, ?, ?, ?, ?)',
        [store_id, api_url, sender_name, username, password]
      );
      res.send({ message: 'SMS Configuration created', insertId: result.insertId });
    }
  } catch (err) {
    res.status(500).send({ error: err.message });
  }
});


// SEND BULK SMS TO CUSTOMER AND SUPPLIERS ====================

router.post('/send/bulk/sms/customer/supplier', auth.authenticateToken, async (req, res) => {
  const connection = await getConnection();

  try {
    const userId = res.locals.id;
    const { message, sendToCustomers, sendToSuppliers } = req.body;

    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    // Get store ID
    const [stores] = await connection.execute(
      `SELECT store_id FROM user_stores WHERE user_id = ? LIMIT 1`,
      [userId]
    );
    if (!stores.length) {
      return res.status(404).json({ error: 'No store assigned to this user.' });
    }

    const store_id = stores[0].store_id;

    // Get SMS config
    const [smsConfig] = await connection.execute(
      `SELECT * FROM sms_configuration WHERE store_id = ? LIMIT 1`,
      [store_id]
    );
    if (!smsConfig.length) {
      return res.status(404).json({ error: 'No SMS configuration found for this store.' });
    }

    const { api_url, sender_name, username, password } = smsConfig[0];
    const encodedAuth = Buffer.from(`${username}:${password}`).toString('base64');

    let recipients = [];

    if (sendToCustomers) {
      const [customers] = await connection.execute(
        `SELECT name, phone, store_id FROM customers WHERE store_id = ?`,
        [store_id]
      );
      recipients.push(
        ...customers
          .map(c => ({
            name: c.name,
            phone: c.phone || '0',
            store_id: c.store_id
          }))
          .filter(r => r.phone !== '0')
      );
    }

    if (sendToSuppliers) {
      const [suppliers] = await connection.execute(
        `SELECT name, phone, store_id FROM suppliers WHERE store_id = ?`,
        [store_id]
      );
      recipients.push(
        ...suppliers
          .map(s => ({
            name: s.name,
            phone: s.phone || '0',
            store_id: s.store_id
          }))
          .filter(r => r.phone !== '0')
      );
    }

    // Remove duplicates by phone
    const uniqueRecipients = Object.values(
      recipients.reduce((acc, cur) => {
        acc[cur.phone] = cur;
        return acc;
      }, {})
    ).filter(r => r.phone !== '0'); // Final check

    if (!uniqueRecipients.length) {
      return res.status(400).json({ error: 'No valid phone numbers found.' });
    }

    let successCount = 0;
    let failureCount = 0;

    for (const recipient of uniqueRecipients) {
      const personalizedMessage = message.replace(/{{\s*name\s*}}/gi, recipient.name || '');

      const payload = {
        from: sender_name,
        text: personalizedMessage,
        to: recipient.phone
      };

      try {
        await axios.post(api_url, payload, {
          headers: {
            'Authorization': `Basic ${encodedAuth}`,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
          },
          timeout: 10000
        });

        await connection.execute(
          `INSERT INTO sms (store_id, phone, message, status, date) VALUES (?, ?, ?, ?, ?)`,
          [store_id, recipient.phone, personalizedMessage, 'true', now]
        );
        successCount++;
      } catch (err) {
        console.error(`Failed to send to ${recipient.phone}:`, err.message);
        await connection.execute(
          `INSERT INTO sms (store_id, phone, message, status, date) VALUES (?, ?, ?, ?, ?)`,
          [store_id, recipient.phone, personalizedMessage, 'false', now]
        );
        failureCount++;
      }
    }

    return res.json({
      message: 'SMS sending process completed.',
      total: uniqueRecipients.length,
      success: successCount,
      failed: failureCount
    });

  } catch (err) {
    console.error('SMS send error:', err);
    return res.status(500).json({ error: 'Failed to send SMS: ' + err.message });
  }
});




// NORMAL SEND SMS / REMAINDER =========================

router.post('/sms/sendto/multiple/receipts/excel/data', auth.authenticateToken, async (req, res) => {
  const { recipients = [], message } = req.body;
  const userId = res.locals.id;
  const connection = await getConnection();

  const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

  try {
    // Get store_id
    const [storeRow] = await connection.execute(
      `SELECT store_id FROM user_stores WHERE user_id = ? LIMIT 1`, 
      [userId]
    );
    const store_id = storeRow[0]?.store_id;
    if (!store_id) return res.status(404).json({ message: 'Store not found for this user.' });

    // Get SMS configuration
    const [configRow] = await connection.execute(
      `SELECT * FROM sms_configuration WHERE store_id = ? LIMIT 1`, 
      [store_id]
    );
    if (!configRow.length) return res.status(404).json({ message: 'SMS configuration not found.' });

    const { api_url, sender_name, username, password } = configRow[0];
    const encodedAuth = Buffer.from(`${username}:${password}`).toString('base64');

    // Remove duplicates by phone
    const uniqueRecipients = Object.values(
      recipients.reduce((acc, cur) => {
        if (cur.phone) acc[cur.phone] = cur;
        return acc;
      }, {})
    );

    if (!uniqueRecipients.length) {
      return res.status(400).json({ message: 'No valid phone numbers found.' });
    }

    let successCount = 0;
    let failureCount = 0;

    for (const recipient of uniqueRecipients) {
      const personalizedMessage = message.replace(/{{\s*name\s*}}/gi, recipient.name || '');

      const payload = {
        from: sender_name,
        text: personalizedMessage,
        to: recipient.phone
      };

      try {
        await axios.post(api_url, payload, {
          headers: {
            'Authorization': `Basic ${encodedAuth}`,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
          },
          timeout: 10000
        });

        await connection.execute(
          `INSERT INTO sms (store_id, phone, message, status, date) VALUES (?, ?, ?, ?, ?)`,
          [store_id, recipient.phone, personalizedMessage, 'true', now]
        );
        successCount++;
      } catch (err) {
        await connection.execute(
          `INSERT INTO sms (store_id, phone, message, status, date) VALUES (?, ?, ?, ?,?)`,
          [store_id, recipient.phone, personalizedMessage, 'false', now]
        );
        failureCount++;
      }
    }

    res.status(200).json({
      message: 'SMS process completed.',
      total: uniqueRecipients.length,
      successCount,
      failureCount
    });
  } catch (error) {
    console.error('SMS Send Error:', error);
    res.status(500).json({ error: 'Internal server error.' });
  }
});


// Check SMS Balance endpoint
router.get('/checksmsBalance', auth.authenticateToken, async (req, res) => {
  const connection = await getConnection();

  try {

    const userId = res.locals.id; 
    // Get Users Assigned Stors
    const [stores] = await connection.execute(
      `
      SELECT *
      FROM user_stores 
      WHERE user_id = ?
      `, [userId]
    );

    const storeId = stores[0].store_id;
    

    // Get SMS Configuration
    const [smsConfig] = await connection.execute(
      `SELECT * FROM sms_configuration WHERE store_id = ? LIMIT 1`, [storeId]
    );

    const username = smsConfig[0].username;
    const password = smsConfig[0].password;

    // Encode to base64
    const encodedAuth = Buffer.from(`${username}:${password}`).toString('base64');

    // Fetch SMS balance from the external API
    const response = await axios.get('https://messaging-service.co.tz/api/sms/v1/balance', {
      headers: {
        'Authorization': `Basic ${encodedAuth}`,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      }
    });

    // Extract and return SMS balance
    const smsBalance = response.data;

    return res.json({ sms_balance: smsBalance });
  } catch (err) {
    console.error('Error fetching SMS balance:', err.message);
    return res.status(500).json({ message: 'Failed to fetch SMS balance: '  });
  }
});


// Delete Customers Data
router.post('/customers/delete', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;             
 
  try {
    // Wait 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Perform the bulk 
    const placeholders = ids.map(() => '?').join(',');
    const sql = `DELETE FROM customers WHERE id IN (${placeholders})`;
    const connection = await getConnection();
    const [result] = await connection.query(sql, ids);

    res.json({ message: `${result.affectedRows} Deleted successfully! `});
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Delete failed', error: err.message });
  }
});


// Enable Customers Data
router.post('/customers/enable', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;             
 
  try {
    // Wait 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Perform the bulk 
    const placeholders = ids.map(() => '?').join(',');
    const sql = `UPDATE customers SET customer_status = "true" WHERE id IN (${placeholders})`;
    const connection = await getConnection();
    const [result] = await connection.query(sql, ids);

    res.json({ message: `${result.affectedRows} Enabled successfully! `});
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Enable failed', error: err.message });
  }
});


// Disable Customers Data
router.post('/customers/disable', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;             
 

  try {
    // Wait 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Perform the bulk 
    const placeholders = ids.map(() => '?').join(',');
    const sql = `UPDATE customers SET customer_status = "false" WHERE id IN (${placeholders})`;
    const connection = await getConnection();
    const [result] = await connection.query(sql, ids);

    res.json({ message: `${result.affectedRows} Disabled successfully! `});
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Disable failed', error: err.message });
  }
});


// Get Customers by id
router.get('/customers/get/by/:id', auth.authenticateToken, async (req, res) => {
  
  const { id } = req.params;
  const connection = await getConnection();

  try {
    const [rows] = await connection.execute(
      `SELECT * FROM customers WHERE id = ?`,
      [id]
    );

    if (rows.length === 0) {
      return res.status(404).json({ message: 'Customer not found' });
    }

    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ message: 'Failed to fetch', error: err.message });
  }

});


// Update Customer Data
router.put('/customers/update/:id', auth.authenticateToken, async (req, res) => {
  const { id } = req.params;
  const {
    name,
      phone,
      store,
      warehouse
  } = req.body;

  const conn = await getConnection();
  const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
  
  try {
    await sleep(3000); // Simulated delay

      // Update bulk
      await conn.query(
        `UPDATE customers 
         SET name = ?, phone = ?, store_id = ?, warehouse_id = ?
         WHERE id = ?`,
        [
          name, phone, store, warehouse, id
        ]
      );

    res.json({ message: 'Updated successfully' });
  

  } catch (err) {
    console.error('Update failed:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get Suppliers List
router.get('/suppliers/list', auth.authenticateToken, async (req, res) => {
  

  const userId = res.locals.id;
    const roleId = res.locals.role;
    const { storeId, warehouseId } = req.query;
  
  
    let connection;
  
    try {
      connection = await getConnection();
  
      // Base query
      let query = `
        SELECT su.*, 
          s.name AS store_name, s.id AS store_id, 
          w.name AS warehouse_name, w.id AS warehouse_id
        FROM suppliers su
        JOIN stores s ON s.id = su.store_id
        JOIN warehouses w ON w.id = su.warehouse_id
      `;
  
      const params = [];
      const whereConditions = [];
  
      // If the user is not an admin, apply restrictions based on their assigned stores and warehouses
      if (!(roleId === 1 || roleId === '1')) {
        const [storeRows] = await connection.query(
          'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
        );
        const [warehouseRows] = await connection.query(
          'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
        );
  
        const storeIds = storeRows.map(r => r.store_id);
        const warehouseIds = warehouseRows.map(r => r.warehouse_id);
  
        // If user has no stores or warehouses assigned, return an empty response
        if (storeIds.length === 0 && warehouseIds.length === 0) {
          return res.json({ array: [] });
        }
  
        // Conditions for stores and warehouses assigned to the user
        const accessConditions = [];
  
        if (storeIds.length > 0) {
          accessConditions.push(`su.store_id IN (${storeIds.map(() => '?').join(',')})`);
          params.push(...storeIds);
        }
  
        if (warehouseIds.length > 0) {
          accessConditions.push(`su.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
          params.push(...warehouseIds);
        }
  
        if (storeIds.length > 0 && warehouseIds.length > 0) {
          whereConditions.push(`(su.store_id IN (${storeIds.map(() => '?').join(',')}) AND su.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
          params.push(...storeIds, ...warehouseIds);
        } else if (storeIds.length > 0) {
          whereConditions.push(`su.store_id IN (${storeIds.map(() => '?').join(',')})`);
          params.push(...storeIds);
        } else if (warehouseIds.length > 0) {
          whereConditions.push(`su.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
          params.push(...warehouseIds);
        }
  
      }
  
      // Admin doesn't need store/warehouse filters, apply optional filters if passed
      if (storeId) {
        whereConditions.push(`su.store_id = ?`);
        params.push(storeId);
      }
  
      if (warehouseId) {
        whereConditions.push(`su.warehouse_id = ?`);
        params.push(warehouseId);
      }
  
      // If there are any where conditions, add them to the query
      if (whereConditions.length > 0) {
        query += ` WHERE ${whereConditions.join(' AND ')}`;
      }
  
      // Sort the result 
      query += ` ORDER BY su.name ASC`;
  
      const [result] = await connection.query(query, params);
      res.json({ array: result });
  
    } catch (err) {
      console.error('Error fetching:', err);
      res.status(500).json({ message: 'Something went wrong', error: err.message });
    }
  
  
  });
  

  // Update Suppliers Data
router.put('/suppliers/update/:id', auth.authenticateToken, async (req, res) => {
  const { id } = req.params;
  const {
    name,
      phone,
      email,
      store,
      warehouse
  } = req.body;

  const conn = await getConnection();
  const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
  
  try {
    await sleep(3000); // Simulated delay

      // Update bulk
      await conn.query(
        `UPDATE suppliers 
         SET email = ?, name = ?, phone = ?, store_id = ?, warehouse_id = ?
         WHERE id = ?`,
        [
          email, name, phone, store, warehouse, id
        ]
      );

      res.json({ message: 'Updated successfully' });
  

  } catch (err) {
    console.error('Update failed:', err);
    res.status(500).json({ message: 'Internal server error ' });
  }
});



// Get Supplier by id
router.get('/suppliers/get/by/:id', auth.authenticateToken, async (req, res) => {
  
  const { id } = req.params;
  const connection = await getConnection();

  try {
    const [rows] = await connection.execute(
      `SELECT * FROM suppliers WHERE id = ?`,
      [id]
    );

    if (rows.length === 0) {
      return res.status(404).json({ message: 'Supplier not found' });
    }

    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ message: 'Failed to fetch', error: err.message });
  }

});


// Enable Suppliers Data
router.post('/suppliers/enable', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;             
  
  try {
    // Wait 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Perform the bulk 
    const placeholders = ids.map(() => '?').join(',');
    const sql = `UPDATE suppliers SET supplier_status = "true" WHERE id IN (${placeholders})`;
    const connection = await getConnection();
    const [result] = await connection.query(sql, ids);

    res.json({ message: `${result.affectedRows} Enabled successfully! `});
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Enable failed', error: err.message });
  }
});


// Disable Suppliers Data
router.post('/suppliers/disable', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;             
  
  try {
    // Wait 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Perform the bulk 
    const placeholders = ids.map(() => '?').join(',');
    const sql = `UPDATE suppliers SET supplier_status = "false" WHERE id IN (${placeholders})`;
    const connection = await getConnection();
    const [result] = await connection.query(sql, ids);

    res.json({ message: `${result.affectedRows} Disabled successfully! `});
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Disable failed', error: err.message });
  }
});



// Delete Suppliers Data
router.post('/suppliers/delete', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body; 
    
  try {
    // Wait 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Perform the bulk 
    const placeholders = ids.map(() => '?').join(',');
    const sql = `DELETE FROM suppliers WHERE id IN (${placeholders})`;
    const connection = await getConnection();
    const [result] = await connection.query(sql, ids);

    res.json({ message: `${result.affectedRows} Deleted successfully! `});
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Delete failed', error: err.message });
  }
});


// Create Purchase Returns

router.post('/create/purchase/return', auth.authenticateToken, async (req, res) => {
  const connection = await getConnection();
  const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

  try {
    await connection.beginTransaction();

    const { purchase_id, items } = req.body;

    // Fetch purchase record
    const [purchases] = await connection.execute(
      `SELECT * FROM purchases WHERE id = ?`,
      [purchase_id]
    );

    if (purchases.length === 0) {
      await connection.rollback();
      return res.json({ message: "Purchase not found." });
    }

    const { store_id, warehouse_id, user_id } = purchases[0];

    if (user_id === res.locals.id) {
      await connection.rollback();
      return res.json({ message: 'You cannot return your own purchase.' });
    }

    // Get active financial year
    const [fyRows] = await connection.execute(
      `SELECT id FROM fy_cycle WHERE store_id = ? AND isActive = 1 LIMIT 1`,
      [store_id]
    );

    if (fyRows.length === 0) {
      await connection.rollback();
      return res.json({ message: "No active financial year found." });
    }

    const fy_id = fyRows[0].id;
    let return_total = 0;

    // Validate stock and calculate return total
    for (const item of items) {
      const returnQty = Number(item.qty);
      const purchasedStock = Number(item.quantity);
      const cost = Number(item.cost);
    
      if (returnQty > purchasedStock) {
        await connection.rollback();
        return res.json({
          message: `Cannot return ${returnQty} units for product name ${item.product_name}`
        });
      }

      item.subtotal = returnQty * cost;
      return_total += item.subtotal;
    }

    // Insert return header
    const [returnResult] = await connection.execute(`
      INSERT INTO purchase_returns 
      (purchase_id, store_id, warehouse_id, fy_id, return_total, created_at, created_by)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `, [
      purchase_id,
      store_id,
      warehouse_id,
      fy_id,
      return_total,
      now,
      res.locals.name
    ]);

    const return_id = returnResult.insertId;

    // Insert each return item
    for (const item of items) {
      const productId = Number(item.product_id);
      const returnQty = Number(item.qty);
      const cost = Number(item.cost);
      const subtotal = returnQty * cost;
      const purchasedStock = Number(item.quantity);

      const newQty = purchasedStock - returnQty;
      const newSubtotal = cost * newQty;
     
      await connection.execute(`
        INSERT INTO purchase_return_items 
        (cost, return_id, product_id, quantity, return_reason, subtotal)
        VALUES (?, ?, ?, ?, ?, ?)
      `, [
        cost,
        return_id,
        productId,
        returnQty,
        item.return_reason,
        subtotal
      ]);

      // Update stock
      await connection.execute(`
        UPDATE products 
        SET qty = qty - ? 
        WHERE id = ? AND warehouse_id = ?
      `, [returnQty, productId, warehouse_id]);

      // Update purchase items
      await connection.execute(`
        UPDATE purchase_items 
        SET quantity = ?, 
            subtotal = ?
        WHERE purchase_id = ? AND product_id = ?
      `, [newQty, newSubtotal, purchase_id, productId]);
    }

    // Update purchase totals
    await connection.execute(`
      UPDATE purchases 
      SET total = total - ?, 
          grand_total = grand_total - ?
      WHERE id = ?
    `, [return_total, return_total, purchase_id]);

    await connection.commit();
   
    return res.status(200).json({ success: true, message: 'Purchase return saved successfully.' });

  } catch (error) {
    await connection.rollback();
    console.error('Purchase return error:', error);
    return res.status(500).json({ success: false, message: 'Internal server error', error: error.message });
  }
});


// GET CURRENT USER ID ===================================

router.get('/current/logged/id', auth.authenticateToken, (req, res) => {
  return res.json({ userId: res.locals.id }); // or res.locals.user_id
});



// Approve Stock Adjustment =====================

router.post('/approval/adjustment/products/stock', auth.authenticateToken, async (req, res) => {
  const { id } = req.body;
  const approverId = res.locals.id;

  let connection;
  try {
    connection = await getConnection();
    await connection.beginTransaction();
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    // Get role name
    const [[{ name: roleName } = {}]] = await connection.query(
      'SELECT name FROM roles WHERE id = (SELECT role FROM users WHERE id = ?)',
      [approverId]
    );

    if (!roleName) {
      return res.status(403).json({ message: 'Invalid role access.' });
    }

    const isAdmin = roleName === 'ADMIN';
    const isManager = roleName === 'MANAGER';

    if (!isAdmin && !isManager) {
    return res.status(403).json({ message: 'Access denied. Only MANAGER or ADMIN allowed.' });
    }

    const [adjustments] = await connection.query(
      `SELECT * FROM stock_adjustments WHERE id = ?`,
      [id]
    );

    if (adjustments.length === 0) {
      return res.status(404).json({ message: 'Adjustment not found.' });
    }

    const adjustment = adjustments[0];

    if (adjustment.adjust_status === 'APPROVED') {
      return res.status(400).json({ message: 'Adjustment already approved.' });
    }

    if (adjustment.user_id === approverId) {
      return res.status(403).json({ message: 'You cannot approve your own adjustment.' });
    }

    // Get all adjusted items
    const [items] = await connection.query(
      `SELECT * FROM stock_adjustments WHERE id = ?`,
      [id]
    );

    for (const item of items) {
      const { refNumber, qty_adjusted, type } = item;

      // Fetch current quantity
      const [[product]] = await connection.query(
        `SELECT qty FROM products WHERE id = ?`,
        [refNumber]
      );

      if (!product) {
        return res.status(400).json({
          message: `Product with id ${refNumber} not found.`
        });
      }

      const currentQty = product.qty;

      if (type === 'Increase') {
        await connection.query(
          `UPDATE products SET qty = qty + ? WHERE id = ?`,
          [qty_adjusted, refNumber]
        );
      } else if (type === 'Decrease') {
        if (qty_adjusted > currentQty) {
          await connection.rollback();
          return res.status(400).json({
            message: `Cannot decrease stock. Available quantity (${currentQty}) is less than adjusted quantity (${qty_adjusted}) for product ID ${refNumber}.`
          });
        }

        await connection.query(
          `UPDATE products SET qty = qty - ? WHERE id = ?`,
          [qty_adjusted, refNumber]
        );
      }
    }

    await connection.query(
      `UPDATE stock_adjustments SET adjust_status = 'APPROVED' WHERE id = ?`,
      [id]
    );

    await connection.commit();
    res.json({ message: 'Stock adjustment approved and stock updated successfully.' });
  } catch (err) {
    if (connection) await connection.rollback();
    console.error('Approval error:', err);
    res.status(500).json({ message: 'Internal server error.' });
  }
});


// Create stock adjustment

router.post('/products/stock/adjust', auth.authenticateToken, async (req, res) => {
  const payload = req.body; // array of adjustment items

  let connection;
  try {
    connection = await getConnection();
    await connection.beginTransaction();
    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    for (const item of payload) {
      const { id, type, adjustmentQty, updatedStock } = item;

      // Get product info
      const [[prod]] = await connection.query(
        `SELECT store_id, warehouse_id FROM products WHERE id = ?`,
        [id]
      );
      const store_id = prod.store_id;
      const warehouse_id = prod.warehouse_id;

      // Get active financial year
      const [[fyrow]] = await connection.query(
        `SELECT name FROM fy_cycle WHERE isActive=1 AND store_id=?`,
        [store_id]
      );
      const fy = fyrow.name;

      // Insert adjustment as PENDING
      await connection.query(
        `INSERT INTO stock_adjustments
          (user_id, adjust_status, fy, store_id, warehouse_id, refNumber,
           type, qty_adjusted, new_qty, adjusted_by, adjusted_at)
         VALUES (?, 'PENDING', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          res.locals.id,
          fy,
          store_id,
          warehouse_id,
          id,
          type,
          adjustmentQty,
          updatedStock,
          res.locals.name,
          now
        ]
      );

      // Log action
      await connection.query(
        `INSERT INTO logs (user_id, store_id, action, description, createdAt, createdBy)
         VALUES (?, ?, 'CREATE STOCK ADJUSTMENT', ?, ?, ?)`,
        [
          res.locals.id,
          store_id,
          `Ref: ${id}, type: ${type}, adjustQty: ${adjustmentQty}, updated: ${updatedStock} by ${res.locals.name}`,
          now,
          res.locals.name
        ]
      );

      // Fetch SMS template
      const [templateRows] = await connection.execute(
        `SELECT * FROM sms_templates WHERE type = 'ADJUSTMENTS' AND store_id = ? LIMIT 1`,
        [store_id]
      );
      const template = templateRows[0]?.message;

      if (!template) continue;

      // Get store/warehouse info
      const [storeInfoRows] = await connection.execute(
        `SELECT s.id as storeId, s.email as email, w.name as warehousename, s.name as storename, s.phone as ownerPhone
         FROM warehouses w
         JOIN stores s ON s.id = w.storeId
         WHERE w.id = ?`,
        [warehouse_id]
      );
      const storeInfo = storeInfoRows[0];

      if (!storeInfo) continue;

      // Get SMS configuration
      const [smsConfigRows] = await connection.execute(
        `SELECT * FROM sms_configuration WHERE store_id = ? LIMIT 1`,
        [store_id]
      );
      const smsConfig = smsConfigRows[0];

      if (!smsConfig) continue;

      const { api_url, sender_name, username, password } = smsConfig;
      const encodedAuth = Buffer.from(`${username}:${password}`).toString('base64');

      const smsText = template
        .replace('{{store}}', storeInfo.storename)
        .replace('{{warehouse}}', storeInfo.warehousename)
        .replace('{{refNo}}', id)
        .replace('{{qty}}', adjustmentQty)
        .replace('{{total}}', updatedStock)
        .replace('{{username}}', res.locals.name)
        .replace('{{date}}', now);

      const smsPayload = {
        from: sender_name,
        text: smsText,
        to: storeInfo.ownerPhone
      };

      try {
        await axios.post(api_url, smsPayload, {
          headers: {
            'Authorization': `Basic ${encodedAuth}`,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
          },
          timeout: 10000
        });

        await connection.execute(
          `INSERT INTO sms (store_id, phone, message, status, date) VALUES (?, ?, ?, ?, ?)`,
          [store_id, storeInfo.ownerPhone, smsText, 'true', now]
        );
      } catch (err) {
        console.error('OWNER SMS failed:', err.message);
        await connection.execute(
          `INSERT INTO sms (store_id, phone, message, status, date) VALUES (?, ?, ?, ?, ?)`,
          [store_id, storeInfo.ownerPhone, smsText, 'false', now]
        );
      }

 // SENDING EMAIL TO STORE OWNER ==============================

 const [emailConfig] = await connection.execute(
  'SELECT * FROM mail_configuration WHERE store_id = ? LIMIT 1',
  [store_id]
);

if (emailConfig.length === 0) {
  return res.status(500).json({ error: 'Email configuration not found.' });
}

const transporter = nodemailer.createTransport({
  host: emailConfig[0].host,
  port: parseInt(emailConfig[0].port),
  secure: parseInt(emailConfig[0].port) === 465,
  auth: {
    user: emailConfig[0].username,
    pass: emailConfig[0].password
  }
});


try {
  await transporter.sendMail({
    from: emailConfig[0].username,
    to: storeInfo.email,
    subject: 'Product Adjustment',
    text: smsText
  });

  await connection.execute(
    `INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, ?)`,
    [storeInfo.email || '', smsText, now, 'true']
  );

} catch (mailError) {
 
  await connection.execute(
    `INSERT INTO mails (email, message, date, status) VALUES (?, ?, ?, ?)`,
    [storeInfo.email || '', smsText, now, 'false']
  );
}

// looping end here ====================================

    }

    await connection.commit();
    return res.json({ message: 'Stock adjustments created successfully and marked as pending.'});

  } catch (err) {
    if (connection) await connection.rollback();
    console.error(err);
    return res.status(500).json({ message: 'Error creating adjustments', error: err.message });
  }
});


// Create Quotations

router.post('/create/quotation', auth.authenticateToken, async (req, res) => {
  const { payload = [], warehouse } = req.body;

  let connection;
  try {
    connection = await getConnection();
    await connection.beginTransaction();

    const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    // Wait 3 seconds before proceeding
    await new Promise(resolve => setTimeout(resolve, 3000));

    const total = payload.reduce((sum, item) => sum + (item.quantity * item.cost), 0);

    // Get store_id from any product in the warehouse
    const [productrow] = await connection.query(
      `SELECT store_id FROM products WHERE warehouse_id = ? LIMIT 1`,
      [warehouse]
    );

    if (!productrow.length) {
      throw new Error('No products found for the specified warehouse');
    }
    const store_id = productrow[0].store_id;

    // Get Active Financial Year for that store
    const [fyrow] = await connection.query(
      `SELECT name FROM fy_cycle WHERE isActive = 1 AND store_id = ? LIMIT 1`,
      [store_id]
    );

    if (!fyrow.length) {
      throw new Error('Active financial year not found for store');
    }
    const fy = fyrow[0].name;

    // Generate Reference Number
    const datePart = moment(now).format('YYYYMMDD');

    const [[{ count }]] = await connection.execute(
      `SELECT COUNT(*) as count FROM quotations WHERE DATE(date_created) = CURDATE() AND warehouse_id = ?`,
      [warehouse]
    );

    const refNumber = `QU-${datePart}-${String(count + 1).padStart(4, '0')}`;

    // Insert into quotations table
    const [quoteResult] = await connection.query(
      `INSERT INTO quotations (refNumber, store_id, warehouse_id, fy_id, date_created, created_by, total) VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [refNumber, store_id, warehouse, fy, now, res.locals.name, total]
    );

    const quotation_id = quoteResult.insertId;

    for (const item of payload) {
      // Get product ID by refNumber
      const [stockRows] = await connection.query(
        `SELECT id FROM products WHERE refNumber = ? LIMIT 1`,
        [item.refNumber]
      );

      if (!stockRows.length) {
        throw new Error(`Product with refNumber ${item.refNumber} not found`);
      }

      const product_id = stockRows[0].id;

      // Insert quotation item
      await connection.query(
        `INSERT INTO quotation_items (quotation_id, product_id, qty, cost, subtotal) VALUES (?, ?, ?, ?, ?)`,
        [quotation_id, product_id, item.quantity, item.cost, item.subtotal]
      );

      // Insert log
      await connection.query(
        `INSERT INTO logs (user_id, store_id, action, description, createdAt, createdBy) VALUES (?, ?, ?, ?, ?, ?)`,
        [
          res.locals.id,
          store_id,
          'CREATE QUOTATION',
          `Stock Quotation ID: ${quotation_id}, qty: ${item.quantity}, subtotal: ${item.subtotal}, created by: ${res.locals.name}`,
          now,
          res.locals.name
        ]
      );
    }

    await connection.commit();
    res.json({ message: 'Quotation created successfully', quotation_id });
  } catch (err) {
    if (connection) await connection.rollback();
    console.error('Create Quotation error:', err);
    res.status(500).json({ message: 'Failed to create quotation', error: err.message });
  }
});


// Create Items Transfer

router.post('/create/items/transfer', auth.authenticateToken, async (req, res) => {
  const { source, destination, payload } = req.body;

  let connection;

  try {
    connection = await getConnection();
    await connection.beginTransaction();

    // Get warehouse info
    const [warehouserow] = await connection.query(
      `SELECT * FROM warehouses WHERE id = ?`,
      [source]
    );
    if (!warehouserow.length) {
      throw new Error('Source warehouse not found');
    }
    const store_id = warehouserow[0].storeId;

    // Get active financial year
    const [fyrow] = await connection.query(
      `SELECT * FROM fy_cycle WHERE isActive = 1 AND store_id = ?`,
      [store_id]
    );
    if (!fyrow.length) {
      throw new Error('Active financial year not found');
    }
    const fy = fyrow[0].name;

    // Generate reference number
    const datePart = now.toISOString().slice(0, 10).replace(/-/g, '');
    const [[{ count }]] = await connection.execute(
      `SELECT COUNT(*) as count FROM transfers WHERE DATE(created_at) = CURDATE() AND destination_warehouse = ?`,
      [destination]
    );
    const refNumber = `TRAN-${datePart}-${String(count + 1).padStart(4, '0')}`;

    // Insert transfer record
    const [transferResult] = await connection.query(
      `INSERT INTO transfers 
        (store_id, source_warehouse, destination_warehouse, fy, refNumber, created_at, created_by) 
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [store_id, source, destination, fy, refNumber, now, res.locals.name]
    );
    const transferId = transferResult.insertId;

    for (const item of payload) {
      // Get product info by id
      const [stockRows] = await connection.query(
        `SELECT * FROM products WHERE id = ?`,
        [item.id]
      );
      if (!stockRows.length) {
        throw new Error(`Product with id ${item.id} not found`);
      }
      const product = stockRows[0];

      // Subtract stock from source warehouse (consider validating qty >= item.quantity)
      await connection.query(
        `UPDATE products SET qty = qty - ? WHERE id = ?`,
        [item.quantity, product.id]
      );

      // Check if product exists in destination warehouse by name
      const [destRows] = await connection.query(
        `SELECT * FROM products WHERE name = ? AND warehouse_id = ?`,
        [product.name, destination]
      );

      if (destRows.length === 0) {
        // Generate random refNumber for new product in destination
        const newRefNumber = Math.floor(100000000 + Math.random() * 900000000);

        await connection.query(
          `INSERT INTO products (
            store_id, warehouse_id, category_id, brand_id, unit_id, refNumber,
            name, qty, cost, price, imei_serial, expire_date, vat, discount,
            product_create_date, product_create_by, product_update_date, product_update_by,
            product_status, product_qty_alert
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            store_id,
            destination,
            product.category_id || 0,
            product.brand_id || 0,
            product.unit_id || 0,
            newRefNumber,
            product.name,
            item.quantity,
            product.cost || 0,
            product.price || 0,
            product.imei_serial || 'null',
            product.expire_date || 'null',
            product.vat || 0,
            product.discount || 0,
            now,
            res.locals.name,
            'null',
            'null',
            'true',
            0
          ]
        );
      } else {
        // Update qty in destination warehouse
        await connection.query(
          `UPDATE products SET qty = qty + ? WHERE id = ?`,
          [item.quantity, destRows[0].id]
        );
      }

      // Insert into transfer_items
      await connection.query(
        `INSERT INTO transfer_items (transfer_id, product_id, instock, quantity, total) VALUES (?, ?, ?, ?, ?)`,
        [transferId, product.id, item.instock, item.quantity, item.total]
      );

      // Insert log
      await connection.query(
        `INSERT INTO logs (user_id, store_id, action, description, createdAt, createdBy) VALUES (?, ?, ?, ?, ?, ?)`,
        [
          res.locals.id,
          store_id,
          'CREATE ITEM TRANSFER',
          `Stock transfer created: Ref: ${transferId}, qty: ${item.quantity}, product id: ${product.id}, total qty: ${item.total}, by ${res.locals.name}`,
          now,
          res.locals.name
        ]
      );
    }

    await connection.commit();
    return res.status(200).json({ message: 'Transfer submitted successfully ' });
  } catch (error) {
    if (connection) await connection.rollback();
    console.error('Transfer Error:', error);
    return res.status(500).json({ message: 'Failed to process transfer', error: error.message });
  }
});


// Get Items Adjustment Lists

router.get('/get/item/adjustment/lists', auth.authenticateToken, async (req, res) => {
    const userId = res.locals.id;
    const roleId = res.locals.role;
  
    const {
      storeId,
      warehouseId,
      filterType,
      startDate,
      endDate,
      sortBy = 'adjusted_at',
      sortOrder = 'DESC',
    } = req.query;
  
    let connection;
  
    try {
      connection = await getConnection();
  
      let query = `
        SELECT 
          a.id,
          a.store_id,
          a.warehouse_id,
          a.fy,
          a.refNumber,
          a.type,
          a.qty_adjusted,
          a.new_qty,
          a.adjusted_by,
          a.adjusted_at,
          a.user_id,
          a.adjust_status,
          s.name AS storename,
          w.name AS warehousename,
          p.name AS productname
        FROM stock_adjustments a
        JOIN stores s ON s.id = a.store_id
        JOIN warehouses w ON w.id = a.warehouse_id
        JOIN products p ON p.id = a.refNumber
      `;
  
      const params = [];
      const whereConditions = [];
  
      // Role-based filtering
      if (!(roleId === 1 || roleId === '1')) {
        const [storeRows] = await connection.query(
          'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
        );
        const [warehouseRows] = await connection.query(
          'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
        );
  
        const storeIds = storeRows.map(r => r.store_id);
        const warehouseIds = warehouseRows.map(r => r.warehouse_id);
  
        if (storeIds.length === 0 && warehouseIds.length === 0) {
          return res.json({ array: [] });
        }
  
        if (storeIds.length > 0 && warehouseIds.length > 0) {
          whereConditions.push(`(a.store_id IN (${storeIds.map(() => '?').join(',')}) AND a.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
          params.push(...storeIds, ...warehouseIds);
        } else if (storeIds.length > 0) {
          whereConditions.push(`a.store_id IN (${storeIds.map(() => '?').join(',')})`);
          params.push(...storeIds);
        } else if (warehouseIds.length > 0) {
          whereConditions.push(`a.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
          params.push(...warehouseIds);
        }
      }
  
      // Optional filters
      if (storeId) {
        whereConditions.push(`a.store_id = ?`);
        params.push(storeId);
      }
  
      if (warehouseId) {
        whereConditions.push(`a.warehouse_id = ?`);
        params.push(warehouseId);
      }
  
      // Handle filterType logic
      const tz = 'Africa/Nairobi';
      const now = moment().tz(tz);
      let start, end;
  
      if (filterType === 'increase') {
        whereConditions.push(`a.type = 'Increase'`);
      } else if (filterType === 'decrease') {
        whereConditions.push(`a.type = 'Decrease'`);
      } else {
        switch (filterType) {
          case 'today':
            start = now.clone().startOf('day');
            end = now.clone().endOf('day');
            break;
          case 'yesterday':
            start = now.clone().subtract(1, 'day').startOf('day');
            end = now.clone().subtract(1, 'day').endOf('day');
            break;
          case 'week':
            start = now.clone().startOf('week');
            end = now.clone().endOf('week');
            break;
          case 'last_week':
            start = now.clone().subtract(1, 'week').startOf('week');
            end = now.clone().subtract(1, 'week').endOf('week');
            break;
          case 'month':
            start = now.clone().startOf('month');
            end = now.clone().endOf('month');
            break;
          case 'year':
            start = now.clone().startOf('year');
            end = now.clone().endOf('year');
            break;
          case 'last_year':
            start = now.clone().subtract(1, 'year').startOf('year');
            end = now.clone().subtract(1, 'year').endOf('year');
            break;
          case 'last_2_years':
            start = now.clone().subtract(2, 'years').startOf('year');
            end = now.clone().subtract(1, 'year').endOf('year');
            break;
          case 'last_3_years':
            start = now.clone().subtract(3, 'years').startOf('year');
            end = now.clone().subtract(1, 'year').endOf('year');
            break;
          case 'last_4_years':
            start = now.clone().subtract(4, 'years').startOf('year');
            end = now.clone().subtract(1, 'year').endOf('year');
            break;
        }
  
        if (!start && startDate && endDate) {
          start = moment.tz(startDate, tz).startOf('day');
          end = moment.tz(endDate, tz).endOf('day');
        }
  
        if (start && end) {
          whereConditions.push(`a.adjusted_at BETWEEN ? AND ?`);
          params.push(start.format('YYYY-MM-DD HH:mm:ss'), end.format('YYYY-MM-DD HH:mm:ss'));
        }
      }
  
      // Apply WHERE clause
      if (whereConditions.length) {
        query += ` WHERE ${whereConditions.join(' AND ')}`;
      }
  
      // Sorting
      const validSortColumns = ['adjusted_at', 'qty_adjusted', 'new_qty', 'type'];
      const orderBy = validSortColumns.includes(sortBy) ? `a.${sortBy}` : 'a.adjusted_at';
      const orderDir = (sortOrder && sortOrder.toUpperCase() === 'ASC') ? 'ASC' : 'DESC';
  
      query += ` ORDER BY ${orderBy} ${orderDir}`;
  
      const [result] = await connection.query(query, params);
  
      res.json({ array: result });
    } catch (error) {
      console.error('Error fetching item adjustment', error);
      res.status(500).json({ message: 'Something went wrong', error: error.message });
    }
  });


// Delete Items Adjustment Data
router.post('/item/adjustment/delete', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;

  try {
    await new Promise(resolve => setTimeout(resolve, 3000));

    const placeholders = ids.map(() => '?').join(',');
    const connection = await getConnection();

    // Delete
    const [result] = await connection.query(
      `DELETE FROM stock_adjustments WHERE id IN (${placeholders})`,
      ids
    );

    res.json({
      message: `${result.affectedRows} items(s) deleted successfully `
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Delete failed ', error: err.message });
  }
});


// Cancel Items Adjustment Data
router.post('/item/adjustment/cancel', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;

  try {
    await new Promise(resolve => setTimeout(resolve, 3000));

    const placeholders = ids.map(() => '?').join(',');
    const connection = await getConnection();

    // Get stock adjustment data
    const [adjustrow] = await connection.query(
      `SELECT * FROM stock_adjustments WHERE id IN (${placeholders})`,
      ids
    );

    if (adjustrow.length === 0) {
      return res.status(404).json({ message: 'Adjustment not found ' });
    }

    const { type, refNumber, qty_adjusted, warehouse_id } = adjustrow[0];

    if (type === 'Increase') {
      await connection.query(
        `UPDATE products SET qty = qty - ? WHERE id IN (${placeholders}) AND warehouse_id = ?`,
        [qty_adjusted, ...ids, warehouse_id]
      );
    }

    if (type === 'Decrease') {
      await connection.query(
        `UPDATE products SET qty = qty + ? WHERE id IN (${placeholders}) AND warehouse_id = ?`,
        [qty_adjusted, ...ids, warehouse_id]
      );
    }

    // Delete adjustments
    await connection.query(
      `DELETE FROM stock_adjustments WHERE id IN (${placeholders})`,
      ids
    );

    res.json({ message: `Canceled successfully` });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Failed ', error: err.message });
  }
});


// Get Items/stocks Quotation Lists

 router.get('/get/item/quotation/lists', auth.authenticateToken, async (req, res) => {
    const userId = res.locals.id;
    const roleId = res.locals.role;
  
    const {
      storeId,
      warehouseId,
      filterType,
      startDate,
      endDate,
      sortBy = 'date_created',
      sortOrder = 'DESC',
    } = req.query;
  
    let connection;
  
    try {
      connection = await getConnection();
  
      let query = `
        SELECT 
          q.*,
          s.name AS storename,
          s.id AS store_id,
          w.name AS warehousename,
          w.id AS warehouse_id
        FROM quotations q
        JOIN stores s ON s.id = q.store_id
        JOIN warehouses w ON w.id = q.warehouse_id
      `;
  
      const params = [];
      const whereConditions = [];
  
      // Role-based filtering
      if (!(roleId === 1 || roleId === '1')) {
        const [storeRows] = await connection.query(
          'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
        );
        const [warehouseRows] = await connection.query(
          'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
        );
  
        const storeIds = storeRows.map(r => r.store_id);
        const warehouseIds = warehouseRows.map(r => r.warehouse_id);
  
        if (storeIds.length === 0 && warehouseIds.length === 0) {
          return res.json({ array: [] });
        }
  
        if (storeIds.length > 0 && warehouseIds.length > 0) {
          whereConditions.push(`(q.store_id IN (${storeIds.map(() => '?').join(',')}) AND q.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
          params.push(...storeIds, ...warehouseIds);
        } else if (storeIds.length > 0) {
          whereConditions.push(`q.store_id IN (${storeIds.map(() => '?').join(',')})`);
          params.push(...storeIds);
        } else if (warehouseIds.length > 0) {
          whereConditions.push(`q.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
          params.push(...warehouseIds);
        }
      }
  
      if (storeId) {
        whereConditions.push(`q.store_id = ?`);
        params.push(storeId);
      }
      if (warehouseId) {
        whereConditions.push(`q.warehouse_id = ?`);
        params.push(warehouseId);
      }
  
      // Timezone-aware date filtering using moment-timezone
      const nairobiNow = moment.tz('Africa/Nairobi');
      let start, end;
  
      if (filterType) {
        switch (filterType) {
          case 'today':
            start = nairobiNow.clone().startOf('day');
            end = nairobiNow.clone().endOf('day');
            break;
          case 'yesterday':
            start = nairobiNow.clone().subtract(1, 'day').startOf('day');
            end = start.clone().endOf('day');
            break;
          case 'week':
            start = nairobiNow.clone().startOf('week');
            end = nairobiNow.clone().endOf('week');
            break;
          case 'last_week':
            start = nairobiNow.clone().subtract(1, 'week').startOf('week');
            end = start.clone().endOf('week');
            break;
          case 'month':
            start = nairobiNow.clone().startOf('month');
            end = nairobiNow.clone().endOf('month');
            break;
          case 'year':
            start = nairobiNow.clone().startOf('year');
            end = nairobiNow.clone().endOf('year');
            break;
          case 'last_year':
            start = nairobiNow.clone().subtract(1, 'year').startOf('year');
            end = start.clone().endOf('year');
            break;
          case 'last_2_years':
            start = nairobiNow.clone().subtract(2, 'year').startOf('year');
            end = nairobiNow.clone().subtract(1, 'year').endOf('year');
            break;
          case 'last_3_years':
            start = nairobiNow.clone().subtract(3, 'year').startOf('year');
            end = nairobiNow.clone().subtract(1, 'year').endOf('year');
            break;
          case 'last_4_years':
            start = nairobiNow.clone().subtract(4, 'year').startOf('year');
            end = nairobiNow.clone().subtract(1, 'year').endOf('year');
            break;
        }
      } else if (startDate && endDate) {
        start = moment.tz(startDate, 'Africa/Nairobi').startOf('day');
        end = moment.tz(endDate, 'Africa/Nairobi').endOf('day');
      }
  
      if (start && end) {
        whereConditions.push(`q.date_created BETWEEN ? AND ?`);
        params.push(start.format('YYYY-MM-DD HH:mm:ss'), end.format('YYYY-MM-DD HH:mm:ss'));
      }
  
      if (whereConditions.length > 0) {
        query += ` WHERE ${whereConditions.join(' AND ')}`;
      }
  
      // Validate sort column
      const validSortColumns = ['date_created', 'total', 'refNumber', 'store_id', 'warehouse_id'];
      const orderBy = validSortColumns.includes(sortBy) ? `q.${sortBy}` : 'q.date_created';
      const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
  
      query += ` ORDER BY ${orderBy} ${orderDir}`;
  
      const [result] = await connection.query(query, params);
  
      res.json({ array: result });
    } catch (err) {
      console.error('Error fetching quotation lists', err);
      res.status(500).json({ message: 'Something went wrong', error: err.message });
    }
  });



// Generate Product Count =============

router.post('/products/create/product/count', auth.authenticateToken, async (req, res) => {
  let conn;
  try {
    const { store, warehouse } = req.body;
    const dateToday = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

    conn = await getConnection();

    // Optional: Simulate delay (keep if needed)
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Check for existing count on the same date
    const [existing] = await conn.query(
      `SELECT * FROM product_count WHERE counted_at = ? AND warehouse_id = ? AND store_id = ?`,
      [dateToday, warehouse, store]
    );

    if (existing.length > 0) {
      return res.status(409).json({
        message: `Count for "${dateToday}" already exists. Please go to download `
      });
    }

    // Get products for the specified store and warehouse
    const [products] = await conn.query(
      'SELECT * FROM products WHERE store_id = ? AND warehouse_id = ?',
      [store, warehouse]
    );

    if (!products.length) {
      return res.status(404).json({ message: 'No products found for selected warehouse/store.' });
    }

    // Begin transaction
    await conn.beginTransaction();

    // Insert into product_count
    const [countResult] = await conn.execute(
      `INSERT INTO product_count (warehouse_id, store_id, counted_by, counted_at) VALUES (?, ?, ?, ?)`,
      [warehouse, store, res.locals.name, dateToday]
    );

    const productCountId = countResult.insertId;

    // Prepare insert values for product_count_item
    const insertValues = products.map(p => [productCountId, p.id, p.qty]);

    // Insert into product_count_item
    await conn.query(
      'INSERT INTO product_count_item (product_count_id, product_id, total_count_qty) VALUES ?',
      [insertValues]
    );

    // Commit transaction
    await conn.commit();

    return res.status(201).json({
      message: `Product count created successfully `
    });

  } catch (err) {
    if (conn) await conn.rollback();
    console.error('Error:', err);
    res.status(500).json({ error: 'Internal server error!' });
  } 
});



// Get Product Count Lists
router.get('/get/product/count/lists', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const { storeId, warehouseId } = req.query;


  let connection;

  try {
    connection = await getConnection();

    // Base query
    let query = `
      SELECT p.counted_at,p.id, 
        s.name AS storename, s.id AS store_id, 
        w.name AS warehousename, w.id AS warehouse_id
      FROM product_count p
      JOIN stores s ON s.id = p.store_id
      JOIN warehouses w ON w.id = p.warehouse_id
    `;

    const params = [];
    const whereConditions = [];

    // If the user is not an admin, apply restrictions based on their assigned stores and warehouses
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      // If user has no stores or warehouses assigned, return an empty response
      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      // Conditions for stores and warehouses assigned to the user
      const accessConditions = [];

      if (storeIds.length > 0) {
        accessConditions.push(`p.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (warehouseIds.length > 0) {
        accessConditions.push(`p.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }

      if (storeIds.length > 0 && warehouseIds.length > 0) {
        whereConditions.push(`(p.store_id IN (${storeIds.map(() => '?').join(',')}) AND p.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
        params.push(...storeIds, ...warehouseIds);
      } else if (storeIds.length > 0) {
        whereConditions.push(`p.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } else if (warehouseIds.length > 0) {
        whereConditions.push(`p.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
      


    }

    // Admin doesn't need store/warehouse filters, apply optional filters if passed
    if (storeId) {
      whereConditions.push(`p.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`p.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // If there are any where conditions, add them to the query
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sort the result 
    query += ` ORDER BY p.counted_at DESC`;

    const [result] = await connection.query(query, params);
    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});


// Download Product Count Excel
router.get('/get/product/count/download/:id', auth.authenticateToken, async (req, res) => {
  let conn;
  const id = req.params.id;
  try {
    // Get a new connection from the pool
    conn = await getConnection(); // get promise-based connection

    const query = `
  SELECT *
  FROM product_count_item pci
  JOIN products p ON p.id = pci.product_id
  JOIN product_count pc ON pc.id = pci.product_count_id
  WHERE pci.product_count_id = ?
  ORDER BY pci.total_count_qty DESC
`;

const [rows] = await conn.query(query, [id]);

    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Stock Summary');

    worksheet.columns = [
      { header: 'Product Name', key: 'name', width: 30 },
      { header: 'Quantity', key: 'qty', width: 15 }
    ];

    rows.forEach(row => {
      worksheet.addRow({
        name: row.name,
        qty: row.total_count_qty
      });
    });

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename=stock-summary.xlsx');

    await workbook.xlsx.write(res);
    res.end();
  } catch (err) {
    res.status(500).json({ message: 'Error generating Excel file' });
  }
});



// GET transfer items by id
router.get('/get/item/transfers/byId/:id', auth.authenticateToken, async (req, res) => {
  const { id } = req.params;
  const connection = await getConnection();

  try {
    const [rows] = await connection.execute(
      `SELECT t.id, t.product_id, t.quantity as qty,
       p.name as name FROM 
      transfer_items t
      LEFT JOIN products p ON p.id = t.product_id
      WHERE t.transfer_id = ?`,
      [id]
    );

    if (rows.length === 0) {
      return res.status(404).json({ message: 'Transfer not found' });
    }
    res.json(rows);
  } catch (err) {
    res.status(500).json({ message: 'Failed to fetch transfer', error: err.message });
  }
});



// GET quotation items by id
router.get('/get/item/quotation/byId/:id', auth.authenticateToken, async (req, res) => {
  const { id } = req.params;
  const connection = await getConnection();

  try {
    const [rows] = await connection.execute(
      `SELECT q.id, q.cost as cost, q.product_id, q.qty as qty,
      q.subtotal as subtotal, p.name as name FROM 
      quotation_items q
      LEFT JOIN products p ON p.id = q.product_id
      WHERE q.quotation_id = ?`,
      [id]
    );

    if (rows.length === 0) {
      return res.status(404).json({ message: 'Quotation not found' });
    }

    res.json(rows);
  } catch (err) {
    res.status(500).json({ message: 'Failed to fetch quotation', error: err.message });
  }
});



// GET AUDIT LOGS LISTS ================================

router.get('/get/audit/logs/lists', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    storeId,
    filterType,
    startDate,
    endDate,
    sortBy = 'createdAt',
    sortOrder = 'DESC',
  } = req.query;

  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT l.*,
        s.name as storename,
        s.id as store_id
      FROM logs l
      JOIN stores s ON s.id = l.store_id
    `;

    const params = [];
    const whereConditions = [];

    // Role-based filtering
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);

      if (storeIds.length === 0) {
        return res.json({ array: [] });
      }

      whereConditions.push(`l.store_id IN (${storeIds.map(() => '?').join(',')})`);
      params.push(...storeIds);
    }

    // Optional override filters
    if (storeId) {
      whereConditions.push(`l.store_id = ?`);
      params.push(storeId);
    }

    // Timezone-aware date filtering using moment-timezone
    const timezone = 'Africa/Nairobi';
    let start, end;

    if (filterType) {
      switch (filterType) {
        case 'today':
          start = moment.tz(timezone).startOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).endOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'yesterday':
          start = moment.tz(timezone).subtract(1, 'day').startOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'day').endOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'week':
          start = moment.tz(timezone).startOf('week').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).endOf('week').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'last_week':
          start = moment.tz(timezone).subtract(1, 'week').startOf('week').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'week').endOf('week').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'month':
          start = moment.tz(timezone).startOf('month').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).endOf('month').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'year':
          start = moment.tz(timezone).startOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).endOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'last_year':
          start = moment.tz(timezone).subtract(1, 'year').startOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'year').endOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'last_2_years':
          start = moment.tz(timezone).subtract(2, 'years').startOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'year').endOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'last_3_years':
          start = moment.tz(timezone).subtract(3, 'years').startOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'year').endOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'last_4_years':
          start = moment.tz(timezone).subtract(4, 'years').startOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'year').endOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        default:
          // If an unknown filterType, do nothing (or handle accordingly)
          break;
      }
    } else if (startDate && endDate) {
      // convert client supplied dates to UTC
      start = moment.tz(startDate, timezone).startOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
      end = moment.tz(endDate, timezone).endOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
    }

    if (start && end) {
      whereConditions.push(`l.createdAt BETWEEN ? AND ?`);
      params.push(start, end);
    }

    // Apply WHERE conditions if any
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sorting
    const columnMap = {
      createdAt: 'l.createdAt',
    };
    const orderBy = columnMap[sortBy] || 'l.createdAt';
    const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    query += ` ORDER BY ${orderBy} ${orderDir}`;

    // Execute query
    const [result] = await connection.query(query, params);

    res.json({
      array: result,
    });
  } catch (err) {
    console.error('Error fetching audit logs', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});


// GET SENT FAILED SMS LISTS ================================

router.get('/get/sent/failed/sms/lists', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    storeId,
    filterType,
    startDate,
    endDate,
    sortBy = 'date',
    sortOrder = 'DESC',
  } = req.query;

  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT sm.id as id, sm.store_id as store_id, sm.phone as phone,
        sm.message as message, sm.date as date, sm.status as status, sm.times,
        s.name as storename,
        s.id as store_id
      FROM sms sm
      JOIN stores s ON s.id = sm.store_id
    `;

    const params = [];
    const whereConditions = [];

    // Only failed SMS
    whereConditions.push(`sm.status = 'false'`);

    // Role-based filtering
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);

      if (storeIds.length === 0) {
        return res.json({ array: [] });
      }

      whereConditions.push(`sm.store_id IN (${storeIds.map(() => '?').join(',')})`);
      params.push(...storeIds);
    }

    // Optional override filter
    if (storeId) {
      whereConditions.push(`sm.store_id = ?`);
      params.push(storeId);
    }

    // Timezone-aware date filtering using moment-timezone
    const timezone = 'Africa/Nairobi';
    let start, end;

    if (filterType) {
      switch (filterType) {
        case 'today':
          start = moment.tz(timezone).startOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).endOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'yesterday':
          start = moment.tz(timezone).subtract(1, 'day').startOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'day').endOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'week':
          start = moment.tz(timezone).startOf('week').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).endOf('week').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'last_week':
          start = moment.tz(timezone).subtract(1, 'week').startOf('week').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'week').endOf('week').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'month':
          start = moment.tz(timezone).startOf('month').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).endOf('month').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'year':
          start = moment.tz(timezone).startOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).endOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'last_year':
          start = moment.tz(timezone).subtract(1, 'year').startOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'year').endOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'last_2_years':
          start = moment.tz(timezone).subtract(2, 'years').startOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'year').endOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'last_3_years':
          start = moment.tz(timezone).subtract(3, 'years').startOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'year').endOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'last_4_years':
          start = moment.tz(timezone).subtract(4, 'years').startOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'year').endOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          break;
      }
    } else if (startDate && endDate) {
      start = moment.tz(startDate, timezone).startOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
      end = moment.tz(endDate, timezone).endOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
    }

    if (start && end) {
      whereConditions.push(`sm.date BETWEEN ? AND ?`);
      params.push(start, end);
    }

    // Apply WHERE conditions
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sorting
    const columnMap = {
      date: 'sm.date'
    };

    const orderBy = columnMap[sortBy] || 'sm.date';
    const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    query += ` ORDER BY ${orderBy} ${orderDir}`;

    // Execute query
    const [result] = await connection.query(query, params);

    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching failed sent sms ', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});



// GET SENT SUCCESS SMS LISTS ================================

router.get('/get/sent/success/sms/lists', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    storeId,
    filterType,
    startDate,
    endDate,
    sortBy = 'date',
    sortOrder = 'DESC',
  } = req.query;

  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT sm.id as id, sm.store_id as store_id, sm.phone as phone,
        sm.message as message, sm.date as date, sm.status as status, sm.times,
        s.name as storename,
        s.id as store_id
      FROM sms sm
      JOIN stores s ON s.id = sm.store_id
    `;

    const params = [];
    const whereConditions = [];

    // Only successful SMS
    whereConditions.push(`sm.status = 'true'`);

    // Role-based filtering
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);

      if (storeIds.length === 0) {
        return res.json({ array: [] });
      }

      whereConditions.push(`sm.store_id IN (${storeIds.map(() => '?').join(',')})`);
      params.push(...storeIds);
    }

    // Optional override filter
    if (storeId) {
      whereConditions.push(`sm.store_id = ?`);
      params.push(storeId);
    }

    // Timezone-aware date filtering
    const timezone = 'Africa/Nairobi';
    let start, end;

    if (filterType) {
      switch (filterType) {
        case 'today':
          start = moment.tz(timezone).startOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).endOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'yesterday':
          start = moment.tz(timezone).subtract(1, 'day').startOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'day').endOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'week':
          start = moment.tz(timezone).startOf('week').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).endOf('week').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'last_week':
          start = moment.tz(timezone).subtract(1, 'week').startOf('week').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'week').endOf('week').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'month':
          start = moment.tz(timezone).startOf('month').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).endOf('month').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'year':
          start = moment.tz(timezone).startOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).endOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'last_year':
          start = moment.tz(timezone).subtract(1, 'year').startOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'year').endOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'last_2_years':
          start = moment.tz(timezone).subtract(2, 'years').startOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'year').endOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'last_3_years':
          start = moment.tz(timezone).subtract(3, 'years').startOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'year').endOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          break;

        case 'last_4_years':
          start = moment.tz(timezone).subtract(4, 'years').startOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          end = moment.tz(timezone).subtract(1, 'year').endOf('year').utc().format('YYYY-MM-DD HH:mm:ss');
          break;
      }
    } else if (startDate && endDate) {
      start = moment.tz(startDate, timezone).startOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
      end = moment.tz(endDate, timezone).endOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
    }

    if (start && end) {
      whereConditions.push(`sm.date BETWEEN ? AND ?`);
      params.push(start, end);
    }

    // Apply WHERE conditions
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sorting
    const columnMap = {
      date: 'sm.date'
    };

    const orderBy = columnMap[sortBy] || 'sm.date';
    const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    query += ` ORDER BY ${orderBy} ${orderDir}`;

    // Execute query
    const [result] = await connection.query(query, params);

    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching success sent sms ', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});


// Get Sales Return Lists ================================

router.get('/get/sales/return/lists', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    storeId,
    warehouseId,
    filterType,
    startDate,
    endDate,
    sortBy = 'created_at',
    sortOrder = 'DESC',
    timezone = 'Africa/Nairobi',
  } = req.query;

  let connection;

  try {
    connection = await getConnection();

    let baseQuery = `
      FROM sale_returns sr
      JOIN stores s ON s.id = sr.store_id
      JOIN warehouses w ON w.id = sr.warehouse_id
      LEFT JOIN fy_cycle f ON f.id = sr.fy_id
    `;

    const params = [];
    const whereConditions = [];

    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query('SELECT store_id FROM user_stores WHERE user_id = ?', [userId]);
      const [warehouseRows] = await connection.query('SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]);

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [], count: 0, total: 0 });
      }

      if (storeIds.length > 0) {
        whereConditions.push(`sr.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (warehouseIds.length > 0) {
        whereConditions.push(`sr.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    if (storeId) {
      whereConditions.push(`sr.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`sr.warehouse_id = ?`);
      params.push(warehouseId);
    }

    let start, end;
    const now = moment().tz(timezone);

    if (filterType) {
      switch (filterType) {
        case 'today': start = now.clone().startOf('day'); end = now.clone().endOf('day'); break;
        case 'yesterday': start = now.clone().subtract(1, 'day').startOf('day'); end = now.clone().subtract(1, 'day').endOf('day'); break;
        case 'week': start = now.clone().startOf('week'); end = now.clone().endOf('week'); break;
        case 'last_week': start = now.clone().subtract(1, 'week').startOf('week'); end = now.clone().subtract(1, 'week').endOf('week'); break;
        case 'month': start = now.clone().startOf('month'); end = now.clone().endOf('month'); break;
        case 'year': start = now.clone().startOf('year'); end = now.clone().endOf('year'); break;
        case 'last_year': start = now.clone().subtract(1, 'year').startOf('year'); end = now.clone().subtract(1, 'year').endOf('year'); break;
        case 'last_2_years': start = now.clone().subtract(2, 'year').startOf('year'); end = now.clone().subtract(1, 'year').endOf('year'); break;
        case 'last_3_years': start = now.clone().subtract(3, 'year').startOf('year'); end = now.clone().subtract(1, 'year').endOf('year'); break;
        case 'last_4_years': start = now.clone().subtract(4, 'year').startOf('year'); end = now.clone().subtract(1, 'year').endOf('year'); break;
      }
    } else if (startDate && endDate) {
      start = moment.tz(startDate, timezone).startOf('day');
      end = moment.tz(endDate, timezone).endOf('day');
    }

    if (start && end) {
      whereConditions.push(`sr.created_at BETWEEN ? AND ?`);
      params.push(start.format('YYYY-MM-DD HH:mm:ss'), end.format('YYYY-MM-DD HH:mm:ss'));
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    const fullQuery = `
      SELECT sr.*, f.name as fy_id, s.name as storename, s.id as store_id, w.name as warehousename, w.id as warehouse_id
      ${baseQuery}
      ${whereClause}
      ORDER BY sr.${sortBy} ${sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC'}
    `;

    const countQuery = `
      SELECT COUNT(*) as count, COALESCE(SUM(sr.return_total), 0) as total
      ${baseQuery}
      ${whereClause}
    `;

    const [dataRows] = await connection.query(fullQuery, params);
    const [[summary]] = await connection.query(countQuery, params);

    res.json({
      array: dataRows,
      count: summary.count,
      total: summary.total,
    });

  } catch (err) {
    console.error('Error fetching sales returns', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});




// Delete Purchases Return Items =====================

router.post('/delete/purchases/return/items', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;

  try {
    await new Promise(resolve => setTimeout(resolve, 3000));

    const placeholders = ids.map(() => '?').join(',');
    const connection = await getConnection();

    // Bulk Response
    const [result] = await connection.query(
      `DELETE FROM purchase_returns WHERE id IN (${placeholders})`,
      [...ids]
    );

    res.json({
      message: `${result.affectedRows} items(s) deleted successfully `
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Delete failed ', error: err.message });
  }
});


// Get purchases returned items by id =======================================


router.get('/get/purchases/returned/item/byId/:id', auth.authenticateToken, async (req, res) => {
  const { id } = req.params;
  const connection = await getConnection();

  try {
    const [rows] = await connection.execute(
      `SELECT pri.id, pri.cost as cost, pri.product_id, pri.quantity as qty,
      pri.subtotal as subtotal, pri.return_id, pri.return_reason, p.name as name, p.id FROM 
      purchase_return_items pri
      JOIN products p ON p.id = pri.product_id
      JOIN purchase_returns pr ON pr.id = pri.return_id
      WHERE pri.return_id = ?`,
      [id]
    );

    if (rows.length === 0) {
      return res.json({ message: 'Purchases not found' });
    }

    res.json(rows);
  } catch (err) {
    res.status(500).json({ message: 'Failed to fetch sales', error: err.message });
  }
});




// Get Purchase Return Lists ================================

router.get('/get/purchases/return/lists', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    storeId,
    warehouseId,
    filterType,
    startDate,
    endDate,
    sortBy = 'created_at',
    sortOrder = 'DESC',
    timezone = 'Africa/Nairobi',
  } = req.query;

  let connection;

  try {
    connection = await getConnection();

    let baseQuery = `
      FROM purchase_returns pr
      JOIN stores s ON s.id = pr.store_id
      JOIN warehouses w ON w.id = pr.warehouse_id
      LEFT JOIN fy_cycle f ON f.id = pr.fy_id
    `;

    const params = [];
    const whereConditions = [];

    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query('SELECT store_id FROM user_stores WHERE user_id = ?', [userId]);
      const [warehouseRows] = await connection.query('SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]);

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [], count: 0, total: 0 });
      }

      if (storeIds.length > 0) {
        whereConditions.push(`pr.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (warehouseIds.length > 0) {
        whereConditions.push(`pr.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    if (storeId) {
      whereConditions.push(`pr.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`pr.warehouse_id = ?`);
      params.push(warehouseId);
    }

    let start, end;
    const now = moment().tz(timezone);

    if (filterType) {
      switch (filterType) {
        case 'today': start = now.clone().startOf('day'); end = now.clone().endOf('day'); break;
        case 'yesterday': start = now.clone().subtract(1, 'day').startOf('day'); end = now.clone().subtract(1, 'day').endOf('day'); break;
        case 'week': start = now.clone().startOf('week'); end = now.clone().endOf('week'); break;
        case 'last_week': start = now.clone().subtract(1, 'week').startOf('week'); end = now.clone().subtract(1, 'week').endOf('week'); break;
        case 'month': start = now.clone().startOf('month'); end = now.clone().endOf('month'); break;
        case 'year': start = now.clone().startOf('year'); end = now.clone().endOf('year'); break;
        case 'last_year': start = now.clone().subtract(1, 'year').startOf('year'); end = now.clone().subtract(1, 'year').endOf('year'); break;
        case 'last_2_years': start = now.clone().subtract(2, 'year').startOf('year'); end = now.clone().subtract(1, 'year').endOf('year'); break;
        case 'last_3_years': start = now.clone().subtract(3, 'year').startOf('year'); end = now.clone().subtract(1, 'year').endOf('year'); break;
        case 'last_4_years': start = now.clone().subtract(4, 'year').startOf('year'); end = now.clone().subtract(1, 'year').endOf('year'); break;
      }
    } else if (startDate && endDate) {
      start = moment.tz(startDate, timezone).startOf('day');
      end = moment.tz(endDate, timezone).endOf('day');
    }

    if (start && end) {
      whereConditions.push(`pr.created_at BETWEEN ? AND ?`);
      params.push(start.format('YYYY-MM-DD HH:mm:ss'), end.format('YYYY-MM-DD HH:mm:ss'));
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    const fullQuery = `
      SELECT pr.*, f.name as fy_id, s.name as storename, s.id as store_id, w.name as warehousename, w.id as warehouse_id
      ${baseQuery}
      ${whereClause}
      ORDER BY pr.${sortBy} ${sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC'}
    `;

    const countQuery = `
      SELECT COUNT(*) as count, COALESCE(SUM(pr.return_total), 0) as total
      ${baseQuery}
      ${whereClause}
    `;

    const [dataRows] = await connection.query(fullQuery, params);
    const [[summary]] = await connection.query(countQuery, params);

    res.json({
      array: dataRows,
      count: summary.count,
      total: summary.total,
    });

  } catch (err) {
    console.error('Error fetching purchase returns', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});







// Get Today Sales Lists ======================================

router.get('/get/today/sales/lists', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  let connection;
  try {
    connection = await getConnection();

    const start = new Date();
    start.setHours(0, 0, 0, 0);
    const end = new Date();
    end.setHours(23, 59, 59, 999);

    let query = `
      SELECT 
        sa.*,
        sa.grand_total AS grand_total,
        sa.total_cost AS total_cost,
        (sa.grand_total - sa.total_cost) AS total_profit,
        ROUND(
          CASE 
            WHEN sa.grand_total > 0 THEN 
              ((sa.grand_total - sa.total_cost) / sa.grand_total) * 100
            ELSE 0
          END, 2
        ) AS total_profit_margin,
        f.name AS fy_id,
        s.name AS storename,
        s.id AS store_id,
        w.name AS warehousename,
        s.phone AS phoneNo,
        s.email AS emailId,
        s.district_id AS district_id,
        d.name AS districtname,
        w.id AS warehouse_id
      FROM sales sa
      JOIN stores s ON s.id = sa.store_id
      JOIN warehouses w ON w.id = sa.warehouse_id
      JOIN districts d ON d.id = s.district_id
      LEFT JOIN fy_cycle f ON f.id = sa.fy_id
      WHERE sa.created_at BETWEEN ? AND ?
    `;

    const params = [start, end];

    // Role-based filtering
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [], total_rows: 0, grand_total_sum: 0 });
      }

      if (storeIds.length > 0 && warehouseIds.length > 0) {
        query += ` AND (sa.store_id IN (${storeIds.map(() => '?').join(',')}) AND sa.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`;
        params.push(...storeIds, ...warehouseIds);
      } else if (storeIds.length > 0) {
        query += ` AND sa.store_id IN (${storeIds.map(() => '?').join(',')})`;
        params.push(...storeIds);
      } else if (warehouseIds.length > 0) {
        query += ` AND sa.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`;
        params.push(...warehouseIds);
      }
    }

    query += ` ORDER BY sa.created_at DESC`;

    const [result] = await connection.query(query, params);

    // Calculate totals
    const total_rows = result.length;
    const grand_total_sum = result.reduce((sum, row) => sum + (parseFloat(row.grand_total) || 0), 0);

    return res.json({
      array: result,
      total_rows,
      grand_total_sum: parseFloat(grand_total_sum.toFixed(2))
    });

  } catch (err) {
    console.error(' Error fetching today sales:', err);
    return res.status(500).json({ message: 'Server error', error: err.message });
  }
});



// GET DATA REQUESTED FOR CANCEL

router.get('/get/cancel/request/sales/lists', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    storeId,
    warehouseId,
    filterType,
    startDate,
    endDate,
    sortBy = 'created_at',
    sortOrder = 'DESC',
  } = req.query;

  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT 
        sa.*,
        sa.grand_total AS grand_total,
        sa.total_cost AS total_cost,
        (sa.grand_total - sa.total_cost) AS total_profit,
        ROUND(
          CASE 
            WHEN sa.grand_total > 0 THEN 
              ((sa.grand_total - sa.total_cost) / sa.grand_total) * 100
            ELSE 
              0
          END, 2
        ) AS total_profit_margin,

        f.name AS fy_id,
        s.name AS storename,
        s.id AS store_id,
        w.name AS warehousename,
        s.phone AS phoneNo,
        s.email AS emailId,
        s.district_id AS district_id,
        d.name AS districtname,
        w.id AS warehouse_id

      FROM sales sa
      JOIN stores s ON s.id = sa.store_id
      JOIN warehouses w ON w.id = sa.warehouse_id
      JOIN districts d ON d.id = s.district_id
      LEFT JOIN fy_cycle f ON f.id = sa.fy_id
    `;

    const params = [];
    const whereConditions = [];

    // Role-based access filtering
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [], total_rows: 0, grand_total_sum: 0 });
      }

      if (storeIds.length > 0 && warehouseIds.length > 0) {
        whereConditions.push(`(sa.store_id IN (${storeIds.map(() => '?').join(',')}) AND sa.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
        params.push(...storeIds, ...warehouseIds);
      } else if (storeIds.length > 0) {
        whereConditions.push(`sa.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } else if (warehouseIds.length > 0) {
        whereConditions.push(`sa.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    // Direct filters by store or warehouse
    if (storeId) {
      whereConditions.push(`sa.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`sa.warehouse_id = ?`);
      params.push(warehouseId);
    }

    const today = new Date();

    // Date filters based on filterType
    if (filterType) {
      const addDateRangeCondition = (start, end) => {
        whereConditions.push(`sa.created_at BETWEEN ? AND ?`);
        params.push(start, end);
      };

      switch (filterType) {
        case 'today': {
          const start = new Date(today);
          start.setHours(0, 0, 0, 0);
          const end = new Date(today);
          end.setHours(23, 59, 59, 999);
          addDateRangeCondition(start, end);
          break;
        }
        case 'yesterday': {
          const start = new Date(today);
          start.setDate(start.getDate() - 1);
          start.setHours(0, 0, 0, 0);
          const end = new Date(start);
          end.setHours(23, 59, 59, 999);
          addDateRangeCondition(start, end);
          break;
        }
        case 'week': {
          const start = new Date(today);
          start.setDate(start.getDate() - start.getDay());
          start.setHours(0, 0, 0, 0);
          const end = new Date(start);
          end.setDate(start.getDate() + 6);
          end.setHours(23, 59, 59, 999);
          addDateRangeCondition(start, end);
          break;
        }
        case 'last_week': {
          const start = new Date(today);
          start.setDate(start.getDate() - start.getDay() - 7);
          start.setHours(0, 0, 0, 0);
          const end = new Date(start);
          end.setDate(start.getDate() + 6);
          end.setHours(23, 59, 59, 999);
          addDateRangeCondition(start, end);
          break;
        }
        case 'month': {
          const start = new Date(today.getFullYear(), today.getMonth(), 1);
          const end = new Date(today.getFullYear(), today.getMonth() + 1, 0);
          end.setHours(23, 59, 59, 999);
          addDateRangeCondition(start, end);
          break;
        }
        case 'year': {
          const start = new Date(today.getFullYear(), 0, 1);
          const end = new Date(today.getFullYear(), 11, 31);
          end.setHours(23, 59, 59, 999);
          addDateRangeCondition(start, end);
          break;
        }
        case 'last_year': {
          const start = new Date(today.getFullYear() - 1, 0, 1);
          const end = new Date(today.getFullYear() - 1, 11, 31);
          end.setHours(23, 59, 59, 999);
          addDateRangeCondition(start, end);
          break;
        }
        case 'last_2_years':
        case 'last_3_years':
        case 'last_4_years': {
          const yearsBack = parseInt(filterType.split('_')[1], 10);
          if (!isNaN(yearsBack)) {
            const start = new Date(today.getFullYear() - yearsBack, 0, 1);
            const end = new Date(today.getFullYear() - 1, 11, 31);
            end.setHours(23, 59, 59, 999);
            addDateRangeCondition(start, end);
          }
          break;
        }
      }
    } else if (startDate && endDate) {
      const start = new Date(startDate);
      start.setHours(0, 0, 0, 0);
      const end = new Date(endDate);
      end.setHours(23, 59, 59, 999);
      whereConditions.push(`sa.created_at BETWEEN ? AND ?`);
      params.push(start, end);
    }

    // Default to 'AWAIT' status if no filterType or date filter provided
    if (!filterType && !(startDate && endDate)) {
      whereConditions.push(`sa.sale_status = 'AWAIT'`);
    }

    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    const columnMap = {
      created_at: 'sa.created_at'
    };

    const orderBy = columnMap[sortBy] || 'sa.created_at';
    const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    query += ` ORDER BY ${orderBy} ${orderDir}`;

    const [result] = await connection.query(query, params);

    const totalRows = result.length;
    const grandTotalSum = result.reduce((sum, row) => sum + parseFloat(row.grand_total || 0), 0);

    res.json({
      array: result,
      total_rows: totalRows,
      grand_total_sum: parseFloat(grandTotalSum.toFixed(2))
    });

  } catch (err) {
    console.error('Error fetching cancel request sales', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});




// Get Pending Sales Lists ======================================


router.get('/get/pending/sales/lists', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    storeId,
    warehouseId,
    filterType,
    startDate,
    endDate,
    sortBy = 'created_at',
    sortOrder = 'DESC',
    timezone = 'Africa/Nairobi',
  } = req.query;

  let connection;

  try {
    connection = await getConnection();

    const baseQuery = `
      FROM sales sa
      JOIN stores s ON s.id = sa.store_id
      JOIN warehouses w ON w.id = sa.warehouse_id
      JOIN districts d ON d.id = s.district_id
      LEFT JOIN fy_cycle f ON f.id = sa.fy_id
    `;

    const whereConditions = [`sa.sale_status = 'DRAFT'`]; // ✅ Enforce only  DRAFT SALES
    const params = [];

    // Role-based restrictions
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query('SELECT store_id FROM user_stores WHERE user_id = ?', [userId]);
      const [warehouseRows] = await connection.query('SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]);

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [], total_rows: 0, grand_total_sum: 0 });
      }

      if (storeIds.length && warehouseIds.length) {
        whereConditions.push(`(sa.store_id IN (${storeIds.map(() => '?').join(',')}) AND sa.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
        params.push(...storeIds, ...warehouseIds);
      } else if (storeIds.length) {
        whereConditions.push(`sa.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } else if (warehouseIds.length) {
        whereConditions.push(`sa.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    if (storeId) {
      whereConditions.push(`sa.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`sa.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // Timezone-aware date filtering
    let start, end;
    const now = moment().tz(timezone);

    switch (filterType) {
      case 'today':
        start = now.clone().startOf('day');
        end = now.clone().endOf('day');
        break;
      case 'yesterday':
        start = now.clone().subtract(1, 'day').startOf('day');
        end = now.clone().subtract(1, 'day').endOf('day');
        break;
      case 'week':
        start = now.clone().startOf('week');
        end = now.clone().endOf('week');
        break;
      case 'last_week':
        start = now.clone().subtract(1, 'week').startOf('week');
        end = now.clone().subtract(1, 'week').endOf('week');
        break;
      case 'month':
        start = now.clone().startOf('month');
        end = now.clone().endOf('month');
        break;
      case 'year':
        start = now.clone().startOf('year');
        end = now.clone().endOf('year');
        break;
      case 'last_year':
        start = now.clone().subtract(1, 'year').startOf('year');
        end = now.clone().subtract(1, 'year').endOf('year');
        break;
      case 'last_2_years':
      case 'last_3_years':
      case 'last_4_years':
        const yearsBack = Number(filterType.split('_')[1]);
        start = now.clone().subtract(yearsBack, 'year').startOf('year');
        end = now.clone().subtract(1, 'year').endOf('year');
        break;
    }

    if (startDate && endDate) {
      start = moment.tz(startDate, timezone).startOf('day');
      end = moment.tz(endDate, timezone).endOf('day');
    }

    if (start && end) {
      whereConditions.push(`sa.created_at BETWEEN ? AND ?`);
      params.push(start.format('YYYY-MM-DD HH:mm:ss'), end.format('YYYY-MM-DD HH:mm:ss'));
    }

    const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

    // Sort safety
    const allowedSortColumns = ['created_at', 'grand_total', 'total_cost', 'id'];
    const orderBy = allowedSortColumns.includes(sortBy) ? `sa.${sortBy}` : 'sa.created_at';
    const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';

    const query = `
      SELECT
        sa.*,
        sa.grand_total,
        sa.total_cost,
        (sa.grand_total - sa.total_cost) AS total_profit,
        ROUND(
          CASE WHEN sa.grand_total > 0 THEN ((sa.grand_total - sa.total_cost) / sa.grand_total) * 100 ELSE 0 END, 2
        ) AS total_profit_margin,
        f.name AS fy_id,
        s.name AS storename,
        s.id AS store_id,
        w.name AS warehousename,
        s.phone AS phoneNo,
        s.email AS emailId,
        s.district_id AS district_id,
        d.name AS districtname,
        w.id AS warehouse_id
      ${baseQuery}
      ${whereClause}
      ORDER BY ${orderBy} ${orderDir}
    `;

    const [result] = await connection.query(query, params);

    const summaryQuery = `
      SELECT
        COUNT(*) AS total_rows,
        IFNULL(SUM(sa.grand_total), 0) AS grand_total_sum
      ${baseQuery}
      ${whereClause}
    `;
    const [summary] = await connection.query(summaryQuery, params);

    res.json({
      array: result,
      total_rows: summary[0].total_rows,
      grand_total_sum: parseFloat(summary[0].grand_total_sum)
    });

  } catch (err) {
    console.error('Error fetching pending sales', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});



// Get Sales Lists ======================================

router.get('/get/sales/lists', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    storeId,
    warehouseId,
    filterType,
    startDate,
    endDate,
    sortBy = 'created_at',
    sortOrder = 'DESC',
    timezone = 'Africa/Nairobi',
  } = req.query;

  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT 
        sa.*,
        sa.grand_total AS grand_total,
        sa.total_cost AS total_cost,
        (sa.grand_total - sa.total_cost) AS total_profit,
        ROUND(
          CASE 
            WHEN sa.grand_total > 0 THEN 
              ((sa.grand_total - sa.total_cost) / sa.grand_total) * 100
            ELSE 0
          END, 2
        ) AS total_profit_margin,
        f.name AS fy_id,
        s.name AS storename,
        s.id AS store_id,
        w.name AS warehousename,
        s.phone AS phoneNo,
        s.email AS emailId,
        s.district_id AS district_id,
        d.name AS districtname,
        w.id AS warehouse_id
      FROM sales sa
      JOIN stores s ON s.id = sa.store_id
      JOIN warehouses w ON w.id = sa.warehouse_id
      JOIN districts d ON d.id = s.district_id
      LEFT JOIN fy_cycle f ON f.id = sa.fy_id
    `;

    const params = [];
    const whereConditions = [];

    // Role-based access control
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query('SELECT store_id FROM user_stores WHERE user_id = ?', [userId]);
      const [warehouseRows] = await connection.query('SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]);

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [], total_rows: 0, grand_total_sum: 0 });
      }

      if (storeIds.length > 0) {
        whereConditions.push(`sa.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }
      if (warehouseIds.length > 0) {
        whereConditions.push(`sa.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    if (storeId) {
      whereConditions.push(`sa.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`sa.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // Timezone-based date filtering
    let start, end;
    const now = moment().tz(timezone);

    if (filterType) {
      switch (filterType) {
        case 'approved':
          whereConditions.push(`sa.sale_status = 'APPROVED'`);
          break;
        case 'draft':
          whereConditions.push(`sa.sale_status = 'DRAFT'`);
          break;
        case 'today':
          start = now.clone().startOf('day').format('YYYY-MM-DD HH:mm:ss');
          end = now.clone().endOf('day').format('YYYY-MM-DD HH:mm:ss');
          break;
        case 'yesterday':
          start = now.clone().subtract(1, 'days').startOf('day').format('YYYY-MM-DD HH:mm:ss');
          end = now.clone().subtract(1, 'days').endOf('day').format('YYYY-MM-DD HH:mm:ss');
          break;
        case 'week':
          start = now.clone().startOf('week').format('YYYY-MM-DD HH:mm:ss');
          end = now.clone().endOf('week').format('YYYY-MM-DD HH:mm:ss');
          break;
        case 'month':
          start = now.clone().startOf('month').format('YYYY-MM-DD HH:mm:ss');
          end = now.clone().endOf('month').format('YYYY-MM-DD HH:mm:ss');
          break;
        case 'year':
          start = now.clone().startOf('year').format('YYYY-MM-DD HH:mm:ss');
          end = now.clone().endOf('year').format('YYYY-MM-DD HH:mm:ss');
          break;
      }
    } else if (startDate && endDate) {
      start = moment.tz(startDate, timezone).startOf('day').format('YYYY-MM-DD HH:mm:ss');
      end = moment.tz(endDate, timezone).endOf('day').format('YYYY-MM-DD HH:mm:ss');
    }

    if (start && end) {
      whereConditions.push(`sa.created_at BETWEEN ? AND ?`);
      params.push(start, end);
    }

    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    const allowedSortColumns = ['created_at', 'grand_total', 'total_cost', 'id'];
    const orderBy = allowedSortColumns.includes(sortBy) ? `sa.${sortBy}` : 'sa.created_at';
    const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';

    query += ` ORDER BY ${orderBy} ${orderDir}`;

    const [result] = await connection.query(query, params);

    // Summary query
    const summaryQuery = `
      SELECT 
        COUNT(*) AS total_rows,
        IFNULL(SUM(sa.grand_total), 0) AS grand_total_sum
      FROM sales sa
      ${whereConditions.length ? 'WHERE ' + whereConditions.join(' AND ') : ''}
    `;
    const [summaryResult] = await connection.query(summaryQuery, params);
    const summary = summaryResult[0];

    res.json({
      array: result,
      total_rows: summary.total_rows,
      grand_total_sum: parseFloat(summary.grand_total_sum)
    });

  } catch (err) {
    console.error('Error fetching sales', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});

// GET sales returned items by id ==============================

router.get('/get/sales/returned/item/byId/:id', auth.authenticateToken, async (req, res) => {
  const { id } = req.params;
  const connection = await getConnection();

  try {
    const [rows] = await connection.execute(
      `SELECT sri.id, sri.price as price, sri.product_id, sri.quantity as qty,
      sri.subtotal as subtotal, sri.return_id, sri.return_reason, p.name as name, p.id FROM 
      sale_return_items sri
      JOIN products p ON p.id = sri.product_id
      JOIN sale_returns sr ON sr.id = sri.return_id
      WHERE sri.return_id = ?`,
      [id]
    );

    if (rows.length === 0) {
      return res.status(404).json({ message: 'Sales not found' });
    }

    res.json(rows);
  } catch (err) {
    res.status(500).json({ message: 'Failed to fetch sales', error: err.message });
  }
});


// GET sales items by sale date (created_at) ==========================
router.get('/get/sales/item/by/date/:date', auth.authenticateToken, async (req, res) => {
  const { date } = req.params;

  try {
    const connection = await getConnection();

    const [rows] = await connection.execute(
      `
      SELECT 
    si.id, si.vat, si.discount, si.price, si.cost, si.totalCost, 
    si.product_id, si.quantity AS qty, si.subtotal, si.sale_id,
    p.name AS name, p.id AS product_id
  FROM sale_items si
  LEFT JOIN products p ON p.id = si.product_id
  WHERE si.sale_id = ?

      `,
      [id]
    );

    if (!rows || rows.length === 0) {
      return res.status(404).json({ success: false, message: 'No sale items found for this sale ID.' });
    }

    res.json(rows);

  } catch (err) {
    console.error('Error fetching sale items by ID:', err);
    return res.status(500).json({
      success: false,
      message: 'Server error while fetching sale items.',
      error: err.message
    });
  }
});


// GET sales items by id ==========================
router.get('/get/sales/item/byId/:id', auth.authenticateToken, async (req, res) => {
  const { id } = req.params;

  try {
    const connection = await getConnection();

    const [rows] = await connection.execute(
      `
      SELECT 
    si.id, si.vat, si.discount, si.price, si.cost, si.totalCost, 
    si.product_id, si.quantity AS qty, si.subtotal, si.sale_id,
    p.name AS name, p.id AS product_id
  FROM sale_items si
  LEFT JOIN products p ON p.id = si.product_id
  WHERE si.sale_id = ?

      `,
      [id]
    );

    if (!rows || rows.length === 0) {
      return res.status(404).json({ success: false, message: 'No sale items found for this sale ID.' });
    }

    res.json(rows);

  } catch (err) {
    console.error('Error fetching sale items by ID:', err);
    return res.status(500).json({
      success: false,
      message: 'Server error while fetching sale items.',
      error: err.message
    });
  }
});



// Delete Audit Logs lists =====================

router.post('/delete/logs/lists/data', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;

  try {
    await new Promise(resolve => setTimeout(resolve, 3000));

  roleId = res.locals.role;
  const isSuperAdmin = (roleId === 1 || roleId === '1');

  // Only allow super admins
  if (!isSuperAdmin) {
    return res.status(403).json({ message: 'Access denied. Super admin only.' });
  }

    const placeholders = ids.map(() => '?').join(',');
    const connection = await getConnection();

    // Bulk Response
    const [result] = await connection.query(
      `DELETE FROM logs WHERE id IN (${placeholders})`,
      [...ids]
    );

    res.json({
      message: `${result.affectedRows} log(s) deleted successfully`
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Delete failed', error: err.message });
  }
});


// Delete SMS lists =====================

router.post('/delete/sms/lists/data', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;

  try {
    await new Promise(resolve => setTimeout(resolve, 3000));

    const placeholders = ids.map(() => '?').join(',');
    const connection = await getConnection();

    // Bulk Response
    const [result] = await connection.query(
      `DELETE FROM sms WHERE id IN (${placeholders})`,
      [...ids]
    );

    res.json({
      message: `${result.affectedRows} sms(s) deleted successfully `
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Delete failed ', error: err.message });
  }
});


// Delete Sales Return Items =====================

router.post('/delete/sales/return/items', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;

  try {
    await new Promise(resolve => setTimeout(resolve, 3000));

    const placeholders = ids.map(() => '?').join(',');
    const connection = await getConnection();

    // Bulk Response
    const [result] = await connection.query(
      `DELETE FROM sale_returns WHERE id IN (${placeholders})`,
      [...ids]
    );

    res.json({
      message: `${result.affectedRows} items(s) deleted successfully `
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Delete failed ', error: err.message });
  }
});


// Request Cancel Sales Data
router.post('/request/cancel/item/sales', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;
  const userId = res.locals.id;

  try {
    await new Promise(resolve => setTimeout(resolve, 3000));

    const placeholders = ids.map(() => '?').join(',');
    const connection = await getConnection();

    // Bulk Response
    const [result] = await connection.query(
      `UPDATE sales SET sale_status = ? WHERE user_id = ? AND id IN (${placeholders})`,
      ['AWAIT', userId, ...ids]
    );

    // Pending Response
     await connection.query(
      `UPDATE pending_deposits SET status = ? WHERE sale_id IN (${placeholders})`,
      ['await', ...ids]
    );

    res.json({
      message: `${result.affectedRows} items(s) requested for cancel successfully `
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Request failed ', error: err.message });
  }
});


// Get Transfer Lists
router.get('/get/transfer/lists', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const { storeId, warehouseId } = req.query;


  let connection;

  try {
    connection = await getConnection();

    // Base query
    let query = `
      SELECT t.*,
  s.name AS storename,
  w.name AS warehousename

      FROM transfers t
      JOIN stores s ON s.id = t.store_id
      JOIN warehouses w ON w.id = t.source_warehouse
    `;

    const params = [];
    const whereConditions = [];

    // If the user is not an admin, apply restrictions based on their assigned stores and warehouses
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      // If user has no stores or warehouses assigned, return an empty response
      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      // Conditions for stores and warehouses assigned to the user
      const accessConditions = [];

      if (storeIds.length > 0) {
        accessConditions.push(`t.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (warehouseIds.length > 0) {
        accessConditions.push(`t.source_warehouse IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }

      if (storeIds.length > 0 && warehouseIds.length > 0) {
        whereConditions.push(`(t.store_id IN (${storeIds.map(() => '?').join(',')}) AND t.source_warehouse IN (${warehouseIds.map(() => '?').join(',')}))`);
        params.push(...storeIds, ...warehouseIds);
      } else if (storeIds.length > 0) {
        whereConditions.push(`t.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } else if (warehouseIds.length > 0) {
        whereConditions.push(`t.source_warehouse IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
      


    }

    // Admin doesn't need store/warehouse filters, apply optional filters if passed
    if (storeId) {
      whereConditions.push(`t.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`t.source_warehouse = ?`);
      params.push(warehouseId);
    }

    // If there are any where conditions, add them to the query
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sort the result 
    query += ` ORDER BY t.created_at DESC`;

    const [result] = await connection.query(query, params);
    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});


// Get Purchase Lists

router.get('/get/purchases/lists', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    storeId,
    warehouseId,
    filterType,
    startDate,
    endDate,
    sortBy = 'created_at',
    sortOrder = 'DESC',
    timezone = 'Africa/Nairobi', // default timezone, adjust if needed
  } = req.query;

  let connection;

  try {
    connection = await getConnection();

    // Base SELECT query
    let baseQuery = `
      SELECT pu.*,
        f.name AS fy_id,
        s.name AS storename,
        w.name AS warehousename
      FROM purchases pu
      JOIN stores s ON s.id = pu.store_id
      JOIN warehouses w ON w.id = pu.warehouse_id
      LEFT JOIN fy_cycle f ON f.id = pu.fy_id
    `;

    const whereConditions = [`pu.purchase_status = 'APPROVED'`];
    const params = [];

    // Role-based access filtering (non-superadmin)
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        `SELECT store_id FROM user_stores WHERE user_id = ?`, [userId]
      );
      const [warehouseRows] = await connection.query(
        `SELECT warehouse_id FROM user_warehouses WHERE user_id = ?`, [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [], totalCount: 0, totalAmount: 0 });
      }

      if (storeIds.length > 0 && warehouseIds.length > 0) {
        whereConditions.push(`(pu.store_id IN (${storeIds.map(() => '?').join(',')}) AND pu.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
        params.push(...storeIds, ...warehouseIds);
      } else if (storeIds.length > 0) {
        whereConditions.push(`pu.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } else if (warehouseIds.length > 0) {
        whereConditions.push(`pu.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    // Additional filters
    if (storeId) {
      whereConditions.push(`pu.store_id = ?`);
      params.push(storeId);
    }
    if (warehouseId) {
      whereConditions.push(`pu.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // Date filtering with timezone awareness
    let start, end;
    const now = moment().tz(timezone);

    if (filterType) {
      switch (filterType) {
        case 'today':
          start = now.clone().startOf('day');
          end = now.clone().endOf('day');
          break;
        case 'yesterday':
          start = now.clone().subtract(1, 'day').startOf('day');
          end = now.clone().subtract(1, 'day').endOf('day');
          break;
        case 'week':
          start = now.clone().startOf('week');
          end = now.clone().endOf('week');
          break;
        case 'last_week':
          start = now.clone().subtract(1, 'week').startOf('week');
          end = now.clone().subtract(1, 'week').endOf('week');
          break;
        case 'month':
          start = now.clone().startOf('month');
          end = now.clone().endOf('month');
          break;
        case 'year':
          start = now.clone().startOf('year');
          end = now.clone().endOf('year');
          break;
        case 'last_year':
          start = now.clone().subtract(1, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_2_years':
          start = now.clone().subtract(2, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_3_years':
          start = now.clone().subtract(3, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_4_years':
          start = now.clone().subtract(4, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        default:
          break;
      }
    } else if (startDate && endDate) {
      start = moment.tz(startDate, timezone).startOf('day');
      end = moment.tz(endDate, timezone).endOf('day');
    }

    if (start && end) {
      whereConditions.push(`pu.created_at BETWEEN ? AND ?`);
      params.push(start.format('YYYY-MM-DD HH:mm:ss'), end.format('YYYY-MM-DD HH:mm:ss'));
    }

    // Append WHERE clause
    if (whereConditions.length > 0) {
      baseQuery += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sorting - whitelist fields
    const sortableFields = ['created_at', 'grand_total', 'refNumber'];
    const orderBy = sortableFields.includes(sortBy) ? `pu.${sortBy}` : 'pu.created_at';
    const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    baseQuery += ` ORDER BY ${orderBy} ${orderDir}`;

    // Execute main query
    const [result] = await connection.query(baseQuery, params);

    // Summary query - total count and sum (run separate query for efficiency)
    let summaryQuery = `
      SELECT 
        COUNT(*) AS totalCount,
        IFNULL(SUM(grand_total), 0) AS totalAmount
      FROM purchases pu
    `;
    if (whereConditions.length > 0) {
      summaryQuery += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    const [summaryResult] = await connection.query(summaryQuery, params);
    const summary = summaryResult[0];

    res.json({
      array: result,
      totalCount: summary.totalCount,
      totalAmount: parseFloat(summary.totalAmount),
    });

  } catch (err) {
    console.error('Error fetching purchases', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});


// GET purchases items by id
router.get('/get/purchases/item/byId/:id', auth.authenticateToken, async (req, res) => {
  const { id } = req.params;
  const connection = await getConnection();

  try {
    const [rows] = await connection.execute(
      `SELECT 
    pi.cost, pi.product_id, pi.quantity AS qty, pi.subtotal, pi.purchase_id,
    p.name AS name
  FROM purchase_items pi
  LEFT JOIN products p ON p.id = pi.product_id
  WHERE pi.purchase_id = ?`,
      [id]
    );

    if (rows.length === 0) {
      return res.status(404).json({ message: 'Purchases not found' });
    }

    res.json(rows);
  } catch (err) {
    res.status(500).json({ message: 'Failed to fetch purchases', error: err.message });
  }
});

// Approval Purchases Data ====================

router.post('/approval/item/purchases/data', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;
  const approverId = res.locals.id;
  const now = moment().tz('Africa/Nairobi').format('YYYY-MM-DD HH:mm:ss');

  try {
    const connection = await getConnection();

    // Delay simulation
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Check user role
    const [[{ name: roleName } = {}]] = await connection.query(
      'SELECT name FROM roles WHERE id = (SELECT role FROM users WHERE id = ?)',
      [approverId]
    );

    if (!['ADMIN', 'MANAGER'].includes(roleName)) {
      return res.status(403).json({ message: 'Access denied. Only MANAGER or ADMIN allowed.' });
    }

    // Get all purchase items
    const placeholders = ids.map(() => '?').join(',');
    const [purchaseItems] = await connection.query(
      `SELECT 
        pi.*, p.store_id, p.warehouse_id, 
        pr.category_id, pr.brand_id, pr.unit_id, pr.refNumber,
        pr.name, pr.batch_no, pr.barcode_no, pr.price, pr.imei_serial,
        pr.expire_date, pr.vat, pr.discount, pr.product_qty_alert
      FROM purchase_items pi
      JOIN purchases p ON p.id = pi.purchase_id
      JOIN products pr ON pr.id = pi.product_id
      WHERE pi.purchase_id IN (${placeholders})`,
      [...ids]
    );

    for (const item of purchaseItems) {
      const {
        product_id, cost, quantity,
        store_id, warehouse_id, category_id, brand_id, unit_id,
        name, batch_no, barcode_no, price,
        imei_serial, expire_date, vat, discount, product_qty_alert
      } = item;

      const refNumber = Math.floor(100000000 + Math.random() * 900000);
    
      const [existing] = await connection.query(
        'SELECT id FROM products WHERE id = ? LIMIT 1',
        [product_id]
      );

      if (existing.length > 0) {
        // Product exists → update qty
        await connection.query(
          `UPDATE products 
           SET cost = ?, qty = qty + ?, 
               product_update_date = ?, 
               product_update_by = ? 
           WHERE id = ?`,
          [cost, quantity, now, res.locals.name, product_id]
        );
      } else {
        // Product doesn't exist → insert new
        await connection.query(
          `INSERT INTO products (
            store_id, warehouse_id, category_id, brand_id, unit_id, refNumber, name, 
            batch_no, barcode_no, qty, cost, price, imei_serial, expire_date, vat, discount,
            product_create_date, product_create_by, product_status, product_qty_alert
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'true', ?)`,
          [
            store_id, warehouse_id, category_id || 0, brand_id || 0, unit_id || 0, refNumber || 0, name,
            batch_no || 0, barcode_no || 0, quantity, cost || 0, price || 0, imei_serial || 0, expire_date || 0, vat || 0, discount || 0,
           now, res.locals.name, product_qty_alert || 0
          ]
        );
      }
    }

    // Update purchase status
    await connection.query(
      `UPDATE purchases 
       SET purchase_status = 'APPROVED' 
       WHERE id IN (${placeholders})`,
      [...ids]
    );

    res.json({
      message: `${ids.length} Purchase(s) approved and processed successfully.`,
    });

  } catch (err) {
    console.error('Approval error:', err);
    res.status(500).json({ message: 'Approval failed', error: err.message });
  }
});


// Get Daily Warehouse Summary Report =================

router.get('/get/daily/summary/report', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  let connection;
  try {
    connection = await getConnection();

    let query = `
      SELECT 
        wdr.report_date,
        wdr.warehouse_id,
        w.name AS warehouse_name,

        MAX(wdr.fy_id) AS fy_id,
        SUM(wdr.total_sales) AS total_sales,
        SUM(wdr.total_sales_cost) AS total_sales_cost,
        SUM(wdr.sales_profit) AS sales_profit,
        SUM(wdr.total_purchases) AS total_purchases,
        SUM(wdr.total_expenses) AS total_expenses,
        SUM(wdr.total_adjusted_qty) AS total_adjusted_qty,

        MAX(wdr.created_at) AS created_at
      FROM warehouse_daily_report wdr
      INNER JOIN warehouses w ON w.id = wdr.warehouse_id
      WHERE 1 = 1
    `;

    const params = [];

    // ===== ROLE FILTER =====
    if (!(roleId === 1 || roleId === '1')) {
      const [rows] = await connection.query(
        `SELECT warehouse_id FROM user_warehouses WHERE user_id = ?`,
        [userId]
      );

      const warehouseIds = rows.map(r => r.warehouse_id);

      if (!warehouseIds.length) {
        return res.json({
          array: [],
          total_rows: 0,
          totals: { sales: 0, purchases: 0, expenses: 0 }
        });
      }

      query += ` AND wdr.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`;
      params.push(...warehouseIds);
    }

    query += `
      GROUP BY 
        wdr.report_date,
        wdr.warehouse_id,
        w.name
      ORDER BY wdr.report_date DESC, w.name ASC
    `;

    const [result] = await connection.query(query, params);

    // ===== TOTALS =====
    const totals = result.reduce(
      (acc, r) => {
        acc.sales += Number(r.total_sales || 0);
        acc.purchases += Number(r.total_purchases || 0);
        acc.expenses += Number(r.total_expenses || 0);
        return acc;
      },
      { sales: 0, purchases: 0, expenses: 0 }
    );

    res.json({
      array: result,
      total_rows: result.length,
      totals
    });

  } catch (err) {
    console.error(' Error fetching daily summary report:', err);
    res.status(500).json({ message: 'Server error' });
  }
});



// Request Cancel Purchases Data =================

router.post('/request/cancel/item/purchases', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;

  try {
    await new Promise(resolve => setTimeout(resolve, 3000));

    const placeholders = ids.map(() => '?').join(',');
    const connection = await getConnection();

    // Bulk Response
    const [result] = await connection.query(
      `UPDATE purchases SET purchase_status = ? WHERE id IN (${placeholders})`,
      ['AWAIT', ...ids]
    );

    res.json({
      message: `${result.affectedRows} Items(s) requested for cancel successfully`
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Request failed', error: err.message });
  }
});


// Get Generated Sales Report ==================

router.get('/get/generated/sales/lists/report', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    storeId,
    warehouseId,
    filterType,
    startDate,
    endDate,
    sortBy = 'created_at',
    sortOrder = 'DESC',
    timezone = 'Africa/Nairobi'
  } = req.query;

  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT 
        sa.*,
        sa.grand_total,
        sa.total_cost,
        (sa.grand_total - sa.total_cost) AS total_profit,
        ROUND(
          CASE 
            WHEN sa.grand_total > 0 THEN 
              ((sa.grand_total - sa.total_cost) / sa.grand_total) * 100
            ELSE 0
          END, 2
        ) AS total_profit_margin,
        f.name AS fy_id,
        s.name AS storename,
        s.id AS store_id,
        w.name AS warehousename,
        s.phone AS phoneNo,
        s.email AS emailId,
        s.district_id AS district_id,
        d.name AS districtname,
        w.id AS warehouse_id
      FROM sales sa
      JOIN stores s ON s.id = sa.store_id
      JOIN warehouses w ON w.id = sa.warehouse_id
      JOIN districts d ON d.id = s.district_id
      LEFT JOIN fy_cycle f ON f.id = sa.fy_id
    `;

    const whereConditions = [`sa.sale_status = 'APPROVED'`];
    const params = [];

    // Role-based access filter
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(`SELECT store_id FROM user_stores WHERE user_id = ?`, [userId]);
      const [warehouseRows] = await connection.query(`SELECT warehouse_id FROM user_warehouses WHERE user_id = ?`, [userId]);

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [], totalRecords: 0, grandTotal: 0 });
      }

      if (storeIds.length && warehouseIds.length) {
        whereConditions.push(`(sa.store_id IN (${storeIds.map(() => '?').join(',')}) AND sa.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
        params.push(...storeIds, ...warehouseIds);
      } else if (storeIds.length) {
        whereConditions.push(`sa.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } else if (warehouseIds.length) {
        whereConditions.push(`sa.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    // Optional filters
    if (storeId) {
      whereConditions.push(`sa.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`sa.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // Date filtering using moment-timezone
    let start, end;
    const now = moment().tz(timezone);

    if (filterType) {
      switch (filterType) {
        case 'today':
          start = now.clone().startOf('day');
          end = now.clone().endOf('day');
          break;
        case 'yesterday':
          start = now.clone().subtract(1, 'day').startOf('day');
          end = now.clone().subtract(1, 'day').endOf('day');
          break;
        case 'week':
          start = now.clone().startOf('week');
          end = now.clone().endOf('week');
          break;
        case 'last_week':
          start = now.clone().subtract(1, 'week').startOf('week');
          end = now.clone().subtract(1, 'week').endOf('week');
          break;
        case 'month':
          start = now.clone().startOf('month');
          end = now.clone().endOf('month');
          break;
        case 'year':
          start = now.clone().startOf('year');
          end = now.clone().endOf('year');
          break;
        case 'last_year':
          start = now.clone().subtract(1, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_2_years':
          start = now.clone().subtract(2, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_3_years':
          start = now.clone().subtract(3, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_4_years':
          start = now.clone().subtract(4, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        case 'approved':
          whereConditions.push(`sa.sale_status = 'APPROVED'`);
          break;
        case 'draft':
          whereConditions.push(`sa.sale_status = 'DRAFT'`);
          break;
      }
    } else if (startDate && endDate) {
      start = moment.tz(startDate, timezone).startOf('day');
      end = moment.tz(endDate, timezone).endOf('day');
    }

    if (start && end) {
      whereConditions.push(`sa.created_at BETWEEN ? AND ?`);
      params.push(start.format('YYYY-MM-DD HH:mm:ss'), end.format('YYYY-MM-DD HH:mm:ss'));
    }

    // Finalize WHERE clause
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sorting (safe)
    const columnMap = { created_at: 'sa.created_at' };
    const orderBy = columnMap[sortBy] || 'sa.created_at';
    const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    query += ` ORDER BY ${orderBy} ${orderDir}`;

    // Execute main query
    const [result] = await connection.query(query, params);

    // Summary query
    let summaryQuery = `
      SELECT 
        COUNT(*) AS totalRecords,
        IFNULL(SUM(sa.grand_total), 0) AS grandTotal
      FROM sales sa
    `;
    if (whereConditions.length > 0) {
      summaryQuery += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    const [summary] = await connection.query(summaryQuery, params);

    res.json({
      array: result,
      totalRecords: summary[0].totalRecords,
      grandTotal: parseFloat(summary[0].grandTotal),
    });

  } catch (err) {
    console.error('Error fetching sales report', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});


// Get Generated Purchases Report

router.get('/get/generated/purchases/lists/report', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    storeId,
    warehouseId,
    filterType,
    startDate,
    endDate,
    sortBy = 'created_at',
    sortOrder = 'DESC',
    timezone = 'Africa/Nairobi'
  } = req.query;

  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT pu.*,
        f.name as fy_id,
        s.name as storename,
        s.id as store_id,
        w.name as warehousename,
        w.id as warehouse_id
      FROM purchases pu
      JOIN stores s ON s.id = pu.store_id
      JOIN warehouses w ON w.id = pu.warehouse_id
      LEFT JOIN fy_cycle f ON f.id = pu.fy_id
    `;

    let totalQuery = `
      SELECT 
        COUNT(*) AS totalCount, 
        SUM(pu.grand_total) AS totalAmount
      FROM purchases pu
      JOIN stores s ON s.id = pu.store_id
      JOIN warehouses w ON w.id = pu.warehouse_id
      LEFT JOIN fy_cycle f ON f.id = pu.fy_id
    `;

    const whereConditions = [`pu.purchase_status = 'APPROVED'`];
    const params = [];

    // Role-based filtering
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(`SELECT store_id FROM user_stores WHERE user_id = ?`, [userId]);
      const [warehouseRows] = await connection.query(`SELECT warehouse_id FROM user_warehouses WHERE user_id = ?`, [userId]);

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [], totalCount: 0, totalAmount: 0 });
      }

      if (storeIds.length && warehouseIds.length) {
        whereConditions.push(`(pu.store_id IN (${storeIds.map(() => '?').join(',')}) AND pu.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
        params.push(...storeIds, ...warehouseIds);
      } else if (storeIds.length) {
        whereConditions.push(`pu.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } else if (warehouseIds.length) {
        whereConditions.push(`pu.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    // Optional filters
    if (storeId) {
      whereConditions.push(`pu.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`pu.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // Date filtering with moment-timezone
    let start, end;
    const now = moment().tz(timezone);

    if (filterType) {
      switch (filterType) {
        case 'today':
          start = now.clone().startOf('day');
          end = now.clone().endOf('day');
          break;
        case 'yesterday':
          start = now.clone().subtract(1, 'day').startOf('day');
          end = now.clone().subtract(1, 'day').endOf('day');
          break;
        case 'week':
          start = now.clone().startOf('week');
          end = now.clone().endOf('week');
          break;
        case 'last_week':
          start = now.clone().subtract(1, 'week').startOf('week');
          end = now.clone().subtract(1, 'week').endOf('week');
          break;
        case 'month':
          start = now.clone().startOf('month');
          end = now.clone().endOf('month');
          break;
        case 'year':
          start = now.clone().startOf('year');
          end = now.clone().endOf('year');
          break;
        case 'last_year':
          start = now.clone().subtract(1, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_2_years':
          start = now.clone().subtract(2, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_3_years':
          start = now.clone().subtract(3, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_4_years':
          start = now.clone().subtract(4, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
      }
    } else if (startDate && endDate) {
      start = moment.tz(startDate, timezone).startOf('day');
      end = moment.tz(endDate, timezone).endOf('day');
    }

    if (start && end) {
      whereConditions.push(`pu.created_at BETWEEN ? AND ?`);
      params.push(start.format('YYYY-MM-DD HH:mm:ss'), end.format('YYYY-MM-DD HH:mm:ss'));
    }

    // Apply WHERE conditions
    if (whereConditions.length > 0) {
      const whereClause = ` WHERE ${whereConditions.join(' AND ')}`;
      query += whereClause;
      totalQuery += whereClause;
    }

    // Sorting
    const sortableFields = ['created_at', 'total', 'grand_total', 'refNumber'];
    const orderBy = sortableFields.includes(sortBy) ? sortBy : 'created_at';
    const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    query += ` ORDER BY pu.${orderBy} ${orderDir}`;

    // Execute queries
    const [result] = await connection.query(query, params);
    const [totalData] = await connection.query(totalQuery, params);

    res.json({
      array: result,
      totalCount: totalData[0]?.totalCount || 0,
      totalAmount: totalData[0]?.totalAmount || 0
    });

  } catch (err) {
    console.error('Error fetching purchase report:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});



// Get Top Selling Products ===========================

// Get Product Lists Data
router.get('/get/topsellingproduct/lists', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const { storeId, warehouseId } = req.query;

  let connection;

  try {
    connection = await getConnection();

    const params = [];
    const whereConditions = [`s.sale_status = 'true'`]; // Keep the base WHERE condition

    // Restrict access for non-admin users
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      const accessConditions = [];

      if (storeIds.length > 0) {
        accessConditions.push(`s.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }

      if (warehouseIds.length > 0) {
        accessConditions.push(`s.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }

      if (accessConditions.length > 0) {
        whereConditions.push(`(${accessConditions.join(' AND ')})`);
      }
    }

    // Admin-level filters (optional)
    if (storeId) {
      whereConditions.push(`s.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`s.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // Final query
    const query = `
      SELECT 
        p.refNumber AS refNumber,
        p.name AS product,
        SUM(si.quantity) AS total_sales,
        SUM(si.quantity * si.price) AS total_amount,
        st.name AS storename, st.id AS store_id, 
        w.name AS warehousename, w.id AS warehouse_id
      FROM 
        sale_items si
      JOIN 
        products p ON si.product_id = p.id
      JOIN 
        sales s ON si.sale_id = s.id
      JOIN 
        stores st ON st.id = s.store_id
      JOIN 
      warehouses w ON w.id = s.warehouse_id

      ${whereConditions.length > 0 ? 'WHERE ' + whereConditions.join(' AND ') : ''}
      GROUP BY 
        si.product_id
      ORDER BY 
        total_sales DESC
      LIMIT 50
    `;

    const [result] = await connection.query(query, params);
    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});


// Get Generated Product Sales Report ====================================

router.get('/get/generated/product/sales/lists/report', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    storeId,
    warehouseId,
    filterType,
    startDate,
    endDate,
    sortBy = 'created_at',
    sortOrder = 'DESC',
    timezone = 'Africa/Nairobi'
  } = req.query;

  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT 
        p.name AS product,
        si.product_id,
        SUM(si.quantity) AS total_sales,
        SUM(si.price * si.quantity) AS total_amount,
        s.name AS storename,
        s.id AS store_id,
        w.name AS warehousename,
        w.id AS warehouse_id,
        sa.created_at
      FROM sale_items si
      JOIN sales sa ON sa.id = si.sale_id
      JOIN stores s ON s.id = sa.store_id
      JOIN warehouses w ON w.id = sa.warehouse_id
      JOIN products p ON p.id = si.product_id
    `;

    const params = [];
    const whereConditions = [`sa.sale_status = 'APPROVED'`];

    // Role-based access
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(`SELECT store_id FROM user_stores WHERE user_id = ?`, [userId]);
      const [warehouseRows] = await connection.query(`SELECT warehouse_id FROM user_warehouses WHERE user_id = ?`, [userId]);

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      if (storeIds.length && warehouseIds.length) {
        whereConditions.push(`(sa.store_id IN (${storeIds.map(() => '?').join(',')}) AND sa.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
        params.push(...storeIds, ...warehouseIds);
      } else if (storeIds.length) {
        whereConditions.push(`sa.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } else if (warehouseIds.length) {
        whereConditions.push(`sa.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    // Filter by specific store or warehouse
    if (storeId) {
      whereConditions.push(`sa.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`sa.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // Time filtering using moment-timezone
    const now = moment().tz(timezone);
    let start, end;

    if (filterType) {
      switch (filterType) {
        case 'today':
          start = now.clone().startOf('day');
          end = now.clone().endOf('day');
          break;
        case 'yesterday':
          start = now.clone().subtract(1, 'day').startOf('day');
          end = now.clone().subtract(1, 'day').endOf('day');
          break;
        case 'week':
          start = now.clone().startOf('week');
          end = now.clone().endOf('week');
          break;
        case 'last_week':
          start = now.clone().subtract(1, 'week').startOf('week');
          end = now.clone().subtract(1, 'week').endOf('week');
          break;
        case 'month':
          start = now.clone().startOf('month');
          end = now.clone().endOf('month');
          break;
        case 'year':
          start = now.clone().startOf('year');
          end = now.clone().endOf('year');
          break;
        case 'last_year':
          start = now.clone().subtract(1, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_2_years':
          start = now.clone().subtract(2, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_3_years':
          start = now.clone().subtract(3, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_4_years':
          start = now.clone().subtract(4, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
      }
    } else if (startDate && endDate) {
      start = moment.tz(startDate, timezone).startOf('day');
      end = moment.tz(endDate, timezone).endOf('day');
    }

    if (start && end) {
      whereConditions.push(`sa.created_at BETWEEN ? AND ?`);
      params.push(start.format('YYYY-MM-DD HH:mm:ss'), end.format('YYYY-MM-DD HH:mm:ss'));
    }

    // WHERE clause
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Grouping
    query += ` GROUP BY si.product_id, s.id, w.id`;

    // Sorting
    const columnMap = {
      created_at: 'sa.created_at',
      total_sales: 'total_sales',
      total_amount: 'total_amount',
      product: 'p.name'
    };

    const orderBy = columnMap[sortBy] || 'sa.created_at';
    const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    query += ` ORDER BY ${orderBy} ${orderDir}`;

    // Execute query
    const [result] = await connection.query(query, params);

    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching product sales report:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});



// Get Generated Product Purchases Report ====================================

router.get('/get/generated/product/purchases/lists/report', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;

  const {
    storeId,
    warehouseId,
    filterType,
    startDate,
    endDate,
    sortBy = 'created_at',
    sortOrder = 'DESC',
    timezone = 'Africa/Nairobi'
  } = req.query;

  let connection;

  try {
    connection = await getConnection();

    let query = `
      SELECT 
        p.name AS product,
        pi.product_id,
        SUM(pi.quantity) AS total_purchases,
        SUM(pi.cost * pi.quantity) AS total_amount,
        s.name AS storename,
        s.id AS store_id,
        w.name AS warehousename,
        w.id AS warehouse_id,
        pu.created_at
      FROM purchase_items pi
      JOIN purchases pu ON pu.id = pi.purchase_id
      JOIN stores s ON s.id = pu.store_id
      JOIN warehouses w ON w.id = pu.warehouse_id
      JOIN products p ON p.id = pi.product_id
    `;

    const params = [];
    const whereConditions = [`pu.purchase_status = 'APPROVED'`];

    // Role-based filtering
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      if (storeIds.length > 0 && warehouseIds.length > 0) {
        whereConditions.push(`(pu.store_id IN (${storeIds.map(() => '?').join(',')}) AND pu.warehouse_id IN (${warehouseIds.map(() => '?').join(',')}))`);
        params.push(...storeIds, ...warehouseIds);
      } else if (storeIds.length > 0) {
        whereConditions.push(`pu.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } else if (warehouseIds.length > 0) {
        whereConditions.push(`pu.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        params.push(...warehouseIds);
      }
    }

    // Optional direct filters
    if (storeId) {
      whereConditions.push(`pu.store_id = ?`);
      params.push(storeId);
    }

    if (warehouseId) {
      whereConditions.push(`pu.warehouse_id = ?`);
      params.push(warehouseId);
    }

    // Timezone-aware date filtering
    const now = moment().tz(timezone);
    let start, end;

    if (filterType) {
      switch (filterType) {
        case 'today':
          start = now.clone().startOf('day');
          end = now.clone().endOf('day');
          break;
        case 'yesterday':
          start = now.clone().subtract(1, 'day').startOf('day');
          end = now.clone().subtract(1, 'day').endOf('day');
          break;
        case 'week':
          start = now.clone().startOf('week');
          end = now.clone().endOf('week');
          break;
        case 'last_week':
          start = now.clone().subtract(1, 'week').startOf('week');
          end = now.clone().subtract(1, 'week').endOf('week');
          break;
        case 'month':
          start = now.clone().startOf('month');
          end = now.clone().endOf('month');
          break;
        case 'year':
          start = now.clone().startOf('year');
          end = now.clone().endOf('year');
          break;
        case 'last_year':
          start = now.clone().subtract(1, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_2_years':
          start = now.clone().subtract(2, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_3_years':
          start = now.clone().subtract(3, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
        case 'last_4_years':
          start = now.clone().subtract(4, 'year').startOf('year');
          end = now.clone().subtract(1, 'year').endOf('year');
          break;
      }
    } else if (startDate && endDate) {
      start = moment.tz(startDate, timezone).startOf('day');
      end = moment.tz(endDate, timezone).endOf('day');
    }

    if (start && end) {
      whereConditions.push(`pu.created_at BETWEEN ? AND ?`);
      params.push(start.format('YYYY-MM-DD HH:mm:ss'), end.format('YYYY-MM-DD HH:mm:ss'));
    }

    // Apply WHERE clause
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Grouping
    query += ` GROUP BY pi.product_id, s.id, w.id`;

    // Sorting
    const columnMap = {
      created_at: 'pu.created_at',
      total_purchases: 'total_purchases',
      total_amount: 'total_amount',
      product: 'p.name'
    };

    const orderBy = columnMap[sortBy] || 'pu.created_at';
    const orderDir = sortOrder.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';
    query += ` ORDER BY ${orderBy} ${orderDir}`;

    const [result] = await connection.query(query, params);

    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching product purchases report:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});



// Delete Items Quotation Data
router.post('/item/quotations/delete', auth.authenticateToken, async (req, res) => {
  const { ids } = req.body;

  try {
    await new Promise(resolve => setTimeout(resolve, 3000));

    const placeholders = ids.map(() => '?').join(',');
    const connection = await getConnection();

    
    // Delete related items first
    await connection.query(
      `DELETE FROM quotation_items WHERE quotation_id IN (${placeholders})`,
      ids
    );

    // Delete
    const [result] = await connection.query(
      `DELETE FROM quotations WHERE id IN (${placeholders})`,
      ids
    );

    res.json({
      message: `${result.affectedRows} items(s) deleted successfully`
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Delete failed ', error: err.message });
  }
});


// INVENTORY EVALUATION ============================

router.post('/get/inventory/evaluation/report', auth.authenticateToken, async (req, res) => {
  const { warehouseId } = req.body;

  try {
    const connection = await getConnection();

    // Build base query
    let query = `
      SELECT
        p.id,
        p.product_status,
        p.name AS product_name,
        w.name AS warehouse_name,
        p.qty AS qty,
        p.cost AS cost,
        (p.qty * p.cost) AS stock_value
      FROM products p
      JOIN warehouses w ON w.id = p.warehouse_id
    `;

    // If warehouseId is provided, add WHERE clause
    const params = [];
    if (warehouseId) {
      query += ' WHERE p.warehouse_id = ? AND p.product_status ="true" ';
      params.push(warehouseId);
    }

    query += ' ORDER BY p.name;';

    const [rows] = await connection.query(query, params);
    res.json(rows);
  } catch (error) {
    console.error('Inventory evaluation error:', error);
    res.status(500).json({ message: 'Failed to fetch inventory evaluation.' });
  }
});



// PROFIT LOSS SUMMARYREPORT =======================================

router.post('/get/profit/loss/summary/report', auth.authenticateToken, async (req, res) => {
  let { startDate, endDate, warehouseId, timezone = 'Africa/Nairobi' } = req.body;
  const roleId = res.locals.role;
  const userId = res.locals.id;

  try {
    const connection = await getConnection();

    const now = moment().tz(timezone);

    if (!startDate || !endDate) {
      startDate = now.clone().startOf('day').format('YYYY-MM-DD HH:mm:ss');
      endDate = now.clone().endOf('day').format('YYYY-MM-DD HH:mm:ss');
    } else {
      startDate = moment.tz(startDate, timezone).startOf('day').format('YYYY-MM-DD HH:mm:ss');
      endDate = moment.tz(endDate, timezone).endOf('day').format('YYYY-MM-DD HH:mm:ss');
    }

    // Optional delay for loading effect
    await new Promise(resolve => setTimeout(resolve, 3000));

    const filters = [`created_at BETWEEN ? AND ?`];
    const salesFilters = [`s.sale_status = 'APPROVED'`, `s.created_at BETWEEN ? AND ?`];
    const purchaseFilters = [`p.purchase_status = 'APPROVED'`, `p.created_at BETWEEN ? AND ?`];

    const params = [startDate, endDate];
    const salesParams = [startDate, endDate];
    const purchaseParams = [startDate, endDate];

    let storeIds = [];
    let warehouseIds = [];

    // Role-based restrictions
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query('SELECT store_id FROM user_stores WHERE user_id = ?', [userId]);
      const [warehouseRows] = await connection.query('SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]);

      storeIds = storeRows.map(r => r.store_id);
      warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({
          sales: 0,
          purchases: 0,
          netProfit: 0,
        });
      }

      if (storeIds.length > 0) {
        const clause = `store_id IN (${storeIds.map(() => '?').join(',')})`;
        filters.push(clause);
        salesFilters.push(`s.${clause}`);
        purchaseFilters.push(`p.${clause}`);
        params.push(...storeIds);
        salesParams.push(...storeIds);
        purchaseParams.push(...storeIds);
      }

      if (warehouseIds.length > 0) {
        const clause = `warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`;
        filters.push(clause);
        salesFilters.push(`s.${clause}`);
        purchaseFilters.push(`p.${clause}`);
        params.push(...warehouseIds);
        salesParams.push(...warehouseIds);
        purchaseParams.push(...warehouseIds);
      }
    }

    if (warehouseId) {
      filters.push(`warehouse_id = ?`);
      salesFilters.push(`s.warehouse_id = ?`);
      purchaseFilters.push(`p.warehouse_id = ?`);
      params.push(warehouseId);
      salesParams.push(warehouseId);
      purchaseParams.push(warehouseId);
    }

    // === SALES ===
    const [sales] = await connection.query(
      `SELECT IFNULL(SUM(grand_total), 0) AS sales,
              IFNULL(SUM(total_cost), 0) AS cost 
       FROM sales s 
       WHERE ${salesFilters.join(' AND ')}`,
      salesParams
    );

    // === PURCHASES ===
    const [purchases] = await connection.query(
      `SELECT IFNULL(SUM(grand_total), 0) AS purchases 
       FROM purchases p 
       WHERE ${purchaseFilters.join(' AND ')}`,
      purchaseParams
    );

    // === FINAL RESPONSE ===
    res.json({
      sales: sales[0].sales,
      purchases: purchases[0].purchases,
      netProfit: sales[0].sales - sales[0].cost,
    });

  } catch (err) {
    console.error('Net profit loss error:', err);
    res.status(500).json({ message: 'Failed to load net profit data' });
  }
});

// DASHBOARD START HERE =======================================

router.post('/get/dashboard/summary', auth.authenticateToken, async (req, res) => {
  let { startDate, endDate, warehouseId, timezone = 'Africa/Nairobi' } = req.body;
  const roleId = res.locals.role;
  const userId = res.locals.id;

  try {
    const connection = await getConnection();

    const now = moment().tz(timezone);

    if (!startDate || !endDate) {
      startDate = now.clone().startOf('day').format('YYYY-MM-DD HH:mm:ss');
      endDate = now.clone().endOf('day').format('YYYY-MM-DD HH:mm:ss');
    } else {
      startDate = moment.tz(startDate, timezone).startOf('day').format('YYYY-MM-DD HH:mm:ss');
      endDate = moment.tz(endDate, timezone).endOf('day').format('YYYY-MM-DD HH:mm:ss');
    }

    const filters = [`created_at BETWEEN ? AND ?`];
    const salesFilters = [`s.sale_status = 'APPROVED'`, `s.created_at BETWEEN ? AND ?`];
    const purchaseFilters = [`p.purchase_status = 'APPROVED'`, `p.created_at BETWEEN ? AND ?`];

    const params = [startDate, endDate];
    const salesParams = [startDate, endDate];
    const purchaseParams = [startDate, endDate];

    let storeIds = [];
    let warehouseIds = [];

    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query('SELECT store_id FROM user_stores WHERE user_id = ?', [userId]);
      const [warehouseRows] = await connection.query('SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]);

      storeIds = storeRows.map(r => r.store_id);
      warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({
          sales: 0, purchases: 0, salesReturn: 0, purchaseReturn: 0,
          netProfit: 0, totalExpenses: 0, totalItemsAvailable: 0,
          totalStockValue: 0, lowStock: [], topProducts: [],
          recentSales: [], topCustomers: []
        });
      }

      if (storeIds.length > 0) {
        const clause = `store_id IN (${storeIds.map(() => '?').join(',')})`;
        filters.push(clause);
        salesFilters.push(`s.${clause}`);
        purchaseFilters.push(`p.${clause}`);
        params.push(...storeIds);
        salesParams.push(...storeIds);
        purchaseParams.push(...storeIds);
      }

      if (warehouseIds.length > 0) {
        const clause = `warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`;
        filters.push(clause);
        salesFilters.push(`s.${clause}`);
        purchaseFilters.push(`p.${clause}`);
        params.push(...warehouseIds);
        salesParams.push(...warehouseIds);
        purchaseParams.push(...warehouseIds);
      }
    }

    if (warehouseId) {
      filters.push(`warehouse_id = ?`);
      salesFilters.push(`s.warehouse_id = ?`);
      purchaseFilters.push(`p.warehouse_id = ?`);
      params.push(warehouseId);
      salesParams.push(warehouseId);
      purchaseParams.push(warehouseId);
    }

    const [sales] = await connection.query(
      `SELECT IFNULL(SUM(grand_total), 0) AS sales,
              IFNULL(SUM(total_cost), 0) AS cost 
       FROM sales s 
       WHERE ${salesFilters.join(' AND ')}`,
      salesParams
    );

    const [purchases] = await connection.query(
      `SELECT IFNULL(SUM(grand_total), 0) AS purchases 
       FROM purchases p 
       WHERE ${purchaseFilters.join(' AND ')}`,
      purchaseParams
    );

    const [salesReturn] = await connection.query(
      `SELECT IFNULL(SUM(return_total), 0) AS salesReturn 
       FROM sale_returns 
       WHERE ${filters.join(' AND ')}`,
      params
    );

    const [purchaseReturn] = await connection.query(
      `SELECT IFNULL(SUM(return_total), 0) AS purchaseReturn 
       FROM purchase_returns 
       WHERE ${filters.join(' AND ')}`,
      params
    );

    const [expenses] = await connection.query(
      `SELECT IFNULL(SUM(amount), 0) AS totalExpenses 
       FROM expenses 
       WHERE ${filters.join(' AND ')}`,
      params
    );

    const itemStockFilters = [];
    const itemStockParams = [];

    if (!(roleId === 1 || roleId === '1')) {
      if (storeIds.length > 0) {
        itemStockFilters.push(`store_id IN (${storeIds.map(() => '?').join(',')})`);
        itemStockParams.push(...storeIds);
      }
      if (warehouseIds.length > 0) {
        itemStockFilters.push(`warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        itemStockParams.push(...warehouseIds);
      }
    }

    if (warehouseId) {
      itemStockFilters.push(`warehouse_id = ?`);
      itemStockParams.push(warehouseId);
    }

    const [itemStats] = await connection.query(
      `SELECT 
         COUNT(*) AS totalItemsAvailable,
         SUM(qty * cost) AS totalStockValue
       FROM products 
       ${itemStockFilters.length > 0 ? `WHERE ${itemStockFilters.join(' AND ')}` : ''}`,
      itemStockParams
    );

    const lowStockFilters = [`qty <= product_qty_alert`];
    const lowStockParams = [];

    if (!(roleId === 1 || roleId === '1')) {
      if (storeIds.length > 0) {
        lowStockFilters.push(`store_id IN (${storeIds.map(() => '?').join(',')})`);
        lowStockParams.push(...storeIds);
      }
      if (warehouseIds.length > 0) {
        lowStockFilters.push(`warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
        lowStockParams.push(...warehouseIds);
      }
    }

    if (warehouseId) {
      lowStockFilters.push(`warehouse_id = ?`);
      lowStockParams.push(warehouseId);
    }

    const [lowStock] = await connection.query(
      `SELECT id, name, refNumber, warehouse_id, store_id, qty, product_qty_alert 
       FROM products 
       WHERE ${lowStockFilters.join(' AND ')} LIMIT 10`,
      lowStockParams
    );

    const [topProducts] = await connection.query(
      `SELECT p.name as name, COUNT(si.id) AS totalSales 
       FROM sale_items si
       JOIN sales s ON s.id = si.sale_id
       JOIN products p ON p.id = si.product_id  
       WHERE ${salesFilters.join(' AND ')} 
       GROUP BY p.name 
       ORDER BY totalSales DESC 
       LIMIT 5`,
      salesParams
    );

    const [recentSales] = await connection.query(
      `SELECT s.*, c.name as customer
       FROM sales s
       JOIN customers c ON c.id = s.customer_id
       WHERE ${salesFilters.join(' AND ')} 
       ORDER BY s.created_at DESC 
       LIMIT 5`,
      salesParams
    );

    const [topCustomers] = await connection.query(
      `SELECT c.name AS name, COUNT(*) AS orders, SUM(s.grand_total) AS total 
       FROM sales s
       JOIN customers c ON c.id = s.customer_id 
       WHERE ${salesFilters.join(' AND ')} 
       GROUP BY c.name 
       ORDER BY total DESC 
       LIMIT 5`,
      salesParams
    );

    res.json({
      sales: sales[0].sales,
      purchases: purchases[0].purchases,
      salesReturn: salesReturn[0].salesReturn,
      purchaseReturn: purchaseReturn[0].purchaseReturn,
      netProfit: sales[0].sales - sales[0].cost,
      totalExpenses: expenses[0].totalExpenses,
      totalItemsAvailable: itemStats[0].totalItemsAvailable,
      totalStockValue: itemStats[0].totalStockValue || 0,
      lowStock,
      topProducts,
      recentSales,
      topCustomers
    });

  } catch (err) {
    console.error('Dashboard summary error:', err);
    res.status(500).json({ message: 'Failed to load dashboard data' });
  }
});


// Payment Statistics =======================================

router.post('/get/dashboard/paystats', auth.authenticateToken, async (req, res) => {
  let { startDate, endDate, warehouseId, timezone = 'Africa/Nairobi' } = req.body;
  const roleId = res.locals.role;
  const userId = res.locals.id;

  try {
    const connection = await getConnection();

    const now = moment().tz(timezone);

    // Format date range using moment-timezone
    if (!startDate || !endDate) {
      startDate = now.clone().startOf('day').format('YYYY-MM-DD HH:mm:ss');
      endDate = now.clone().endOf('day').format('YYYY-MM-DD HH:mm:ss');
    } else {
      startDate = moment.tz(startDate, timezone).startOf('day').format('YYYY-MM-DD HH:mm:ss');
      endDate = moment.tz(endDate, timezone).endOf('day').format('YYYY-MM-DD HH:mm:ss');
    }

    let warehouseIds = [];

    // Apply role-based warehouse filtering
    if (!(roleId === 1 || roleId === '1')) {
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?',
        [userId]
      );
      warehouseIds = warehouseRows.map(r => r.warehouse_id);
      if (warehouseIds.length === 0) return res.json([]);
    }

    const salesFilters = [`sale_status = 'APPROVED'`, `created_at BETWEEN ? AND ?`];
    const purchaseFilters = [`purchase_status = 'APPROVED'`, `created_at BETWEEN ? AND ?`];
    const salesParams = [startDate, endDate];
    const purchaseParams = [startDate, endDate];

    // Add accessible warehouses
    if (warehouseIds.length > 0) {
      const placeholders = warehouseIds.map(() => '?').join(',');
      salesFilters.push(`warehouse_id IN (${placeholders})`);
      purchaseFilters.push(`warehouse_id IN (${placeholders})`);
      salesParams.push(...warehouseIds);
      purchaseParams.push(...warehouseIds);
    }

    // Add selected warehouse filter
    if (warehouseId) {
      salesFilters.push(`warehouse_id = ?`);
      purchaseFilters.push(`warehouse_id = ?`);
      salesParams.push(warehouseId);
      purchaseParams.push(warehouseId);
    }

    const combinedParams = [...salesParams, ...purchaseParams];

    const query = `
      SELECT 
        date,
        SUM(sales) AS totalSales,
        SUM(purchases) AS totalPurchases
      FROM (
        SELECT DATE(created_at) AS date, SUM(grand_total) AS sales, 0 AS purchases
        FROM sales
        WHERE ${salesFilters.join(' AND ')}
        GROUP BY DATE(created_at)

        UNION ALL

        SELECT DATE(created_at) AS date, 0 AS sales, SUM(grand_total) AS purchases
        FROM purchases
        WHERE ${purchaseFilters.join(' AND ')}
        GROUP BY DATE(created_at)
      ) AS combined
      GROUP BY date
      ORDER BY date ASC
      LIMIT 30
    `;

    const [paystats] = await connection.query(query, combinedParams);
    res.json(paystats);

  } catch (err) {
    console.error('Paystats dashboard error:', err);
    res.status(500).json({ message: 'Failed to load payment statistics' });
  }
});



// WEEKLY PURCHASES GRAPH =========================================

router.post('/get/dashboard/weekly-purchases', auth.authenticateToken, async (req, res) => {
  const roleId = res.locals.role;
  const userId = res.locals.id;
  const { warehouseId, timezone = 'Africa/Nairobi' } = req.body;

  try {
    const connection = await getConnection();

    // Define date range (last 7 days)
    const now = moment().tz(timezone).startOf('day');
    const fromDate = now.clone().subtract(6, 'days').format('YYYY-MM-DD HH:mm:ss');
    const toDate = now.clone().endOf('day').format('YYYY-MM-DD HH:mm:ss');

    // Base filters
    const purchaseFilters = [`p.purchase_status = 'APPROVED'`, `p.created_at BETWEEN ? AND ?`];
    const purchaseParams = [fromDate, toDate];

    // Role-based access
    if (!(roleId === 1 || roleId === '1')) {
      const [wRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?',
        [userId]
      );

      const warehouseIds = wRows.map(w => w.warehouse_id);

      if (warehouseIds.length === 0) {
        return res.json({ labels: [], data: [] });
      }

      purchaseFilters.push(`p.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
      purchaseParams.push(...warehouseIds);
    }

    // Optional specific warehouse filter
    if (warehouseId) {
      purchaseFilters.push(`p.warehouse_id = ?`);
      purchaseParams.push(warehouseId);
    }

    // Query for matching purchases
    const [rows] = await connection.query(
      `
      SELECT p.created_at, p.grand_total
      FROM purchases p
      WHERE ${purchaseFilters.join(' AND ')}
      `,
      purchaseParams
    );

    // Prepare 7-day data
    const dailyTotals = {};
    for (let i = 0; i < 7; i++) {
      const date = now.clone().subtract(6 - i, 'days').format('YYYY-MM-DD');
      dailyTotals[date] = 0;
    }

    // Sum purchases per day
    for (const row of rows) {
      const localDate = moment.utc(row.created_at).tz(timezone).format('YYYY-MM-DD');
      if (dailyTotals.hasOwnProperty(localDate)) {
        dailyTotals[localDate] += parseFloat(row.grand_total || 0);
      }
    }

    const labels = Object.keys(dailyTotals);
    const data = Object.values(dailyTotals);

    res.json({ labels, data });

  } catch (err) {
    console.error('Weekly purchases error:', err.message);
    res.status(500).json({ message: 'Failed to fetch weekly purchases trends' });
  }
});


// WEEKLY SALES GRAPH =========================================

router.post('/get/dashboard/weekly-sales', auth.authenticateToken, async (req, res) => {
  const roleId = res.locals.role;
  const userId = res.locals.id;
  const { warehouseId, timezone = 'Africa/Nairobi' } = req.body;

  try {
    const connection = await getConnection();

    const now = moment().tz(timezone).startOf('day');
    const fromDate = now.clone().subtract(6, 'days').format('YYYY-MM-DD HH:mm:ss');
    const toDate = now.clone().endOf('day').format('YYYY-MM-DD HH:mm:ss');

    const salesFilters = [`s.sale_status = 'APPROVED'`, `s.created_at BETWEEN ? AND ?`];
    const salesParams = [fromDate, toDate];

    // Role-based filtering
    if (!(roleId === 1 || roleId === '1')) {
      const [wRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );
      const warehouseIds = wRows.map(w => w.warehouse_id);

      if (warehouseIds.length === 0) {
        return res.json({ labels: [], data: [] });
      }

      salesFilters.push(`s.warehouse_id IN (${warehouseIds.map(() => '?').join(',')})`);
      salesParams.push(...warehouseIds);
    }

    if (warehouseId) {
      salesFilters.push(`s.warehouse_id = ?`);
      salesParams.push(warehouseId);
    }

    // Fetch raw sales
    const [rows] = await connection.query(
      `
      SELECT s.created_at, s.grand_total
      FROM sales s
      WHERE ${salesFilters.join(' AND ')}
      `,
      salesParams
    );

    // Group in JS
    const dailyTotals = {};
    for (let i = 0; i < 7; i++) {
      const date = now.clone().subtract(6 - i, 'days').format('YYYY-MM-DD');
      dailyTotals[date] = 0;
    }

    for (const row of rows) {
      const localDate = moment.utc(row.created_at).tz(timezone).format('YYYY-MM-DD');
      if (dailyTotals.hasOwnProperty(localDate)) {
        dailyTotals[localDate] += parseFloat(row.grand_total);
      }
    }

    const labels = Object.keys(dailyTotals);
    const data = Object.values(dailyTotals);

    res.json({ labels, data });

  } catch (err) {
    console.error('Weekly sales error:', err.message);
    res.status(500).json({ message: 'Failed to fetch weekly sales trends' });
  }
});




// Get Financial Year Lists =====================================

router.get('/get/financial-year/lists', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  const roleId = res.locals.role;
  const { storeId } = req.query;


  let connection;

  try {
    connection = await getConnection();

    // Base query
    let query = `
      SELECT fy.*, 
        s.name AS storename, s.id AS store_id
      FROM fy_cycle fy
      JOIN stores s ON s.id = fy.store_id
    `;

    const params = [];
    const whereConditions = [];

    // If the user is not an admin, apply restrictions based on their assigned stores and warehouses
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      

      const storeIds = storeRows.map(r => r.store_id);
     
      // If user has no stores or warehouses assigned, return an empty response
      if (storeIds.length === 0 ) {
        return res.json({ array: [] });
      }

      // Conditions for stores and warehouses assigned to the user
      const accessConditions = [];

      if (storeIds.length > 0) {
        accessConditions.push(`fy.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      }


      if (storeIds.length > 0 ) {
        whereConditions.push(`(fy.store_id IN (${storeIds.map(() => '?').join(',')}))`);
        params.push(...storeIds);
      } else if (storeIds.length > 0) {
        whereConditions.push(`fy.store_id IN (${storeIds.map(() => '?').join(',')})`);
        params.push(...storeIds);
      } 
      
    }

    // Admin doesn't need store/warehouse filters, apply optional filters if passed
    if (storeId) {
      whereConditions.push(`fy.store_id = ?`);
      params.push(storeId);
    }

    // If there are any where conditions, add them to the query
    if (whereConditions.length > 0) {
      query += ` WHERE ${whereConditions.join(' AND ')}`;
    }

    // Sort the result 
    query += ` ORDER BY fy.name ASC`;

    const [result] = await connection.query(query, params);
    res.json({ array: result });

  } catch (err) {
    console.error('Error fetching:', err);
    res.status(500).json({ message: 'Something went wrong', error: err.message });
  }
});


// GET PURCHASES REPORT BY DATES=====================

router.post('/get/dates/purchases/reports', auth.authenticateToken, async (req, res) => {
  const { startDate, endDate, timezone = 'Africa/Nairobi' } = req.body;
  const userId = res.locals.id;
  const roleId = res.locals.role;

  let connection;
  try {
    connection = await getConnection();

    let assignedWarehouseIds = [];

    // Super admins see all warehouses
    if (roleId === 1 || roleId === '1') {
      const [allWarehouses] = await connection.query(`SELECT id FROM warehouses`);
      assignedWarehouseIds = allWarehouses.map(w => w.id);
    } else {
      const [userWarehouses] = await connection.query(
        `SELECT warehouse_id FROM user_warehouses WHERE user_id = ?`, [userId]
      );
      assignedWarehouseIds = userWarehouses.map(w => w.warehouse_id);
    }

    if (assignedWarehouseIds.length === 0) {
      return res.status(200).json({
        summary: [],
        purchase_items: [],
        totals: {
          totalTransactions: 0,
          totalAmountPurchased: 0,
          totalDiscount: 0
        }
      });
    }

    const allSummaries = [];
    const allItems = [];

    let totalTransactions = 0;
    let totalAmountPurchased = 0;
    let totalDiscount = 0;

    // Convert incoming start/end dates to UTC using moment-timezone
    const startUTC = moment.tz(startDate, timezone).startOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
    const endUTC = moment.tz(endDate, timezone).endOf('day').utc().format('YYYY-MM-DD HH:mm:ss');

    for (const warehouseId of assignedWarehouseIds) {
      // Check if purchases exist for this warehouse in date range
      const [hasPurchases] = await connection.query(`
        SELECT 1 FROM purchases
        WHERE purchase_status = 'APPROVED'
          AND created_at BETWEEN ? AND ?
          AND warehouse_id = ?
        LIMIT 1
      `, [startUTC, endUTC, warehouseId]);

      if (hasPurchases.length === 0) continue;

      // Summary
      const [summary] = await connection.query(`
        SELECT 
          p.store_id,
          COUNT(p.id) AS total_purchases_transaction,
          SUM(p.grand_total) AS total_amount_purchased,
          SUM(p.order_discount) AS total_order_discount,
          w.name AS warehousename,
          w.id AS warehouse_id,
          st.name AS storename
        FROM purchases p
        JOIN warehouses w ON w.id = p.warehouse_id
        JOIN stores st ON st.id = p.store_id
        WHERE p.purchase_status = 'APPROVED'
          AND p.created_at BETWEEN ? AND ?
          AND p.warehouse_id = ?
        GROUP BY p.warehouse_id
      `, [startUTC, endUTC, warehouseId]);

      summary.forEach(s => {
        s.startDate = startDate;
        s.endDate = endDate;

        totalTransactions += parseInt(s.total_purchases_transaction);
        totalAmountPurchased += parseFloat(s.total_amount_purchased || 0);
        totalDiscount += parseFloat(s.total_order_discount || 0);
      });

      if (summary.length > 0) {
        allSummaries.push(...summary);
      }

      // Purchase Items
      const [items] = await connection.query(`
        SELECT 
          pi.*, 
          p.name AS product_name,
          pu.invoiceNo,
          pu.warehouse_id,
          pi.created_at AS purchase_date
        FROM purchase_items pi
        JOIN purchases pu ON pu.id = pi.purchase_id
        JOIN products p ON p.id = pi.product_id
        WHERE pu.purchase_status = 'APPROVED'
          AND pi.created_at BETWEEN ? AND ?
          AND pu.warehouse_id = ?
        ORDER BY pi.created_at DESC
      `, [startUTC, endUTC, warehouseId]);

      allItems.push(...items);
    }

    await new Promise(resolve => setTimeout(resolve, 1500)); // Optional UX delay

    res.status(200).json({
      summary: allSummaries,
      purchase_items: allItems,
      totals: {
        totalTransactions,
        totalAmountPurchased,
        totalDiscount
      }
    });

  } catch (err) {
    console.error(' Error fetching date purchases report:', err);
    res.status(500).json({ message: 'Server error' });
  }
});



// GET SALES REPORT BY DATES=====================

router.post('/get/dates/sales/reports', auth.authenticateToken, async (req, res) => {
  const { startDate, endDate, timezone = 'Africa/Nairobi' } = req.body;
  const userId = res.locals.id;
  const roleId = res.locals.role;

  let connection;
  try {
    connection = await getConnection();

    let assignedWarehouseIds = [];

    if (roleId === 1 || roleId === '1') {
      const [allWarehouses] = await connection.query(`SELECT id FROM warehouses`);
      assignedWarehouseIds = allWarehouses.map(w => w.id);
    } else {
      const [userWarehouses] = await connection.query(
        `SELECT warehouse_id FROM user_warehouses WHERE user_id = ?`, [userId]
      );
      assignedWarehouseIds = userWarehouses.map(w => w.warehouse_id);
    }

    if (assignedWarehouseIds.length === 0) {
      return res.status(200).json({
        summary: [],
        sale_items: [],
        totals: {
          totalTransactions: 0,
          totalAmountSold: 0,
          totalDiscount: 0
        }
      });
    }

    const allSummaries = [];
    const allItems = [];

    let totalTransactions = 0;
    let totalAmountSold = 0;
    let totalDiscount = 0;

    // Convert to UTC using timezone
    const startUTC = moment.tz(startDate, timezone).startOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
    const endUTC = moment.tz(endDate, timezone).endOf('day').utc().format('YYYY-MM-DD HH:mm:ss');

    for (const warehouseId of assignedWarehouseIds) {
      const [hasSales] = await connection.query(`
        SELECT 1 FROM sales
        WHERE sale_status = 'APPROVED'
          AND created_at BETWEEN ? AND ?
          AND warehouse_id = ?
        LIMIT 1
      `, [startUTC, endUTC, warehouseId]);

      if (hasSales.length === 0) continue;

      // Summary
      const [summary] = await connection.query(`
        SELECT 
          s.store_id,
          COUNT(s.id) AS total_sales_transaction,
          SUM(s.grand_total) AS total_amount_sold,
          SUM(s.order_discount) AS total_order_discount,
          w.name AS warehousename,
          w.id AS warehouse_id,
          st.name AS storename
        FROM sales s
        JOIN warehouses w ON w.id = s.warehouse_id
        JOIN stores st ON st.id = s.store_id
        WHERE s.sale_status = 'APPROVED'
          AND s.created_at BETWEEN ? AND ?
          AND s.warehouse_id = ?
        GROUP BY s.warehouse_id
      `, [startUTC, endUTC, warehouseId]);

      if (summary.length > 0) {
        summary.forEach(s => {
          s.startDate = startDate;
          s.endDate = endDate;
          totalTransactions += parseInt(s.total_sales_transaction);
          totalAmountSold += parseFloat(s.total_amount_sold || 0);
          totalDiscount += parseFloat(s.total_order_discount || 0);
        });
        allSummaries.push(...summary);
      }

      // Sale Items
      const [items] = await connection.query(`
        SELECT 
          si.*, 
          p.name AS product_name,
          s.invoiceNo,
          s.warehouse_id,
          si.created_at AS sale_date
        FROM sale_items si
        JOIN sales s ON s.id = si.sale_id
        JOIN products p ON p.id = si.product_id
        WHERE s.sale_status = 'APPROVED'
          AND si.created_at BETWEEN ? AND ?
          AND s.warehouse_id = ?
        ORDER BY si.created_at DESC
      `, [startUTC, endUTC, warehouseId]);

      allItems.push(...items);
    }

    await new Promise(resolve => setTimeout(resolve, 1500)); // Optional delay

    res.status(200).json({
      summary: allSummaries,
      sale_items: allItems,
      totals: {
        totalTransactions,
        totalAmountSold,
        totalDiscount
      }
    });

  } catch (err) {
    console.error(' Error fetching date sales report:', err);
    res.status(500).json({ message: 'Server error' });
  }
});


// GET WAREHOUSES PURCHASES REPORT =====================

router.post('/get/warehouses/purchases/reports', auth.authenticateToken, async (req, res) => {
  const { warehouse, startDate, endDate, timezone = 'Africa/Nairobi' } = req.body;

  let connection;
  try {
    connection = await getConnection();

    // Convert to UTC using timezone
    const startUTC = moment.tz(startDate, timezone).startOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
    const endUTC = moment.tz(endDate, timezone).endOf('day').utc().format('YYYY-MM-DD HH:mm:ss');

    // Fetch distinct warehouse(s) involved in purchases
    const [warehouseRows] = await connection.query(`
      SELECT DISTINCT p.warehouse_id, w.name AS warehousename
      FROM purchases p
      JOIN warehouses w ON w.id = p.warehouse_id
      WHERE p.purchase_status = 'APPROVED'
        AND p.created_at BETWEEN ? AND ?
        ${warehouse ? 'AND p.warehouse_id = ?' : ''}
    `, warehouse ? [startUTC, endUTC, warehouse] : [startUTC, endUTC]);

    if (warehouseRows.length === 0) {
      return res.status(200).json({
        summary: [],
        purchase_items: [],
        totals: {
          totalTransactions: 0,
          totalAmountPurchased: 0,
          totalDiscount: 0
        }
      });
    }

    const allSummaries = [];
    const allItems = [];

    let totalTransactions = 0;
    let totalAmountPurchased = 0;
    let totalDiscount = 0;

    for (const wh of warehouseRows) {
      const warehouseId = wh.warehouse_id;

      // Summary grouped by warehouse
      const [summary] = await connection.query(`
        SELECT 
          p.store_id,
          COUNT(p.id) AS total_purchases_transaction,
          SUM(p.grand_total) AS total_amount_purchased,
          SUM(p.order_discount) AS total_order_discount,
          w.name AS warehousename,
          w.id AS warehouse_id,
          st.name AS storename
        FROM purchases p
        JOIN warehouses w ON w.id = p.warehouse_id
        JOIN stores st ON st.id = p.store_id
        WHERE p.purchase_status = 'APPROVED'
          AND p.created_at BETWEEN ? AND ?
          AND p.warehouse_id = ?
        GROUP BY p.warehouse_id
      `, [startUTC, endUTC, warehouseId]);

      if (summary.length > 0) {
        summary.forEach(s => {
          totalTransactions += parseInt(s.total_purchases_transaction);
          totalAmountPurchased += parseFloat(s.total_amount_purchased || 0);
          totalDiscount += parseFloat(s.total_order_discount || 0);
        });
        allSummaries.push(...summary);
      }

      // Purchase Items per warehouse
      const [items] = await connection.query(`
        SELECT 
          pi.*, 
          p.name AS product_name,
          pu.invoiceNo,
          pu.warehouse_id,
          pu.created_at AS purchase_date
        FROM purchase_items pi
        JOIN purchases pu ON pu.id = pi.purchase_id
        JOIN products p ON p.id = pi.product_id
        WHERE pu.purchase_status = 'APPROVED'
          AND pi.created_at BETWEEN ? AND ?
          AND pu.warehouse_id = ?
        ORDER BY pi.created_at DESC
      `, [startUTC, endUTC, warehouseId]);

      allItems.push(...items);
    }

    await new Promise(resolve => setTimeout(resolve, 1000)); // UX delay

    res.status(200).json({
      summary: allSummaries,
      purchase_items: allItems,
      totals: {
        totalTransactions,
        totalAmountPurchased,
        totalDiscount
      }
    });

  } catch (err) {
    console.error('Error fetching warehouse purchases report:', err);
    res.status(500).json({ message: 'Server error' });
  }
});


// GET WAREHOUSES SALES REPORT =====================

router.post('/get/warehouses/sales/reports', auth.authenticateToken, async (req, res) => {
  const { warehouse, startDate, endDate, timezone = 'Africa/Nairobi' } = req.body;

  let connection;
  try {
    connection = await getConnection();

    // Convert to UTC based on provided timezone
    const startUTC = moment.tz(startDate, timezone).startOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
    const endUTC = moment.tz(endDate, timezone).endOf('day').utc().format('YYYY-MM-DD HH:mm:ss');

    // Fetch distinct warehouses that made sales in given period
    const [warehouseRows] = await connection.query(`
      SELECT DISTINCT s.warehouse_id, w.name AS warehousename
      FROM sales s
      JOIN warehouses w ON w.id = s.warehouse_id
      WHERE s.sale_status = 'APPROVED'
        AND s.created_at BETWEEN ? AND ?
        ${warehouse ? 'AND s.warehouse_id = ?' : ''}
    `, warehouse ? [startUTC, endUTC, warehouse] : [startUTC, endUTC]);

    if (warehouseRows.length === 0) {
      return res.status(200).json({
        summary: [],
        sale_items: [],
        totals: {
          totalTransactions: 0,
          totalAmountSold: 0,
          totalDiscount: 0
        }
      });
    }

    const allSummaries = [];
    const allItems = [];

    let totalTransactions = 0;
    let totalAmountSold = 0;
    let totalDiscount = 0;

    for (const wh of warehouseRows) {
      const warehouseId = wh.warehouse_id;

      // Summary for each warehouse
      const [summary] = await connection.query(`
        SELECT 
          s.store_id,
          COUNT(s.id) AS total_sales_transaction,
          SUM(s.grand_total) AS total_amount_sold,
          SUM(s.order_discount) AS total_order_discount,
          w.name AS warehousename,
          w.id AS warehouse_id,
          st.name AS storename
        FROM sales s
        JOIN warehouses w ON w.id = s.warehouse_id
        JOIN stores st ON st.id = s.store_id
        WHERE s.sale_status = 'APPROVED'
          AND s.created_at BETWEEN ? AND ?
          AND s.warehouse_id = ?
        GROUP BY s.warehouse_id
      `, [startUTC, endUTC, warehouseId]);

      if (summary.length > 0) {
        const s = summary[0];
        totalTransactions += parseInt(s.total_sales_transaction);
        totalAmountSold += parseFloat(s.total_amount_sold || 0);
        totalDiscount += parseFloat(s.total_order_discount || 0);
        allSummaries.push(...summary);
      }

      // Sale Items per warehouse
      const [items] = await connection.query(`
        SELECT 
          si.*, 
          p.name AS product_name,
          s.invoiceNo,
          s.warehouse_id,
          si.created_at AS sale_date
        FROM sale_items si
        JOIN sales s ON s.id = si.sale_id
        JOIN products p ON p.id = si.product_id
        WHERE s.sale_status = 'APPROVED'
          AND si.created_at BETWEEN ? AND ?
          AND s.warehouse_id = ?
        ORDER BY si.created_at DESC
      `, [startUTC, endUTC, warehouseId]);

      allItems.push(...items);
    }

    await new Promise(resolve => setTimeout(resolve, 1000)); // Optional UX delay

    res.status(200).json({
      summary: allSummaries,
      sale_items: allItems,
      totals: {
        totalTransactions,
        totalAmountSold,
        totalDiscount
      }
    });

  } catch (err) {
    console.error('Error fetching warehouse sales report:', err);
    res.status(500).json({ message: 'Server error' });
  }
});



// GET USER PURCHASES REPORT =====================

router.post('/get/user/purchases/reports', auth.authenticateToken, async (req, res) => {
  const { userId, startDate, endDate, timezone = 'Africa/Nairobi' } = req.body;

  let connection;
  try {
    connection = await getConnection();

    // Convert to UTC range based on provided or default timezone
    const startUTC = moment.tz(startDate, timezone).startOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
    const endUTC = moment.tz(endDate, timezone).endOf('day').utc().format('YYYY-MM-DD HH:mm:ss');

    // Get distinct warehouses where the user made purchases
    const [warehouseRows] = await connection.query(`
      SELECT DISTINCT p.warehouse_id, w.name AS warehousename
      FROM purchases p
      JOIN warehouses w ON w.id = p.warehouse_id
      WHERE p.user_id = ?
        AND p.purchase_status = 'APPROVED'
        AND p.created_at BETWEEN ? AND ?
    `, [userId, startUTC, endUTC]);

    if (warehouseRows.length === 0) {
      return res.status(200).json({
        summary: [],
        purchase_items: [],
        totals: {
          totalTransactions: 0,
          totalAmountPurchased: 0,
          totalDiscount: 0
        }
      });
    }

    const allSummaries = [];
    const allItems = [];

    let totalTransactions = 0;
    let totalAmountPurchased = 0;
    let totalDiscount = 0;

    for (const wh of warehouseRows) {
      const warehouseId = wh.warehouse_id;

      // Summary per warehouse
      const [summary] = await connection.query(`
        SELECT 
          p.store_id,
          p.user_id,
          u.name AS user_name,
          COUNT(p.id) AS total_purchases_transaction,
          SUM(p.grand_total) AS total_amount_purchased,
          SUM(p.order_discount) AS total_order_discount,
          w.name AS warehousename,
          w.id AS warehouse_id,
          st.name AS storename
        FROM purchases p
        JOIN users u ON u.id = p.user_id
        JOIN warehouses w ON w.id = p.warehouse_id
        JOIN stores st ON st.id = p.store_id
        WHERE p.purchase_status = 'APPROVED'
          AND p.created_at BETWEEN ? AND ?
          AND p.user_id = ?
          AND p.warehouse_id = ?
        GROUP BY p.user_id, w.id
      `, [startUTC, endUTC, userId, warehouseId]);

      if (summary.length > 0) {
        summary.forEach(s => {
          totalTransactions += parseInt(s.total_purchases_transaction);
          totalAmountPurchased += parseFloat(s.total_amount_purchased || 0);
          totalDiscount += parseFloat(s.total_order_discount || 0);
        });
        allSummaries.push(...summary);
      }

      // Purchase Items
      const [items] = await connection.query(`
        SELECT 
          pi.*, 
          p.name AS product_name,
          pu.invoiceNo,
          pu.user_id,
          pu.warehouse_id,
          pi.created_at AS purchase_date
        FROM purchase_items pi
        JOIN purchases pu ON pu.id = pi.purchase_id
        JOIN products p ON p.id = pi.product_id
        WHERE pu.purchase_status = 'APPROVED'
          AND pi.created_at BETWEEN ? AND ?
          AND pu.user_id = ?
          AND pu.warehouse_id = ?
        ORDER BY pi.created_at DESC
      `, [startUTC, endUTC, userId, warehouseId]);

      allItems.push(...items);
    }

    await new Promise(resolve => setTimeout(resolve, 2000)); // UX delay

    res.status(200).json({
      summary: allSummaries,
      purchase_items: allItems,
      totals: {
        totalTransactions,
        totalAmountPurchased,
        totalDiscount
      }
    });

  } catch (err) {
    console.error('Error fetching user purchases report:', err);
    res.status(500).json({ message: 'Server error' });
  }
});


// GET USER SALES REPORT =====================

router.post('/get/user/sales/reports', auth.authenticateToken, async (req, res) => {
  const { userId, startDate, endDate, timezone = 'Africa/Nairobi' } = req.body;

  let connection;
  try {
    connection = await getConnection();

    // Convert startDate and endDate to UTC based on timezone
    const startUTC = moment.tz(startDate, timezone).startOf('day').utc().format('YYYY-MM-DD HH:mm:ss');
    const endUTC = moment.tz(endDate, timezone).endOf('day').utc().format('YYYY-MM-DD HH:mm:ss');

    // Step 1: Get all warehouse IDs where the user made sales
    const [warehouseRows] = await connection.query(`
      SELECT DISTINCT s.warehouse_id, w.name AS warehousename
      FROM sales s
      JOIN warehouses w ON w.id = s.warehouse_id
      WHERE s.user_id = ?
        AND s.sale_status = 'APPROVED'
        AND s.created_at BETWEEN ? AND ?
    `, [userId, startUTC, endUTC]);

    if (warehouseRows.length === 0) {
      return res.status(200).json({ summary: [], sale_items: [], totals: { totalTransactions: 0, totalAmountSold: 0, totalDiscount: 0 } });
    }

    const allSummaries = [];
    const allItems = [];
    let totalTransactions = 0;
    let totalAmountSold = 0;
    let totalDiscount = 0;

    for (const wh of warehouseRows) {
      const warehouseId = wh.warehouse_id;

      // Step 2: Get summary for each warehouse
      const [summary] = await connection.query(`
        SELECT 
          s.store_id,
          s.user_id,
          u.name AS user_name,
          COUNT(s.id) AS total_sales_transaction,
          SUM(s.grand_total) AS total_amount_sold,
          SUM(s.order_discount) AS total_order_discount,
          w.name AS warehousename,
          w.id AS warehouse_id,
          st.name AS storename
        FROM sales s
        JOIN users u ON u.id = s.user_id
        JOIN warehouses w ON w.id = s.warehouse_id
        JOIN stores st ON st.id = s.store_id
        WHERE s.sale_status = 'APPROVED'
          AND s.created_at BETWEEN ? AND ?
          AND s.user_id = ?
          AND s.warehouse_id = ?
        GROUP BY s.user_id, w.id
      `, [startUTC, endUTC, userId, warehouseId]);

      if (summary.length > 0) {
        const s = summary[0];
        totalTransactions += parseInt(s.total_sales_transaction);
        totalAmountSold += parseFloat(s.total_amount_sold || 0);
        totalDiscount += parseFloat(s.total_order_discount || 0);
        allSummaries.push(...summary);
      }

      // Step 3: Get items per warehouse
      const [items] = await connection.query(`
        SELECT 
          si.*, 
          p.name AS product_name,
          s.invoiceNo,
          s.user_id,
          s.warehouse_id,
          si.created_at AS sale_date
        FROM sale_items si
        JOIN sales s ON s.id = si.sale_id
        JOIN products p ON p.id = si.product_id
        WHERE s.sale_status = 'APPROVED'
          AND si.created_at BETWEEN ? AND ?
          AND s.user_id = ?
          AND s.warehouse_id = ?
        ORDER BY si.created_at DESC
      `, [startUTC, endUTC, userId, warehouseId]);

      allItems.push(...items);
    }

    // Optional delay for UI loading indicators
    await new Promise(resolve => setTimeout(resolve, 500));

    res.status(200).json({
      summary: allSummaries,
      sale_items: allItems,
      totals: {
        totalTransactions,
        totalAmountSold,
        totalDiscount
      }
    });

  } catch (err) {
    console.error('Error fetching user sales report:', err);
    res.status(500).json({ message: 'Server error' });
  }
});


// Users by warehouse =================================

router.get('/get/users/by-warehouses', auth.authenticateToken, async (req, res) => {
  const userId = res.locals.id;
  let conn;

  try {
    conn = await getConnection();

    // Get role name
    const [[userRow]] = await conn.query(`
      SELECT r.name AS role_name 
      FROM users u 
      JOIN roles r ON r.id = u.role 
      WHERE u.id = ?
    `, [userId]);

    const isAdmin = userRow?.role_name?.toUpperCase() === 'ADMIN';

    let rows = [];

    if (isAdmin) {
      // Admin: get all users with aggregated warehouse IDs
      [rows] = await conn.query(`
        SELECT 
          u.id AS userId,
          u.name AS username,
          GROUP_CONCAT(uw.warehouse_id) AS warehouse_ids
        FROM users u
        LEFT JOIN user_warehouses uw ON uw.user_id = u.id
        GROUP BY u.id, u.name
        ORDER BY u.name
      `);
    } else {
      // Get current user's assigned warehouse(s)
      const [userWarehouses] = await conn.query(`
        SELECT warehouse_id FROM user_warehouses WHERE user_id = ?
      `, [userId]);

      const warehouseIds = userWarehouses.map(w => w.warehouse_id);

      if (warehouseIds.length === 0) {
        return res.status(200).json({ users: [] }); // no access
      }

      const placeholders = warehouseIds.map(() => '?').join(',');

      [rows] = await conn.query(`
        SELECT 
          u.id AS userId,
          u.name AS username,
          GROUP_CONCAT(uw.warehouse_id) AS warehouse_ids
        FROM user_warehouses uw
        JOIN users u ON u.id = uw.user_id
        WHERE uw.warehouse_id IN (${placeholders})
        GROUP BY u.id, u.name
        ORDER BY u.name
      `, warehouseIds);
    }

    // Convert warehouse_ids string into array for convenience
    const users = rows.map(r => ({
      userId: r.userId,
      username: r.username,
      warehouse_ids: r.warehouse_ids ? r.warehouse_ids.split(',').map(id => Number(id)) : []
    }));

    res.status(200).json({ users });

  } catch (error) {
    console.error('❌ Error fetching users by warehouse:', error);
    res.status(500).json({ message: 'Server error' });
  }
});


// Overall Financial Years Report ======================

router.post('/report/financial-year-overall', auth.authenticateToken, async (req, res) => {
  const roleId = res.locals.role;
  const { storeId, warehouseId, fy } = req.body;

  let connection;

  try {
    connection = await getConnection();

    const salesWhere = [`sale_status = 'APPROVED'`];
    const purchaseWhere = [`purchase_status = 'APPROVED'`];
    const expenseWhere = [`approved = 1`];

    const salesParams = [];
    const purchaseParams = [];
    const expenseParams = [];

    // Common filters (if not superadmin)
    if (roleId !== 1) {
      if (storeId) {
        salesWhere.push(`store_id = ?`);
        purchaseWhere.push(`store_id = ?`);
        expenseWhere.push(`store_id = ?`);
        salesParams.push(storeId);
        purchaseParams.push(storeId);
        expenseParams.push(storeId);
      }
      if (warehouseId) {
        salesWhere.push(`warehouse_id = ?`);
        purchaseWhere.push(`warehouse_id = ?`);
        expenseWhere.push(`warehouse_id = ?`);
        salesParams.push(warehouseId);
        purchaseParams.push(warehouseId);
        expenseParams.push(warehouseId);
      }
    }

    // If filtering by financial year, add that to subqueries too
    if (fy) {
      salesWhere.push(`fy_id = ?`);
      purchaseWhere.push(`fy_id = ?`);
      expenseWhere.push(`fy_id = ?`);
      salesParams.push(fy);
      purchaseParams.push(fy);
      expenseParams.push(fy);
    }

    const query = `
      SELECT 
        fy.id AS financialYearId,
        fy.name AS financialYearName,
        COALESCE(salesData.totalSales, 0) AS totalSales,
        COALESCE(purchaseData.totalPurchases, 0) AS totalPurchases,
        COALESCE(expensesData.totalExpenses, 0) AS totalExpenses
      FROM fy_cycle fy
      INNER JOIN (
        SELECT fy_id
        FROM (
          SELECT fy_id, SUM(grand_total) AS totalSales
          FROM sales
          WHERE ${salesWhere.join(' AND ')}
          GROUP BY fy_id
        ) AS salesSummary
      ) salesCheck ON salesCheck.fy_id = fy.id
      LEFT JOIN (
        SELECT fy_id, SUM(grand_total) AS totalSales
        FROM sales
        WHERE ${salesWhere.join(' AND ')}
        GROUP BY fy_id
      ) salesData ON salesData.fy_id = fy.id
      LEFT JOIN (
        SELECT fy_id, SUM(grand_total) AS totalPurchases
        FROM purchases
        WHERE ${purchaseWhere.join(' AND ')}
        GROUP BY fy_id
      ) purchaseData ON purchaseData.fy_id = fy.id
      LEFT JOIN (
        SELECT fy_id, SUM(amount) AS totalExpenses
        FROM expenses
        WHERE ${expenseWhere.join(' AND ')}
        GROUP BY fy_id
      ) expensesData ON expensesData.fy_id = fy.id
      ORDER BY fy.startedAt DESC
    `;

    const params = [
      ...salesParams,
      ...salesParams,
      ...purchaseParams,
      ...expenseParams
    ];

    const [rows] = await connection.query(query, params);

    const chartData = {
      labels: rows.map(r => r.financialYearName),
      sales: rows.map(r => r.totalSales),
      purchases: rows.map(r => r.totalPurchases),
      expenses: rows.map(r => r.totalExpenses),
    };

    res.json({
      success: true,
      data: {
        table: rows,
        chart: chartData
      }
    });

  } catch (err) {
    console.error('Error fetching financial year report:', err);
    res.status(500).json({ success: false, message: 'Something went wrong', error: err.message });
  }
});

// GET Monthly Overall Report

router.get('/report/monthly-summary', auth.authenticateToken, async (req, res) => {
  let connection;
  try {
    const userId = res.locals.id;
    const roleId = res.locals.role;

    connection = await getConnection();

    // Separate params arrays for each query
    const salesParams = [];
    const purchaseParams = [];
    const expenseParams = [];

    const salesWhere = [`sale_status = 'APPROVED'`];
    const purchaseWhere = [`purchase_status = 'APPROVED'`];
    const expenseWhere = [`approved = 1`];

    // Restrict based on user role (non-admins)
    if (!(roleId === 1 || roleId === '1')) {
      const [storeRows] = await connection.query(
        'SELECT store_id FROM user_stores WHERE user_id = ?', [userId]
      );
      const [warehouseRows] = await connection.query(
        'SELECT warehouse_id FROM user_warehouses WHERE user_id = ?', [userId]
      );

      const storeIds = storeRows.map(r => r.store_id);
      const warehouseIds = warehouseRows.map(r => r.warehouse_id);

      if (storeIds.length === 0 && warehouseIds.length === 0) {
        return res.json({ array: [] });
      }

      if (storeIds.length > 0) {
        const placeholders = storeIds.map(() => '?').join(',');
        salesWhere.push(`s.store_id IN (${placeholders})`);
        purchaseWhere.push(`p.store_id IN (${placeholders})`);
        expenseWhere.push(`e.store_id IN (${placeholders})`);

        salesParams.push(...storeIds);
        purchaseParams.push(...storeIds);
        expenseParams.push(...storeIds);
      }

      if (warehouseIds.length > 0) {
        const placeholders = warehouseIds.map(() => '?').join(',');
        salesWhere.push(`s.warehouse_id IN (${placeholders})`);
        purchaseWhere.push(`p.warehouse_id IN (${placeholders})`);
        expenseWhere.push(`e.warehouse_id IN (${placeholders})`);

        salesParams.push(...warehouseIds);
        purchaseParams.push(...warehouseIds);
        expenseParams.push(...warehouseIds);
      }
    }

    // Queries with monthNumber and monthName
    const salesQuery = `
      SELECT MONTH(s.created_at) AS monthNumber,
             DATE_FORMAT(s.created_at, '%M') AS monthName,
             SUM(s.grand_total) AS total,
             'Sales' AS type
      FROM sales s
      WHERE ${salesWhere.join(' AND ')}
      GROUP BY monthNumber, monthName
      ORDER BY monthNumber
    `;

    const purchasesQuery = `
      SELECT MONTH(p.created_at) AS monthNumber,
             DATE_FORMAT(p.created_at, '%M') AS monthName,
             SUM(p.grand_total) AS total,
             'Purchases' AS type
      FROM purchases p
      WHERE ${purchaseWhere.join(' AND ')}
      GROUP BY monthNumber, monthName
      ORDER BY monthNumber
    `;

    const expensesQuery = `
      SELECT MONTH(e.created_at) AS monthNumber,
             DATE_FORMAT(e.created_at, '%M') AS monthName,
             SUM(e.amount) AS total,
             'Expenses' AS type
      FROM expenses e
      WHERE ${expenseWhere.join(' AND ')}
      GROUP BY monthNumber, monthName
      ORDER BY monthNumber
    `;

    // Run queries separately with their own params
    const [salesRows] = await connection.query(salesQuery, salesParams);
    const [purchaseRows] = await connection.query(purchasesQuery, purchaseParams);
    const [expenseRows] = await connection.query(expensesQuery, expenseParams);

    // Combine results
    const result = [...salesRows, ...purchaseRows, ...expenseRows];

    // Sort by monthNumber (1 to 12)
    result.sort((a, b) => a.monthNumber - b.monthNumber);

    res.json({ array: result });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Server error' });
  }
});


// Get User Selectors ====================

router.get('/users/demo/selectable/accounts', async (req, res) => {
  const conn = await getConnection();
  const [rows] = await conn.query(`
    
    SELECT u.id, u.name, r.name as role_name, u.role, u.phone, '123456' AS password
      FROM users u
      JOIN roles r ON r.id = u.role
      INNER JOIN user_stores us ON us.user_id = u.id
      INNER JOIN stores s ON s.id = us.store_id
      WHERE s.name = 'DEMO STORE' AND u.userStatus = 'true'
    
    `);
  res.json(rows);
});



// INTEGRATION WITH TRA EFD SYSTEM ===========================


// Submit sale to TRA VFD ===================================
router.post('/send/sales_data/to/tra/system', async (req, res) => {
  const { saleId } = req.body;
  let connection;

  try {
    connection = await getConnection();
    // Get sale and items from DB
    const [saleRows] = await connection.execute('SELECT * FROM sales WHERE id = ?', [saleId]);
    if (!saleRows.length) return res.status(404).send({ message: 'Sale not found' });

    const sale = saleRows[0];
    const [items] = await connection.execute('SELECT * FROM sale_items WHERE sale_id = ?', [saleId]);

    // Prepare payload
    const payload = {
      invoiceNumber: sale.invoice_no,
      datetime: new Date().toISOString(),
      customer: {
        name: sale.customer_name,
        tin: sale.customer_tin
      },
      items: items.map(item => ({
        name: item.product_name,
        qty: item.quantity,
        price: item.unit_price,
        tax: item.tax_amount || 0
      })),
      totalAmount: sale.total_amount
    };

    // Send to TRA VFD
    const vfdRes = await axios.post('https://vfd.tra.go.tz/api/fiscalize', payload, {
      headers: {
        Authorization: `Bearer ${process.env.TRA_API_KEY}`,
        'Content-Type': 'application/json'
      }
    });

    const vfdData = vfdRes.data;

    // Save VFD response to DB
    await connection.execute(
      'UPDATE sales SET fiscal_receipt_no = ?, fiscal_code = ?, qr_code = ?, fiscal_submission_status = ? WHERE id = ?',
      [vfdData.receiptNo, vfdData.fiscalCode, vfdData.qrCode, 'SUCCESS', saleId]
    );

    res.status(200).send({ message: 'Fiscalized', ...vfdData });
  } catch (error) {
    console.error(error);
    await connection.execute('UPDATE sales SET fiscal_submission_status = ? WHERE id = ?', ['FAILED', saleId]);
    res.status(500).send({ message: 'Failed to fiscalize', error: error.message });
  }
});







// LIPA KWA MPESA INTEGRATION =======================



// === CONFIG (Replace with your own credentials) ===
const consumerKey = '0NcbLZ5DjlMybLsLTNw8lXDi4RjwXM6NyJhQkXt7xGteAbzp';
const consumerSecret = 'KAco5bUGHGkrGLmb5sjKrfNGkDApYpArAKARq1V68K3L9gYopLk8BTSDfMG99Jgx';
const shortcode = '174379'; // Test Paybill
const passkey = 'bfb279f9aa9bdbcf158e97dd71a467cd2e0c893059b10f78e6b72ada1ed2c919'; // Test passkey
const callbackURL = 'https://yourdomain.com/api/mpesa/callback'; // Replace with your callback URL

// === Generate Access Token ===
async function getAccessToken() {
  const credentials = Buffer.from(`${consumerKey}:${consumerSecret}`).toString('base64');
  const response = await axios.get('https://sandbox.safaricom.co.ke/oauth/v1/generate?grant_type=client_credentials', {
    headers: {
      Authorization: `Basic ${credentials}`,
    },
  });
  return response.data.access_token;
}

// === Generate Password for STK Push ===
function generatePassword(shortcode, passkey, timestamp) {
  const data = `${shortcode}${passkey}${timestamp}`;
  return Buffer.from(data).toString('base64');
}

// === Get Timestamp in Format YYYYMMDDHHMMSS ===
function getTimestamp() {
  const now = new Date();
  const pad = n => n.toString().padStart(2, '0');
  return `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}`;
}

// === STK Push Endpoint ===
router.post('/stkpush', async (req, res) => {
  try {
    let { amount = 10, phone = '0757732297', accountReference = 'DUKA' } = req.body;

    // Convert to correct format: 2547XXXXXXX
    if (phone.startsWith('0')) {
      phone = '254' + phone.substring(1);
    }

    const timestamp = getTimestamp();
    const password = generatePassword(shortcode, passkey, timestamp);
    const accessToken = await getAccessToken();

    const stkPayload = {
      BusinessShortCode: shortcode,
      Password: password,
      Timestamp: timestamp,
      TransactionType: 'CustomerPayBillOnline',
      Amount: amount,
      PartyA: phone,
      PartyB: shortcode,
      PhoneNumber: phone,
      CallBackURL: callbackURL,
      AccountReference: accountReference,
      TransactionDesc: 'Payment to Duka App',
    };

    // Send STK Push request
    const stkResponse = await axios.post(
      'https://sandbox.safaricom.co.ke/mpesa/stkpush/v1/processrequest',
      stkPayload,
      {
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
      }
    );

    console.log(' STK Push Request Sent:', stkResponse.data);

    res.status(200).json({
      success: true,
      message: 'STK Push initiated successfully',
      data: stkResponse.data,
    });
  } catch (error) {
    const errData = error.response?.data || error.message;
    console.error('STK Push Error:', errData);

    res.status(500).json({
      success: false,
      message: 'Failed to initiate STK Push',
      error: errData,
    });
  }
});


// Pre Fill user form
router.get('/prefill/user/profile', auth.authenticateToken, async (req, res) => {
  let conn;

  try {
    const userId = res.locals.id;

    conn = await getConnection();

    const [rows] = await conn.query(
      `SELECT id, name, email, phone 
       FROM users 
       WHERE id = ?`,
      [userId]
    );

    if (!rows.length) {
      return res.status(404).json({ message: 'User not found' });
    }

    const userData = rows[0];
    res.json({ userData });

  } catch (err) {
    console.error(err);
    res.status(500).json({ message: 'Server error' });
  }
});


// Complete Profile

router.post('/complete/profile', auth.authenticateToken, async (req, res) => {
  let conn;

  try {
    const { name, nida, email, region_id, role, phone } = req.body;

    conn = await getConnection();

    await conn.beginTransaction();

    /* ============================= */
    /* CHECK USER EXISTS */
    /* ============================= */
    const [users] = await conn.query(
      `SELECT id FROM users WHERE nin = ?`,
      [nida]
    );

    if (!users.length) {
      return res.status(404).json({
        message: 'User not found'
      });
    }

    const userId = users[0].id;

    /* ============================= */
    /* CHECK DUPLICATE REGION */
    /* ============================= */
    const [existing] = await conn.query(
      `SELECT * FROM user_region WHERE user_id = ? AND region_id = ?`,
      [userId, region_id]
    );

    if (existing.length > 0) {
      await conn.rollback();
      return res.status(400).json({
        message: `User already assigned to this region`
      });
    }

    /* ============================= */
    /* DYNAMIC UPDATE */
    /* ============================= */
    let fields = [];
    let values = [];

    if (name !== null && name !== '') {
      fields.push('name = ?');
      values.push(name);
    }

    if (email !== null && email !== '') {
      fields.push('email = ?');
      values.push(email);
    }

    if (phone !== null && Number(phone) !== 0) {
      fields.push('phone = ?');
      values.push(phone);
    }

    if (role !== null && role !== '') {
      fields.push('role = ?');
      values.push(role);
    }

    // Always mark profile complete
    fields.push('profileCompleted = 1');

    /* ============================= */
    /* EXECUTE UPDATE ONLY IF HAS DATA */
    /* ============================= */
    if (fields.length > 0) {
      const sql = `
        UPDATE users 
        SET ${fields.join(', ')}
        WHERE nin = ?
      `;

      values.push(nida);

      await conn.query(sql, values);
    }

    /* ============================= */
    /* INSERT REGION */
    /* ============================= */
    await conn.query(
      `INSERT INTO user_region (user_id, region_id)
       VALUES (?, ?)`,
      [userId, region_id]
    );

    await conn.commit();

    return res.status(200).json({
      success: true,
      message: 'Profile completed successfully'
    });

  } catch (err) {
    if (conn) await conn.rollback();

    console.error('COMPLETE PROFILE ERROR:', err);

    return res.status(500).json({
      success: false,
      message: 'Internal server error'
    });
  }
});


// Do not Exceed Over Here

module.exports = router;