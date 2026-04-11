const express = require('express');
var cors = require('cors');
const api = require('./API/api');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static('PDF'));
app.use('/api',api);
module.exports = app;