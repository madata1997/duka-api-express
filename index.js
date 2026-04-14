const express = require('express');
var cors = require('cors');
const api = require('./API/api');

const app = express();

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.use(express.static('PDF'));

//  Add this
app.get('/', (req, res) => {
  res.send('API is running 🚀');
});

// API routes
app.use('/api', api);

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
