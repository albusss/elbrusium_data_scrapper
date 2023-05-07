const { Sequelize } = require("sequelize");

const dbStatus = process.env.DB_LIVE === "true" ? "live" : "local";
if (dbStatus == "local") {
  const connection = new Sequelize(
    process.env.DB_NAME,
    process.env.DB_USER,
    process.env.DB_PASSWORD,
    {
      host: process.env.DB_HOST,
      dialect: "mysql",
    }
  );
  connection
    .authenticate()
    .then(() => {
      console.log(`Connected to ${process.env.DB_NAME}`);
    })
    .catch((err) => {
      console.log(`Error while connecting to database: ""`);
    });
  global.connection = connection;
  module.exports = connection;
}
if (dbStatus == "live") {
  const connection = new Sequelize("scraping", "root", "12345", {
    logging: true,
    host: "localhost",
    dialect: "mysql",
  });
  connection
    .authenticate()
    .then(() => {
      console.log(`Connected to ${process.env.DB_NAME}`);
    })
    .catch((err) => {
      console.log(`Error while connecting to database`);
    });
  global.connection = connection;
  module.exports = connection;
}
