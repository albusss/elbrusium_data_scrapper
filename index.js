const express = require("express");
const morgan = require("morgan");
require("dotenv").config();
const app = express();

app.use(express.json());
app.use(morgan("dev"));

app.use(express.urlencoded({ extended: true }));

app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header(
    "Access-Control-Allow-Headers",
    "x-www-form-urlencoded, Origin, X-Requested-With, Content-Type, Accept, Authorization, *"
  );
  if (req.method === "OPTIONS") {
    res.header(
      "Access-Control-Allow-Methods",
      "GET, PUT, POST, PATCH, DELETE, OPTIONS"
    );
    res.setHeader("Access-Control-Allow-Credentials", true);
    return res.status(200).json({});
  }
  next();
});

const routes = {
  adminRouter: require("./src/routes/admin.routes"),
  userRouter: require("./src/routes/user.routes"),
};

app.use("/api/admin", routes.adminRouter);
app.use("/api/user", routes.userRouter);

require("./src/cron/delete.cron");

const port = process.env.PORT || 4000;

app.listen(port, () => {
  console.log(`App listening on port ${port}`);
});
