const { QueryTypes } = require("sequelize");
const connection = require("../db/db.connection");
const bcrypt = require("bcryptjs");
const getAccessToken = require("../middleware/assign.token");
const { exec } = require("child_process");
const path = require("path");
const fs = require("fs");
const dotenv = require("dotenv");

exports.login = async (req, res, next) => {
  try {
    let { email, password } = req.body;
    let validate_password = false;

    let check_email = await connection.query(
      `select * from users where email = '${email}' 
      `,
      {
        type: QueryTypes.SELECT,
      }
    );
    if (check_email && check_email.length > 0) {
      let storedHashPassword = check_email[0].password;
      validate_password = await bcrypt.compare(password, storedHashPassword);

      if (validate_password) {
        let token = await getAccessToken(check_email[0]);
        delete check_email[0].password;
        res.status(200).json({
          status: true,
          message: "Logged in successfully",
          user: check_email[0],
          token,
        });
      } else {
        res.status(200).json({
          status: false,
          message: "Enterted Password does not matched",
          user: [],
        });
      }
    } else {
      res.status(200).json({
        status: false,
        message: "The email address you entered isn't connected to an account.",
        user: [],
      });
    }
  } catch (error) {
    res.status(200).json({
      status: false,
      message: "Error while loggin in",
      error: error.message,
      user: [],
    });
  }
};
exports.addSite = async (req, res) => {
  try {
    const { site_name, site_url } = req.body;
    let add_site = await connection.query(
      `INSERT INTO sites (site_name,site_url) values ("${site_name}","${site_url}")`
    );

    if (add_site) {
      res.status(200).json({
        status: true,
        sites: "Successfully added new site",
      });
    } else {
      res.status(200).json({
        status: false,
        sites: "Error while adding new site",
      });
    }
  } catch (error) {
    res.status(200).json({
      status: false,
      message: error.message,
    });
  }
};
exports.getSitesList = async (req, res) => {
  try {
    let get_active_sites = await connection.query(
      `SELECT * FROM sites WHERE scraping_status = "Y" `,
      { type: QueryTypes.SELECT }
    );

    if (get_active_sites && get_active_sites.length > 0) {
      res.status(200).json({
        status: true,
        sites: get_active_sites,
      });
    } else {
      res.status(200).json({
        status: false,
        sites: [],
        message: "Error !! there should be at least one scraping site in db",
      });
    }
  } catch (error) {
    res.status(200).json({
      status: false,
      message: error.message,
    });
  }
};
exports.updateSiteStatus = async (req, res) => {
  try {
    const { status, sites_id } = req.body;

    await connection.query(
      `UPDATE sites SET scraping_status="${status}" WHERE sites_id = ${sites_id}`
    );
    if (update_status) {
      res.status(200).json({
        status: true,
        sites: "Successfully updated site status",
      });
      console.log("after");
    } else {
      res.status(200).json({
        status: false,
        sites: "Error while updating site status",
      });
    }
  } catch (error) {
    res.status(200).json({
      status: false,
      message: error.message,
    });
  }
};
exports.deleteSite = async (req, res) => {
  try {
    const sites_id = req.params.id;
    let get_active_sites = await connection.query(
      `SELECT * FROM sites WHERE sites_id = "${sites_id}" `,
      { type: QueryTypes.SELECT }
    );

    if (get_active_sites && get_active_sites.length > 0) {
      let delete_site = await connection.query(
        `DELETE FROM sites WHERE sites_id = ${sites_id}`
      );

      if (delete_site) {
        res.status(200).json({
          status: true,
          sites: "Successfully deleted site",
        });
      } else {
        res.status(200).json({
          status: false,
          sites: "Error while deleting site",
        });
      }
    } else {
      res.status(200).json({
        status: false,
        sites: "No site found in db against this site_id",
      });
    }
  } catch (error) {
    res.status(200).json({
      status: false,
      message: error.message,
    });
  }
};
exports.setConsumers = async (req, res) => {
  let transaction;
  try {
    transaction = await connection.transaction();
    const { consumer_count } = req.body;

    let delete_scraping_record = await connection.query(
      `DELETE FROM scraping_details`,
      {
        transaction,
      }
    );

    if (delete_scraping_record) {
      let delete_queues_record = await connection.query(
        `DELETE FROM rabbitmq_scraping`,
        {
          transaction,
        }
      );

      if (delete_queues_record) {
        let delete_users_record = await connection.query(
          `DELETE FROM users WHERE admin="N" `,
          {
            transaction,
          }
        );
        if (delete_users_record) {
          //rest consumer value
          try {
            await updateConsumerCount(consumer_count, transaction);
            console.log("CONSUMER_COUNT updated successfully");
          } catch (error) {
            console.error(`Error updating CONSUMER_COUNT: ${error.message}`);
          }

          //restart RabbitMQ
          await new Promise((resolve) => {
            restartRabbitMQ(transaction);
            resolve();
          });
          //restart server
          await new Promise((resolve) => {
            restartServer(transaction);
            resolve();
          });

          if (transaction) await transaction.commit();
          res.status(200).json({
            status: true,
            sites: "Successfully deleted site",
          });
        } else {
          if (transaction) await transaction.rollback();
          res.status(200).json({
            status: false,
            sites: "Error while deleting user record",
          });
        }
      } else {
        if (transaction) await transaction.rollback();
        res.status(200).json({
          status: false,
          sites: "Error while deleting queue record",
        });
      }
    } else {
      if (transaction) await transaction.rollback();
      res.status(200).json({
        status: false,
        sites: "Error while deleting scraping record",
      });
    }
  } catch (error) {
    if (transaction) await transaction.rollback();
    res.status(200).json({
      status: false,
      message: error.message,
    });
  }
};
async function restartRabbitMQ(transaction) {
  try {
    // Get the RabbitMQ installation directory path
    const rabbitmqSbinDir =
      "C:\\Program Files\\RabbitMQ Server\\rabbitmq_server-3.11.11\\sbin";
    const fullDirPath = path.normalize(rabbitmqSbinDir);

    console.log(fullDirPath, "fullDirPath");
    // Change the current working directory to the RabbitMQ installation directory
    exec(`cd ${fullDirPath}`);

    const options = {
      cwd: fullDirPath,
      env: {
        ERLANG_HOME: "C:\\Program Files\\Erlang OTP",
      },
    };
    // Stop, reset, and start RabbitMQ service
    const restartProcess = exec(
      `cmd /c rabbitmqctl.bat stop_app && rabbitmqctl.bat reset && rabbitmqctl.bat start_app`,
      options
    );
    restartProcess.stdout.on("data", (data) => {
      console.log(`RabbitMQ service restarted: ${data}`);
    });
  } catch (error) {
    if (transaction) await transaction.rollback();

    console.error(`Error executing command: ${error.message}`);
  }
}
async function restartServer(transaction) {
  try {
    console.log("Restarting server...");

    setTimeout(() => {
      exec("pm2 restart 0", (error, stdout, stderr) => {
        if (error) {
          console.error(`Error: ${error.message}`);
          return;
        }
        if (stderr) {
          console.error(`Stderr: ${stderr}`);
          return;
        }
        console.log(`Stdout: ${stdout}`);
      });
    }, 100);
  } catch (error) {
    if (transaction) await transaction.rollback();
    console.error(`Error while restarting server ${error.message}`);
  }
}
async function updateConsumerCount(consumerCount, transaction) {
  try {
    // Load the existing environment variables from the .env file
    const envConfig = dotenv.parse(fs.readFileSync(".env"));

    // Update the CONSUMER_COUNT value
    envConfig.CONSUMER_COUNT = consumerCount.toString();

    // Write the updated environment variables to the .env file
    fs.writeFileSync(
      ".env",
      Object.entries(envConfig)
        .map(([k, v]) => `${k}=${v}`)
        .join("\n")
    );
  } catch (error) {
    if (transaction) await transaction.rollback();
    console.error(`Error while chaning consumer value ${error.message}`);
  }
}
