var cron = require("node-cron");
const connection = require("../db/db.connection");
cron.schedule("0 0 0/24 * * *", async () => {
  console.log(
    "CRON JOB IS IN PROGRESS __________________________________________________________________________"
  );
  let transaction;
  try {
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
          if (transaction) await transaction.commit();
        } else {
          if (transaction) await transaction.rollback();
          console.log("Error while deleting user record");
        }
      } else {
        if (transaction) await transaction.rollback();
        console.log("Error while deleting queue record");
      }
    } else {
      if (transaction) await transaction.rollback();
      console.log("Error while deleting scraping record");
    }
  } catch (err) {
    console.log("Catch Error is ", err);
    if (transaction) await transaction.rollback();
  }
});
