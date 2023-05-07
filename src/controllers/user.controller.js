const connection = require("../db/db.connection");
const { scrapingPublisher } = require("../utils/publisher.rabbitmq");
const { deletePublisher } = require("../utils/publisher.rabbitmq");
const { scrapConsumer } = require("../utils/consumer.rabbitmq");
const { deleteConsumer } = require("../utils/consumer.rabbitmq");
const { QueryTypes } = require("sequelize");

let consumer_status = true;

exports.scrapUserDetails = async (req, res, next) => {
  try {
    let { first_name, last_name, email, city, state, queue_array } = req.body;
    let current_date = new Date().toISOString().slice(0, 19).replace("T", " ");
    let queue_id_array = [];

    const ids_Wrapped_In_Quotes = queue_array.map(
      (queue_array) => `'${queue_array}'`
    );
    let temp_sites_id = `(${ids_Wrapped_In_Quotes.join(",")})`;

    let check_if_already_scraped = await connection.query(
      `SELECT * FROM users p
            INNER JOIN rabbitmq_scraping qs ON qs.user_id=p.user_id
            INNER JOIN scraping_details sd ON sd.queue_id_scraping=qs.queue_id_scraping
            WHERE p.email="${email}" AND qs.sites_id IN ${temp_sites_id}
            GROUP BY qs.sites_id`,
      {
        type: QueryTypes.SELECT,
      }
    );
    //console.log(check_if_already_scraped[0], "check_if_already_scraped[0]")
    let scraped_sites_id = [];
    for (i = 0; i < check_if_already_scraped.length; i++) {
      scraped_sites_id.push(check_if_already_scraped[i].sites_id);
    }
    if (scraped_sites_id.length === queue_array.length) {
      res.status(200).json({
        status: true,
        message: "Successfully scraped user data",
        user_id: check_if_already_scraped[0].user_id,
      });
    } else {
      let queue_int_array = queue_array.map(Number);
      const sites_final_Array = queue_int_array.filter(function (obj) {
        return scraped_sites_id.indexOf(obj) === -1;
      });
      //console.log(sites_final_Array, "result")

      let add_user_query = `insert into users (first_name,last_name, email, city,state)`;
      add_user_query += `values ("${first_name}","${last_name}","${email}","${city}","${state}")`;
      add_user_query += `ON DUPLICATE KEY UPDATE city="${city}",state="${state}",updated_at="${current_date}"`;
      let add_user = await connection.query(add_user_query, {});

      if (add_user) {
        let user_id = add_user[0];
        let add_queue_query = `insert into rabbitmq_scraping ( user_id ,sites_id,firstName,lastName,city,state) values`;
        for (let i = 0; i < sites_final_Array.length; i++) {
          if (i == sites_final_Array.length - 1) {
            add_queue_query += `('${user_id}', '${sites_final_Array[i]}',"${first_name}","${last_name}","${city}","${state}")`;
          } else {
            add_queue_query += `('${user_id}', '${sites_final_Array[i]}',"${first_name}","${last_name}","${city}","${state}"),`;
          }
        }
        let add_queue = await connection.query(add_queue_query, {});

        if (add_queue) {
          let first_queue_id = add_queue[0];
          queue_id_array = Array(sites_final_Array.length)
            .fill(0)
            .map((e, i) => i + first_queue_id);
          console.log(queue_id_array);

          if (consumer_status === true) {
            let cousmer_length = process.env.CONSUMER_COUNT;

            console.log("inside consumer method");

            for (let i = 0; i < cousmer_length; i++) {
              console.log(i, "times called");
              await scrapConsumer(`key${i}`, user_id, sites_final_Array, res);
            }

            consumer_status = false;
          }

          setTimeout(async () => {
            await scrapingPublisher(queue_id_array);
          }, 1500);
        } else {
          res.status(200).json({
            status: false,
            message: "Error while adding queue details in database",
          });
        }
      } else {
        res.status(200).json({
          status: false,
          message: "Error while adding user details in database",
        });
      }
    }
  } catch (err) {
    console.log(err);
    res.status(200).json({
      status: false,
      message: err,
    });
  }
};
exports.deleteUserDetails = async (req, res, next) => {
  try {
    let { scraping_id, user_id, sites_id } = req.body;

    let add_queue = await connection.query(
      `insert into rabbitmq ( user_id ,sites_id) values ("${user_id}","${sites_id}")`
    );

    if (add_queue) {
      await deleteConsumer("key0", scraping_id, res);

      setTimeout(async () => {
        await deletePublisher(scraping_id);
      }, 500);
    } else {
      res.status(200).json({
        status: false,
        message: "Error while adding queue details in database",
      });
    }
  } catch (err) {
    console.log(err);
    res.status(200).json({
      status: false,
      message: err,
    });
  }
};

// let add_user_query = `
//   INSERT INTO users (first_name, last_name, email, city, state, consumer_started)
//   VALUES ("${first_name}", "${last_name}", "${email}", "${city}", "${state}",
//     CASE WHEN EXISTS(SELECT * FROM users) THEN NULL ELSE 1 END
//   )
//   ON DUPLICATE KEY UPDATE city="${city}", state="${state}", updated_at="${current_date}"
// `;
