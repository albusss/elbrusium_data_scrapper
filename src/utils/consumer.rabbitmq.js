const amqp = require("amqplib");
const mysql = require("mysql");
const MySQLEvents = require("@rodrigogs/mysql-events");
const db_connection = require("../db/db.connection");
const { QueryTypes } = require("sequelize");
const { abCheckParser } = require("../parser/abcheck");
const { beenVerifiedParser } = require("../parser/beenverified");
const { floridaResidentParser } = require("../parser/florida");
const { myLifeParser } = require("../parser/mylife");
const { peekYouParser } = require("../parser/peekyou");
const { peopleFinderParser } = require("../parser/peoplefinder");
const { spokeoParser } = require("../parser/spokeo");
const { whitePagesParser } = require("../parser/whitepages");
const { instantCheckMate } = require("../parser/instantcheckmate");
const { inteliusParser } = require("../parser/intelius");
const { radarisParser } = require("../parser/radaris");
const { truthFinder } = require("../parser/truthfinder");
const { usPhoneBook } = require("../parser/usphonebook");
const { usSearchParser } = require("../parser/ussearch");

exports.scrapConsumer = async (key, user_id, res) => {
  try {
    const connection = await amqp.connect("amqp://localhost");
    const channel = await connection.createChannel();
    const exhcange_name = "scraping_exchange";
    await channel.assertExchange(exhcange_name, "topic", { durable: false });
    const q = await channel.assertQueue("", { autoDelete: false });

    console.log(`Waiting for messages in queue: ${q.queue}`);
    await channel.prefetch(1);
    await channel.bindQueue(q.queue, exhcange_name, key);

    await channel.consume(
      q.queue,
      async (message) => {
        let msg = message.content.toString();
        let channel_ack = false;
        console.log(
          "ack on message",
          msg,
          "is -------------------------------",
          channel_ack
        );
        let check_sites_id = await db_connection.query(
          `SELECT sites_id,queue_key
                    FROM rabbitmq_scraping 
                    WHERE queue_id_scraping="${msg}" `,
          {
            type: QueryTypes.SELECT,
          }
        );
        let scraper_id = check_sites_id[0].sites_id;

        console.log("Listening on message", msg, ` in key ${key}`);

        let timeout = setTimeout(async () => {
          console.log("inside timeout condition *********** on msg", msg);
          console.log(channel_ack, "channel_ack");
          console.log(!channel_ack == channel_ack);
          console.log(!channel_ack === channel_ack);

          if (!channel_ack) {
            console.log("inside timeout ack--------------------");

            channel_ack = true;
            let get_scraping_status = await db_connection.query(
              `SELECT queue_status
                    FROM rabbitmq_scraping
                    WHERE queue_id_scraping="${msg}" `,
              {
                type: QueryTypes.SELECT,
              }
            );
            let queue_status = get_scraping_status[0].queue_status;

            if (queue_status != "Solved") {
              let update_status = await db_connection.query(
                `UPDATE rabbitmq_scraping SET queue_status="Unsolved" WHERE queue_id_scraping="${msg}" `
              );
            }
            channel.ack(message);
            console.log("after ack in timeout");
          }
        }, 50000);

        switch (scraper_id) {
          case 1:
            console.log(
              "been verified called -----------------------------------------------------"
            );
            let beenVerifiedParserResult = await beenVerifiedParser();
            if (beenVerifiedParserResult) {
              console.log("inside successcase on", msg);
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                console.log("ack on  msg------------>", parseInt(msg));
                channel.ack(message);
                clearTimeout(timeout);
              }
            } else {
              console.log("***** inside scraper fail condition ****");
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                let get_scraping_status = await db_connection.query(
                  `SELECT queue_status FROM rabbitmq_scraping WHERE queue_id_scraping="${msg}" `,
                  {
                    type: QueryTypes.SELECT,
                  }
                );
                let queue_status = get_scraping_status[0].queue_status;
                if (queue_status != "Solved") {
                  let update_status = await db_connection.query(
                    `UPDATE rabbitmq_scraping SET queue_status="Unsolved" WHERE queue_id_scraping="${msg}" `
                  );
                }
                channel.ack(message);
                clearTimeout(timeout);
                console.log("after ack in timeout");
              }
            }
            break;
          case 3:
            console.log(
              "my life called -----------------------------------------------------"
            );
            let myLifeParserResult = await myLifeParser();
            if (myLifeParserResult) {
              console.log("inside successcase on", msg);
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                console.log("ack on  msg------------>", parseInt(msg));
                channel.ack(message);
                clearTimeout(timeout);
              }
            } else {
              console.log("***** inside scraper fail condition ****");
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                let get_scraping_status = await db_connection.query(
                  `SELECT queue_status FROM rabbitmq_scraping WHERE queue_id_scraping="${msg}" `,
                  {
                    type: QueryTypes.SELECT,
                  }
                );
                let queue_status = get_scraping_status[0].queue_status;
                if (queue_status != "Solved") {
                  let update_status = await db_connection.query(
                    `UPDATE rabbitmq_scraping SET queue_status="Unsolved" WHERE queue_id_scraping="${msg}" `
                  );
                }
                channel.ack(message);
                clearTimeout(timeout);
                console.log("after ack in timeout");
              }
            }
            break;
          case 4:
            console.log(
              "folirda resident called ----------------------------------------"
            );
            let floridaResidentParserResult = await floridaResidentParser();
            if (floridaResidentParserResult) {
              console.log("inside successcase on", msg);
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                console.log("ack on  msg------------>", parseInt(msg));
                channel.ack(message);
                clearTimeout(timeout);
              }
            } else {
              console.log("***** inside scraper fail condition ****");
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                let get_scraping_status = await db_connection.query(
                  `SELECT queue_status FROM rabbitmq_scraping WHERE queue_id_scraping="${msg}" `,
                  {
                    type: QueryTypes.SELECT,
                  }
                );
                let queue_status = get_scraping_status[0].queue_status;
                if (queue_status != "Solved") {
                  let update_status = await db_connection.query(
                    `UPDATE rabbitmq_scraping SET queue_status="Unsolved" WHERE queue_id_scraping="${msg}" `
                  );
                }
                channel.ack(message);
                clearTimeout(timeout);
                console.log("after ack in timeout");
              }
            }
            break;
          case 7:
            console.log(
              "ab check called -------------------------------------------------------"
            );
            let abCheckParserResult = await abCheckParser();
            if (abCheckParserResult) {
              console.log("inside successcase on", msg);
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                console.log("ack on  msg------------>", parseInt(msg));
                channel.ack(message);
                clearTimeout(timeout);
              }
            } else {
              console.log("***** inside scraper fail condition ****");
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                let get_scraping_status = await db_connection.query(
                  `SELECT queue_status FROM rabbitmq_scraping WHERE queue_id_scraping="${msg}" `,
                  {
                    type: QueryTypes.SELECT,
                  }
                );
                let queue_status = get_scraping_status[0].queue_status;
                if (queue_status != "Solved") {
                  let update_status = await db_connection.query(
                    `UPDATE rabbitmq_scraping SET queue_status="Unsolved" WHERE queue_id_scraping="${msg}" `
                  );
                }
                channel.ack(message);
                clearTimeout(timeout);
                console.log("after ack in timeout");
              }
            }
            break;
          case 8:
            console.log(
              "spokeo called -----------------------------------------------------"
            );
            let spokeoParserResult = await spokeoParser();
            if (spokeoParserResult) {
              console.log("inside successcase on", msg);
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                console.log("ack on  msg------------>", parseInt(msg));
                channel.ack(message);
                clearTimeout(timeout);
              }
            } else {
              console.log("***** inside scraper fail condition ****");
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                let get_scraping_status = await db_connection.query(
                  `SELECT queue_status FROM rabbitmq_scraping WHERE queue_id_scraping="${msg}" `,
                  {
                    type: QueryTypes.SELECT,
                  }
                );
                let queue_status = get_scraping_status[0].queue_status;
                if (queue_status != "Solved") {
                  let update_status = await db_connection.query(
                    `UPDATE rabbitmq_scraping SET queue_status="Unsolved" WHERE queue_id_scraping="${msg}" `
                  );
                }
                channel.ack(message);
                clearTimeout(timeout);
                console.log("after ack in timeout");
              }
            }
            break;
          case 9:
            console.log(
              "peekyou called -----------------------------------------------------"
            );
            let peekYouParserResult = await peekYouParser();
            if (peekYouParserResult) {
              console.log("inside successcase on", msg);
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                console.log("ack on  msg------------>", parseInt(msg));
                channel.ack(message);
                clearTimeout(timeout);
              }
            } else {
              console.log("***** inside scraper fail condition ****");
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                let get_scraping_status = await db_connection.query(
                  `SELECT queue_status FROM rabbitmq_scraping WHERE queue_id_scraping="${msg}" `,
                  {
                    type: QueryTypes.SELECT,
                  }
                );
                let queue_status = get_scraping_status[0].queue_status;
                if (queue_status != "Solved") {
                  let update_status = await db_connection.query(
                    `UPDATE rabbitmq_scraping SET queue_status="Unsolved" WHERE queue_id_scraping="${msg}" `
                  );
                }
                channel.ack(message);
                clearTimeout(timeout);
                console.log("after ack in timeout");
              }
            }
            break;
          case 10:
            console.log(
              "people finder called -----------------------------------------------------"
            );
            let peopleFinderParserResult = await peopleFinderParser();
            if (peopleFinderParserResult) {
              console.log("inside successcase on", msg);
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                console.log("ack on  msg------------>", parseInt(msg));
                channel.ack(message);
                clearTimeout(timeout);
              }
            } else {
              console.log("***** inside scraper fail condition ****");
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                let get_scraping_status = await db_connection.query(
                  `SELECT queue_status FROM rabbitmq_scraping WHERE queue_id_scraping="${msg}" `,
                  {
                    type: QueryTypes.SELECT,
                  }
                );
                let queue_status = get_scraping_status[0].queue_status;
                if (queue_status != "Solved") {
                  let update_status = await db_connection.query(
                    `UPDATE rabbitmq_scraping SET queue_status="Unsolved" WHERE queue_id_scraping="${msg}" `
                  );
                }
                channel.ack(message);
                clearTimeout(timeout);
                console.log("after ack in timeout");
              }
            }
            break;
          case 12:
            console.log(
              "white pages called -----------------------------------------------------"
            );
            let whitePagesParserResult = await whitePagesParser();
            if (whitePagesParserResult) {
              console.log("inside successcase on", msg);
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                console.log("ack on  msg------------>", parseInt(msg));
                channel.ack(message);
                clearTimeout(timeout);
              }
            } else {
              console.log("***** inside scraper fail condition ****");
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                let get_scraping_status = await db_connection.query(
                  `SELECT queue_status FROM rabbitmq_scraping WHERE queue_id_scraping="${msg}" `,
                  {
                    type: QueryTypes.SELECT,
                  }
                );
                let queue_status = get_scraping_status[0].queue_status;
                if (queue_status != "Solved") {
                  let update_status = await db_connection.query(
                    `UPDATE rabbitmq_scraping SET queue_status="Unsolved" WHERE queue_id_scraping="${msg}" `
                  );
                }
                channel.ack(message);
                clearTimeout(timeout);
                console.log("after ack in timeout");
              }
            }
            break;
          case 13:
            console.log(
              "us search called -----------------------------------------------------"
            );
            let usSearchParserResult = await usSearchParser();
            if (usSearchParserResult) {
              console.log("inside successcase on", msg);
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                console.log("ack on  msg------------>", parseInt(msg));
                channel.ack(message);
                clearTimeout(timeout);
              }
            } else {
              console.log("***** inside scraper fail condition ****");
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                let get_scraping_status = await db_connection.query(
                  `SELECT queue_status FROM rabbitmq_scraping WHERE queue_id_scraping="${msg}" `,
                  {
                    type: QueryTypes.SELECT,
                  }
                );
                let queue_status = get_scraping_status[0].queue_status;
                if (queue_status != "Solved") {
                  let update_status = await db_connection.query(
                    `UPDATE rabbitmq_scraping SET queue_status="Unsolved" WHERE queue_id_scraping="${msg}" `
                  );
                }
                channel.ack(message);
                clearTimeout(timeout);
                console.log("after ack in timeout");
              }
            }
            break;
          case 14:
            console.log(
              "us phone book called -----------------------------------------------------"
            );
            let usPhoneBookParserResult = await usPhoneBook();
            if (usPhoneBookParserResult) {
              console.log("inside successcase on", msg);
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                console.log("ack on  msg------------>", parseInt(msg));
                channel.ack(message);
                clearTimeout(timeout);
              }
            } else {
              console.log("***** inside scraper fail condition ****");
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                let get_scraping_status = await db_connection.query(
                  `SELECT queue_status FROM rabbitmq_scraping WHERE queue_id_scraping="${msg}" `,
                  {
                    type: QueryTypes.SELECT,
                  }
                );
                let queue_status = get_scraping_status[0].queue_status;
                if (queue_status != "Solved") {
                  let update_status = await db_connection.query(
                    `UPDATE rabbitmq_scraping SET queue_status="Unsolved" WHERE queue_id_scraping="${msg}" `
                  );
                }
                channel.ack(message);
                clearTimeout(timeout);
                console.log("after ack in timeout");
              }
            }
            break;
          case 15:
            console.log(
              "truth finder called -----------------------------------------------------"
            );
            let truthFinderParserResult = await truthFinder();
            if (truthFinderParserResult) {
              console.log("inside successcase on", msg);
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                console.log("ack on  msg------------>", parseInt(msg));
                channel.ack(message);
                clearTimeout(timeout);
              }
            } else {
              console.log("***** inside scraper fail condition ****");
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                let get_scraping_status = await db_connection.query(
                  `SELECT queue_status FROM rabbitmq_scraping WHERE queue_id_scraping="${msg}" `,
                  {
                    type: QueryTypes.SELECT,
                  }
                );
                let queue_status = get_scraping_status[0].queue_status;
                if (queue_status != "Solved") {
                  let update_status = await db_connection.query(
                    `UPDATE rabbitmq_scraping SET queue_status="Unsolved" WHERE queue_id_scraping="${msg}" `
                  );
                }
                channel.ack(message);
                clearTimeout(timeout);
                console.log("after ack in timeout");
              }
            }
            break;
          case 16:
            console.log(
              "radis parsr called -----------------------------------------------------"
            );
            let radarisParserResult = await radarisParser();
            if (radarisParserResult) {
              console.log("inside successcase on", msg);
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                console.log("ack on  msg------------>", parseInt(msg));
                channel.ack(message);
                clearTimeout(timeout);
              }
            } else {
              console.log("***** inside scraper fail condition ****");
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                let get_scraping_status = await db_connection.query(
                  `SELECT queue_status FROM rabbitmq_scraping WHERE queue_id_scraping="${msg}" `,
                  {
                    type: QueryTypes.SELECT,
                  }
                );
                let queue_status = get_scraping_status[0].queue_status;
                if (queue_status != "Solved") {
                  let update_status = await db_connection.query(
                    `UPDATE rabbitmq_scraping SET queue_status="Unsolved" WHERE queue_id_scraping="${msg}" `
                  );
                }
                channel.ack(message);
                clearTimeout(timeout);
                console.log("after ack in timeout");
              }
            }
            break;
          case 17:
            console.log(
              "intellius parser called -----------------------------------------------------"
            );
            let intelliusParserResult = await inteliusParser();
            if (intelliusParserResult) {
              console.log("inside successcase on", msg);
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                console.log("ack on  msg------------>", parseInt(msg));
                channel.ack(message);
                clearTimeout(timeout);
              }
            } else {
              console.log("***** inside scraper fail condition ****");
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                let get_scraping_status = await db_connection.query(
                  `SELECT queue_status FROM rabbitmq_scraping WHERE queue_id_scraping="${msg}" `,
                  {
                    type: QueryTypes.SELECT,
                  }
                );
                let queue_status = get_scraping_status[0].queue_status;
                if (queue_status != "Solved") {
                  let update_status = await db_connection.query(
                    `UPDATE rabbitmq_scraping SET queue_status="Unsolved" WHERE queue_id_scraping="${msg}" `
                  );
                }
                channel.ack(message);
                clearTimeout(timeout);
                console.log("after ack in timeout");
              }
            }
            break;
          case 18:
            console.log(
              "instant check mate called -----------------------------------------------------"
            );
            let instantCheckMateParserResult = await instantCheckMate();
            if (instantCheckMateParserResult) {
              console.log("inside successcase on", msg);
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                console.log("ack on  msg------------>", parseInt(msg));
                channel.ack(message);
                clearTimeout(timeout);
              }
            } else {
              console.log("***** inside scraper fail condition ****");
              console.log(channel_ack, "channel_ack");
              if (!channel_ack) {
                channel_ack = true;
                let get_scraping_status = await db_connection.query(
                  `SELECT queue_status FROM rabbitmq_scraping WHERE queue_id_scraping="${msg}" `,
                  {
                    type: QueryTypes.SELECT,
                  }
                );
                let queue_status = get_scraping_status[0].queue_status;
                if (queue_status != "Solved") {
                  let update_status = await db_connection.query(
                    `UPDATE rabbitmq_scraping SET queue_status="Unsolved" WHERE queue_id_scraping="${msg}" `
                  );
                }
                channel.ack(message);
                clearTimeout(timeout);
                console.log("after ack in timeout");
              }
            }
            break;
          default:
            console.log("Invalid scraper_id");
            channel.ack(message);
            clearTimeout(timeout);
            break;
        }
      },
      { noAck: false }
    );
  } catch (error) {
    console.log(error);
    res.status(200).json({
      status: false,
      message: error,
    });
  }
};
// exports.deleteConsumer = async (key, scraping_id, res) => {
//     try {
//         const connection = await amqp.connect('amqp://localhost');
//         const channel = await connection.createChannel();
//         const exchange_name = "delete_exchange";
//         await channel.assertExchange(exchange_name, 'topic', { durable: false });
//         const q = await channel.assertQueue('', { autoDelete: false });

//         console.log(`Waiting for messages in queue: ${q.queue}`);

//         await channel.prefetch(1);
//         await channel.bindQueue(q.queue, exchange_name, key)
//         let count = 0;
//         await channel.consume(q.queue, async message => {
//             let parser_result
//             let msg = message.content.toString();
//             if (scraping_id[count] === 1) {
//                 console.log("inside parser")
//                 //! delete parse here parser_result = await beenVerifiedParser(first_name, last_name, state);
//             }

//             setTimeout(async function () {
//                 if (message !== null) {
//                     console.log("***** inside scraper fail condition ****")

//                     let response = await markDelete(msg, parser_result, user_id, scraping_id[count])
//                     if (response) {
//                         channel.ack(message);
//                         count++;

//                         if (count == scraping_id.length) {
//                             console.log("inside close method")
//                             await channel.close();
//                             res.status(200).json({
//                                 status: true,
//                                 message: "Successfully deleted user data",
//                             });
//                         }
//                     }
//                     else {
//                         res.status(200).json({
//                             status: false,
//                             message: "Error while updating db fields",
//                         });
//                     }
//                 }
//             }, 10000);
//         }, { noAck: false })
//     } catch (error) {
//         res.status(200).json({
//             status: false,
//             message: error,
//         });
//     }
// }
// const markDelete = async (scraping_id) => {
//     try {
//         let current_date = new Date().toISOString().slice(0, 19).replace("T", " ");
//         let update_delete_status = await connection.query(
//             `UPDATE scraping_details sd,
//              INNER JOIN rabbitmq r ON r.queue_id = sd.queue_id
//              SET r.queue_status = "Solved",r.updated_at = "${current_date}",sd.delete_status="Y",sd.updated_at="${current_date}" WHERE sd.scraping_id = "${scraping_id}"`);

//         if (update_delete_status) {
//             return true;
//         } else {
//             console.log("error while adding scraping details")
//             return false;
//         }
//     } catch (err) {
//         console.log(err);
//         return false;
//     }
// }
