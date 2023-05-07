const amqp = require("amqplib");
//const db_connection = require("../db/db.connection");

exports.scrapingPublisher = async (queue_id) => {
  try {
    const connection = await amqp.connect("amqp://localhost");
    const channel = await connection.createChannel();
    const exchangeName = "scraping_exchange";
    await channel.assertExchange(exchangeName, "topic", { durable: false });

    let cousmer_length = process.env.CONSUMER_COUNT;
    const chunk_size = Math.ceil(queue_id.length / cousmer_length); // determine the size of each chunk
    let start_idx = 0;

    for (let i = 0; i < cousmer_length; i++) {
      const end_idx = Math.min(start_idx + chunk_size, queue_id.length); // determine the end index of the current chunk
      const new_queue_id_array = queue_id.slice(start_idx, end_idx); // slice the array to get the current chunk
      const key = "key" + i; // determine the queue key for the current chunk

      console.log(`Sending messages ${new_queue_id_array} to queue "${key}"`);

      // let queue_id_scraping = `(${new_queue_id_array.join(",")})`;
      // let update_queue_status_query = await db_connection.query(
      //   `UPDATE rabbitmq_scraping Set queue_key="${key}" WHERE queue_id_scraping IN ${queue_id_scraping}`
      // );
      for (const id of new_queue_id_array) {
        channel.publish(exchangeName, key, Buffer.from(String(id))); // publish each message to the current queue
      }

      start_idx = end_idx;
    }
    // for (let i = 0; i < queue_id.length; i++) {
    //   let id = queue_id[i].toString();
    //   console.log(`Sending message ${id} to queue "key0" `);
    //   channel.publish(exchangeName, "key0", Buffer.from(String(id)));
    // }
    // setTimeout(() => {
    //     connection.close();
    //     //channel.close();//closes virtual link within created connection
    // }, 60000)
  } catch (error) {
    console.error(error);
  }
};
exports.deletePublisher = async (queue_id) => {
  try {
    //let queue_id = ["1", "2"]
    const connection = await amqp.connect("amqp://localhost");
    const channel = await connection.createChannel();
    const exchangeName = "delete_exchange";
    await channel.assertExchange(exchangeName, "topic", { durable: false });
    for (let i = 0; i < queue_id.length; i++) {
      let id = queue_id[i].toString();
      console.log(`Sending message ${id} to queue "delete_Key0" `);
      channel.publish(exchangeName, "delete_Key0", Buffer.from(String(id)));
    }
    setTimeout(() => {
      connection.close();
    }, 15000);
  } catch (error) {
    console.error(error);
  }
};
