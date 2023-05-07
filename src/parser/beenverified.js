const connection = require("../db/db.connection");
const puppeteer = require("puppeteer-extra");
const { QueryTypes } = require("sequelize");
const { executablePath } = require("puppeteer");
const stealthPlugin = require("puppeteer-extra-plugin-stealth");
puppeteer.use(stealthPlugin());
exports.beenVerifiedParser = async (req, res) => {
  let transaction;
  try {
    transaction = await connection.transaction();
    let current_date = new Date().toISOString().slice(0, 19).replace("T", " ");

    let get_scraping_fields = await connection.query(
      `SELECT * FROM rabbitmq_scraping WHERE queue_status = 'Processing' AND sites_id="1"
        ORDER BY created_at ASC`,
      {
        type: QueryTypes.SELECT,
        transaction,
      }
    );
    // console.log(get_scraping_fields[0]);
    let firstName = get_scraping_fields[0]["firstName"];
    let lastName = get_scraping_fields[0]["lastName"];
    let state = get_scraping_fields[0]["state"];
    let queue_id = get_scraping_fields[0]["queue_id_scraping"];
    // console.log(firstName, "firstName");
    // console.log(lastName);
    //console.log(state);
    const scrapeURL = "https://www.beenverified.com/app/optout/search";
    const browser = await puppeteer.launch({
      executablePath: executablePath(),
      ignoreDefaultArgs: ["--disable-extensions"],
      args: ["--no-sandbox", "--single-process"],
      headless: true,
    });
    //const browser = await puppeteer.launch({ headless: true, executablePath: executablePath() });
    const page = await browser.newPage();
    //await page.setCacheEnabled(false) // --- new line ---
    await page.setUserAgent(
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36"
    ); // new line
    await page.goto(scrapeURL, {
      waitUntil: "domcontentloaded",
      timeout: 0,
    });
    //console.log("--------");
    await page.waitForSelector("#ember6");
    await page.type("#ember6", `${firstName}`);
    await page.type("#ember7", `${lastName}`);
    await page.select('select[name="state"]', `${state}`);
    await page.click("#optout-search");
    await page.waitForSelector("#static-wrapper-v2");
    await page.waitForSelector("div.card-content");
    const results = await page.evaluate(() => {
      let titleNodeList = Array.from(
        document.querySelectorAll("div.card-content")
      );
      let res = [];
      let infoArr = [];
      let dataObj = {};
      let born;
      let aliases;
      let relatives;
      let knownLocations;
      titleNodeList.map((td) => {
        const nameContent = td
          .querySelector("h3.person-name")
          .textContent.trim();
        const nameAge = nameContent.split(",");
        const name = nameAge[0] ? nameAge[0].trim() : "";
        const age = nameAge[1] ? nameAge[1].trim() : "";
        const location = td.querySelector("p.person-city").textContent.trim();
        let personalInfo = Array.from(td.querySelectorAll("div.person-info"));
        infoArr = [];
        dataObj = {};
        dataObj.name = name;
        dataObj.age = age;
        dataObj.location = location;
        personalInfo.map((pi) => {
          let tag = pi.querySelector(".info-label").innerText;
          let value = pi.querySelector("p.info-data").innerText;
          if (tag == "Born") {
            born = value;
            dataObj.born = born;
          }
          if (tag == "Aliases") {
            aliases = value;
            dataObj.aliases = aliases;
          }
          if (tag == "Relatives") {
            relatives = value;
            dataObj.relatives = relatives;
          }
          if (tag == "Known locations") {
            knownLocations = value;
            dataObj.knownLocations = knownLocations;
          }
        });
        res.push(dataObj);
      });
      return res;
    });
    await browser.close();
    //console.log("results",results)
    let scraping_site_url = `https://www.beenverified.com/app/search/person?fname=${firstName}&ln=${lastName}&optout=true&state=${state}`;
    let add_scraping_details_query = `INSERT INTO scraping_details (queue_id_scraping,name,birthday,location,known_locations,age,relatives,alias,scraping_site_url) VALUES `;

    for (let i = 0; i < results.length; i++) {
      if (results[i].aliases === undefined) {
        results[i].aliases = "NULL";
      }
      if (results[i].relatives === undefined) {
        results[i].relatives = "NULL";
      }
      if (results[i].born === undefined) {
        results[i].born = "NULL";
      }
      if (results[i].knownLocations === undefined) {
        results[i].knownLocations = "NULL";
      }
      if (results[i].age === " ") {
        results[i].age = "NULL";
      }

      if (i == results.length - 1) {
        add_scraping_details_query += `('${queue_id}', '${results[i].name}','${results[i].born}','${results[i].location}','${results[i].knownLocations}','${results[i].age}','${results[i].relatives}','${results[i].aliases}','${scraping_site_url}')`;
      } else {
        add_scraping_details_query += `('${queue_id}', '${results[i].name}','${results[i].born}','${results[i].location}','${results[i].knownLocations}','${results[i].age}','${results[i].relatives}','${results[i].aliases}','${scraping_site_url}'),`;
      }
    }
    let add_scraping_details = await connection.query(
      add_scraping_details_query,
      { transaction }
    );
    if (add_scraping_details) {
      let update_queue_status_query = await connection.query(
        `UPDATE rabbitmq_scraping Set queue_status="Solved",updated_at="${current_date}" WHERE queue_id_scraping = ${queue_id}`,
        { transaction }
      );
      if (update_queue_status_query) {
        console.log("ok--------------------");
        if (transaction) await transaction.commit();
        return true;
      } else {
        if (transaction) await transaction.rollback();
        return false;
      }
    } else {
      if (transaction) await transaction.rollback();
      return false;
    }
  } catch (e) {
    console.log(e);
    if (transaction) await transaction.rollback();
    return false;
  }
};
