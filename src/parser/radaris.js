const connection = require("../db/db.connection");
const puppeteer = require("puppeteer-extra");
const { QueryTypes } = require("sequelize");
const { executablePath } = require("puppeteer");
const stealthPlugin = require("puppeteer-extra-plugin-stealth");
puppeteer.use(stealthPlugin());
let RUA = require("random-useragent");

exports.radarisParser = async (req, res) => {
  let transaction;
  let browser;
  let page;
  try {
    transaction = await connection.transaction();
    let current_date = new Date().toISOString().slice(0, 19).replace("T", " ");

    let get_scraping_fields = await connection.query(
      `SELECT * FROM rabbitmq_scraping WHERE queue_status = 'Processing' AND sites_id="16"
        ORDER BY created_at ASC`,
      {
        type: QueryTypes.SELECT,
        transaction,
      }
    );
    console.log(get_scraping_fields[0]);
    let firstName = get_scraping_fields[0]["firstName"];
    let lastName = get_scraping_fields[0]["lastName"];
    let state = get_scraping_fields[0]["state"];
    let city = get_scraping_fields[0]["city"];
    let queue_id = get_scraping_fields[0]["queue_id_scraping"];
    console.log(firstName, "firstName");
    console.log(lastName);
    console.log(state);

    try {
      browser = await puppeteer.launch({
        slowMo: 100,
        headless: true,
        devtools: false,
        args: [
          `--proxy-server=${process.env.PROXY_IP}`,
          "--no-sandbox",
          "--disable-setuid-sandbox",
          "--disable-gpu",
          "--disable-dev-shm-usage",
          "--single-process",
        ],
        executablePath: executablePath(),
      });

      page = await browser.newPage();

      await page.setJavaScriptEnabled(true);
      await page.setDefaultNavigationTimeout(0);
      await page.setDefaultTimeout(30000);
      await page.setRequestInterception(true);

      await page.setViewport({
        width: 1920 + Math.floor(Math.random() * 100),
        height: 3000 + Math.floor(Math.random() * 100),
        deviceScaleFactor: 1,
        hasTouch: false,
        isLandscape: false,
        isMobile: false,
      });

      page.on("request", (req) => {
        if (
          req.resourceType() === "image" ||
          req.resourceType() === "stylesheet" ||
          req.resourceType() === "font" ||
          req.url().includes("amazon") ||
          req.url().includes("youtube") ||
          req.url().includes("google") ||
          req.url().includes("adservice")
        ) {
          req.abort();
        } else {
          req.continue();
        }
      });

      await page.authenticate({
        username: process.env.PROXY_USER,
        password: process.env.PROXY_PASS,
      });
      await page.goto(
        `https://radaris.com/ng/search?ff=${firstName}&fl=${lastName}&fs=${state}&fc=${city}`
      );
    } catch (error) {
      console.log(`BROWSER_ERROR: ${error}`);
      return;
    }

    try {
      console.log("waiting for #tbl_ps");
      await page.waitForSelector("#tbl_ps");
    } catch (error) {
      console.log(`SELECTOR_ERROR: ${error}`);
      return;
    }

    let results;

    try {
      results = await page.evaluate(() => {
        let titleNodeList = Array.from(
          document.querySelectorAll("div.card.teaser-card")
        );
        let res = [];
        titleNodeList.map((td, index) => {
          const nameNode = td.querySelector("a.card-title");
          const ageNode = td.querySelector("div.age-wr.age");
          const locationNode = td.querySelector("p.res-in");
          const linkNode = td.querySelector("a");

          const name = nameNode ? nameNode.textContent : "";
          let agebirth = ageNode ? ageNode.textContent : "";
          let age = agebirth.slice(0, 8).replace("/", "").trim();
          let born = agebirth.slice(-8);
          const location = locationNode ? locationNode.textContent : "";
          const link = linkNode ? linkNode.href : "";
          res.push({ name, age, born, location, link });
        });
        return res;
      });
      // await page.waitForSelector('#blocker');
      console.log(results);
      console.log(JSON.stringify({ message: results, error: null }));
    } catch (error) {
      console.log(`EVALUATE_ERROR: ${error}`);
      return;
    }

    await browser.close();
    //console.log("results",results)
    console.log(results.length);

    // let scraping_site_url = link
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
        add_scraping_details_query += `('${queue_id}', '${results[i].name}','${results[i].born}','${results[i].location}','${results[i].knownLocations}','${results[i].age}','${results[i].relatives}','${results[i].aliases}','${results[i].link}}')`;
      } else {
        add_scraping_details_query += `('${queue_id}', '${results[i].name}','${results[i].born}','${results[i].location}','${results[i].knownLocations}','${results[i].age}','${results[i].relatives}','${results[i].aliases}','${results[i].link}'),`;
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
