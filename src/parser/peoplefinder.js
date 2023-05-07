const connection = require("../db/db.connection");
const puppeteer = require("puppeteer-extra");
const { QueryTypes } = require("sequelize");
const { executablePath } = require("puppeteer");
const stealthPlugin = require("puppeteer-extra-plugin-stealth");
puppeteer.use(stealthPlugin());
let RUA = require("random-useragent");

exports.peopleFinderParser = async (req, res) => {
  let transaction;
  try {
    transaction = await connection.transaction();
    let current_date = new Date().toISOString().slice(0, 19).replace("T", " ");

    let get_scraping_fields = await connection.query(
      `SELECT * FROM rabbitmq_scraping WHERE queue_status = 'Processing' AND sites_id="10"
        ORDER BY created_at ASC`,
      {
        type: QueryTypes.SELECT,
        transaction,
      }
    );
    let city = "";
    let state = "";
    console.log(get_scraping_fields[0]);
    let firstName = get_scraping_fields[0]["firstName"];
    let lastName = get_scraping_fields[0]["lastName"];
    // let state = get_scraping_fields[0]["state"];
    let queue_id = get_scraping_fields[0]["queue_id_scraping"];
    console.log(firstName, "firstName");
    console.log(lastName);
    console.log(state);
    console.log("creating browser......");
    browser = await puppeteer.launch({
      slowMo: 100,
      headless: true,
      devtools: true,
      executablePath: executablePath(),
      args: [
        `--proxy-server=${process.env.Proxy_IP}`,
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-gpu",
        "--disable-dev-shm-usage",
        "--single-process",
      ],
    });

    page = await browser.newPage();

    await page.setJavaScriptEnabled(true);
    await page.setDefaultNavigationTimeout(0);
    await page.setDefaultTimeout(30000);
    await page.setRequestInterception(true);
    await page.setUserAgent(
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36"
    ); // new line

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
      "https://www.peoplefinders.com/people/" +
        firstName +
        "-" +
        lastName +
        "?landing=all"
    );
    await page.waitForSelector("a.record");
    console.log("starting to evaluate");
    const results = await page.evaluate(() => {
      let res = [];
      let dataObj = {};
      let allProfileList = Array.from(document.querySelectorAll("a.record"));
      if (!allProfileList.length) {
        return res;
      }
      let profileList = allProfileList.slice(0, 10);
      profileList.map((td) => {
        dataObj = {};
        let linkd = td.getAttribute("href");
        let namefeild = td.querySelector("h4.record__title").textContent.trim();
        let name1 = namefeild.slice(0, -2).replace(",", "");
        let name2 = name1.replace("Deceas", "").trim();
        res.push({
          //name: td.querySelector('h4.record__title').textContent.replace(', Deceased', '').trim(),
          name: name2,
          link: "https://www.peoplefinders.com/" + linkd,
          location: td.querySelector("div.col-lg-2 ul.clean-list").textContent,
          age: td
            .querySelector("div.col-lg-1")
            .textContent.replace("Age", "")
            .trim(),
        });
      });
      return res;
    });
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
