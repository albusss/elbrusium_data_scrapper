const connection = require("../db/db.connection");
const puppeteer = require("puppeteer-extra");
const { QueryTypes } = require("sequelize");
const { executablePath } = require("puppeteer");
const stealthPlugin = require("puppeteer-extra-plugin-stealth");
puppeteer.use(stealthPlugin());
let RUA = require("random-useragent");

exports.whitePagesParser = async (req, res) => {
  let transaction;
  try {
    transaction = await connection.transaction();
    let current_date = new Date().toISOString().slice(0, 19).replace("T", " ");

    let get_scraping_fields = await connection.query(
      `SELECT * FROM rabbitmq_scraping WHERE queue_status = 'Processing' AND sites_id="12"
        ORDER BY created_at ASC`,
      {
        type: QueryTypes.SELECT,
        transaction,
      }
    );
    console.log(get_scraping_fields[0]);
    let firstName = get_scraping_fields[0]["firstName"];
    let lastName = get_scraping_fields[0]["lastName"];
    let city = get_scraping_fields[0]["city"];
    let state = get_scraping_fields[0]["state"];
    let queue_id = get_scraping_fields[0]["queue_id_scraping"];
    console.log(firstName, "firstName");
    console.log(lastName);
    console.log(state);
    console.log("creating browser......");
    browser = await puppeteer.launch({
      slowMo: 100,
      headless: false,
      //devtools: true,
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
    console.log("creating browser");

    page = await browser.newPage();
    console.log("creating page");

    await page.setJavaScriptEnabled(true);
    await page.setDefaultNavigationTimeout(0);
    await page.setDefaultTimeout(30000);
    await page.setRequestInterception(true);

    console.log("setting view port.......");
    await page.setViewport({
      width: 1920 + Math.floor(Math.random() * 100),
      height: 3000 + Math.floor(Math.random() * 100),
      deviceScaleFactor: 1,
      hasTouch: false,
      isLandscape: false,
      isMobile: false,
    });
    console.log("setting page requests.....");
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
    console.log("authenticating proxy.....");
    await page.authenticate({
      username: process.env.PROXY_USER,
      password: process.env.PROXY_PASS,
    });
    console.log("launching page......");
    await page.goto(
      "https://www.whitepages.com/name/" +
        firstName +
        "-" +
        lastName +
        "/" +
        city +
        "-" +
        state
    );
    console.log("waiting for selector");
    await page.waitForSelector(".results-container");
    await page.waitForSelector(".serp-card");
    await page.waitForSelector(".serp-icon-container");
    console.log("evaluating...................");
    const results = await page.evaluate(() => {
      let titleNodeList = Array.from(document.querySelectorAll(".serp-card"));
      let res = [];
      let dataObj = {};

      titleNodeList.map((td) => {
        //let linkd = td.getAttribute('href');
        dataObj = {};
        let name = td
          .querySelector("div.name-wrap")
          .childNodes[0].textContent.trim();
        let location = td
          .querySelector("div.name-wrap > .person-location")
          .textContent.trim();
        let locationPure = location.replace(/\n/g, "").replace(/ +(?= )/g, "");
        let age = td
          .querySelector("div.age-border > .person-age")
          .textContent.trim();

        dataObj.name = name;
        dataObj.location = locationPure;
        dataObj.age = age;

        // res.push({
        //     name: td.querySelector('div.name-wrap').childNodes[0].textContent.trim(),
        //     link: 'https://www.whitepages.com' + link2 ,
        //     location: locationPure,
        //     age: td.querySelector('div.age-border > .person-age').textContent.trim(),
        // });
        res.push(dataObj);
      });

      return res;
    });
    await browser.close();
    //console.log("results",results)
    console.log(results.length);
    let scraping_site_url = "https://www.whitepages.com";
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
        add_scraping_details_query += `('${queue_id}', '${results[i].name}','${results[i].born}','${results[i].location}','${results[i].knownLocations}','${results[i].age}','${results[i].relatives}','${results[i].aliases}','${scraping_site_url}}')`;
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
