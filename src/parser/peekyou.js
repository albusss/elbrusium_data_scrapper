const connection = require("../db/db.connection");
const puppeteer = require("puppeteer-extra");
const { QueryTypes } = require("sequelize");
const { executablePath } = require("puppeteer");
const stealthPlugin = require("puppeteer-extra-plugin-stealth");
puppeteer.use(stealthPlugin());
let RUA = require("random-useragent");

exports.peekYouParser = async (req, res) => {
  let transaction;
  try {
    transaction = await connection.transaction();
    let current_date = new Date().toISOString().slice(0, 19).replace("T", " ");

    let get_scraping_fields = await connection.query(
      `SELECT * FROM rabbitmq_scraping WHERE queue_status = 'Processing' AND sites_id="9"
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
    // console.log("creating browser......");
    browser = await puppeteer.launch({
      slowMo: 100,
      headless: true,
      // devtools: true,
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
    const USER_AGENT =
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36";
    const userAgent = RUA.getRandom();
    const UA = userAgent || USER_AGENT;
    //console.log("creating page.....");
    page = await browser.newPage();
    // console.log("setting default navigations etc......");
    await page.setJavaScriptEnabled(true);
    await page.setDefaultNavigationTimeout(0);
    await page.setDefaultTimeout(30000);
    await page.setRequestInterception(true);
    await page.setUserAgent(UA);
    // console.log("setting view port.....");
    await page.setViewport({
      width: 1920 + Math.floor(Math.random() * 100),
      height: 3000 + Math.floor(Math.random() * 100),
      deviceScaleFactor: 1,
      hasTouch: false,
      isLandscape: false,
      isMobile: false,
    });
    // console.log("setting request.....");
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
    // console.log("authenticating page......");
    await page.authenticate({
      username: process.env.PROXY_USER,
      password: process.env.PROXY_PASS,
    });
    //console.log("launching page....");
    await page.goto(
      `https://www.peekyou.com/usa/${state}/${firstName}_${lastName}`
    );
    // console.log("waiting for selector......");
    // if(await document.querySelector('div#resultsContainerProfiles')){
    //   console.log('waiting inside first check')
    //   await page.waitForSelector('div#resultsContainerProfiles')
    // }
    // console.log("starting next check");
    await page.waitForSelector("div#resultsContainerProfiles");
    // if(await page.waitForSelector('#resultsContainer')){
    //   await page.waitForSelector('#resultsContainer')
    // }
    //await page.waitForSelector('#resultsContainer')

    //console.log("starting to evaluate.......");
    const results = await page.evaluate(() => {
      let titleNodeList = Array.from(document.querySelectorAll(".w-100"));
      let titleNodeList2 = Array.from(document.querySelectorAll(".w-100 h2"));
      let res = [];
      let dataObj = {};
      titleNodeList.map((td) => {
        dataObj = {};
        let url = "https://www.peekyou.com";
        let link = td.querySelector(".w-100 a").getAttribute("href");
        let name = td.querySelector(".w-100 span").innerText;
        let ageContent = td.querySelector("h2:nth-child(1)").innerText;
        let age = ageContent.slice(-2);
        let location = td.querySelector("p.locations").innerText;
        dataObj.name = name;
        let ageValue = Number(age);
        if (ageValue && Number.isInteger(ageValue)) {
          let age = JSON.stringify(ageValue);
          dataObj.age = age;
        } else {
          dataObj.age = "";
        }
        dataObj.location = location;
        dataObj.link = url + link;

        res.push(dataObj);
      });
      return res;
    });
    await browser.close();
    //console.log("results",results)
    //  console.log(results.length);
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
