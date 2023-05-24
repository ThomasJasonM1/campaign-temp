const path = require('path');
require('dotenv').config({path: path.resolve(__dirname, '../.env')});
const fs = require('fs');
const fsPromises = fs.promises;
const axios = require('axios').default;
const bPromise = require("bluebird");
// const moment = require('moment');
const db = require('./db');
const afterDate = '2023-05-22T01:00:00.000Z';
const eventList = [ 'BRAND_ADD', 'BRAND_IDENTITY_STATUS_UPDATE', 'BRAND_DELETE', 'CAMPAIGN_ADD', 'CAMPAIGN_BILLED',
    'CAMPAIGN_SHARE_DELETE', 'CAMPAIGN_NUDGE', 'CAMPAIGN_SHARE_ADD', 'CAMPAIGN_DCA_COMPLETE', 'CAMPAIGN_EXPIRED',
    'CAMPAIGN_RESUBMITTED', 'CAMPAIGN_SHARE_ACCEPT' ];
const pageLimit = 50;

async function getDate(dateTimeAfter, milliseconds) {
    if (!dateTimeAfter) return null;
    const result = new Date(dateTimeAfter);
    result.setMilliseconds(result.getMilliseconds() + milliseconds);
    return result;
}

async function getBrandEvents(conn, lastDateUsed) {
    console.log('lastDateUsed', lastDateUsed);
    console.log('Calling getBrandCreations');
    try {
        const baseUrl = 'https://platform.text-em-all.io/events';
        const queryParams = `?type=webhook:tcr:received&limit=${pageLimit}&asc=true`;
        const nextDate = await getDate(lastDateUsed ?lastDateUsed : afterDate, 1);
        const afterDateVar = nextDate ? `&after=${nextDate.toISOString()}` : '';
        const url = `${baseUrl}${queryParams}${afterDateVar}`;
        console.log(url);
        const events = await axios
            .get(url)
            .catch(e => {
                console.log('Error getting data', JSON.stringify(e));
            });
        
        const savedData = await bPromise
            .map(events.data.items, event => saveData(conn, event), {concurrency: 3})
            .catch(err => console.log(`Mapping Event Error: ${err}`));
            // console.log('events', events.data);
        return events.data;
    } catch (error) {
        fsPromises.appendFile('errors.txt', `Error on getBrandEvents, lastDateUsed ${lastDateUsed}: ${JSON.stringify(error)}\n`);
        return console.log(`Error getting data from TCR: ${error}`);
    }
}

async function saveData(conn, event) {
    try {
        let { data, time, meta } = event;
        if (!eventList.includes(meta.senderEventType.value)) return;
        time = new Date(time);
        data = JSON.parse(data);
        const { eventType, brandId, campaignId, description, brandIdentityStatus } = data;
        const params = { 
            eventCategory: eventType.toLowerCase().startsWith('brand') ? 'Brand' : 'Campaign', 
            eventType,
            brandId,
            campaignId: campaignId || null,
            description: brandIdentityStatus == 'BRAND_IDENTITY_STATUS_UPDATE' ? `${brandIdentityStatus}: ${description}` : description,
            time,
        };
        const dbResponse = await db.executeSprocConnected(conn, 'temp_SaveEvent', params);

        if (eventType == 'BRAND_IDENTITY_STATUS_UPDATE' && 'brandIdentityStatus' != 'VERIFIED' && dbResponse[0].BrandID != 123)
        {
            console.log('Getting Error Details')
            const tcrParams = {
                auth: {
                    username: process.env.TCR_APIKEY,
                    password: process.env.TCR_SECRET,
                }
            };
            const feedback = await axios.get(
                `https://csp-api.campaignregistry.com/v2/brand/feedback/${brandId}`,
                tcrParams
            )
            .catch(e => {
                const { brandId: be2 } = JSON.parse(event.data)
                fsPromises.appendFile('errors.txt', `Error on saveData.Error, BrandID ${be2}: ${JSON.stringify(error)}\n`);
                return console.log('Error getting campaign', JSON.stringify(e));
            });

            console.log(feedback.data);

            const feedbackData = feedback?.data?.category;
            if (feedbackData.length < 1) return dbResponse;
            const savedData = await Promise.all(feedbackData.map(async (f, indx) => {
                console.log('f', f);
                const { id, description: errDesc, fields } = f;
                const errTime = await getDate(time, 1);
                const errorParams = { 
                    eventCategory: 'Brand', 
                    eventType: 'BRAND_FAIL',
                    brandId,
                    campaignId,
                    description: `ID: ${id} | Error Description: ${errDesc} | Fields:${fields.map((i) => ` ${i}`)}`,
                    time: errTime ? new Date(errTime) : time,
                };
                const savedErrorData = await db.executeSprocConnected(conn, 'temp_SaveEvent', errorParams);
            }))
            
        }
    
        return dbResponse;
    } catch (error) {
        const { brandId: be1 } = JSON.parse(event.data)
        fsPromises.appendFile('errors.txt', `Error on saveData, BrandID ${be1}: ${JSON.stringify(error)}\n`);
        console.log(`Error saving data to saveData: ${error}`);
    }
}

async function close(conn) {
    console.log('Closing DB Connection');
    await conn.close();
    return console.log('Done');
}

async function waitSeconds(seconds) {
    return new Promise(resolve => {
        return setTimeout(resolve, seconds * 1000)
    })
}

async function getData(conn, lastCall) {
    console.log('Calling getData');
    if (lastCall == null || lastCall.items.length > 0)
    {
        let lastDate = lastCall?.items ? lastCall.items[pageLimit - 1].time : null;
        const lastResult = await getBrandEvents(conn, lastDate);
        const wait = await waitSeconds(5);
        return getData(conn, lastResult);
    } else {
        const closeConn = await close(conn);
        return closeConn;
    }
}

async function doWork() {
    console.log('Calling doWork');
    const conn = await db.getPoolPromiseConnection('metrics');
    const work = await getData(conn);
    return work;
}

doWork();