const path = require('path');
require('dotenv').config({path: path.resolve(__dirname, '../.env')});
const fs = require('fs');
const fsPromises = fs.promises;
const axios = require('axios').default;
const bPromise = require("bluebird");
const { XMLParser } = require("fast-xml-parser");
// const moment = require('moment');
const db = require('./db');
const afterDate = '2023-05-22T01:00:00.000Z'; 
const pageLimit = 40;
const tnStatusesToSave = ['RECEIVED', 'COMPLETE', 'PARTIAL', 'FAILED']

const alwaysArray = [
    // 'TnOptionOrder.TnOptionGroups.TnOptionGroup',
    'TnOptionOrder.TnOptionGroups.TnOptionGroup.TelephoneNumbers.TelephoneNumber',
    'TnOptionOrder.ErrorList.Error',
];
const parserOptions = {
    ignoreAttributes: false,
    isArray: (name, jpath, isLeafNode, isAttribute) => { 
        if( alwaysArray.indexOf(jpath) !== -1) return true;
    }
};
const parser = new XMLParser(parserOptions);

async function getEventType(orderStatus, isCampaignBeingRemoved, isError){
    if (orderStatus === 'RECEIVED' && !isCampaignBeingRemoved)
        return 'TN_ORDER_CREATE'
    if (orderStatus === 'RECEIVED' && isCampaignBeingRemoved)
        return 'TN_DISCONNECT_REQUEST'
    if ((orderStatus === 'COMPLETE' && !isCampaignBeingRemoved && !isError) ||
        (orderStatus === 'PARTIAL' && !isCampaignBeingRemoved && !isError))
        return 'TN_ORDER_COMPLETE'
    if ((orderStatus === 'COMPLETE' && isCampaignBeingRemoved && !isError) ||
        (orderStatus === 'PARTIAL' && isCampaignBeingRemoved && !isError))
        return 'TN_DISCONNECT_COMPLETE'
    if ((orderStatus === 'FAILED' && !isCampaignBeingRemoved && isError) ||
        (orderStatus === 'PARTIAL' && !isCampaignBeingRemoved && isError))
        return 'TN_ORDER_FAIL'
    if ((orderStatus === 'FAILED' && isCampaignBeingRemoved && isError) ||
        (orderStatus === 'PARTIAL' && isCampaignBeingRemoved && isError)) 
        return 'TN_DISCONNECT_FAIL'

}

async function getDate(dateTimeAfter, milliseconds) {
    if (!dateTimeAfter) return null;
    const result = new Date(dateTimeAfter);
    result.setMilliseconds(result.getMilliseconds() + milliseconds);
    return result;
}

async function getTnEvents(nextDate) {
    console.log('nextDate', nextDate);
    console.log('Calling getTnEvents');
    try {
        const baseUrl = 'https://platform.text-em-all.io/events';
        const queryParams = `?type=webhook:bandwidth:received&@senderEventType=tnoptions&limit=${pageLimit}&asc=true`;
        const nextDateToUse = await getDate(nextDate || afterDate, 1);
        const afterDateVar = nextDateToUse ? `&after=${nextDateToUse.toISOString()}` : '';
        const url = `${baseUrl}${queryParams}${afterDateVar}`;
        console.log(url);
        const events = await axios
            .get(url)
            .catch(e => {
                console.log('Error getting data', JSON.stringify(e));
            });

        return events.data;
    } catch (error) {
        fsPromises.appendFile('errors.txt', `Error on getTnEvents: ${JSON.stringify(error)}\n`);
        return console.log(`Error getting data from getTnEvents: ${error}`);
    }
}

async function getDataFromBw(orderId) {
    console.log(`Getting Phone Details From BW for OrderID: ${orderId}`);
    try {
        const bwParams = {
            auth: {
                username: process.env.BW_USERNAME,
                password: process.env.BW_PASSWORD,
            },
            headers: {
                'Accept-Encoding': '*/*',
                Connection: 'keep-alive',
                'Content-Type': 'application/xml',
            },
        };
        const bwResponse = await axios.get(
            `https://dashboard.bandwidth.com/api/accounts/5004956/tnoptions/${orderId}`,
            bwParams
        )
        .catch(e => {
            fsPromises.appendFile('errors.txt', `Error in getDataFromBw, OrderID ${orderId}: ${JSON.stringify(error)}\n`);
            return console.log('Error getting order from BW', JSON.stringify(e));
        });

        const jObj = await parser.parse(bwResponse.data);
        // console.log('jObj', jObj.TnOptionOrder.TnOptionGroups);
        return jObj;
    } 
    catch (err) {
        console.log(`Error getting phone number details from BW for OrderID ${orderId}, Err: ${err}`);
    }
}

async function saveData(conn, event) {
    try {
        let { data, time, meta } = event;
        const eventTime = new Date(time);
        const parsedData = await parser.parse(data);
        const { OrderId, Status, Message } = parsedData.Notification
        if (!tnStatusesToSave.includes(Status)) return {};
        const bwData = await getDataFromBw(OrderId);
        const { TnOptionGroups, ErrorList } = bwData.TnOptionOrder;
        const { A2pSettings, TelephoneNumbers } = TnOptionGroups.TnOptionGroup;
        // If A2pSettings is null the tnoptions order was to change something other than the Campaign
        if (!A2pSettings) return parsedData;
        const isCampaignBeingRemoved = A2pSettings.Action === 'delete';
        let fullNumberList = TelephoneNumbers.TelephoneNumber.map(t => t); // Array.from(TelephoneNumbers.TelephoneNumber);
        let errorTnList = await ErrorList.Error && ErrorList.Error.length > 0 && Status !== 'RECEIVED' ?
            ErrorList.Error.map(e => e.TelephoneNumber) : [];

        
        console.log('errorTnList', errorTnList);
        const saveNumberList = await fullNumberList.filter(el => {
            return !errorTnList.includes(el);
        });

        console.log('Saving Errors');
        const saveErrors = await errorTnList.length > 0 && ErrorList.Error && ErrorList.Error.length > 0 ?
            await Promise.all(ErrorList.Error.map(async (tnErr) => {
                const eType = await getEventType(Status, isCampaignBeingRemoved, true);
                const params = { 
                    eventCategory: 'TN Options', 
                    eventType: eType,
                    phone: tnErr.TelephoneNumber.toString(),
                    campaignId: A2pSettings.CampaignId,
                    description: `Code: ${tnErr.Code}, Description: ${tnErr.Description}`,
                    time: eventTime,
                };
                const dbResponseErrs = await db.executeSprocConnected(conn, 'temp_SaveEvent', params)
                .catch(dbSaveErr => console.log(`Error saving campaignID ${campaignId}, Error: ${dbSaveErr}`));
                return dbResponseErrs;
                // saveNumberList = saveNumberList.filter(item => item !== tnErr.TelephoneNumber);
        })) : null;

        console.log('saveNumberList', saveNumberList);
        const saveSuccesses = await saveNumberList.length > 0 ?
            await Promise.all(saveNumberList.map(async (tn) => {
                const eType = await getEventType(Status, isCampaignBeingRemoved, false);
                console.log('CampaignId', A2pSettings.CampaignId);
                console.log('eType', eType);
                const params = { 
                    eventCategory: 'TN Options', 
                    eventType: eType,
                    phone: tn.toString(),
                    campaignId: A2pSettings.CampaignId,
                    description: `Status: ${Status}, OrderID: ${OrderId}, Message: ${Message}`,
                    time: eventTime,
                };
                const dbResponseComps = await db.executeSprocConnected(conn, 'temp_SaveEvent', params)
                .catch(dbSaveErr => console.log(`Error saving campaignID ${campaignId}, Error: ${dbSaveErr}`));
                return dbResponseComps;
            })) : null;

        // console.log('time', time, Status);
        // console.log('meta', meta);
        // console.log('parsedData', parsedData);
        // console.log('ErrorList', ErrorList);
        // console.log('bwData', typeof(bwData.TnOptionOrder.TnOptionGroups.TnOptionGroup), bwData.TnOptionOrder.TnOptionGroups.TnOptionGroup /*, A2pSettings, TelephoneNumbers, typeOf(TelephoneNumbers)*/);
        return parsedData;
    } catch (error) {
        fsPromises.appendFile('errors.txt', `Error on saveData, Event ${JSON.stringify(event)}: ${JSON.stringify(error)}\n`);
        console.log(`Error saving data in fn saveData: ${error}`);
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
        // if (nextPageToGet > 1) return getData(conn, { items: [] });
        let lastDate = lastCall?.items ? lastCall.items[pageLimit - 1].time : null;
        const lastResult = await getTnEvents(lastDate);
        const arrLen = lastResult?.items?.length > 0;
        const saveDataResult = arrLen ? await bPromise
            .map(lastResult.items, item => saveData(conn, item), {concurrency: 1})
            .catch(err => console.log(`Mapping Event Error: ${err}`)) : null;
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

