const fs = require('fs');
const fsPromises = fs.promises;
const path = require('path');
require('dotenv').config({path: path.resolve(__dirname, '../.env')});
const axios = require('axios').default;
const { XMLParser } = require("fast-xml-parser");
const parser = new XMLParser();

const db = require('./db');
const startingPage = 1;

async function getCampaignsFromTcr(currentPage) {
    console.log('Calling getCampaignsFromTcr');
    try {
        const params = {
            auth: {
                username: process.env.TCR_APIKEY,
                password: process.env.TCR_SECRET,
            }
        };
        const campaigns = await axios.get(
            `https://csp-api.campaignregistry.com/v2/campaign?page=${currentPage}&recordsPerPage=10`,
            params
        )
        .catch(e => {
            console.log('Error getting campaign', JSON.stringify(e));
        });

        const { page, totalRecords, records } = await campaigns.data;
        const returnObject = { page, totalRecords, records };
        return returnObject;
    } catch (error) {
        fsPromises.appendFile('errors.txt', `Error on page ${currentPage}: ${JSON.stringify(error)}\n`);
        return console.log(`Error in getCampaignsFromTcr: ${error}`);
    }
}

async function checkDcaStatusIsDone(campaignId) {
    console.log('Calling checkDcaStatusIsDone');
    try {
        const params = {
            auth: {
                username: process.env.TCR_APIKEY,
                password: process.env.TCR_SECRET,
            }
        };
        const dcaArr = await axios.get(
            `https://csp-api.campaignregistry.com/v2/campaign/${campaignId}/mnoIdsWithDcaElected`,
            params
        )
        .catch(e => {
            console.log('Error getting DCA Data for Campaign ' + campaignId, JSON.stringify(e));
        });
        
        // 10017=AT&T, 10035=T-Mobile
        return dcaArr.data.includes(10017) && dcaArr.data.includes(10035);
    } catch (error) {
        fsPromises.appendFile('errors.txt', `Error with CampaignID ${campaignId}, Data: ${JSON.stringify(data)}, Error: ${JSON.stringify(error)}\n`);
        return console.log(`Error in checkDcaStatusIsDone: ${error}`);
    }
}

async function checkCampaignApprovalStatus(campaignId) {
    console.log('Calling checkCampaignApprovalStatus');
    try {
        const params = {
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
        const bwArr = await axios.get(
            `https://dashboard.bandwidth.com/api/accounts/5004956/campaignManagement/10dlc/campaigns/imports/${campaignId}`,
            params
        )
        .catch(e => {
            console.log('Error getting Campaign Approval Status for Campaign ' + campaignId, JSON.stringify(e));
        });
        const jObj = await parser.parse(bwArr.data);
        const { SecondaryDcaSharingStatus, HasSubId } = jObj.LongCodeImportCampaignResponse.ImportedCampaign;

        return { SecondaryDcaSharingStatus, HasSubId};
    } catch (error) {
        fsPromises.appendFile('errors.txt', `Error with CampaignID ${campaignId}, Error: ${JSON.stringify(error)}\n`);
        return console.log(`Error in checkDcaStatusIsDone: ${error}`);
    }
}

async function saveData(conn, data) {
    console.log('Calling saveData');
    try {
        const { brandId, campaignId, createDate, usecase, status } = data;
        const isDca2Completed = await checkDcaStatusIsDone(campaignId);
        const campaignDetails = await checkCampaignApprovalStatus(campaignId);
        const params = { 
            BrandID: brandId, 
            CampaignID: campaignId,
            CreateDate: createDate,
            UseCaseLabel: usecase,
            IsActive: status === 'ACTIVE',
            IsDcaApproved: isDca2Completed,
            CampaignApprovalStatus: campaignDetails.SecondaryDcaSharingStatus,
            HasSubID: campaignDetails.HasSubId,
        };
        const dbResponse = await db.executeSprocConnected(conn, 'spAddCampaign', params);
    
        return dbResponse;
    } catch (error) {
        fsPromises.appendFile('errors.txt', `Error saving CampaignID, Data: ${JSON.stringify(data)}, Error: ${JSON.stringify(error)}\n`);
        console.log(`Error saving data to spAddCampaign: ${error}`);
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
    if (lastCall == null || lastCall.records.length > 0)
    {
        const nextPage = lastCall?.page ? lastCall.page + 1 : startingPage;
        const lastResult = await getCampaignsFromTcr(nextPage);
        console.log('Got results for TCR, on page: ', lastResult?.page);
        const arrLen = lastResult?.records?.length > 0;
        const saveDataResult = arrLen ? await Promise.all(lastResult.records.map(r => saveData(conn, r))) : null;
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
