const fs = require('fs');
const fsPromises = fs.promises;
const path = require('path');
require('dotenv').config({path: path.resolve(__dirname, '../.env')});
const axios = require('axios').default;
const db = require('./db');
const startingPage = 1;

async function getBrandsFromTcr(currentPage) {
    console.log('Calling getBrandsFromTcr');
    try {
        const params = {
            auth: {
                username: process.env.TCR_APIKEY,
                password: process.env.TCR_SECRET,
            }
        };
        const brands = await axios.get(
            `https://csp-api.campaignregistry.com/v2/brand?page=${currentPage}&recordsPerPage=20`,
            params
        )
        .catch(e => {
            console.log('Error getting campaign', JSON.stringify(e));
        });

        const { page, totalRecords, records } = await brands.data;
        const returnObject = { page, totalRecords, records };

        return returnObject;
    } catch (error) {
        fsPromises.appendFile('errors.txt', `Error on page ${currentPage}: ${JSON.stringify(error)}\n`);
        return console.log(`Error getting data from TCR: ${error}`);
    }
}

async function saveData(conn, data) {
    console.log('Calling saveData');
    try {
        const { brandId, entityType, identityStatus, createDate, referenceId, optionalAttributes } = data;
        const IsRussel3000 = optionalAttributes['russell3000'] || false;
        const params = { 
            BrandID: brandId, 
            OrganizationType: entityType, 
            VerificationStatus: identityStatus, 
            CreateDate: createDate,
            OriginatingAccountID: referenceId.replace(/[^0-9]+/g, ''),
            IsRussel3000,
        };
        const dbResponse = await db.executeSprocConnected(conn, 'spAddBrand', params);
    
        return dbResponse;
    } catch (error) {
        console.log(`Error saving data to spAddBrand: ${error}`);
    }
}

async function close(conn) {
    console.log('Closing DB Connection');
    await conn.close();
    return console.log('Done');
  }

async function getData(conn, lastCall) {
    console.log('Calling getData');
    if (lastCall == null || lastCall.records.length > 0)
    {
        const nextPage = lastCall?.page ? lastCall.page + 1 : startingPage;
        const lastResult = await getBrandsFromTcr(nextPage);
        console.log('Got results for TCR, on page: ', lastResult.page);
        const arrLen = lastResult?.records?.length > 0;
        const saveDataResult = arrLen ? await Promise.all(lastResult.records.map(r => saveData(conn, r))) : null;
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
