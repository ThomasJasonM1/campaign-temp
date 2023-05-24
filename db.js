const path = require('path');
require('dotenv').config({path: path.resolve(__dirname, './.env')});
const fs = require('fs');
const fsPromises = fs.promises;
const sql = require('mssql');
const _ = require('underscore');

async function getSqlConnectionParams(dbName) {
  return {
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    server: process.env.DB_SERVER,
    database: dbName || process.env.DB_NAME,
    connectionTimeout: 7000,
    requestTimeout: 7000,
    trustServerCertificate: true,
  }
};

async function getPoolPromise(dbName) {
  const connParams = await getSqlConnectionParams(dbName);
  const pool = new sql.ConnectionPool(connParams);

  pool.on('error', (err) => {
    console.log(`Lost db connection: ${err}`);
  });

  return pool
    .connect()
    .then((poolResult) => {
      console.log('Database connection established.');
      return poolResult;
    })
    .catch((err) => {
      console.log(`Error connecting to the db: ${err}`);
    });
}

async function getPoolPromiseConnection(dbName) {
  return new Promise(async (resolve) => {
    const poolPromise = await getPoolPromise(dbName);
    return resolve(poolPromise)
  })
}

async function getSqlConnection(dbName) {
  const connParams = await getSqlConnectionParams(dbName);
  return sql.connect(connParams);
}

const handleSprocError = (error, sprocName, sqlParams, options, resolve, reject) => {
  reject();
  return console.log(`Error: ${error}`);
};

const executeSproc = (procedureName, inputParams, options) => {
  return new Promise((resolve, reject) => {
    getPoolPromise()
      .then((connection) => {
        if (!connection) {
          console.log(`${procedureName} connection fail`);
          return handleSprocError('Database connection fail', procedureName, inputParams, options, resolve, reject);
        }

        const request = connection.request();

        console.log('inputParams', inputParams);
        if (inputParams) {
          _.each(inputParams, (value, key) => {
            if (typeof value === 'number' && value % 1 !== 0) {
              request.input(key, sql.Decimal(9, 6), value);
            } else if (typeof value === 'number' && value > 2147483647) {
              request.input(key, sql.BigInt, value);
            } else {
              request.input(key, value);
            }
          });
        }

        // console.log(request);

        return request
          .execute(procedureName)
          .then((result) => {
            resolve((result || {}).recordset || []);
          })
          .catch((err) => {
            console.log(`failed to call SP ${procedureName}:  ${err}`);
            return handleSprocError(err, procedureName, inputParams, options, resolve, reject);
          })
          .finally(() => {
            sql.close();
          });
      })
      .catch((err) => {
        console.log(`general error calling ${procedureName} ${err}`);
        return handleSprocError(err, procedureName, inputParams, options, resolve, reject);
      })
  });
};

const executeSprocConnected = (connection, procedureName, inputParams, options) => {
  return new Promise((resolve, reject) => {
    if (!connection) {
      console.log(`${procedureName} connection fail`);
      return handleSprocError('Database connection fail', procedureName, inputParams, options, resolve, reject);
    }

    const request = connection.request();

    console.log('inputParams', inputParams);
    if (inputParams) {
      _.each(inputParams, (value, key) => {
        if (typeof value === 'number' && value % 1 !== 0) {
          request.input(key, sql.Decimal(9, 6), value);
        } else if (typeof value === 'number' && value > 2147483647) {
          request.input(key, sql.BigInt, value);
        } else {
          request.input(key, value);
        }
      });
    }

    // console.log(request);

    return request
      .execute(procedureName)
      .then((result) => {
        resolve((result || {}).recordset || []);
      })
      .catch((err) => {
        console.log(`failed to call SP ${procedureName}:  ${err}`);
        fsPromises.appendFile('errors.txt', `Error with brand ${inputParams.BrandID}: ${JSON.stringify(err)}\n`);
        return handleSprocError(err, procedureName, inputParams, options, resolve, reject);
      })
      .finally(() => {
        sql.close();
      });
  });
};

module.exports = {
  executeSproc,
  getPoolPromise,
  getSqlConnection,
  sql,
  getPoolPromiseConnection,
  executeSprocConnected,
};