"use strict";
const request = require("request");
const { Core } = require("@adobe/aio-sdk");
const {
  errorResponse,
  stringParameters,
  checkMissingRequestInputs,
} = require("../../utils");

const stateLib = require("@adobe/aio-lib-state");

async function getBatchesToProcess(params) {
  return new Promise(function (resolve, reject) {
    try {
      var options = {
        method: "GET",
        headers: {
          "x-api-key": params.apiKey,
          "x-gw-ims-org-id": params.imsOrg,
          Authorization: `Bearer ${params.token}`,
          Accept: "application/vnd.adobe.xed+json",
          "x-sandbox-name": params.sandboxName || "prod",
        },
        url: `https://platform.adobe.io/data/foundation/catalog/batches?createdAfter=${params.lastSuccessfulRun}&status=success&orderBy=desc:created&dataSet=${params.datasetID}&properties=id`,
      };
      request(options, function (error, response, body) {
        if (error) reject(error);
        else {
          resolve(JSON.parse(body));
        }
      });
    } catch (e) {
      reject(e);
    }
  });
}

// main function that will be executed by Adobe I/O Runtime
async function main(params) {
  // create a Logger
  const logger = Core.Logger("main", { level: params.LOG_LEVEL || "info" });

  try {
    // 'info' is the default level if not set
    logger.info("Calling the get-batches");

    // log parameters, only if params.LOG_LEVEL === 'debug'
    logger.debug(stringParameters(params));

    // check for missing request input parameters and headers
    const requiredParams = ["apiKey", "sandboxName", "imsOrg", "datasetID"];
    const requiredHeaders = [];
    const errorMessage = checkMissingRequestInputs(
      params,
      requiredParams,
      requiredHeaders
    );
    if (errorMessage) {
      // return and log client errors
      return errorResponse(400, errorMessage, logger);
    }

    const nowEpoch = new Date().getTime();

    const state = await stateLib.init();
    const res = await state.get("accessToken");
    const token = res.value;

    // get Last Run Time
    const lastSuccessfulRun = await state.get("lastSuccessfulRun");

    const batches = await getBatchesToProcess({
      ...params,
      token: token,
      lastSuccessfulRun: lastSuccessfulRun
        ? lastSuccessfulRun.value
        : 1559775880000,
    });

    logger.debug("data = " + JSON.stringify(data, null, 2));
    const newAccepted = [];
    const r = Object.keys(batches).map(async (key) => {
      // get
      const res = await state.get(key); // res = { value, expiration }
      if (!res) {
        await state.put(key, { status: "Accepted" });
        newAccepted.push(key);
        return `Batch ID : ${key} ACCEPTED`;
      } else {
        return `Batch ID : ${key} Already Queued!`;
      }
    });
    const response = {
      statusCode: 200,
      body: r,
    };

    // get
    const currentQueue = await state.get("batchQueue");
    const updatedQueue = [...currentQueue, ...newAccepted];

    await state.put("batchQueue", updatedQueue, { ttl: -1 }); //Update Queue
    await state.put("lastSuccessfulRun", nowEpoch, { ttl: -1 }); // Update Last run time

    return response;
  } catch (error) {
    // log any server errors
    logger.error(error);
    // return with 500
    return errorResponse(500, "server error", logger);
  }
}

exports.main = main;
