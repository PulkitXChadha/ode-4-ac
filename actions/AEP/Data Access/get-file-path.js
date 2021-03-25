"use strict";
const request = require("request");
const { Core } = require("@adobe/aio-sdk");
const {
  errorResponse,
  stringParameters,
  checkMissingRequestInputs,
} = require("../../utils");

const stateLib = require("@adobe/aio-lib-state");
const openwhisk = require("openwhisk");
require("dotenv").config();

async function getFilePath(params) {
  return new Promise(function (resolve, reject) {
    try {
      var options = {
        method: "GET",
        headers: {
          "x-api-key": params.apiKey,
          "x-gw-ims-org-id": params.imsOrg,
          Authorization: `Bearer ${params.token}`,
          "x-sandbox-name": params.sandboxName || "prod",
        },
        url: `https://platform.adobe.io/data/foundation/export/files/${params.dataSetFileId}`,
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
    logger.info("Calling the get-file-path");

    // log parameters, only if params.LOG_LEVEL === 'debug'
    logger.debug(stringParameters(params));

    // check for missing request input parameters and headers
    const requiredParams = [
      "apiKey",
      "sandboxName",
      "imsOrg",
      "dataSetFileId",
      "batchID",
    ];
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

    const state = await stateLib.init();
    const res = await state.get("accessToken");
    const token = res.value;

    await state.delete(`${params.batchID}-${params.dataSetFileId}`); //delete batch state

    let datasetFileStatus = {
      id: params.dataSetFileId,
      batchID: params.batchID,
      files: [],
      status: { state: "Processing", message: "Gathering all file paths" },
    };
    const filePath = await getFilePath({
      ...params,
      token: token,
    });

    var ow = openwhisk();
    for (let file of filePath.data) {
      let payload = {
        name: "ode4ac-0.0.1/get-parquet-file-content",
        params: {
          apiKey: process.env.SERVICE_API_KEY,
          sandboxName: params.sandboxName,
          batchID: params.batchID,
          dataSetFileId: params.dataSetFileId,
          imsOrg: process.env.IMS_ORG,
          filePath: file._links.self.href,
          fileName: file.name,
          fileSize: file.length,
        },
      };

      const activation = await ow.actions.invoke(payload);
      const state = {
        id: file.name,
        datasetid: params.dataSetFileId,
        batchID: params.batchID,
        fileSize: file.length,
        filePath: file._links.self.href,
        fileName: file.name,
        status: {
          state: "Accepted",
          message: "",
        },
        activationId: activation.activationId,
      };

      datasetFileStatus["files"].push(state);
      logger.debug(`Added files: ${JSON.stringify(state)}`);
    }

    await state.put(
      `${params.batchID}-${params.dataSetFileId}`,
      datasetFileStatus
    ); // update DatasetFile batch state

    const data = await state.get(`${params.batchID}-${params.dataSetFileId}`); // get batch state
    logger.debug(
      `State ${params.batchID}-${params.dataSetFileId} = ${JSON.stringify(
        data
      )}`
    );

    return params;
  } catch (error) {
    // log any server errors
    logger.error(error);
    // return with 500
    return errorResponse(500, "server error", logger);
  }
}

exports.main = main;
