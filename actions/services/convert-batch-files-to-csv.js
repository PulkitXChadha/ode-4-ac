"use strict";
const { Core } = require("@adobe/aio-sdk");
const {
  errorResponse,
  stringParameters,
  checkMissingRequestInputs,
} = require("../utils");

const filesLib = require("@adobe/aio-lib-files");
const openwhisk = require("openwhisk");
require("dotenv").config();

// main function that will be executed by Adobe I/O Runtime
async function main(params) {
  // create a Logger
  const logger = Core.Logger("main", { level: params.LOG_LEVEL || "info" });

  try {
    // 'info' is the default level if not set
    logger.info("Calling the convert-batch-files-to-csv");

    // log parameters, only if params.LOG_LEVEL === 'debug'
    logger.debug(stringParameters(params));

    // check for missing request input parameters and headers
    const requiredParams = ["batchID"];
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

    const files = await filesLib.init();
    const batchFiles = await files.list(`${params.batchID}/`);
    var ow = openwhisk();

    const covertedFiles = [];
    for (let file of batchFiles) {
      const fileProps = await files.getProperties(`${file}`);
      if (!fileProps.isDirectory && file.endsWith(".parquet")) {
        let payload = {
          name: "ode4ac-0.0.1/convert-parquet-to-csv",
          params: {
            fields: params.fields,
            filePath: file,
          },
        };
        const activation = await ow.actions.invoke(payload);
        covertedFiles.push({
          batchID: params.batchID,
          filePath: file,
          activationId: activation.activationId,
        });
      }
    }

    const response = {
      statusCode: 200,
      headers: {
        "Content-Type": "application/json",
      },
      body: { covertedFiles: covertedFiles },
    };

    return response;
  } catch (error) {
    // log any server errors
    logger.error(error);
    // return with 500
    return errorResponse(500, "server error", logger);
  }
}

exports.main = main;
