/*
 * <license header>
 */

/**
 * This is a sample action showcasing how to access an external Adobe Experience Platform: Realtime Customer Profile API
 *
 * Note:
 * You might want to disable authentication and authorization checks against Adobe Identity Management System for a generic action. In that case:
 *   - Remove the require-adobe-auth annotation for this action in the manifest.yml of your application
 *   - Remove the Authorization header from the array passed in checkMissingRequestInputs
 *   - The two steps above imply that every client knowing the URL to this deployed action will be able to invoke it without any authentication and authorization checks against Adobe Identity Management System
 *   - Make sure to validate these changes against your security requirements before deploying the action
 */

const { Core } = require("@adobe/aio-sdk");
const {
  errorResponse,
  stringParameters,
  checkMissingRequestInputs,
} = require("../utils");
const parquet = require("parquetjs-lite");
const filesLib = require("@adobe/aio-lib-files");
const fs = require("fs");
const { Readable } = require("stream");
const { Transform } = require("json2csv");

// main function that will be executed by Adobe I/O Runtime
async function main(params) {
  // create a Logger
  const logger = Core.Logger("main", { level: params.LOG_LEVEL || "info" });

  try {
    logger.info("Calling get-file-presigned-url"); // 'info' is the default level if not set

    logger.debug(stringParameters(params)); // log parameters, only if params.LOG_LEVEL === 'debug'

    // check for missing request input parameters and headers
    const requiredParams = ["filePath"];
    const requiredHeaders = [];
    const errorMessage = checkMissingRequestInputs(
      params,
      requiredParams,
      requiredHeaders
    );
    if (errorMessage) {
      return errorResponse(400, errorMessage, logger); // return and log client errors
    }

    const files = await filesLib.init();
    const listOfFiles = await files.list("/");
    logger.debug(listOfFiles);
    const presignUrl = await files.generatePresignURL(params.filePath, {
      expiryInSeconds: 120,
    });

    logger.debug(presignUrl);
    const response = {
      statusCode: 200,
      headers: {
        "Content-Type": "application/json",
      },
      body: { presignUrl: presignUrl },
    };

    return response;
  } catch (error) {
    logger.error(error); // log any server errors
  }
}

exports.main = main;
