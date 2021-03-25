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

// main function that will be executed by Adobe I/O Runtime
async function main(params) {
  // create a Logger
  const logger = Core.Logger("main", { level: params.LOG_LEVEL || "info" });

  try {
    logger.info("Calling convert-parquet-to-json"); // 'info' is the default level if not set

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

    const files = await filesLib.init(); // init when env vars __OW_API_KEY and __OW_NAMESPACE are set (e.g. when running in an OpenWhisk action)

    const parquetFileName = `${params.filePath}`;

    /// download to local directory recursively (works for files as well)
    await files.copy(parquetFileName, parquetFileName, { localDest: true });
    let reader = await parquet.ParquetReader.openFile(parquetFileName); // create new ParquetReader that reads from 'fruits.parquet`
    let cursor = reader.getCursor(); // create a new cursor

    // read all records from the file and print them
    let record = null;
    var wtStream = await files.createWriteStream(`${parquetFileName}.json`);

    while ((record = await cursor.next())) {
      // logger.debug(JSON.stringify(record));
      wtStream.write(JSON.stringify(record));
    }

    await reader.close();
    wtStream.end(); // close string

    // list all files in the account
    const listOfFiles = await files.list("/");
    logger.debug(listOfFiles);

    //get presignURL
    const presignUrl = await files.generatePresignURL(
      `${parquetFileName}.json`,
      { expiryInSeconds: 120 }
    );

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
