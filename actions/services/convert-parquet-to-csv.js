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
    logger.info("Calling convert-parquet-to-csv"); // 'info' is the default level if not set

    logger.debug(stringParameters(params)); // log parameters, only if params.LOG_LEVEL === 'debug'

    // check for missing request input parameters and headers
    const requiredParams = ["filePath", "fields"];
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

    /// download to local directory recursively (works for files as well)
    await files.copy(params.filePath, params.filePath, { localDest: true });

    let reader = await parquet.ParquetReader.openFile(params.filePath); // create new ParquetReader that reads from 'fruits.parquet`
    let cursors = [];
    let csvFields = [];
    params.fields.map((field) => {
      cursors.push(reader.getCursor([field]));
      csvFields.push(field.join(`.`));
    });

    logger.debug(`Reading file ${params.filePath}`);
    const recordsToProcess = reader.getRowCount();
    logger.debug(`# Of recs: ${reader.getRowCount()}`);

    const opts = { fields: csvFields };
    const transformOpts = { highWaterMark: 16384, encoding: "utf-8" };
    const json2csv = new Transform(opts, transformOpts);
    const wrStream = await files.createWriteStream(`${params.filePath}.csv`);

    const inStream = new Readable();
    inStream._read = () => {};
    const processor = inStream.pipe(json2csv).pipe(wrStream);

    logger.debug(`Starting to read local file ${params.filePath}`);
    for (let rec = 0; rec <= recordsToProcess; rec++) {
      let rec = {};
      for (c of cursors) {
        let data = await c.next();
        rec = { ...rec, ...data };
      }
      inStream.push(JSON.stringify(rec));
    }

    // You can also listen for events on the conversion and see how the header or the lines are coming out.
    wrStream.on("close", async () => {
      logger.debug(`Finished read of local file ${params.filePath}`);
      const used = process.memoryUsage().heapUsed / 1024 / 1024;
      logger.debug(
        `The script uses approximately ${Math.round(used * 100) / 100} MB`
      );
    });
    await reader.close();
    inStream.push(null);

    const presignUrlCSV = await files.generatePresignURL(
      `${params.filePath}.csv`,
      {
        expiryInSeconds: 300,
      }
    );
    const presignUrlParquet = await files.generatePresignURL(params.filePath, {
      expiryInSeconds: 300,
    });
    logger.debug(`presignUrlCSV = ${presignUrlCSV}`);
    logger.debug(`presignUrlParquet = ${presignUrlParquet}`);
    const response = {
      statusCode: 200,
      headers: {
        "Content-Type": "application/json",
      },
      body: {
        presignUrlCSV: presignUrlCSV,
        presignUrlParquet: presignUrlParquet,
        recordCount: recordsToProcess.toString(),
        filePath: `${params.filePath}.csv`,
      },
    };
    return response;
  } catch (error) {
    logger.error(error); // log any server errors
  }
}

exports.main = main;
