"use strict";
const request = require("request");
const { Core } = require("@adobe/aio-sdk");
const {
  errorResponse,
  stringParameters,
  checkMissingRequestInputs,
} = require("../../utils");

const stateLib = require("@adobe/aio-lib-state");
const filesLib = require("@adobe/aio-lib-files");
const fs = require("fs");
const zlib = require("zlib");

async function getPartialFileContent(params) {
  // create a Logger
  const logger = Core.Logger("getPartialFileContent", {
    level: params.LOG_LEVEL || "info",
  });

  return new Promise(function (resolve, reject) {
    try {
      var options = {
        method: "GET",
        headers: {
          "x-api-key": params.apiKey,
          "x-gw-ims-org-id": params.imsOrg,
          Authorization: `Bearer ${params.token}`,
          "x-sandbox-name": params.sandboxName || "prod",
          "Accept-Encoding": "gzip, deflate, br",
          Range: params.range,
        },
        url: `${params.filePath}`,
        gzip: true,
        encoding: null,
      };

      request(options, async function (error, response, body) {
        if (error) reject(error);
        else {
          resolve(body);
        }
      });
    } catch (e) {
      // log parameters, only if params.LOG_LEVEL === 'debug'
      logger.debug(JSON.stringify(e));
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
    logger.info("Calling the get-parquet-file-content");

    // log parameters, only if params.LOG_LEVEL === 'debug'
    logger.debug(stringParameters(params));

    // check for missing request input parameters and headers
    const requiredParams = [
      "apiKey",
      "sandboxName",
      "imsOrg",
      "dataSetFileId",
      "filePath",
      "fileName",
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
    const files = await filesLib.init();

    await state.delete(
      `${params.batchID}-${params.dataSetFileId}-${params.fileName}`
    ); //delete file state

    let fileState = {
      batchID: params.batchID,
      dataSetFileId: params.dataSetFileId,
      fileName: params.fileName,
      filePath: params.filePath,
      fileSize: params.fileSize,
      status: {
        state: "Accepted",
        message: "",
      },
    };
    // write private file
    var wtStream = await files.createWriteStream(
      `${params.batchID}/${params.dataSetFileId}/${params.fileName}`
    );
    let i = 0;
    const chunkSize = 26214400;
    //26214400; //25 mb

    for (i = -1; i <= params.fileSize; i = i + chunkSize) {
      const data = await getPartialFileContent({
        ...params,
        range: `bytes=${i + 1}-${i + chunkSize}`,
        token: token,
      });

      logger.debug(`Got bytes=${i + 1}-${i + chunkSize}`);

      // readable.push(data);
      wtStream.write(data);
    }
    // readable.push(null);
    logger.debug("File Download complete");
    wtStream.end(); // close string

    //get presignURL
    const presignUrl = await files.generatePresignURL(
      `${params.batchID}/${params.dataSetFileId}/${params.fileName}`,
      { expiryInSeconds: 3000 }
    );
    logger.debug(presignUrl);

    fileState["status"] = {
      state: "Success",
      message: "files saved!!!",
    };
    await state.put(
      `${params.batchID}-${params.dataSetFileId}-${params.fileName}`,
      fileState
    ); // Update file state to finish

    fileState =
      (await state.get(
        `${params.batchID}-${params.dataSetFileId}-${params.fileName}`
      )) || {}; // get file state

    logger.debug(
      `filestate ${params.batchID}-${params.dataSetFileId}-${
        params.fileName
      } = ${JSON.stringify(fileState)}`
    );
    const response = {
      statusCode: 200,
      headers: {
        "Content-Type": "application/json",
      },
      body: { fileState: fileState, presignUrl: presignUrl },
    };
    const used = process.memoryUsage().heapUsed / 1024 / 1024;
    logger.debug(
      `The script uses approximately ${Math.round(used * 100) / 100} MB`
    );
    return response;
  } catch (error) {
    // log any server errors
    logger.error(error);
    // return with 500
    return errorResponse(500, "server error", logger);
  }
}

exports.main = main;
