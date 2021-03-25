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

async function getBatchFiles(params) {
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
        url: `https://platform.adobe.io/data/foundation/export/batches/${params.batchID}/files`,
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
    logger.info("Calling the get-batch-files");

    // log parameters, only if params.LOG_LEVEL === 'debug'
    logger.debug(stringParameters(params));

    // check for missing request input parameters and headers
    const requiredParams = ["apiKey", "sandboxName", "imsOrg", "batchID"];
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

    const currentBatchState = (await state.get(`${params.batchID}`)) || {}; //delete batch state)
    const currentBatchStateValue = currentBatchState.value || {}; //delete batch state)
    logger.debug(
      `Current Batch State ${params.batchID} = ${JSON.stringify(
        currentBatchStateValue
      )}`
    );
    if (params.forceProcess || false) {
      const datasetfiles = currentBatchStateValue.datasetFileIDs || [];
      for (let dsf of datasetfiles) {
        const currentdsfstate =
          (await state.get(`${params.batchID}-${dsf.id}`)) || {}; //delete batch state)
        const currentdsfstateValue = currentdsfstate.value || {}; //delete batch state)
        const files = currentdsfstateValue.files || [];
        for (let f of files) {
          logger.debug(`DeletingState ${params.batchID}-${dsf.id}-${f.id}`);
          await state.delete(`${params.batchID}-${dsf.id}-${f.id}`); //delete batch state)
        }
        logger.debug(`DeletingState ${params.batchID}-${dsf.id}`);
        await state.delete(`${params.batchID}-${dsf.id}`); //delete batch state)
      }
      logger.debug(`DeletingState ${params.batchID}`);
      await state.delete(`${params.batchID}`); //delete batch state)
    } else {
      switch (currentBatchStateValue.status.state) {
        case "Accepted":
          logger.debug(
            `Skipping cause State ${params.batchID} = ${JSON.stringify(
              currentBatchStateValue
            )}`
          );
          return params;
          break;
        case "Processing":
          logger.debug(
            `Skipping cause State ${params.batchID} = ${JSON.stringify(
              currentBatchStateValue
            )}`
          );
          return params;
          break;
        case "Success":
          logger.debug(
            `Skipping cause State ${params.batchID} = ${JSON.stringify(
              currentBatchStateValue
            )}`
          );
          return params;
          break;
        default:
        // code block
      }
    }

    let batchStatus = {
      status: {
        state: "Processing",
        message: "Retrieving list of Dataset Files",
      },
      datasetFileIDs: [],
    };
    const batchFiles = await getBatchFiles({
      ...params,
      token: token,
    });

    var ow = openwhisk();
    for (let fileID of batchFiles.data) {
      let payload = {
        name: "ode4ac-0.0.1/process-datasetfile",
        params: {
          apiKey: process.env.SERVICE_API_KEY,
          sandboxName: params.sandboxName,
          batchID: params.batchID,
          dataSetFileId: fileID.dataSetFileId,
          imsOrg: process.env.IMS_ORG,
        },
      };
      const activation = await ow.actions.invoke(payload);
      const state = {
        id: fileID.dataSetFileId,
        batchID: params.batchID,
        status: {
          state: "Accepted",
          message: "",
        },
        activationId: activation.activationId,
      };
      batchStatus["datasetFileIDs"].push(state);
      logger.debug(`Added DatasetfileID: ${JSON.stringify(state)}`);
    }
    await state.put(`${params.batchID}`, batchStatus); // update batch state

    const data = await state.get(`${params.batchID}`); // get batch state
    logger.debug(`State ${params.batchID} = ${JSON.stringify(data)}`);

    return params;
  } catch (error) {
    // log any server errors
    logger.error(error);
    // return with 500
    return errorResponse(500, "server error", logger);
  }
}

exports.main = main;
