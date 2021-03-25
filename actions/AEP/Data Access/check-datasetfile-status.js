"use strict";
const { Core } = require("@adobe/aio-sdk");
const {
  errorResponse,
  stringParameters,
  checkMissingRequestInputs,
} = require("../../utils");

const stateLib = require("@adobe/aio-lib-state");
const THRESHOLD_COUNT = 3;
const openwhisk = require("openwhisk");
const { v4: uuid4 } = require("uuid");
// main function that will be executed by Adobe I/O Runtime
async function main(params) {
  // create a Logger
  const logger = Core.Logger("main", { level: params.LOG_LEVEL || "info" });

  try {
    // 'info' is the default level if not set
    logger.info("Calling the check-datasetfile-status");

    // log parameters, only if params.LOG_LEVEL === 'debug'
    logger.debug(stringParameters(params));

    // check for missing request input parameters and headers
    const requiredParams = ["batchID", "dataSetFileId"];
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
    // get dataSetFile state
    let dataSetFileState =
      (await state.get(`${params.batchID}-${params.dataSetFileId}`)) || {};

    logger.debug(
      `current dataSetFileState ${params.batchID}-${
        params.dataSetFileId
      } = ${JSON.stringify(dataSetFileState)}`
    );
    let currentdataSetFileStateValue = dataSetFileState.value || {};
    let invocationCount = params.invocationCount || 1;

    let files = [];

    //get file state for all files in datasetfile
    for (let file of currentdataSetFileStateValue.files) {
      const fileState =
        (await state.get(
          `${params.batchID}-${params.dataSetFileId}-${file.id}`
        )) || {}; // get file state
      const fileStateValue = fileState.value || {};
      file["status"] =
        {
          ...fileStateValue.status,
        } || { ...file.status } ||
        {};
      files.push(file);
    }
    //update status array
    currentdataSetFileStateValue.files = [...files];
    // check individual file status
    if (
      currentdataSetFileStateValue.files.length > 0 &&
      currentdataSetFileStateValue.files.filter(
        (id) => id.status.state != "Success"
      ).length == 0
    ) {
      // Update DatasetFile state to finish
      currentdataSetFileStateValue["status"] = {
        state: "Success",
        message: "All Files Downloaded",
      };
      await state.put(
        `$${params.batchID}-${params.dataSetFileId}`,
        currentdataSetFileStateValue
      );
    } // check iteration threshold
    else if (invocationCount < THRESHOLD_COUNT) {
      var ow = openwhisk();
      var now = new Date();
      now.setMinutes(now.getMinutes() + 1); //add a minute to current dateTime
      const jobId = uuid4();
      const batchID = params.batchID;
      const dataSetFileId = params.dataSetFileId;
      const LOG_LEVEL = params.LOG_LEVEL;
      invocationCount++;
      const triggerParams = {
        date: `${now.toISOString()}`,
        deleteAfterFire: "rules",
        minutes: 1,
        trigger_payload: { batchID, dataSetFileId, invocationCount, LOG_LEVEL },
      };
      await ow.triggers.create({
        name: `${jobId}-check-datasetfile-trigger`,
      });

      await ow.rules.create({
        name: `${jobId}-check-datasetfile-trigger-rule`,
        action: "ode4ac-0.0.1/check-datasetfile-status",
        trigger: `${jobId}-check-datasetfile-trigger`,
      });

      await ow.feeds.create({
        name: "/whisk.system/alarms/once",
        trigger: `${jobId}-check-datasetfile-trigger`,
        params: triggerParams,
      });
    } else {
      currentdataSetFileStateValue["status"] = {
        state: "Suspended",
        message: `Processing took more that ${THRESHOLD_COUNT} Invocations`,
      };
    }
    // Update DatasetFile to suspended
    await state.put(
      `${params.batchID}-${params.dataSetFileId}`,
      currentdataSetFileStateValue
    );
    dataSetFileState = await state.get(
      `${params.batchID}-${params.dataSetFileId}`
    ); // get dataSetFileId state

    logger.debug(
      `updated datasetfilestate ${params.batchID}-${
        params.dataSetFileId
      } =${JSON.stringify(dataSetFileState)}`
    );

    const response = {
      statusCode: 200,
      body: dataSetFileState,
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
