"use strict";
const { Core } = require("@adobe/aio-sdk");
const {
  errorResponse,
  stringParameters,
  checkMissingRequestInputs,
} = require("../../utils");

const stateLib = require("@adobe/aio-lib-state");
const THRESHOLD_COUNT = 5;
const openwhisk = require("openwhisk");
const { v4: uuid4 } = require("uuid");
// main function that will be executed by Adobe I/O Runtime
async function main(params) {
  // create a Logger
  const logger = Core.Logger("main", { level: params.LOG_LEVEL || "info" });

  try {
    // 'info' is the default level if not set
    logger.info("Calling the check-batch-status");

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

    const state = await stateLib.init();

    /*
    Get Batch Status
    Loop through Datasetfile status
      if state not found ignore
        update current batch state with state not found complete
      if state found and not success continue
        update current batch Processing as complete
      if state found and success
        update current batch state as complete

    */
    let batchState = (await state.get(`${params.batchID}`)) || {}; // get batch state
    logger.debug(
      `current batchState ${params.batchID} = ${JSON.stringify(batchState)}`
    );
    let currentbatchStateValue = batchState.value || {};
    let invocationCount = params.invocationCount || 1;

    let updatedDatasetFile = [];
    //Loop through Datasetfile status
    for (let datasetFile of currentbatchStateValue.datasetFileIDs || []) {
      const batchDatasetFileState =
        (await state.get(`${params.batchID}-${datasetFile.id}`)) || {}; // get dataset state
      const batchDatasetFileStateValue = batchDatasetFileState.value || {};
      datasetFile["status"] =
        {
          ...batchDatasetFileStateValue.status,
        } || { ...datasetFile.status } ||
        {};
      updatedDatasetFile.push(datasetFile);
    }

    //update status array
    currentbatchStateValue.datasetFileIDs = [...updatedDatasetFile];
    //if all files successful update batch status
    //if invocation under threshold continue checks
    if (
      currentbatchStateValue.datasetFileIDs.length > 0 &&
      currentbatchStateValue.datasetFileIDs.filter(
        (id) => id.status.state != "Success"
      ).length == 0
    ) {
      currentbatchStateValue["status"] = {
        state: "Success",
        message: "All Files Downloaded",
      };
      await state.put(`${params.batchID}`, currentbatchStateValue); // update batch state
    } else if (invocationCount < THRESHOLD_COUNT) {
      var ow = openwhisk();

      var now = new Date();
      now.setMinutes(now.getMinutes() + 1); //add a minute to current dateTime
      const jobId = uuid4();
      const batchID = params.batchID;
      const LOG_LEVEL = params.LOG_LEVEL;
      invocationCount++;
      const triggerParams = {
        date: `${now.toISOString()}`,
        deleteAfterFire: "rules",
        minutes: 1,
        trigger_payload: { batchID, invocationCount, LOG_LEVEL },
      };
      await ow.triggers.create({
        name: `${jobId}-check-batch-trigger`,
      });

      await ow.rules.create({
        name: `${jobId}-check-batch-trigger-rule`,
        action: "ode4ac-0.0.1/check-batch-status",
        trigger: `${jobId}-check-batch-trigger`,
      });

      await ow.feeds.create({
        name: "/whisk.system/alarms/once",
        trigger: `${jobId}-check-batch-trigger`,
        params: triggerParams,
      });
    } else {
      currentbatchStateValue["status"] = {
        state: "Suspended",
        message: `Processing took more that ${THRESHOLD_COUNT} Invocations`,
      };
    }

    await state.put(`${params.batchID}`, currentbatchStateValue); // Update batch state
    // get batch state
    batchState = await state.get(`${params.batchID}`);
    logger.debug(
      `updated batchState ${params.batchID} = ${JSON.stringify(batchState)}`
    );
    const response = {
      statusCode: 200,
      body: batchState,
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
