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

const { context, getToken } = require("@adobe/aio-lib-ims");
const fs = require("fs");
const { Core, CustomerProfile } = require("@adobe/aio-sdk");
const {
  errorResponse,
  stringParameters,
  checkMissingRequestInputs,
} = require("../utils");
const stateLib = require("@adobe/aio-lib-state");
// main function that will be executed by Adobe I/O Runtime
async function main(params) {
  // create a Logger
  const logger = Core.Logger("main", { level: params.LOG_LEVEL || "info" });

  try {
    // 'info' is the default level if not set
    logger.info("Calling get-ims-token");

    // check for missing request input parameters and headers
    const requiredParams = [
      "apiKey",
      "clientSecret",
      "techAcctId",
      "techAcctEmail",
      "metaScope",
      "imsOrg",
      "privateKey",
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

    // the IMS context configuration is passed as an action parameter
    const imsContextConfig = {
      client_id: params.apiKey,
      client_secret: params.clientSecret,
      technical_account_id: params.techAcctId,
      technical_account_email: params.techAcctEmail,
      meta_scopes: params.metaScope,
      ims_org_id: params.imsOrg,
      private_key: params.privateKey,
    };

    await context.set("my_ctx", imsContextConfig);
    const token = await getToken("my_ctx");

    const state = await stateLib.init(); // init when env vars __OW_API_KEY and __OW_NAMESPACE are set (e.g. when running in an OpenWhisk action)
    await state.put("accessToken", token);

    logger.info(`Token Save successfully`);
  } catch (error) {
    // log any server errors
    logger.error(error);
  }
}

exports.main = main;
