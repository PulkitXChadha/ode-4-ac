const openwhisk = require("openwhisk");
require("dotenv").config();
async function createTrigger() {
  var options = {
    apihost: process.env.AIO_runtime_apihost,
    namespace: process.env.AIO_runtime_namespace,
    api_key: process.env.AIO_runtime_auth,
  };
  var ow = openwhisk(options);

  var now = new Date();
  now.setMinutes(now.getMinutes() + 4); //add a minute to current dateTime
  console.log(now.toISOString());
  let params = {
    batchID: `01ER7GD3J08NDD2Q5TGR8PSNH6`,
  };
  let batchID = params.batchID;
  const triggerParams = {
    minutes: 1,
    trigger_payload: params,
  };

  console.log(
    await ow.triggers.create({
      name: `${params.batchID}-trigger`,
      overwrite: true,
    })
  );
  console.log(
    await ow.rules.create({
      name: `${params.batchID}-rule`,
      action: "ode4ac-0.0.1/check-batch-status",
      trigger: `${params.batchID}-trigger`,
      overwrite: true,
    })
  );
  console.log(
    await ow.feeds.create({
      feedName: "/whisk.system/alarms/interval",
      trigger: `${params.batchID}-trigger`,
      params: triggerParams,
    })
  );
}
createTrigger();
