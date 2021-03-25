const openwhisk = require("openwhisk");
require("dotenv").config();

var options = {
  apihost: process.env.AIO_runtime_apihost,
  namespace: process.env.AIO_runtime_namespace,
  api_key: process.env.AIO_runtime_auth,
};
var ow = openwhisk(options);

// // Test get-ims-token.js
let payload = {
  name: "ode4ac-0.0.1/check-datasetfile-status",
  params: {
    imsOrg: "22A746245D84C1B50A495CD5@AdobeOrg",
    apiKey: "301a98400e8549cb81baf69a4d3b0eee",
    LOG_LEVEL: "debug",
    dataSetFileId: "01ERCPAR1TRH0FGF3PF7AJTYDM-1",
    sandboxName: "sandbox23",
    batchID: "01ERCPAR1TRH0FGF3PF7AJTYDM",
  },
};
ow.actions.invoke(payload).then((result) => console.log(result));
