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
  name: "ode4ac-0.0.1/process-datasetfile",
  params: {
    apiKey: process.env.SERVICE_API_KEY,
    sandboxName: "sandbox23",
    imsOrg: process.env.IMS_ORG,
    batchID: "01ERCPAR1TRH0FGF3PF7AJTYDM",
    dataSetFileId: "01ERCPAR1TRH0FGF3PF7AJTYDM-1",
  },
};
ow.actions.invoke(payload).then((result) => console.log(result));
