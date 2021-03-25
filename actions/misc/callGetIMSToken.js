const openwhisk = require('openwhisk');
require('dotenv').config()

var options = {
    apihost: process.env.AIO_runtime_apihost,
    namespace: process.env.AIO_runtime_namespace,
    api_key: process.env.AIO_runtime_auth
}
var ow = openwhisk(options)

// Test get-ims-token.js
let payload = {
    name: "ode4ac-0.0.1/get-ims-token"
}
ow.actions.invoke(payload).then(result => console.log(result));
