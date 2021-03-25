/*const openwhisk = require("openwhisk");
require("dotenv").config();

var options = {
  apihost: process.env.AIO_runtime_apihost,
  namespace: process.env.AIO_runtime_namespace,
  api_key: process.env.AIO_runtime_auth,
};
var ow = openwhisk(options);

// // Test get-ims-token.js
let payload = {
  name: "ode4ac-0.0.1/convert-parquet-to-csv",
  params: {
    filePath:
      "01ERCPAR1TRH0FGF3PF7AJTYDM/01ERCPAR1TRH0FGF3PF7AJTYDM-1/00000-97-4c7be00c-5ce9-40e9-affd-ad388e852fa1-00000.parquet",
    fields: [
      "mobilePhone.number",
      "person.name.lastName",
      "person.name.firstName",
      "personalEmail.address",
    ],
  },
};
ow.actions.invoke(payload).then((result) => console.log(result));

*/

const parquet = require("parquetjs-lite");
const filesLib = require("@adobe/aio-lib-files");
const fs = require("fs");
const { Readable } = require("stream");
const { Transform } = require("json2csv");

async function convertParquetToCSV(params) {
  return new Promise(async function (resolve, reject) {
    try {
    } catch (e) {
      reject(e);
    }
  });
}

// main function that will be executed by Adobe I/O Runtime
async function readParquet() {
  let reader = await parquet.ParquetReader.openFile(params.filePath); // create new ParquetReader that reads from 'fruits.parquet`
  let cursors = [];
  let csvFields = [];
  params.fields.map((field) => {
    cursors.push(reader.getCursor([field]));
    csvFields.push(field.join(`.`));
  });

  console.log(`Reading file ${params.filePath}`);
  const recordsToProcess = reader.getRowCount();
  console.log(`# Of recs: ${reader.getRowCount()}`);

  console.log(`fileMetaData: ${JSON.stringify(reader.metadata)}`);

  const opts = { fields: csvFields };
  const transformOpts = { highWaterMark: 16384, encoding: "utf-8" };
  const json2csv = new Transform(opts, transformOpts);
  const output = fs.createWriteStream("temp.csv", { encoding: "utf8" });

  const wrStream = await fs.createWriteStream(`${params.filePath}.csv`);

  const inStream = new Readable();
  inStream._read = () => {};
  const processor = inStream.pipe(json2csv).pipe(wrStream);

  // You can also listen for events on the conversion and see how the header or the lines are coming out.
  wrStream.on("close", () => {
    console.log(`Finished read of local file ${params.filePath}`);
    const used = process.memoryUsage().heapUsed / 1024 / 1024;
    console.log(
      `The script uses approximately ${Math.round(used * 100) / 100} MB`
    );
    return {
      recordCount: recordsToProcess.toString(),
      filePath: `${params.filePath}.csv`,
    };
  });

  console.log(`Starting to read local file ${params.filePath}`);
  // for (let rec = 0; rec <= recordsToProcess; rec++) {
  //   let rec = {};
  //   for (c of cursors) {
  //     let data = await c.next();
  //     rec = { ...rec, ...data };
  //   }

  //   inStream.push(JSON.stringify(rec));
  // }
  await reader.close();

  inStream.push(null);
}

const params = {
  fields: [
    ["mobilePhone", "number"],
    ["personalEmail", "address"],
    ["person", "name", "lastName"],
    ["person", "name", "firstName"],
  ],
  filePath: "/Users/pulkitchadha/Downloads/large.parquet",
  // filePath: "test.parquet",
};

readParquet(params);
