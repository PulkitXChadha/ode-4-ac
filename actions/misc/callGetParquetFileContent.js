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
  name: "ode4ac-0.0.1/get-parquet-file-content",
  params: {
    apiKey: process.env.SERVICE_API_KEY,
    sandboxName: "sandbox23",
    imsOrg: process.env.IMS_ORG,
    LOG_LEVEL: "debug",
    dataSetFileId: "01ERCPAR1TRH0FGF3PF7AJTYDM-1",
    fileName: "00000-97-4c7be00c-5ce9-40e9-affd-ad388e852fa1-00000.parquet",
    fileSize: "155100538",
    filePath:
      "https://platform.adobe.io:443/data/foundation/export/files/01ERCPAR1TRH0FGF3PF7AJTYDM-1?path=00000-97-4c7be00c-5ce9-40e9-affd-ad388e852fa1-00000.parquet",
    batchID: "01ERCPAR1TRH0FGF3PF7AJTYDM",
  },
};

ow.actions.invoke(payload).then((result) => console.log(result));

/*
"use strict";
const request = require("request");
const fs = require("fs");
const zlib = require("zlib");
const stream = require("stream");
async function getPartialFileContent(params) {
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
      console.log(e);
    }
  });
}

// main function that will be executed by Adobe I/O Runtime
async function getData(params) {
  var wtStream = fs.createWriteStream("test.parquet", {
    flags: "a", // 'a' means appending (old data will be preserved)
  });
  // const readable = new stream.Readable();
  // readable.setEncoding("binary");
  // readable._read = () => {}; // _read is required but you can noop it
  // readable.pipe(wtStream); // consume the stream
  const token =
    "eyJ4NXUiOiJpbXNfbmExLWtleS0xLmNlciIsImFsZyI6IlJTMjU2In0.eyJpZCI6IjE2MDY3NTYxMjc3MDZfZjRkMWQ2ZDktOGM2Yi00ZGZmLTg3M2YtNGE2ZWQ2ZWEwOTRhX3VlMSIsImNsaWVudF9pZCI6IjUxNTExNGIzNTM4MTRkYjFiZGI2NzE0YzRiM2YyNTk2IiwidXNlcl9pZCI6IjUzOUM2MkQxNUUwRTI1MzUwQTQ5NUNFMUB0ZWNoYWNjdC5hZG9iZS5jb20iLCJzdGF0ZSI6IntcInNlc3Npb25cIjpcImh0dHBzOi8vaW1zLW5hMS5hZG9iZWxvZ2luLmNvbS9pbXMvc2Vzc2lvbi92MS9aVGhsWmpJNE56UXRZVEV3T0MwME56WTBMVGxtT0RjdFpXVTNZbVk0WVdJNU5UUmpMUzAxTXpsRE5qSkVNVFZGTUVVeU5UTTFNRUUwT1RWRFJURkFkR1ZqYUdGalkzUXVZV1J2WW1VdVkyOXRcIn0iLCJ0eXBlIjoiYWNjZXNzX3Rva2VuIiwiYXMiOiJpbXMtbmExIiwiZmciOiJVN09XSTVHNkhMUDU1UDZERzRKM1FHQUFCST09PT09PSIsIm1vaSI6ImI0YTU4YzU3IiwiYyI6ImJ5STg0VFduSUhaWjVaNXVTdk12NUE9PSIsImV4cGlyZXNfaW4iOiI4NjQwMDAwMCIsInNjb3BlIjoiYWRkaXRpb25hbF9pbmZvLmpvYl9mdW5jdGlvbixhZG9iZWlvX2FwaSxvcGVuaWQsdXNlcl9tYW5hZ2VtZW50X3NkayxzZXNzaW9uLEFkb2JlSUQscmVhZF9vcmdhbml6YXRpb25zLGFkZGl0aW9uYWxfaW5mby5yb2xlcyxhZGRpdGlvbmFsX2luZm8ucHJvamVjdGVkUHJvZHVjdENvbnRleHQiLCJjcmVhdGVkX2F0IjoiMTYwNjc1NjEyNzcwNiJ9.AqBpqCz0xV5hwKomWzG1zCKSnYahv6rnETfM3V4h0eyMzKdXg5Syzuo23_HEOB6xbcbzVDYAr8ATK-lLYNSFo6QgVhF5t0-yHIYpkgZmOfgF6FNAmxLsrcwD7cuiZ5bm4BvveK-CqOAlS2_tBzWFSwaNEyzgw8-Y8QYpxqAi6sY2Tc6e3TLVkHd3BKokzJbzawI_hVY7IIHisvYe49lpz7rtrUTToVXrQFQiyvGY8yzEXSPaohLTUO47LVc-1ZvGei7HXAp5UrBNd4FtGkT9AbCdkMib01quFIclT3zdXG6J0EwzStsfuFu6a3MA4tFWrpyqG5qzh2nU7i02X3LaOg";

  wtStream.setDefaultEncoding("binary");
  let i = 0;
  const chunkSize = 26214400;
  //0-1000
  //1001
  for (i = -1; i <= params.fileSize; i = i + chunkSize) {
    const data = await getPartialFileContent({
      ...params,
      range: `bytes=${i + 1}-${i + chunkSize}`,
      token: token,
    });
    // readable.push(data);
    wtStream.write(data);
  }
  // readable.push(null);
  console.log("File Download complete");
  wtStream.end(); // close string
}

let params = {
  apiKey: "515114b353814db1bdb6714c4b3f2596",
  sandboxName: "sandbox23",
  dataSetFileId: "01ER7GD3J08NDD2Q5TGR8PSNH6-1",
  imsOrg: "22A746245D84C1B50A495CD5@AdobeOrg",
  fileSize: "155100538",
  fileName: "00000-97-4c7be00c-5ce9-40e9-affd-ad388e852fa1-00000.parquet",
  batchID: "01ERCPAR1TRH0FGF3PF7AJTYDM",
  LOG_LEVEL: "debug",
  filePath:
    "https://platform.adobe.io:443/data/foundation/export/files/01ERCPAR1TRH0FGF3PF7AJTYDM-1?path=00000-97-4c7be00c-5ce9-40e9-affd-ad388e852fa1-00000.parquet",
};

getData(params);
*/
