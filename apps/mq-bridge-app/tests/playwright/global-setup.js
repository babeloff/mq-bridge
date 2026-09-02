/**
 * Build the app once, before any worker starts. Each worker then execs the same
 * binary for its own server (see app-server.js), so the whole run compiles once
 * no matter how many workers it uses.
 */
const { buildBinary } = require("./app-server");

module.exports = () => {
  process.env.MQB_APP_BINARY = buildBinary();
};
