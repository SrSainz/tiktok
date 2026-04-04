const { proxyViaNas } = require("../lib/nas-proxy");

module.exports = (req, res) => {
  proxyViaNas(req, res, "/api");
};
