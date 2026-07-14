const fs = require("fs");
const path = require("path");

const publicDir = path.resolve(__dirname, "public");
fs.mkdirSync(publicDir, { recursive: true });

fs.copyFileSync(path.join(__dirname, "app", "static", "index.html"), path.join(publicDir, "index.html"));
fs.writeFileSync(
  path.join(publicDir, "config.js"),
  `window.RADAR_API_URL = ${JSON.stringify(process.env.RADAR_API_URL || "")};\n`,
  "utf8"
);
