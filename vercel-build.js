const fs = require("fs");
const path = require("path");

const publicDir = path.resolve(__dirname, "public");
fs.mkdirSync(publicDir, { recursive: true });

const sourceIndex = path.join(__dirname, "app", "static", "index.html");
const outputIndex = path.join(publicDir, "index.html");
if (fs.existsSync(sourceIndex)) {
  fs.copyFileSync(sourceIndex, outputIndex);
}
if (!fs.existsSync(outputIndex)) {
  throw new Error("public/index.html nao encontrado para deploy estatico");
}
fs.writeFileSync(
  path.join(publicDir, "config.js"),
  `window.RADAR_API_URL = ${JSON.stringify(process.env.RADAR_API_URL || "")};\n`,
  "utf8"
);
