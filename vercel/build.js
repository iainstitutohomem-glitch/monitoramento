const fs = require("fs");
const path = require("path");

const root = path.resolve(__dirname, "..");
const publicDir = path.resolve(__dirname, "public");
fs.mkdirSync(publicDir, { recursive: true });

fs.copyFileSync(path.join(root, "app", "static", "index.html"), path.join(publicDir, "index.html"));

const configPath = path.join(publicDir, "config.js");
if (!fs.existsSync(configPath)) {
  fs.writeFileSync(configPath, 'window.RADAR_API_URL = "";\\n', "utf8");
}
