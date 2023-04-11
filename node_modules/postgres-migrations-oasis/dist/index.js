"use strict";
function __export(m) {
    for (var p in m) if (!exports.hasOwnProperty(p)) exports[p] = m[p];
}
Object.defineProperty(exports, "__esModule", { value: true });
var create_1 = require("./create");
exports.createDb = create_1.createDb;
var migrate_1 = require("./migrate");
exports.migrate = migrate_1.migrate;
__export(require("./types"));
