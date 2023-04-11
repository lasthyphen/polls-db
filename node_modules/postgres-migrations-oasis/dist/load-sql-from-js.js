"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const path = require("path");
exports.loadSqlFromJs = (filePath) => {
    const migrationModule = require(filePath);
    if (!migrationModule.generateSql) {
        throw new Error(`
Invalid javascript migration file: '${path.basename(filePath)}'.
It must to export a 'generateSql' function.`);
    }
    const generatedValue = migrationModule.generateSql();
    if (typeof generatedValue !== "string") {
        throw new Error(`
Invalid javascript migration file: '${path.basename(filePath)}'.
'generateSql' function must return a string literal.`);
    }
    return generatedValue;
};
