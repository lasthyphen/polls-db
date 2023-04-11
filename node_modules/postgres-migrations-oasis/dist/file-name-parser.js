"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const parseId = (id) => {
    const parsed = parseInt(id, 10);
    if (isNaN(parsed)) {
        throw new Error(`
Migration file name should begin with an integer ID.'`);
    }
    return parsed;
};
exports.parseFileName = (fileName) => {
    const result = /^(-?\d+)[-_]?(.*).(sql|js)$/gi.exec(fileName);
    if (!result) {
        throw new Error(`
Invalid file name: '${fileName}'.`);
    }
    const [, id, name, type] = result;
    const lowerType = type.toLowerCase();
    if (lowerType !== "js" && lowerType !== "sql") {
        throw new Error("Not a JS or SQL file");
    }
    return {
        id: parseId(id),
        name: name == null || name === "" ? fileName : name,
        type: lowerType,
    };
};
