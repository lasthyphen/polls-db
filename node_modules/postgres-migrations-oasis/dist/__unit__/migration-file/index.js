"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ava_1 = require("ava");
const migration_file_1 = require("../../migration-file");
ava_1.default("Hashes of JS files should be the same when the SQL is the same", async (t) => {
    const [js1, js2] = await Promise.all([
        migration_file_1.load(__dirname + "/fixtures/different-js-same-sql-1/1_js.js"),
        migration_file_1.load(__dirname + "/fixtures/different-js-same-sql-2/1_js.js"),
    ]);
    t.is(js1.hash, js2.hash);
});
ava_1.default("Hashes of JS files should be different when the SQL is different", async (t) => {
    const [js1, js2] = await Promise.all([
        migration_file_1.load(__dirname + "/fixtures/same-js-different-sql-1/1_js.js"),
        migration_file_1.load(__dirname + "/fixtures/same-js-different-sql-2/1_js.js"),
    ]);
    t.not(js1.hash, js2.hash);
});
