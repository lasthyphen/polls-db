"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ava_1 = require("ava");
const file_name_parser_1 = require("../../file-name-parser");
ava_1.default("parse name: 1.sql", t => {
    const parsed = file_name_parser_1.parseFileName("1.sql");
    t.deepEqual(parsed, {
        id: 1,
        name: "1.sql",
        type: "sql",
    }, "should parse correctly without name, the parsed name must be the fileName");
});
ava_1.default("parse name: 1file.sql", t => {
    const parsed = file_name_parser_1.parseFileName("1file.sql");
    t.deepEqual(parsed, {
        id: 1,
        name: "file",
        type: "sql",
    }, "should parse correctly without separator");
});
ava_1.default("parse name: 1-file.sql", t => {
    const parsed = file_name_parser_1.parseFileName("1-file.sql");
    t.deepEqual(parsed, {
        id: 1,
        name: "file",
        type: "sql",
    }, "should parse correctly with dash separator");
});
ava_1.default("parse name: 1_file.sql", t => {
    const parsed = file_name_parser_1.parseFileName("1_file.sql");
    t.deepEqual(parsed, {
        id: 1,
        name: "file",
        type: "sql",
    }, "should parse correctly with underscore separator");
});
ava_1.default("parse name: 1-2_file.sql", t => {
    const parsed = file_name_parser_1.parseFileName("1-2_file.sql");
    t.deepEqual(parsed, {
        id: 1,
        name: "2_file",
        type: "sql",
    }, "should parse correctly returning everything after dash separator as name");
});
ava_1.default("parse name: 1_2_file.sql", t => {
    const parsed = file_name_parser_1.parseFileName("1_2_file.sql");
    t.deepEqual(parsed, {
        id: 1,
        name: "2_file",
        type: "sql",
    }, "should parse correctly returning everything after underscore separator as name");
});
ava_1.default("parse name: 1_file.SQL", t => {
    const parsed = file_name_parser_1.parseFileName("1_file.SQL");
    t.deepEqual(parsed, {
        id: 1,
        name: "file",
        type: "sql",
    }, "should parse correctly with case insensitive");
});
ava_1.default("parse name: 0001_file.sql", t => {
    const parsed = file_name_parser_1.parseFileName("0001_file.sql");
    t.deepEqual(parsed, {
        id: 1,
        name: "file",
        type: "sql",
    }, "should parse correctly with left zeros");
});
ava_1.default("parse name: not_file.sql", t => {
    const err = t.throws(() => file_name_parser_1.parseFileName("not_file.sql"));
    t.regex(err.message, /Invalid file name/);
    t.regex(err.message, /not_file/, "Should name the problem file");
});
