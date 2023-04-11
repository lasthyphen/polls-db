import { Client } from "pg";
import { Logger, Migration } from "./types";
export declare const runMigration: (migrationTableName: string, client: Pick<Client, "query">, log?: Logger) => (migration: Migration) => Promise<Migration>;
