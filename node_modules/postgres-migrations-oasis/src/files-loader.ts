import * as fs from "fs"
import * as path from "path"
import {promisify} from "util"
import {load as loadMigrationFile, loadRaw} from "./migration-file"
import {Logger, Migration, MigrateDBConfig} from "./types"

const readDir = promisify(fs.readdir)

const isValidFile = (fileName: string) => /.(sql|js)$/gi.test(fileName)

export const load = async (
  directory: string,
  log: Logger,
  config: MigrateDBConfig,
): Promise<Array<Migration>> => {
  log(`Loading migrations from: ${directory}`)

  const fileNames = await readDir(directory)
  log(`Found migration files: ${fileNames}`)

  if (fileNames != null) {
    const migrationFiles = [
      ...fileNames.map(fileName => path.resolve(directory, fileName)),
    ].filter(isValidFile)

    const unorderedMigrations = await Promise.all(
      migrationFiles.map(loadMigrationFile),
    )

    unorderedMigrations.push(
      loadRaw(
        "0_create-migrations-table.sql",
        `CREATE TABLE IF NOT EXISTS "${config.tableName!}" (
      id integer PRIMARY KEY,
      name varchar(100) UNIQUE NOT NULL,
      hash varchar(40) NOT NULL, -- sha1 hex encoded hash of the file name and contents, to ensure it hasn't been altered since applying the migration
      executed_at timestamp DEFAULT current_timestamp
    );
    `,
      ),
    )

    // Arrange in ID order
    return unorderedMigrations.sort((a, b) => a.id - b.id)
  }

  return []
}
