const SettingsManager = require("@snooful/settings-base");

const sqlite = require("sqlite");
const { Database } = require("sqlite3");

const log = require("debug")("snooful:sqlite-settings");

/**
 * Manages settings through a SQLite database.
 */
class SQLiteSettingsManager extends SettingsManager {
	/**
	 * @param {string} databasePath The path to the database to store settings in.
	 */
	constructor(databasePath) {
		super();

		this.databasePath = databasePath;
		this.insertStatement = null;
	}

	/**
	 * @param {string} databasePath The path to the database to store settings in.
	 * @returns {Database} The opened database.
	 */
	async open(databasePath) {
		const database = await sqlite.open({
			driver: Database,
			filename: databasePath,
		});
		log("opened settings database");

		return database;
	}

	/**
	 * Creates a settings table for the database if one does not exist already.
	 * @param {Database} database The database to create the table in.
	 */
	async createTable(database) {
		await database.run("CREATE TABLE IF NOT EXISTS settings (subreddit VARCHAR(20) PRIMARY KEY, settings TEXT)");
		log("ensured the settings table exists");
	}

	/**
	 * Caches settings from a database.
	 * @param {Database} database The database to cache settings from.
	 */
	async cacheSettings(database) {
		const rows = await database.all("SELECT CAST(subreddit as TEXT) as namespace, settings FROM settings");
		log("got rows of settings");

		for (const row of rows) {
			try {
				this.settings[row.namespace] = JSON.parse(row.settings);
				log("cached settings for the '%s' namespace", row.namespace);
			} catch (error) {
				log("could not cache settings for the '%s' namespace: %o", row.namespace, error);
			}
		}
	}

	async prepareInsertStatement(database) {
		const insertStatement = await database.prepare("INSERT OR REPLACE INTO settings VALUES(?, ?)");
		log("prepared the insert statement");
		return insertStatement;
	}

	/**
	 * Initializes the database.
	 */
	async initialize() {
		const database = await this.open(this.databasePath);
		await this.createTable(database);

		this.insertStatement = await this.prepareInsertStatement(database);
		await this.cacheSettings(database);
	}


	update(namespace) {
		if (this.insertStatement === null) {
			throw new Error("The database has not been initialized yet");
		}

		log("updating settings database for the '%s' namespace", namespace);
		return this.insertStatement.run(namespace, JSON.stringify(this.settings[namespace]));
	}
}

SQLiteSettingsManager.extension = ".sqlite3";

module.exports = SQLiteSettingsManager;