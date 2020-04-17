const base = require("@snooful/settings-base");

const sqlite = require("sqlite");
const { Database } = require("sqlite3");

/**
 * Manages settings through a SQLite database.
 */
class SQLiteSettingsManager extends base.SettingsManager {
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
		base.debug("opened settings database");

		return database;
	}

	/**
	 * Creates a settings table for the database if one does not exist already.
	 * @param {Database} database The database to create the table in.
	 */
	async createTable(database) {
		await database.run("CREATE TABLE IF NOT EXISTS settings (subreddit VARCHAR(20) PRIMARY KEY, settings TEXT)");
		base.debug("ensured the settings table exists");
	}

	/**
	 * Caches settings from a database.
	 * @param {Database} database The database to cache settings from.
	 */
	async cacheSettings(database) {
		const rows = await database.all("SELECT CAST(subreddit as TEXT) as subreddit, settings FROM settings");
		base.debug("got rows of settings");

		for (const row of rows) {
			try {
				this.settings[row.subreddit] = JSON.parse(row.settings);
				base.debug("cached settings for r/%s", row.subreddit);
			} catch (error) {
				base.debug("could not cache settings for r/%s: %o", row.subreddit, error);
			}
		}
	}

	async prepareInsertStatement(database) {
		const insertStatement = await database.prepare("INSERT OR REPLACE INTO settings VALUES(?, ?)");
		base.debug("prepared the insert statement");
		return insertStatement;
	}

	/**
	 * Initializes the database.
	 */
	async init() {
		const database = await this.open(this.databasePath);
		await this.createTable(database);

		this.insertStatement = await this.prepareInsertStatement(database);
		await this.cacheSettings(database);
	}


	update(subreddit) {
		if (this.insertStatement === null) {
			throw new Error("The database has not been initialized yet");
		}

		base.debug(`updating settings database for r/${subreddit}`);
		return this.insertStatement.run(subreddit, JSON.stringify(this.settings[subreddit]));
	}
}
module.exports.SettingsManager = SQLiteSettingsManager;

module.exports.extension = ".sqlite3";