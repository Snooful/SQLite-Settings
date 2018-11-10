const base = require("@snooful/settings-base");
const sqlite = require("sqlite");

/**
 * Manages settings.
 */
class SQLiteSettingsManager extends base.SettingsManager {
	/**
	 * @param {string} databasePath The path to the database to store settings in.
	 */
	constructor(databasePath) {
		super();

		/**
		 * The settings cache.
		 */
		this.settings = {};

		this.setStatement = null;

		// Open the database and init
		sqlite.open(databasePath).then(database => {
			base.debug("opened settings database");

			this.database = database;
			this.init();
		});

	}

	/**
	 * Initializes the database.
	 */
	async init() {
		await this.database.run("CREATE TABLE IF NOT EXISTS settings (subreddit VARCHAR(20) PRIMARY KEY, settings TEXT)").then(() => {
			base.debug("ensured the settings table exists");
		});

		const rows = this.database.all("SELECT CAST(subreddit as TEXT) as subreddit, settings FROM settings").then(rows => {
			base.debug("got rows of settings");
			rows.forEach(row => {
				base.debug("caching settings for r/%s", row.subreddit);
				this.settings[row.subreddit] = JSON.parse(row.settings);
			});
		});

		this.database.prepare("INSERT OR REPLACE INTO settings VALUES(?, ?)").then(statement => {
			base.debug("prepared the set statement");
			this.setStatement = statement;
		});
	}

	async update(subreddit) {
		base.debug(`updating settings database for r/${subreddit}`);
		return this.setStatement.run(subreddit, JSON.stringify(this.settings[subreddit]));
	}
}
module.exports.SettingsManager = SQLiteSettingsManager;

module.exports.extension = ".sqlite3";