import { DocumentStore, DeleteByQueryOperation, IndexQuery} from 'ravendb';

export class CronHistory {
    constructor(
		id = null,
        startedAt = null
    ) {
		Object.assign(this, {
			id,
			startedAt
		});
	}
}


let RAVEN_STORE;
// A package for running jobs synchronized across multiple processes
SyncedCron = {
    _entries: {},
    running: false,
    options: {
		//Log job run details to console
		log: true,

		logger: null,

		ravenDbCertificate: null,
		ravenDbServers: null,
		ravenDbDatabase: null,

		//Name of collection to use for synchronisation and logging
		collectionName: 'cronHistory',

		//Default to using localTime
		utc: false,

		//TTL in seconds for history records in collection to expire
		//NOTE: Unset to remove expiry but ensure you remove the index from
		//mongo by hand
		collectionTTL: 172800
    },
    config: function(opts) {
      	this.options = _.extend({}, this.options, opts);

        /**
         * RavenDB Setup
         */
        if(!this.options.ravenDbCertificate){
          throw new Error("SyncedCron.config is missing ravenDbCertificate");
        }
        if(!this.options.ravenDbDatabase){
          throw new Error("SyncedCron.config is missing ravenDbDatabase");
        }
        if(!this.options.ravenDbDatabase){
          throw new Error("SyncedCron.config is missing ravenDbDatabase");
        }

        let authOptions = {
          certificate: this.options.ravenDbCertificate,
          type: "pfx",
          //password: 'my passphrase' // optional  
        };
        
        RAVEN_STORE = new DocumentStore(this.options.ravenDbServers, this.options.ravenDbDatabase, authOptions);
        // Default to the first available node for consistency
        RAVEN_STORE.conventions.readBalanceBehavior = 'None'; // RoundRobin or None - https://ravendb.net/docs/article-page/4.2/nodejs/client-api/configuration/load-balance-and-failover
        // RavenDB likes to use JS Classes to define the collection, but we want to hard set it based on dynamic input
		// Dynamically change the class name as RavenDB uses it for the collection name.
		Object.defineProperty (CronHistory, 'name', {value: this.options.collectionName});
		RAVEN_STORE.conventions.findCollectionName = clazz => {
			if (clazz === CronHistory) {
				return this.options.collectionName;
			}
			return DocumentConventions.defaultGetCollectionName(clazz);
		};

        RAVEN_STORE.initialize();
        /**
         * END RavenDB Setup
         */
    }
}

Later = Npm.require('later');

/*
  Logger factory function. Takes a prefix string and options object
  and uses an injected `logger` if provided, else falls back to
  Meteor's `Log` package.

  Will send a log object to the injected logger, on the following form:

    message: String
    level: String (info, warn, error, debug)
    tag: 'SyncedCron'
*/
function createLogger(prefix) {
	check(prefix, String);

	// Return noop if logging is disabled.
	if(SyncedCron.options.log === false) {
		return function() {};
	}

	return function(level, message) {
		check(level, Match.OneOf('info', 'error', 'warn', 'debug'));
		check(message, String);

		var logger = SyncedCron.options && SyncedCron.options.logger;

		if(logger && _.isFunction(logger)) {

		logger({
			level: level,
			message: message,
			tag: prefix
		});

		} else {
			Log[level]({ message: prefix + ': ' + message });
		}
	}
}

var log;

Meteor.startup(function() {
	var options = SyncedCron.options;

	log = createLogger('SyncedCron');

	['info', 'warn', 'error', 'debug'].forEach(function(level) {
		log[level] = _.partial(log, level);
	});

	// Don't allow TTL less than 5 minutes so we don't break synchronization
	var minTTL = 300;

	// Use UTC or localtime for evaluating schedules
	if (options.utc)
		Later.date.UTC();
	else
		Later.date.localTime();

});

var scheduleEntry = function(entry) {
	var schedule = entry.schedule(Later.parse);
	entry._timer =
		SyncedCron._laterSetInterval(SyncedCron._entryWrapper(entry), schedule);

	log.info('Scheduled "' + entry.name + '" next run @'
		+ Later.schedule(schedule).next(1));
}

// add a scheduled job
// SyncedCron.add({
//   name: String, //*required* unique name of the job
//   schedule: function(laterParser) {},//*required* when to run the job
//   job: function() {}, //*required* the code to run
// });
SyncedCron.add = function(entry) {
	check(entry.name, String);
	check(entry.schedule, Function);
	check(entry.job, Function);
	check(entry.persist, Match.Optional(Boolean));

	if (entry.persist === undefined) {
		entry.persist = true;
	}

	// check
	if (!this._entries[entry.name]) {
		this._entries[entry.name] = entry;

		// If cron is already running, start directly.
		if (this.running) {
			scheduleEntry(entry);
		}
	}
}

// Start processing added jobs
SyncedCron.start = function() {
	var self = this;

	Meteor.startup(function() {
		// Schedule each job with later.js
		_.each(self._entries, function(entry) {
			scheduleEntry(entry);
		});
		self.running = true;
	});
}

// Return the next scheduled date of the first matching entry or undefined
SyncedCron.nextScheduledAtDate = function(jobName) {
	var entry = this._entries[jobName];

	if (entry)
		return Later.schedule(entry.schedule(Later.parse)).next(1);
}

// Remove and stop the entry referenced by jobName
SyncedCron.remove = function(jobName) {
	var entry = this._entries[jobName];

	if (entry) {
		if (entry._timer)
		entry._timer.clear();

		delete this._entries[jobName];
		log.info('Removed "' + entry.name + '"');
  }
}

// Pause processing, but do not remove jobs so that the start method will
// restart existing jobs
SyncedCron.pause = function() {
	if (this.running) {
		_.each(this._entries, function(entry) {
			entry._timer.clear();
		});
		this.running = false;
	}
}

// Stop processing and remove ALL jobs
SyncedCron.stop = function() {
	_.each(this._entries, function(entry, name) {
		SyncedCron.remove(name);
	});
	this.running = false;
}

// The meat of our logic. Checks if the specified has already run. If not,
// records that it's running the job, runs it, and records the output
SyncedCron._entryWrapper = function(entry) {
	var self = this;
	var options = SyncedCron.options;

	if(!RAVEN_STORE){
		throw new Error("SyncedCron.config must be run first");
	}
	return async function(intendedAt) {
		intendedAt = new Date(intendedAt.getTime());
		intendedAt.setMilliseconds(0);

		var jobHistory;

		if (entry.persist) {
			jobHistory = {
				intendedAt: intendedAt.toISOString(),
				name: entry.name,
				startedAt: new Date().toISOString()
			};

			// If we have a dup key error, another instance has already tried to run
			// this job.
			try {
				
				let ravenSession = RAVEN_STORE.openSession({
					// RavenDB can act as a multi-master node. Make sure the session is accepted by all nodes!
					TransactionMode: "ClusterWide"
				});

				let now = new Date();
				let expiry = new Date(+now.getTime() + options.collectionTTL * 1000);

				let cronHistory = new CronHistory(`cronHistory/${jobHistory.name}_${jobHistory.intendedAt}`, jobHistory.startedAt)
				// Store the name and intended at as the document Id to force uniqueness
				await ravenSession.store(cronHistory);
				// RavenDB has ttl on the document level, not the index level (which is really flexible).
				ravenSession.advanced.getMetadataFor(cronHistory)["@expires"] = expiry;
				// Save changes
				await ravenSession.saveChanges();

			} catch(e) {
				log.info(e);
				// TODO: catch unique error
				if (e.code === 11000) {
					log.info('Not running "' + entry.name + '" again.');
					return;
				}
				throw e;
			};
		}

		// run and record the job
		try {
			log.info('Starting "' + entry.name + '".');
			var output = entry.job(intendedAt, entry.name); // <- Run the actual job

			log.info('Finished "' + entry.name + '".');
			if(entry.persist) {
				let ravenSession = RAVEN_STORE.openSession();
				// Make sure we do a `load` as it has guaranteed existence compared to `query` that uses an index as is `eventually consistent`.
				var doc = await ravenSession.load(`cronHistory/${jobHistory.name}_${jobHistory.intendedAt}`);
				doc.finishedAt = new Date().toISOString();
				doc.result = output;
				// Commit the transaction
				await ravenSession.saveChanges()
			}
		} catch(e) {
			log.info('Exception "' + entry.name +'" ' + ((e && e.stack) ? e.stack : e));
			if(entry.persist) {
				let ravenSession = RAVEN_STORE.openSession();
				var doc = await ravenSession.load(`cronHistory/${jobHistory.name}_${jobHistory.intendedAt}`);
				doc.finishedAt = new Date().toISOString();
				doc.error = (e && e.stack) ? e.stack : e
				// Commit the transaction
				await ravenSession.saveChanges()
			}
		}
  	};
}

// for tests
SyncedCron._reset = async function() {
	if(!RAVEN_STORE){
		throw new Error("SyncedCron.config must be run first");
	}
	this._entries = {};
	var options = SyncedCron.options;

	const indexQuery = new IndexQuery();
	// Get all the items from the collection - this will create an auto index.
	indexQuery.query = `from ${options.collectionName}`;
	const operation = new DeleteByQueryOperation(indexQuery);
	const asyncOp = await RAVEN_STORE.operations.send(operation);

	await asyncOp.waitForCompletion();
	this.running = false;
}

// ---------------------------------------------------------------------------
// The following two functions are lifted from the later.js package, however
// I've made the following changes:
// - Use Meteor.setTimeout and Meteor.clearTimeout
// - Added an 'intendedAt' parameter to the callback fn that specifies the precise
//   time the callback function *should* be run (so we can co-ordinate jobs)
//   between multiple, potentially laggy and unsynced machines

// From: https://github.com/bunkat/later/blob/master/src/core/setinterval.js
SyncedCron._laterSetInterval = function(fn, sched) {

	var t = SyncedCron._laterSetTimeout(scheduleTimeout, sched),
		done = false;

	/**
	 * Executes the specified function and then sets the timeout for the next
	 * interval.
	 */
	function scheduleTimeout(intendedAt) {
		if(!done) {
			try {
				fn(intendedAt);
			} catch(e) {
				log.info('Exception running scheduled job ' + ((e && e.stack) ? e.stack : e));
			}
			t = SyncedCron._laterSetTimeout(scheduleTimeout, sched);
		}
	}

	return {

		/**
		* Clears the timeout.
		*/
		clear: function() {
			done = true;
			t.clear();
		}
	};
};

// From: https://github.com/bunkat/later/blob/master/src/core/settimeout.js
SyncedCron._laterSetTimeout = function(fn, sched) {

	var s = Later.schedule(sched), t;
	scheduleTimeout();

	/**
	 * Schedules the timeout to occur. If the next occurrence is greater than the
	 * max supported delay (2147483647 ms) than we delay for that amount before
	 * attempting to schedule the timeout again.
	 */
	function scheduleTimeout() {
		var now = Date.now(),
			next = s.next(2, now);

		// don't schedlue another occurence if no more exist synced-cron#41
		if (! next[0])
		return;

		var diff = next[0].getTime() - now,
			intendedAt = next[0];

		// minimum time to fire is one second, use next occurrence instead
		if(diff < 1000) {
			diff = next[1].getTime() - now;
			intendedAt = next[1];
		}

		if(diff < 2147483647) {
			t = Meteor.setTimeout(function() { fn(intendedAt); }, diff);
		}
		else {
			t = Meteor.setTimeout(scheduleTimeout, 2147483647);
		}
  	}
	return {
		/**
		* Clears the timeout.
		*/
		clear: function() {
			Meteor.clearTimeout(t);
		}
	};
};
// ---------------------------------------------------------------------------
