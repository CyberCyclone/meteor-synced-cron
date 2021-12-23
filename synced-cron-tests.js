Later = Npm.require('later');
const fs = require('fs');
import { DocumentStore} from 'ravendb';


Later.date.localTime(); // corresponds to SyncedCron.options.utc: true;


var TestEntry = {
	name: 'Test Job',
	schedule: function(parser) {
		return parser.cron('15 10 * * ? *'); // not required
	},
	job: function() {
		return 'ran';
	}
};

/**
 * RavenDB Setup
 */
let assetPath = Assets.absoluteFilePath('private/'+Meteor.settings.private.ravendb.certificate_filename);

const ravenDb_certificate = new Buffer.from( fs.readFileSync(assetPath));
const ravenDb_servers = Meteor.settings.private.ravendb.servers;
const ravenDb_database = 'testnet';
let authOptions = {
  certificate: ravenDb_certificate,
  type: "pfx",
  //password: 'my passphrase' // optional  
};
export const RAVEN_STORE = new DocumentStore(ravenDb_servers, ravenDb_database, authOptions);
// Default to the first available node for consistency
RAVEN_STORE.conventions.readBalanceBehavior = 'None'; // RoundRobin or None - https://ravendb.net/docs/article-page/4.2/nodejs/client-api/configuration/load-balance-and-failover
// RavenDB likes to use JS Classes to define the collection, but we want to hard set it based on dynamic input
RAVEN_STORE.initialize();
/**
 * END RavenDB Setup
 */

Tinytest.addAsync('Syncing works', async (test) =>{

	SyncedCron.config({
		ravenDbCertificate: ravenDb_certificate,
		ravenDbServers: ravenDb_servers,
		ravenDbDatabase: ravenDb_database,
	});

	await SyncedCron._reset();

	const ravenSession = RAVEN_STORE.openSession();
	test.equal(await ravenSession.query({ collection: "cronHistory" }).count(), 0, "Collection is not empty");

	// added the entry ok
	SyncedCron.add(TestEntry);
	test.equal(_.keys(SyncedCron._entries).length, 1, "SyncedCron._entries is not 1");

	var entry = SyncedCron._entries[TestEntry.name];
	var intendedAt = new Date(); //whatever
	intendedAt.setMilliseconds(0);

	// first run
	await SyncedCron._entryWrapper(entry)(intendedAt);
	test.equal(await ravenSession.query({ collection: "cronHistory" }).count(), 1, "cronHistory collection should be 1");
	var jobHistory1 = await ravenSession.load(`cronHistory/${TestEntry.name}_${intendedAt.toISOString()}`);

	test.equal(jobHistory1.result, 'ran', "jobHistory1.result did not match 'ran'");

	// second run
	await SyncedCron._entryWrapper(entry)(intendedAt);
	test.equal(await ravenSession.query({ collection: "cronHistory" }).count(), 1, "cronHistory collection should still be 1"); // should still be 1
	var jobHistory2 = await ravenSession.load(`cronHistory/${TestEntry.name}_${intendedAt.toISOString()}`);
  	test.equal(jobHistory1._id, jobHistory2._id, "jobHistory1._id  jobHistory2._id do not match");
});

Tinytest.addAsync('Exceptions work', async(test) => {
	SyncedCron.config({
		ravenDbCertificate: ravenDb_certificate,
		ravenDbServers: ravenDb_servers,
		ravenDbDatabase: ravenDb_database,
	});

	await SyncedCron._reset();
	SyncedCron.add(_.extend({}, TestEntry, {
			job: function() {
				throw new Meteor.Error('Haha, gotcha!');
			}
		})
	);

	var entry = SyncedCron._entries[TestEntry.name];
	var intendedAt = new Date(); //whatever

	// error without result
	await SyncedCron._entryWrapper(entry)(intendedAt);
	const ravenSession = RAVEN_STORE.openSession();
	test.equal(await ravenSession.query({ collection: "cronHistory" }).count(), 1);
	
	intendedAt.setMilliseconds(0);
	var jobHistory1 = await ravenSession.load(`cronHistory/${TestEntry.name}_${intendedAt.toISOString()}`);

	test.equal(jobHistory1.result, undefined);
	test.matches(jobHistory1.error, /Haha, gotcha/);
});

Tinytest.addAsync('SyncedCron.nextScheduledAtDate works', async(test) => {
	SyncedCron.config({
		ravenDbCertificate: ravenDb_certificate,
		ravenDbServers: ravenDb_servers,
		ravenDbDatabase: ravenDb_database,
	});
	await SyncedCron._reset();
	const ravenSession = RAVEN_STORE.openSession();
	test.equal(await ravenSession.query({ collection: "cronHistory" }).count(), 0, "cronHistory is not empty");

	// addd 2 entries
	SyncedCron.add(TestEntry);

	var entry2 = _.extend({}, TestEntry, {
		name: 'Test Job2',
		schedule: function(parser) {
			return parser.cron('30 11 * * ? *');
		}
	});
	SyncedCron.add(entry2);

	test.equal(_.keys(SyncedCron._entries).length, 2, "SyncedCron._entries doesn't equal 2");

	SyncedCron.start();

	var date = SyncedCron.nextScheduledAtDate(entry2.name);
	var correctDate = Later.schedule(entry2.schedule(Later.parse)).next(1);

	test.equal(date, correctDate, "Scheduled date and correct date don't match");
});

// Tests SyncedCron.remove in the process
Tinytest.addAsync('SyncedCron.stop works', async(test) => {
	SyncedCron.config({
		ravenDbCertificate: ravenDb_certificate,
		ravenDbServers: ravenDb_servers,
		ravenDbDatabase: ravenDb_database,
	});
	await SyncedCron._reset();
	const ravenSession = RAVEN_STORE.openSession();
	test.equal(await ravenSession.query({ collection: "cronHistory" }).count(), 0);

	// addd 2 entries
	SyncedCron.add(TestEntry);

	var entry2 = _.extend({}, TestEntry, {
		name: 'Test Job2',
		schedule: function(parser) {
			return parser.cron('30 11 * * ? *');
		}
	});
	SyncedCron.add(entry2);

	SyncedCron.start();

	test.equal(_.keys(SyncedCron._entries).length, 2);

	SyncedCron.stop();

	test.equal(_.keys(SyncedCron._entries).length, 0);
});

Tinytest.addAsync('SyncedCron.pause works', async(test) => {
	SyncedCron.config({
		ravenDbCertificate: ravenDb_certificate,
		ravenDbServers: ravenDb_servers,
		ravenDbDatabase: ravenDb_database,
	});
	await SyncedCron._reset();
	const ravenSession = RAVEN_STORE.openSession();
	test.equal(await ravenSession.query({ collection: "cronHistory" }).count(), 0);

	// addd 2 entries
	SyncedCron.add(TestEntry);

	var entry2 = _.extend({}, TestEntry, {
		name: 'Test Job2',
		schedule: function(parser) {
			return parser.cron('30 11 * * ? *');
		}
	});
	SyncedCron.add(entry2);

	SyncedCron.start();

	test.equal(_.keys(SyncedCron._entries).length, 2);

	SyncedCron.pause();

	test.equal(_.keys(SyncedCron._entries).length, 2);
	test.isFalse(SyncedCron.running);

	SyncedCron.start();

	test.equal(_.keys(SyncedCron._entries).length, 2);
	test.isTrue(SyncedCron.running);

});

// Tests SyncedCron.remove in the process
Tinytest.addAsync('SyncedCron.add starts by it self when running', async(test) => {
	SyncedCron.config({
		ravenDbCertificate: ravenDb_certificate,
		ravenDbServers: ravenDb_servers,
		ravenDbDatabase: ravenDb_database,
	});
	await SyncedCron._reset();

	const ravenSession = RAVEN_STORE.openSession();
	test.equal(await ravenSession.query({ collection: "cronHistory" }).count(), 0);
	test.equal(SyncedCron.running, false);
	Log._intercept(2);

	SyncedCron.start();

	test.equal(SyncedCron.running, true);

	// addd 1 entries
	SyncedCron.add(TestEntry);

	test.equal(_.keys(SyncedCron._entries).length, 1);

	SyncedCron.stop();

	var intercepted = Log._intercepted();
	test.equal(intercepted.length, 2);

	test.equal(SyncedCron.running, false);
	test.equal(_.keys(SyncedCron._entries).length, 0);
});

Tinytest.addAsync('SyncedCron.config can customize the options object', async(test) => {

	SyncedCron.config({
		ravenDbCertificate: ravenDb_certificate,
		ravenDbServers: ravenDb_servers,
		ravenDbDatabase: ravenDb_database,
		log: false,
		collectionName: 'foo',
		utc: true,
		collectionTTL: 0
	});

	await SyncedCron._reset();


	test.equal(SyncedCron.options.log, false);
	test.equal(SyncedCron.options.collectionName, 'foo');
	test.equal(SyncedCron.options.utc, true);
	test.equal(SyncedCron.options.collectionTTL, 0);
});

Tinytest.addAsync('SyncedCron can log to injected logger', async(test, done) => {
	SyncedCron.config({
		ravenDbCertificate: ravenDb_certificate,
		ravenDbServers: ravenDb_servers,
		ravenDbDatabase: ravenDb_database,
		log: false,
		collectionName: 'foo',
		utc: true,
		collectionTTL: 0
	});

	await SyncedCron._reset();

	var logger = function() {
		test.isTrue(true);

		SyncedCron.stop();
		done();
	};

	SyncedCron.options.logger = logger;

	SyncedCron.add(TestEntry);
	SyncedCron.start();

	SyncedCron.options.logger = null;
});

Tinytest.addAsync('SyncedCron should pass correct arguments to logger', async(test, done) => {
	SyncedCron.config({
		ravenDbCertificate: ravenDb_certificate,
		ravenDbServers: ravenDb_servers,
		ravenDbDatabase: ravenDb_database,
		log: false,
		collectionName: 'foo',
		utc: true,
		collectionTTL: 0
	});

	await SyncedCron._reset();

	var logger = function(opts) {
		test.include(opts, 'level');
		test.include(opts, 'message');
		test.include(opts, 'tag');
		test.equal(opts.tag, 'SyncedCron');

		SyncedCron.stop();
		done();
	};

	SyncedCron.options.logger = logger;

	SyncedCron.add(TestEntry);
	SyncedCron.start();

	SyncedCron.options.logger = null;

});

Tinytest.addAsync('Single time schedules don\'t break', async(test) => {
	// create a once off date 1 sec in the future
	var date = new Date(new Date().valueOf() + 1 * 1000);
	var schedule = Later.parse.recur().on(date).fullDate();

	// this would throw without our patch for #41
	SyncedCron._laterSetTimeout(_.identity, schedule);
});


Tinytest.addAsync('Do not persist when flag is set to false', async(test) =>{
	SyncedCron.config({
		ravenDbCertificate: ravenDb_certificate,
		ravenDbServers: ravenDb_servers,
		ravenDbDatabase: ravenDb_database,
		log: false,
		collectionName: 'foo',
		utc: true,
		collectionTTL: 0
	});

	await SyncedCron._reset();

	var testEntryNoPersist = _.extend({}, TestEntry, {persist: false});

	SyncedCron.add(testEntryNoPersist);

	const now = new Date();
	await SyncedCron._entryWrapper(testEntryNoPersist)(now);
	const ravenSession = RAVEN_STORE.openSession();
	test.equal(await ravenSession.query({ collection: "foo" }).count(), 0);
});