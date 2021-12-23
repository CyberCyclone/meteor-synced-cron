Package.describe({
  summary: "Allows you to define and run scheduled jobs across multiple servers.",
  version: "1.0.0",
  name: "cybercyclone:synced-cron-ravendb",
  git: "https://github.com/CyberCyclone/meteor-synced-cron-ravendb"
});

Npm.depends({later: "1.1.6", ravendb: "5.0.3"});

Package.onUse(function (api) {
  api.versionsFrom('METEOR@1.3');
  api.use(['underscore', 'check', 'logging', 'ecmascript'], 'server');
  api.addFiles(['synced-cron-server.js'], "server");
  api.export('SyncedCron', 'server');
});

Package.onTest(function (api) {
  api.use(['check'], 'server');
  api.use(['tinytest', 'underscore', 'logging', 'ecmascript']);
  api.addFiles(['synced-cron-server.js', 'synced-cron-tests.js'], ['server']);
  api.addAssets('private/meteorjs-testnet.pfx', 'server');

});
