const parseargs = require('minimist');

exports.buildConf = function() {
  let conf = {
    couchdb: {},
    directory: null,
    allDatabases: false,
    createDatabase: false,
    database: "",
    version: false,
    jsonStringifySpace: false,
    stripRevs: true,
    designDoc: false,
    views: false,
    help: false,
    numberPerPage: 500,
    concurrency: 1,
    verbose: false,
    maxDocPerPage: 1000,
  }

  const args = parseargs(process.argv.slice(2));
  conf.couchdb = buildCouchDBConfig(args);

  if(args.version) {
    conf.version = true;
    return conf;
  }

  if(args.help) {
    conf.help = true;
    return conf;
  }

  if(args.verbose) {
    conf.verbose = true;
  }

  if(args.concurrency) {
    conf.concurrency = args.concurrency;
  }

  if(args.numberPerPage) {
    conf.numberPerPage = args.numberPerPage;
  }

  if(args.k) {
    conf.stripRevs = false;
  }

  if(args.D) {
    conf.designDoc = args.D;
  }

  if(args.v) {
    conf.views = args.v;
  }

  if(args.a || args.all) {
    conf.allDatabases = true;
  }

  if(args.d) {
    conf.database = args.d;
  }

  if(args["create-database"]) {
    conf.createDatabase = true;
  }

  if(args["max-doc-per-page"]) {
    conf.maxDocPerPage = args["max-doc-per-page"];
  }

  if(args.o || args.directory) {
    conf.directory = args.directory || args.o;
  }

  return conf;
}

function buildCouchDBConfig(args) {
  let conf = {
    url: "",
    port: 5984,
    host: "localhost",
    protocol: "http",
    auth: false
  };

  if(args.h) {
      conf.host = args.h;
  }

  if(args.P) {
      conf.port = args.P;
  }

  if(args.r) {
      conf.protocol = args.r;
  }

  conf.auth = buildAuth(args.u, args.p);
  conf.url = buildCouchDBURL(conf);

  return conf;
}

function buildAuth(user, pass) {
  let auth = "";
  if(user) {
    auth = user + ":";
  }

  if(pass) {
    auth += pass;
  }

  if(auth === "") {
    return false;
  }

  return auth;
}

function buildCouchDBURL(conf) {
  let couchDBUrl = conf.protocol + '://';

  if(conf.auth) {
    couchDBUrl += conf.auth + "@";
  }

  couchDBUrl += conf.host + ':' + conf.port + '/';

  return couchDBUrl;
}

