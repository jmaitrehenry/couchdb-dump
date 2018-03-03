const async = require('async');
const couchreq = require('request');
const fs = require('fs');
const requestPromise = require('request-promise');

const pkg = require('../package.json');
const util = require('./utils');

exports.couchdbload = async function(){
  const conf = util.buildConf();

  if(conf.version) {
    console.log('v' + pkg.version);
    return;
  }

  if(conf.help || !conf.couchdb.host || !conf.couchdb.port || (!conf.database && !conf.allDatabases)) {
    console.log("usage: cdbload [-u username] [-p password] [-h host] [-P port] [-r protocol] [--verbose] -d database");
    return;
  }

  if(conf.database) {
    if(conf.createDatabase) {
      if(conf.verbose) {
        console.debug('Create DB: ' + conf.database);
      }
      await createDatabase(conf, conf.database);
    }
    return loadStdin(conf);
  }

  if(!conf.directory) {
    console.error("Missing --directory option");
    return;
  }

  stats = fs.lstatSync(conf.directory);
  if(!stats.isDirectory()) {
    console.error(conf.directory + ' is not a directory');
    return;
  }

  files = fs.readdirSync(conf.directory);

  if(files.length === 0) {
    console.error('Directory is empty');
    return;
  }

  const q = async.queue(function(task, cb) {
    loadWithFile(task.conf, task.file, cb);
  }, conf.concurrency);

  files.map((file) => {
    if(fs.lstatSync(conf.directory + '/' + file).isFile()) {
      q.push({conf: conf, file: conf.directory + '/' + file});
    }
  });
};

async function loadWithFile(conf, file, cb) {
  if(conf.verbose) {
    console.debug('Uploading file ' + file);
  }

  const database = databaseFromFileName(file);
  console.log('db: '+ database);

  if(conf.createDatabase) {
    await createDatabase(conf, database);
  }

  data = fs.readFileSync(file);
  await requestPromise.post({
    url: conf.couchdb.url + database + "/_bulk_docs",
    headers: {
      "Accept": "application/json",
      "Content-type": "application/json"
    },
    body: data
  }).then(() => {
    if(conf.verbose) {
      console.debug('Upload done!')
    }
    cb();
  })
  .catch((err) => {
    console.error('Could not upload dump ' + file);
    console.error('CouchDB error: ' + err.statusCode + ' - ' + err.error);

    if(conf.verbose) {
      console.debug(err);
    }
    cb();
  })
}

async function createDatabase(conf, database) {
  if(conf.verbose) {
    console.debug('PUT ' + conf.couchdb.url + database);
  }

  await requestPromise
    .put(conf.couchdb.url + database, {resolveWithFullResponse: true})
    .then((resp) => {
      if(resp.statusCode !== 201) {
        console.error(resp.statusCode + ' is not allowed here!');
      }
    }).catch((err) => {
      if(err.statusCode !== 412) {
        console.error('Could not create database ' + database);
        console.error('CouchDB error: ' + err.statusCode + ' - ' + err.error);

        if(conf.verbose) {
          console.debug(err);
        }
      }
    })
}

function databaseFromFileName(file) {
  const regex = /([0-9a-z_-]+)(.[0-9]+)?.json$/i;

  res = regex.exec(file);
  if(!res[1]) {
    console.debug('Could not extract the database name from ' + file)
    return null;
  }
  return res[1];
}

function loadStdin(conf) {
  if(conf.verbose) {
    console.debug('CouchDB URL: ' + conf.couchdb.url + conf.database + "/_bulk_docs",);
  }

  process.stdin.pipe(couchreq({
    method: "POST",
    url: conf.couchdb.url + conf.database + "/_bulk_docs",
    headers: {
      "Accept": "application/json",
      "Content-type": "application/json"
    }
  }, function(err, res, body){
    if(err){
      return console.error(err);
    }

    if(conf.verbose) {
      console.log('CouchDB status code: ' + res.statusCode);
      console.log('CouchDB response: ' + res.statusMessage);
    }

    if(res.statusCode === 400) {
      console.error('CouchDB error: ' + res.body);
    }
  }));
}

