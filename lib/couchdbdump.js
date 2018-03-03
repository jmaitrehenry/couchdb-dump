const fs = require('fs');
const queue = require('async/queue');
const requestPromise = require('request-promise');
const async = require('async');
const mkdirp = require('mkdirp');

const pkg = require('../package.json');
const util = require('./utils');

const writeQueue = async.queue(function(task, cb) {
  writeToFile(task.data, task.directory, task.database, task.maxDocuments, cb);
}, 1);

exports.couchdbdump = function() {
  const conf = util.buildConf();

  if(conf.version) {
    console.log('v' + pkg.version);
    return;
  }

  if(conf.help || !conf.couchdb.host || !conf.couchdb.port || (!conf.database && !conf.allDatabases)) {
    console.error("usage: cdbdump [-u username] [-p password] [-h host] [-P port] [-r protocol] [-s json-stringify-space] [-k dont-strip-revs] [-D design doc] [-v view] [-d database|-a all-databases] [--concurency] [-o output-directory]");
    return;
  }

  let output = process.stdout;
  if(conf.directory) {
    mkdirp.sync(conf.directory);
  }

  if(conf.allDatabases) {
    const q = async.queue(function(task, cb) {
      dump(task.conf, task.db, false, 0, () => {
        writeQueue.push({
          data: ']}\n',
          directory:  task.conf.directory,
          database: task.db,
          maxDocuments: task.conf.maxDocPerPage
        });
        cb();
      });
    }, conf.concurrency);

    requestPromise(conf.couchdb.url + '_all_dbs')
    .then(function(data) {
      dbs = JSON.parse(data);
      dbs.map((db)  => {
        if(conf.directory) {
          mkdirp.sync(conf.directory);
          output = fs.createWriteStream(conf.directory + '/' + db + '.json');
        }
        q.push({db: db, conf: conf, output: output});
      });
    })
    .catch(function(err) {
      console.error(err);
    });
    return;
  }

  dump(conf, conf.database, true, 0, () => {
    process.stdin.write(']}\n')
  });
};

function dump(conf, database, stdin, offset = 0, cb = () => {}) {
  if(conf.verbose) {
    console.debug('Dumping ' + database + ' offset: ' + offset);
  }

  const request = buildRequest(conf, database, offset);
  requestPromise
    .get(request)
    .then(function(data) {
      let docs = JSON.parse(data);

      if(docs.rows.length > 0) {
        
        docs.rows = docs.rows.map((doc) => {
          // if(doc.id.substring(0, 8) === '_design/') {
            delete doc.doc['_rev'];
          // }
          return doc.doc;
        });

        if(stdin) {
          process.stdout.write(docs.rows.join(','));
        } else {
          writeQueue.push({
            data: docs.rows,
            directory:  conf.directory,
            database: database,
            maxDocuments: conf.maxDocPerPage
          });
        }
        return dump(conf, database, stdin, (offset + conf.numberPerPage), cb);
      }
      cb();
    })
    .catch(function(err) {
      console.error(err.statusCode + ' - ' + err.error);
      cb()
    });
}

let writeToFileMetadata = {
  db: []
}

function writeToFile(data, directory, db, maxDocuments, cb) {
  if(!writeToFileMetadata.db[db]) {
    writeToFileMetadata.db[db] = {
      output: fs.createWriteStream(directory + '/' + db + '.json'),
      fileNumber: 0,
      docToWrite: []
    }
  }

  if(writeToFileMetadata.db[db].fileNumber === 0 && writeToFileMetadata.db[db].docToWrite.length === 0)  {
    writeToFileMetadata.db[db].output.write('{"docs": [ ');
  }

  if(data === ']}\n') {
    writeToFileMetadata.db[db].output.end(writeToFileMetadata.db[db].docToWrite.join(',') + ']}\n');
    cb();
    return;
  } 

  data.map((doc) => {
    writeToFileMetadata.db[db].docToWrite.push(JSON.stringify(doc));

    if(writeToFileMetadata.db[db].docToWrite.length > maxDocuments) {
      writeToFileMetadata.db[db].output.end(writeToFileMetadata.db[db].docToWrite.join(',') + ']}\n');
      writeToFileMetadata.db[db].fileNumber++;
      writeToFileMetadata.db[db].docCounter = 0;
      writeToFileMetadata.db[db].docToWrite = [];

      const file = directory + '/' + db + '.' + writeToFileMetadata.db[db].fileNumber + '.json'
      writeToFileMetadata.db[db].output = fs.createWriteStream(file);
      writeToFileMetadata.db[db].output.write('{"docs": [ ');
    }
  });
  cb();
}

function buildRequest(conf, database, offset) {
  let request = {
    qs: {
      include_docs: "true",
      attachments: "true",
      // startkey: '"_design"',
      // endkey:'"_design0"',
      limit: conf.numberPerPage,
      skip: offset
    },
    headers: {
      "Accept": "application/json"
    }
  }


  request.url = conf.couchdb.url + database;
  if(conf.designDoc && conf.views) {
    request.url += '/_design/' + conf.designDoc + '/_view/' + conf.views;
  } else {
    request.url += '/_all_docs';
  }

  return request;
}
