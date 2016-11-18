var BigQuery = require('google-cloud/node_modules/@google-cloud/bigquery');

const projectData = require('./auth.json');

const config = {
  projectId: projectData['project_id'],
  keyFilename: './auth.json'
};

const ds = 'atomic';

var DataBase = function() {
  this.conn = BigQuery(config);
};

DataBase.prototype.insertInto = function(table, rows, options) {
  var table = this.conn.dataset(ds).table(table);

  table.insert(rows, (options || {}), function (err, insertErrors, apiResponse) {
    if (err) {
      return console.log('Error while inserting data: %s', err);
    }
    console.log('Response of insert into %s, %s', table['id'], JSON.stringify(apiResponse, null, 2));
    console.log('Inserted %d row(s)', rows.length);
  });
}

DataBase.prototype.tables = function(callback) {
  var dataset = this.conn.dataset(ds);
  dataset.getTables(function (err, tables) {
    if (err) {
      return callback(err);
    }

    return callback(tables.map((d) => {
      return d['metadata']['id']
                .replace(config.projectId + ':', '')
                .replace(ds + '.', '');
    }));
  });
};

DataBase.toTableName = (schemaName) => {
  const schemaInfo = schemaName.replace('iglu:', '').split('/');
  return [
    schemaInfo[0],
    schemaInfo[1],
    schemaInfo[3].split('-')[0]
  ].join('_').replace(/(\.)/gi, '_').toUnderscore();
};

module.exports = DataBase;
