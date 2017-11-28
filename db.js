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
  //Here I should get the table based on name
  // and insert in the right partition whenever the table is partitioned
  table.insert(rows, (options || {}), function (err, insertErrors, apiResponse) {
    if (err) {
      return console.log('Error while inserting data: %s', err);
    }
    console.log('Response of insert into %s, %s', table['id'], JSON.stringify(apiResponse, null, 2));
    console.log('Inserted %d row(s)', rows.length);
  });
}

DataBase.prototype.table = function(tableName, callback) {
  var dataset = this.conn.dataset(ds);
  dataset.table(tableName).get(function (err, table, apiResponse) {
    if (err) {
      return callback(err);
    }
    return callback(null, table);
  });
};

DataBase.partition = function(row, table){
  var partitioning = table['metadata']['timePartitioning'];

  if(partitioning !== undefined && partitioning['field'] !== undefined) {
    return row[partitioning['field']].substring(0,10).replace(/\-/g, '');
  }

  return;
};

DataBase.toTableName = (schemaName) => {
  const schemaInfo = schemaName.replace('iglu:', '').split('/');
  return [
    schemaInfo[0],
    schemaInfo[1],
    schemaInfo[3].split('-')[0]
  ].join('_').replace(/(\.)/gi, '_').toUnderscore();
};

DataBase.tableName = (table) => {
  return table['metadata']['tableReference']['tableId'];
}

DataBase.isPartitioned = (table) => {
  return table['metadata']['timePartitioning'] !== undefined;
}

module.exports = DataBase;
