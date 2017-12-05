'use strict';
var BigQueryClient = require('google-cloud/node_modules/@google-cloud/bigquery');
var extend = require('util')._extend;

const projectData = require('./auth.json');

const config = {
  projectId: projectData['project_id'],
  keyFilename: './auth.json'
};

const ds = 'atomic';

const PARTITION_FIELDS = process.env['PARTITION_FIELDS'].split(',');

var BigQuery = function() {
  this.conn = BigQueryClient(config);
};

BigQuery.prototype.insertInto = function(tableName, rows, options) {
  //Here I should get the table based on name
  // and insert in the right partition whenever the table is partitioned
  this.table(tableName, (err, table) => {
    // First, we get the table object with all available information
    if (err) {
      console.log(`Sorry! We've got an error while fetching the table ${tableName}.\nERROR: ${err}`);
      return;
    }
    var isPartitioned = BigQuery.isPartitioned(table);
    rows.forEach((row) => {
      let localOptions = extend({}, options);
      // Insert if table is not partitioned or partition column is not defined
      var tablePartition = isPartitioned ? BigQuery.partitionName(row, table) : undefined;
      if (!isPartitioned) {
        table.insert(row, localOptions, function(err, insertErrors, apiResponse) {
          if (err) {
            return console.log('Error while inserting data: %s', err);
          }
          console.log('Response of insert into %s, %s', table['id'], JSON.stringify(apiResponse, null, 2));
          console.log('Inserted 1 row.');
        });
      } else {
        // Insert into partitioned table
        this.table(tablePartition, (err, table) => {
          if (err) {
            console.log(`Sorry! We've got an error while fetching the table ${tablePartition}.\nERROR: ${err}`);
            return;
          }
          table.insert(row, localOptions, function(err, insertErrors, apiResponse) {
            if (err) {
              return console.log('Error while inserting data: %s', err);
            }
            console.log('Response of insert into %s, %s', table['id'], JSON.stringify(apiResponse, null, 2));
            console.log('Inserted 1 row.');
          });
        });

      }
    });
  });
};

BigQuery.prototype.table = function(tableName, callback) {
  var dataset = this.conn.dataset(ds);
  dataset.table(tableName).get(function(err, table, apiResponse) {
    if (err) {
      return callback(err);
    }
    return callback(null, table);
  });
};

BigQuery.partitionName = function(row, table) {
  var schema = table.metadata.schema.fields;
  var timestampFields = schema.filter((field) => {
    return field.type == 'TIMESTAMP' && field.name != 'root_tstamp';
  }).map((field) => field.name);

  var partitionField = PARTITION_FIELDS.find((field) => {
    return timestampFields.indexOf(field) > 0;
  })

  if (partitionField) {
    var partitionDate = row[partitionField].substring(0, 10).replace(/\-/g, '');
    return [BigQuery.tableName(table), partitionDate].join('$');
  }

  return BigQuery.tableName(table);
};

BigQuery.tableName = (table) => table['metadata']['tableReference']['tableId'];

BigQuery.isPartitioned = (table) => table['metadata']['timePartitioning'] !== undefined;

module.exports = BigQuery;
