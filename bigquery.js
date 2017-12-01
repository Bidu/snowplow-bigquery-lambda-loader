var BigQueryClient = require('google-cloud/node_modules/@google-cloud/bigquery');

const projectData = require('./auth.json');

const config = {
  projectId: projectData['project_id'],
  keyFilename: './auth.json'
};

const ds = 'atomic';

const PARTITION_FIELDS = ['updated_at', 'created_at', 'fired_at', 'ts'];

var BigQuery = function() {
  this.conn = BigQueryClient(config);
};

BigQuery.prototype.insertInto = function(tableName, rows, options) {
  //Here I should get the table based on name
  // and insert in the right partition whenever the table is partitioned
  rows.forEach((row) => {
    // First, we get the table object with all available information
    this.table(tableName, function(err, table) {
      if (err) {
        console.log(`Sorry! We've got an error while fetching the table ${tableName}.\nERROR: ${err}`);
        return;
      }
      var isPartitioned = BigQuery.isPartitioned(table);
      var tablePartition = isPartitioned ? BigQuery.partition(row, table) : undefined;
      // Insert if table is not partitioned or partition column is not defined
      if (!isPartitioned || tablePartition === undefined) {
        table.insert(row, (options || {}), function(err, insertErrors, apiResponse) {
          if (err) {
            return console.log('Error while inserting data: %s', err);
          }
          console.log('Response of insert into %s, %s', table['id'], JSON.stringify(apiResponse, null, 2));
          console.log('Inserted %d row(s)', rows.length);
        });
      } else {
        // Insert into partitioned table
        this.table(tablePartition, function(err, table) {
          if (err) {
            console.log(`Sorry! We've got an error while fetching the table ${tablePartition}.\nERROR: ${err}`);
            return;
          }
          table.insert(row, options || {}, function(err, insertErrors, apiResponse) {
            if (err) {
              return console.log('Error while inserting data: %s', err);
            }
            console.log('Response of insert into %s, %s', table['id'], JSON.stringify(apiResponse, null, 2));
            console.log('Inserted %d row(s)', rows.length);
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

BigQuery.partition = function(row, table) {
  var schema = table.metadata.schema.fields;
  var timestampFields = schema.filter((field) => {
    return field.type == 'TIMESTAMP' && field.name != 'root_tstamp';
  }).map((field) => field.name);

  var partitionField = PARTITION_FIELDS.find((field) => {
    return timestampFields.indexOf(field) > 0;

  })

  if (partitionField) {
    var partitionDate = row[partitionField].substring(0, 10).replace(/\-/g, '');
    return [BigQuery.tableName(table), partitionDate].join('$')
  }

  return BigQuery.tableName(table);
};

BigQuery.tableName = (table) => table['metadata']['tableReference']['tableId'];

BigQuery.isPartitioned = (table) => table['metadata']['timePartitioning'] !== undefined;

module.exports = BigQuery;
