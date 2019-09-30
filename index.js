var BigQueryClient = require('./bigquery.js');
var Parser = require('./parser.js');

var bq = new BigQueryClient();

exports.handler = (event, context, callback) => {
  var events = [],
    nestedData = {};

  event.Records.forEach((record) => {
    const payload = new Buffer(record.kinesis.data, 'base64').toString();
    const event = Parser.event(payload),
      contexts = Parser.contexts(event);

      Parser.nestedEvents(contexts, nestedData);

    events.push(event);
  });

  bq.insertInto('events', events, { raw: true });

  Object.keys(nestedData).forEach((tableName) => {
    bq.insertInto(tableName, nestedData[tableName]);
  });

  callback(null, `Successfully processed ${event.Records.length} records.`);
};
