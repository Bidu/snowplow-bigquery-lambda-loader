var DataBase = require('./db.js');
var Parser = require('./parser.js');

var db = new DataBase();

exports.handler = (event, context, callback) => {
  var events = [],
    nestedData = {};

  event.Records.forEach((record) => {
    const payload = new Buffer(record.kinesis.data, 'base64').toString();
    const event = Parser.event(payload),
      contexts = Parser.contexts(event),

      Parser.nestedEvents(contexts, nestedData);

    events.push(event);
  });

  db.insertInto('events', events, { raw: true });

  Object.keys(nestedData).forEach((tableName) => {
    db.insertInto(tableName, nestedData[tableName]);
  });

  callback(null, `Successfully processed ${event.Records.length} records.`);
};
