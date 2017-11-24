var DataBase = require('./db.js');
var Formatter = require('./formatter.js');

require('./extend.js');

var db = new DataBase();

exports.handler = (event, context, callback) => {
  var events = [],
      nestedData = {};

  event.Records.forEach((record) => {
    const payload = new Buffer(record.kinesis.data, 'base64').toString();
    const event = Formatter.event(payload),
          contexts = Formatter.contexts(event);

    Object.keys(contexts).forEach((c) => {
      var key = DataBase.toTableName(c);

      if(nestedData[key] === undefined) {
        nestedData[key] = [];
      }

      nestedData[key].push(contexts[c]);
    });

    events.push(event);
  });

  db.insertInto('events', events, { raw: true });

  db.tables(function(tables) {
    Object.keys(nestedData).forEach((table) => {
      var tableName = DataBase.tableName(table);
      if(tables.indexOf(table) === -1) {
        console.log('Table %s not found into database', tableName);
        return;
      }

      db.insertInto(tableName, nestedData[tableName]);
    });
  });

  callback(null, `Successfully processed ${event.Records.length} records.`);
};
