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

  db.table('events', function(err, table) {
    db.insertInto(table, events, { raw: true });
  });

  Object.keys(nestedData).forEach((tableName) => {
    db.table(tableName, function(err, table){
      if(err){
        console.log(err);
        return;
      }

      // Partition search should be made by the method insertInto
      if(!DataBase.isPartitioned(table)){
        db.insertInto(table, nestedData[tableName]);
      } else {
        nestedData[tableName].forEach((row) => {
          var partition = db.partition(table, row);

          db.table([tableName, partition].join('$'), function(err, table) {
            if(err) {
              console.log(err);
              return;
            }

            db.insertInto(table, row);
          });
        });

      }
    });
  });

  callback(null, `Successfully processed ${event.Records.length} records.`);
};
