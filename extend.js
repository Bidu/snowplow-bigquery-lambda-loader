String.prototype.toUnderscore = function(){
  return this.replace(/([^_])([A-Z])/g, "$1_$2").toLowerCase();
};

String.prototype.toTableName = function(){
  const schemaInfo = this.replace('iglu:', '').split('/');
  return [
    schemaInfo[0],
    schemaInfo[1],
    schemaInfo[3].split('-')[0]
  ].join('_').replace(/(\.)/gi, '_').toUnderscore();
};

Object.defineProperty(
    Object.prototype,
    'renameProperty',
    {
        writable : false, // Cannot alter this property
        enumerable : false, // Will not show up in a for-in loop.
        configurable : false, // Cannot be deleted via the delete operator
        value : function (oldName, newName) {
            // Do nothing if the names are the same
            if (oldName == newName) {
                return this;
            }
            // Check for the old property name to
            // avoid a ReferenceError in strict mode.
            if (this.hasOwnProperty(oldName)) {
                this[newName] = this[oldName];
                delete this[oldName];
            }
            return this;
        }
    }
);
