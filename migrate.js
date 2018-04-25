const _ = require('lodash')

const indexQuery = 'select indexdef from pg_indexes where tablename = $1';

const getIndices = (pgm, tableName) => {
    return pgm.db.query(indexQuery, tableName).then(result => {
        return result.rows.map(row => {
            const match = /CREATE\s*(UNIQUE)?\s*INDEX (?:\S+?) ON (?:\S+?) USING (\S+) \((\S+)\s*(\S*?)\)/.exec(row.indexdef)
            return {
                unique: match[1] === 'UNIQUE',
                method: match[2],
                column: match[3],
                operatorClass: match[4]
            }
        })
    })
};

const getHistoryTableName = tableName => {
    return `${tableName}_history`
};

function createChildTable(pgm, tableName, baseTableName, fields = {}) {
    const historyTableName = getHistoryTableName(tableName)
    pgm.createTable(tableName, _.merge({
        id: 'id'
    }, fields), {
        inherits: baseTableName
    });

    return getIndices(pgm, [baseTableName]).then(index => {
        index.forEach(index => {
            if (index.column !== 'id') {
                pgm.createIndex(tableName, index.column, {method: index.method, operatorClass: index.operatorClass, unique: index.unique})
            }
        });

        pgm.createTable(historyTableName, {
            history_id: 'id'
        }, {
            like: tableName
        });
        pgm.createTrigger(tableName, 'versioning_trigger', {
            when: 'BEFORE',
            operation: ['INSERT', 'UPDATE', 'DELETE'],
            level: 'row',
            function: "versioning",
            functionArgs: ['sys_period', historyTableName, true]
        });
    });
}

function dropChildTable(pgm, tableName) {
    pgm.dropTrigger(tableName, 'versioning_trigger', {ifExists: true});
    pgm.dropTable(getHistoryTableName(tableName), {ifExists: true});
    pgm.dropTable(tableName, {ifExists: true});
}

module.exports = {createChildTable, dropChildTable}