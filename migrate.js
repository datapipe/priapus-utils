const indexQuery = `
select
    a.attname as column_name
from
    pg_class t,
    pg_class i,
    pg_index ix,
    pg_attribute a
where
    t.oid = ix.indrelid
    and i.oid = ix.indexrelid
    and a.attrelid = t.oid
    and a.attnum = ANY(ix.indkey)
    and t.relkind = 'r'
    and t.relname = $1
order by
    t.relname,
    i.relname;`;

const getIndexedColumns = (pgm, tableName) => {
    return pgm.db.query(indexQuery, tableName).then(result => {
        return result.rows.map(r => r.column_name)
    })
};

const getHistoryTableName = tableName => {
    return `${tableName}_history`
};

async function createChildTable(pgm, tableName, baseTableName) {
    const historyTableName = getHistoryTableName(tableName)
    pgm.createTable(tableName, {
        id: 'id'
    }, {
        inherits: baseTableName
    });

    return getIndexedColumns(pgm, [baseTableName]).then(columns => {
        columns.forEach(column => {
            pgm.createIndex(tableName, column)
        });

        pgm.createTable(historyTableName, {}, {
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