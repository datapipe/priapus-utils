exports.addTimestampSupport = function (converter) {
    const _input = converter.input
    converter.input = function convertInput(data, options) {
        if (data instanceof Date) {
            return {S: data.toISOString()};
        } else {
            return _input(data, options)
        }
    }

    const _output = converter.output

    converter.output = function convertOutput(data, options) {
        for (var type in data) {
            var values = data[type];
            if (type === 'S' && /\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d\.\d+([+-][0-2]\d:[0-5]\d|Z)/.test(values)) {
                return Date.parse(values)
            }
        }
        return _output(data, options)
    }
}

exports.putResource = function (dynamodb, tableName, resourceId, properties, callback) {
    const params = {
        Item: {
            "urn": resourceId,
            "properties": properties,
        },
        ReturnConsumedCapacity: "TOTAL",
        ReturnItemCollectionMetrics: "SIZE",
        TableName: tableName
    };

    return dynamodb.put(params, callback)
}

exports.updateResource = function (dynamodb, tableName, resourceId, properties, changedAt, callback) {
    const params = {
        Key: {
            "urn": resourceId
        },
        UpdateExpression: "SET changed_at = :changed_at, properties = :properties, change_source = :change_source",
        // ReturnConsumedCapacity: "TOTAL",
        // ReturnItemCollectionMetrics: "SIZE",
        ConditionExpression: "attribute_not_exists(urn) OR (changed_at < :changed_at AND properties <> :properties)",
        ExpressionAttributeValues: {
            ":changed_at": changedAt,
            ":properties": properties,
            ":change_source": 'collector'
        },
        TableName: tableName
    };
    return dynamodb.update(params, callback)
}