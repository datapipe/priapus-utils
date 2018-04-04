['dynamo', 'db', 'urn'].forEach(function (i) {
    exports[i] = require(`./${i}`)
})




