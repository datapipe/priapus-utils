exports.build = function (provider, providerAccountId, resourceType, region, resourceId) {
    return `urn:rax:${provider}:${providerAccountId}:${resourceType}:${region}:${resourceId}`
}

exports.parse = function (urn) {
    const parts = urn.split(':')
    return {
        raw: urn,
        namespace: parts[1],
        provider: parts[2],
        providerAccountId: parts[3],
        service: parts[4],
        resourceType: parts[5],
        region: parts[6],
        resourceId: parts[7]
    }
}
