'use strict';

const Knex = require('knex');
const _ = require('lodash')
const AWS = require('aws-sdk')
const process = require('process')

const host_param = '/zamboni/development/rds_database_instance_address'
const port_param = '/zamboni/development/rds_database_port'
const database_param = '/zamboni/development/rds_database_name'
const user_param = '/zamboni/development/rds_database_username'
const password_param = '/zamboni/development/rds_database_password'

function connect() {
    const ssm = new AWS.SSM({region: process.env.AWS_REGION || 'us-east-1'});
    return ssm.getParameters({
        Names: [host_param, port_param, database_param, user_param, password_param],
        WithDecryption: true
    }).promise().then((data) => {
        const getParam = (name) => {
            return _.find(data.Parameters, {Name: name}).Value
        };
        const connection = {
            host: process.env.rds_database_instance_address || getParam(host_param),
            port: process.env.rds_database_port || getParam(port_param),
            database: process.env.rds_database_name || getParam(database_param),
            user: process.env.rds_database_username || getParam(user_param),
            password: process.env.rds_database_password || getParam(password_param)
        }
        return Knex({
            client: 'pg',
            acquireConnectionTimeout: 5000,
            connection: connection
        });
    })
}

function destroyResource(db, trx, tn) {
    const tableName = tn;
    return (urn) => {
        console.log(`Destroying resource: ${urn.raw}`);
        return db(tableName).transacting(trx).where({urn: urn}).del()
    }
}


function upsertResource(db, trx, tn) {
    const tableName = tn;
    return (urn, image, fields) => {
        console.log(`Inserting resource: ${urn.raw}`);
        const now = new Date().toISOString();
        return db(tableName).transacting(trx).where({urn: urn.raw}).then((rows) => {
            if (rows.length > 0) {
                console.log(`Updating resource: ${urn.raw}`);
                return db(tableName).transacting(trx).where({urn: urn.raw}).update(_.merge({
                    body: image.properties,
                    updated_at: now
                }, fields))
            } else {
                console.log(`Inserting resource: ${urn.raw}`);
                return db(tableName).transacting(trx).insert(_.merge({
                    urn: urn.raw,
                    provider_account_id: urn.providerAccountId,
                    service: urn.service,
                    resource_type: urn.resourceType,
                    region: urn.region,
                    change_source: 'collector',
                    resource_id: urn.resourceId,
                    body: image.properties,
                    created_at: now,
                    updated_at: now
                }, fields))
            }
        })
    }
}

module.exports = {connect, upsertResource, destroyResource}
