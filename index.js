"use strict";
let pg = require('pg');
let asyncP = require('dv-async-promise');

class PgQueryClient {
    constructor (client, done) {
        this.pg = client;
        this.done = done;
    }
}

class PgQuery {
    constructor (config) {
        this.dsn = config.dsn ? config.dsn : '';
        this.pool = new pg.Pool({
            connectionString: this.dsn,
            max: config['maxPoolSize'] ? config['maxPoolSize'] : 1,
            min: config['minPoolSize'] ? config['minPoolSize'] : 0,
        });
        this.fixedClient = null;
    }
    static error (message, extra) {
        let error = new Error(message);
        error.extra = extra;
        return error;
    }
    setFixedClient (_client) { this.fixedClient = _client; }
    getClient () {
        if (this.fixedClient) {
            return Promise.resolve(new PgQueryClient(this.fixedClient.pg, () => { }));
        }
        return new Promise((resolve, reject) => {
            this.pool.connect((err, connect, done) => {
                if (err) {
                    reject(PgQuery.error("Could not connect to database", { err: err }));
                } else {
                    resolve(new PgQueryClient(connect, done));
                }
            });
        });
    }
    getPersonalConnection () {
        return new Promise((resolve, reject) => {
            let client = new pg.Client(this.dsn);
            client.connect((err) => {
                if (err) {
                    reject(PgQuery.error("Could not connect to database", { err: err }));
                } else {
                    resolve(new PgQueryClient(client, () => client.end()));
                }
            });
        });
    }
    _internalQuery (sql, params, isMultiple) {
        return this.getClient().then((client) => {
            return new Promise((resolve, reject) => {
                client.pg.query(sql, params)
                    .then((res) => {
                        client.done();
                        resolve(isMultiple ? (res.rows ? res.rows : [ ]) : (res.rows[0] ? res.rows[0] : null));
                    })
                    .catch((err) => {
                        client.done();
                        reject(PgQuery.error("Query error", { query: sql, params: params, err: err }));
                    });
            });
        });
    }
    _internalQueryIterator (sql, params, iterator) {
        return this.getClient().then((client) => {
            return new Promise((resolve, reject) => {
                let query = client.pg.query(sql, params);
                query.on('row', (row) => { iterator(row); });
                query.on('end', (result) => { client.done(); resolve(result); });
                query.on('error', (err) => {
                    client.done();
                    reject(PgQuery.error("Query error", { query: sql, params: params, err: err }));
                });
            });
        });
    }
    listen (eventName) {
        return this.getPersonalConnection().then((client) => new Promise((resolve) => {
            client.pg.on('notification', function(msg) {
                if (msg.channel === eventName) {
                    resolve();
                    client.done();
                }
            });
            client.pg.query('LISTEN ' + eventName);
        }));
    }
    transaction (sequence) {
        let transaction = new PgQuery({ dsn: this.dsn });
        return this.getClient().then((client) => transaction.setFixedClient(client))
            .then(() => transaction.execute('BEGIN'))
            .then(() => asyncP.waterfall(sequence, (func) => func.call(this, transaction)))
            .then(() => transaction.execute('COMMIT'))
            .then(() => transaction.fixedClient.done())
            .catch((error) =>
                transaction.execute('ROLLBACK')
                    .then(() => transaction.fixedClient.done())
                    .then(() => Promise.reject(error))
            );
    }
    fetchOne (sql, params) {
        return this._internalQuery(sql, params, false);
    }
    fetchAll (sql, params) {
        return this._internalQuery(sql, params, true);
    }
    execute (sql, params) {
        return this._internalQuery(sql, params, null);
    }
    eachRow (sql, params, iterator) {
        return this._internalQueryIterator(sql, params, iterator);
    }
    insert (table, vars, returning) {
        let k = 1;
        let columns = [ ];
        let values = [ ];
        let params = [ ];
        for (let key in vars) {
            if (!vars.hasOwnProperty(key)) continue;
            columns.push('"' + key + '"');
            values.push('$' + k);
            params.push(vars[key]);
            k++;
        }
        let sql = "insert into " + table  + " (" + columns.join(', ') + ') values (' + values.join(', ') + ')' + (returning ? (' returning ' + returning) : '');
        if (returning) {
            return this.fetchOne(sql, params).then((row) => row[returning]);
        } else {
            return this.execute(sql, params);
        }
    }
    update (table, vars, cond) {
        let updates = [ ];
        let conditions = [ ];
        let params = [ ];
        let k = 1;
        for (let key in vars) {
            if (!vars.hasOwnProperty(key)) continue;
            updates.push('"' + key + '" = $' + k);
            params.push(vars[key]);
            k++;
        }
        for (let key in cond) {
            if (!cond.hasOwnProperty(key)) continue;
            conditions.push('"' + key + '" = $' + k);
            params.push(cond[key]);
            k++;
        }
        let sql = "UPDATE " + table  + " SET " + updates.join(', ') + ' WHERE (' + conditions.join(') and (') + ')';
        return this.execute(sql, params);
    }
    upsert (table, vars, cond) {
        let k = 1;
        let columns = [ ];
        let values = [ ];
        let params = [ ];
        let updates = [ ];
        let onDuplicateKey = [ ];
        for (let key in cond) {
            if (!cond.hasOwnProperty(key)) continue;
            columns.push('"' + key + '"');
            values.push('$' + k);
            params.push(cond[key]);
            onDuplicateKey.push('"' + key + '"');
            k++;
        }
        for (let key in vars) {
            if (!vars.hasOwnProperty(key)) continue;
            columns.push('"' + key + '"');
            values.push('$' + k);
            params.push(vars[key]);
            k++;
        }
        for (let key in vars) {
            if (!vars.hasOwnProperty(key)) continue;
            updates.push('"' + key + '" = $' + k);
            params.push(vars[key]);
            k++;
        }
        let sql = "insert into " + table  + " (" + columns.join(', ') + ') values (' + values.join(', ') + ')';
        sql += ' on conflict (' + onDuplicateKey + ') do ';
        sql += updates === { } ? 'nothing' : 'update set ' + updates.join(', ');
        return this.execute(sql, params);
    }
    remove (table, cond) {
        let conditions = [ ];
        let params = [ ];
        let k = 1;
        for (let key in cond) {
            if (!cond.hasOwnProperty(key)) continue;
            conditions.push('"' + key + '" = $' + k);
            params.push(cond[key]);
            k++;
        }
        let where = conditions.length > 0 ? ' WHERE (' + conditions.join(') and (') + ')' : '';
        let sql = 'DELETE FROM ' + table + where;
        return this.execute(sql, params);
    }
}

module.exports = PgQuery;