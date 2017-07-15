'use strict';
// INSPIRED BY BHIMA 1.X
// https://github.com/IMA-WorldHealth/bhima-1.X
const q = require('q');
const mysql = require('mysql');

class Query {

  constructor(db) {
    this.db = db;
  }

  exec(sql, params) {
    const deferred = q.defer();

    this.db.pool.getConnection((error, connection) => {
      if (error) { return deferred.reject(error); }

      // format the SQL statement using MySQL's escapes
      const statement = mysql.format(sql.trim(), params);

      connection.query(statement, (err, rows) => {
        connection.release();
        return (err) ? deferred.reject(err) : deferred.resolve(rows);
      });

    });

    return deferred.promise;
  }
}

module.exports = Query;