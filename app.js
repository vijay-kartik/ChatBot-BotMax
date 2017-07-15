var express = require('express');
var bodyparser = require('body-parser');
var mysql = require("mysql");
const util = require('util');
const fs = require('fs');
const Transaction = require('./transaction');
const Query = require('./query');


mysql.pool = mysql.createPool({ 
	connectionLimit: 100,
	host: "localhost",
	user: "root",
	password: "root",
	database: "chatapp",
	socketPath: '/Applications/MAMP/tmp/mysql/mysql.sock'	
});

var app = express();

// Enable CORS
app.use(function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});

// To read JSON posted data
app.use(bodyparser.json());

app.use("/bus/schedule", busSchedule);
app.use("/bus/duration", busDuration);

function busSchedule(req, res, next) {

	var sql = "SELECT * FROM bus_timings WHERE type = 'N'";
	
	var d = new Date();
    var n = d.getDay();
    if ( n == 0 || n == 6 ) {
    	sql = "SELECT * FROM bus_timings WHERE type = 'H'";
    } else if ( n == 5 ) {
    	sql = "SELECT * FROM bus_timings WHERE type = 'N' OR type = 'F'";
    }

	const query = new Query(mysql);
	query.exec(sql)
	.then(function(prows){
		res.json(prows);
	})
	.catch(function(err) {
		console.log('Query Error:' + err);
		res.send("Database error:<br>" + err);
	});
}

function busDuration(req, res, next) {
	var result = {};
	result['result'] = 'The bus ride can take anywhere from 45 mins to 1 hour 30 mins depending on traffic';
	res.json(result);
}


app.use("/refresh", refresh);
app.use("/refreshi", refreshIncremental);
app.use("/update", update);
// app.use("/schema", schema);
// app.use("/index", index);

// Send the full refresh / initial dump

function refresh(req, res, next) {
	const transaction = new Transaction(mysql);
	transaction
	.addQuery('SELECT * FROM user')
	.addQuery('SELECT * FROM interaction')
	.addQuery('SELECT * FROM contact')
	.addQuery('SELECT * FROM customer')
	.addQuery('SELECT MAX(id) AS id FROM transactions')
	.execute()
	.then(function(results) {
		var users = results[0];
		var interactions = results[1];
		var contacts = results[2];
		var customers = results[3];
		var transaction_id = results[4][0].id;
		
		var json = {};
		json['users'] = users;
		json['interactions'] = interactions;
		json['contacts'] = contacts;
		json['customers'] = customers;
		json['transaction_id'] = transaction_id;

		res.json(json);
	})
	.catch(function(err) {
		console.log('Query Error:' + err);
		res.send("Database error:<br>" + err);
	});
}

// Send the incremental updates collected through the transaction log according to their last transaction id
function refreshIncremental(req, res, next) {
	console.log(req.body.id);
	var transaction_id = req.body.id;

	var updatesToSend = [];

	var updatedRows = {};
	var updatedIndices = [];
	var deletedRows = [];

	const query = new Query(mysql);
	query.exec('SELECT * FROM transactions WHERE id > ?', [transaction_id])
	.then(function(prows){
		for (var i = 0; i < prows.length; i++) {

			var type = prows[i].type;
			var row_id = prows[i].row_id;

			var isRowDeleted = false;

			for (var j = 0; j < deletedRows.length; j++) {
				if (row_id == deletedRows[j]) {
					isRowDeleted = true;
				}
			}

			if (!isRowDeleted) {

				var doesIndexExist = false;

				for (var j = 0; j < updatedIndices.length; j++) {
					if (row_id == updatedIndices[j]) {
						doesIndexExist = true;
					}
				}

				if (!doesIndexExist) {
					updatedIndices.push(row_id);
				}

				updatedRows[row_id] = type;

				if (type == 'd') {
					deletedRows.push(row_id);
				}

			}
		}

		const transaction = new Transaction(mysql);

		for (var i = 0; i < updatedIndices.length; i++) {
			transaction.addQuery('SELECT * FROM ?? WHERE id = ?', [prows[i].table_name, prows[i].row_id]);
		}
		transaction.addQuery('SELECT MAX(id) AS id FROM transactions', []);

		transaction
		.execute()
		.then(function(rows){
			for (var i = 0; i < rows.length - 1; i++){
				var e = {
					table_name: prows[i].table_name,
					type: updatedRows[updatedIndices[i]],
					element : {
						id: prows[i].row_id,
					}
				}
				if (updatedRows[updatedIndices[i]] != 'd') {
					e.element = rows[i][0];
				}
				updatesToSend.push(e);				
			}
			updatesToSend.push(rows[rows.length - 1][0].id);
			res.json(updatesToSend);
		})
		.catch(function(err){
			console.log('Query Error:' + err);
			res.send("Database error:<br>" + err);
		})	
	})
	.catch(function(err) {
		console.log('Query Error:' + err);
		res.send("Database error:<br>" + err);
	});
}

function createInsertStatement(e) {

	var keys = [];
	var values = [];

	var statement = {};
	statement["fields"] = '( ';
	statement["values"] = '( ';
	delete e["update_id"];
	delete e["type"];
	delete e["sync_status"];
	for (var field in e) {
		if (e.hasOwnProperty(field)) {
			statement["fields"] += mysql.escapeId(field) + ', ';	
			statement["values"] += mysql.escape(e[field]) + ', ';	
		}
	}

	statement["fields"] = statement["fields"].slice(0, -2);
	statement["values"] = statement["values"].slice(0, -2);

	statement["fields"] += ' )';
	statement["values"] += ' )';

	return statement;
}

function createUpdateStatement(e) {

	var keys = [];
	var values = [];

	var statement = "";

	delete e["id"];
	delete e["update_id"];
	delete e["type"];
	delete e["sync_status"];

	for (var field in e) {
		if (e.hasOwnProperty(field)) {
			statement += mysql.escapeId(field) + ' = ' +mysql.escape(e[field]) + ', ';	
		}
	}

	statement = statement.slice(0, -2);

	return statement;
}

function update(req, res, next) {
	var updates = req.body;
	// updates = test_data;

	// for (var i = 0; i < 46; i++) {
	// 	switch(i%4) {
	// 		case 0:
	// 			updates.push(updates[0]);
	// 			break;
	// 		case 1:
	// 			updates.push(updates[1]);
	// 			break;
	// 		case 2:
	// 			updates.push(updates[2]);
	// 			break;
	// 		case 3:
	// 			updates.push(updates[3]);
	// 			break;
	// 	}
	// }
	console.time("update");
	console.time("file");
	res.send();

	var timestamp = + new Date();
	fs.writeFile('./json/' + timestamp + '.json', JSON.stringify(updates), 'utf8', function(){
		console.timeEnd("file");
	});
	
	const transaction = new Transaction(mysql);

	for (var i = 0; i < updates.length; i++) {
		console.log(updates[i]);
		var element = updates[i]["element"];
		var type = element["type"];
		var id = element["id"];
		element["sync_status"] = 0;
		var table_name = updates[i]["table_name"];
		var table_name_update = table_name + "_updates";
		switch (type) {
			case 'i':
				var insertBlock = createInsertStatement(element);
				transaction.addQuery('INSERT INTO ?? ' + insertBlock["fields"] + ' VALUES ' + insertBlock["values"], [table_name]);
				break;
			case 'u':
				var updateBlock = createUpdateStatement(element);
				transaction.addQuery('UPDATE ?? SET ' + updateBlock + ' WHERE id = ?', [table_name, id]);
				break;
			case 'd':
				transaction.addQuery('DELETE FROM ?? WHERE id = ?', [table_name, id]);
				break;
		}
		transaction.addQuery('INSERT INTO transactions (table_name, type, row_id) VALUES (?,?,?)', [table_name, type, id]);
	}

	transaction.execute()
	.then(function(){
		console.timeEnd("update");
	})
	.catch(function(err) {
		console.log('Query Error:' + err);
		res.send("Database error:<br>" + err);
	});
}

function writeUpdatesToDb(updates) {

}

var test_data = [
	{
		"element": {
			"update_id":13,
			"id":6,
			"first":"ravi",
			"last":"deshmukh",
			"username":"damn not nice username",
			"password":"rapw",
			"type":"u",
			"sync_status":1
		},
		"table_name":"user"
	},
	{
		"element": {
			"update_id":13,
			"id":43,
			"company_name":"ravi",
			"company_type":"deshmukh",
			"address":"rauser",
			"owner_id":2,
			"type":"i",
			"sync_status":1
		},
		"table_name":"customer"
	},
	{
		"element": {
			"update_id":13,
			"id":31,
			"customer_id":1,
			"phone":"deshmukh",
			"first":"rauser",
			"last":"rapw",
			"title":"rapw",
			"email":"rapw",
			"owner_id":1,
			"type":"i",
			"sync_status":1
		},
		"table_name":"contact"
	},
	{
		"element": {
			"update_id":13,
			"id":93,
			"date":"2013-08-30 19:05:00",
			"interaction_type":"deshmukh",
			"description":"rauser",
			"contact_id":1,
			"type":"i",
			"sync_status":1
		},
		"table_name":"interaction"
	}
]

var server = app.listen(3333, function () {
	console.log('mysql app listening on port 3333!');
});