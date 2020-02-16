const express = require('express');
const bodyParser = require('body-parser');
const sqlite3 = require('sqlite3').verbose();

const app = express();

const host = 80;

const server = app.listen(host, (request, response) => {
	const host = server.address().address;
	const port = server.address().port;
	console.log("Daytona Server listening at http://%s:%s", host, port);
});

app.set('port', process.env.PORT || 8080);


app.use(bodyParser.urlencoded({
		extended: false
	}));
app.use(bodyParser.json());

let db = null;

const connectDB =  () => {
	db = new sqlite3.Database(':memory:', (err) => {
		if (err) {
			return console.error(err.message);
		}
		initialDBSetup();

		console.log('Connected to the in-memory SQlite database.');
	});
}
connectDB();

const closeDB = () => {
	db.close((err) => {
		if (err) {
			console.error(err.message);
		}
		console.log('Close the database connection.');
	});
}

const initialDBSetup = () => {
	/* User table setup */
	const sqlCreateUserTable = `
		CREATE TABLE IF NOT EXISTS users (
			userId INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT
		)`;

	db.run(sqlCreateUserTable, ()=> {
		console.log('users table created successfully...')
	});

	/* Trades table setup */
	const sqlCreateTradeTable = `
		CREATE TABLE IF NOT EXISTS trades (
			id INTEGER PRIMARY KEY,
			type TEXT,
			symbol TEXT,
			shares INTEGER DEFAULT 0,
			price REAL,
			timestamp TEXT,
			userId INTEGER,
			CONSTRAINT users_fk_userId FOREIGN KEY (userId)
				REFERENCES users(userId) ON UPDATE CASCADE ON DELETE CASCADE
		)`;

	db.run(sqlCreateTradeTable, (err)=> {
		console.log('trades table created successfully...')
	});
}

/* Get All Users */
app.get('/users', (req, res) => {
	try {
		const sql = `SELECT * FROM users`;
		db.all(sql, (error, result) => {
			if (error) {
				res.send({ error });
				return;
			}
			res.status(200).send(result);
		})
	} catch (e) {
		res.status(500).send({message: "Internal Server Error.", e});
	}
});

/* Get all the trades */
app.get('/trades', (req, res) => {
	try {
		const sql = `SELECT * FROM trades LEFT JOIN users ON users.userId = trades.userId ORDER BY trades.id`;
		db.all(sql, (error, data) => {
			if (error) {
				res.send({ error });
				return;
			}
			
			const results = nestedUser(data);

			if (!results.length) {
				res.status(404).send({message: 'Trades not found.'});
				return;
			}

			res.status(200).send(results);
		})
	} catch (e) {
		res.status(500).send({message: "Internal Server Error.", e});
	}
});

/* Create the trades */
app.post('/trades', (req, res) => {
	try {
		const trade = req.body;
		const user = {
			id: trade.user.id,
			name: trade.user.name,
		}
		delete trade.user;
		trade.userId = user.id;

		const sqlUser = `INSERT INTO users (userId, name) VALUES (?, ?)`;

		db.run(sqlUser, [user.id, user.name], function(err) {
		});

		sqlTrade = `INSERT INTO trades (id, type, symbol, shares, price, timestamp, userId) VALUES (?, ?, ?, ? ,?, ?, ?)`;
	  db.run(sqlTrade, [trade.id, trade.type, trade.symbol, trade.shares, trade.price, trade.timestamp, trade.userId], function(err) {
			if (err) {
				if (err.code === 'SQLITE_CONSTRAINT') {
					res.status(400).send({code: 400, message: 'Trade id already found.'});
					return;
				}
				res.status(500).send({ err });
				return;
			}
			res.status(200).send({ message: 'trade creted successfully.' });
		});
		
	} catch (e) {
		res.status(500).send({message: "Internal Server Error.", e});
	}
});

/* Filter trades by userId */
app.get('/trades/:userId', (req, res) => {
	const { userId } = req.params;
	try {
		const sql = `SELECT * FROM trades LEFT JOIN users ON users.userId = trades.userId WHERE trades.userId=? ORDER BY trades.id`;
		db.all(sql, [userId], (error, data) => {
			if (error) {
				res.send({ error });
				return;
			}
			
			const results = nestedUser(data);

			if (!results.length) {
				res.status(404).send({message: 'UserId not found.'});
				return;
			}

			res.status(200).send(results);
		})
	} catch (e) {
		res.status(500).send({message: "Internal Server Error.", e});
	}
});

/* Filter trades by symbol, type, start and end */
app.get('/stocks/:stockSymbol/trades', (req, res) => {
	const { stockSymbol } = req.params;
	const { type, start, end } = req.query;
	
	try {

		const sql = `
			SELECT * FROM trades LEFT JOIN users ON users.userId = trades.userId 
			WHERE trades.symbol=? ${type ? ' AND type=?' : ''} ${start && end ? ' AND date(datetime(timestamp)) BETWEEN ? AND ?' : ''}
			ORDER BY trades.id
		`;
		
		const params = [stockSymbol];

		if (type) params.push(type);
		if (start && end) {
			params.push(start);
			params.push(end);
		}

		db.all(sql, params, (error, data) => {
			if (error) {
				res.send({ error });
				return;
			}

			const results = nestedUser(data);

			if (!results.length) {
				res.status(404).send({message: 'Trades not found.'});
				return;
			}
			res.status(200).send(results);
		})
	} catch (e) {
		res.status(500).send({message: "Internal Server Error.", e});
	}
});

/* Find highest and lower price by symbol, start and end */
app.get('/stocks/:stockSymbol/price', (req, res) => {
	const { stockSymbol } = req.params;
	const { start, end } = req.query;
	
	try {

		const sql = `
			SELECT * FROM trades LEFT JOIN users ON users.userId = trades.userId 
			WHERE trades.symbol=? ${start && end ? ' AND date(datetime(timestamp)) BETWEEN ? AND ?' : ''}
			ORDER BY trades.id
		`;
		
		const params = [stockSymbol];

		if (start && end) {
			params.push(start);
			params.push(end);
		}

		db.all(sql, params, (error, data) => {
			if (error) {
				res.send({ error });
				return;
			}

			if (!data.length) {
				res.status(404).send({message: 'There are no trades in the given date range.'});
				return;
			}

			const [highest, lowest] = findMaxMinPrice(data);
			const result = {
				symbol: stockSymbol,
				highest: highest.price,
				lowest: lowest.price
			}
			res.status(200).send(result);
		})
	} catch (e) {
		res.status(500).send({message: "Internal Server Error.", e});
	}
});


/* delete all the trades */
app.delete('/trades', (req, res) => {
	try {
		const sql = `DELETE FROM trades`;
		db.run(sql, (error, data) => {
			if (error) {
				res.send({ error });
				return;
			}
			res.status(200).send({message: 'Delete all the trades successfully.'});
		})
	} catch (e) {
		res.status(500).send({message: "Internal Server Error.", e});
	}
});

const nestedUser = (data)=> {
	const results = data.map((d)=> {
		const user = { id: d.userId, name: d.name };
		delete d.userId;
		delete d.name;
		return {...d, user}
	});
	return results;
} 

const findMaxMinPrice = (data)=> {
	const highest = data.reduce(function(a, b) {
    return a.price > b.price ? a : b
	});

	const lowest = data.reduce(function(a, b) {
    return a.price < b.price ? a : b
	});
	return [highest, lowest];
} 