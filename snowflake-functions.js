const snowflakeLib = require('snowflake-sdk');
var snowflake_connection;

const snowflakeConnect=function() {
		if (snowflake_connection!=null){
			return new Promise(function(resolve, reject) {
				resolve(snowflake_connection);
			});
		}
		return new Promise(function(resolve, reject) {
			snowflakeLib.configure({insecureConnect: true});
			var snowflake = snowflakeLib.createConnection({
			  account: process.env.SNOWFLAKE_ACCOUNT,
			  username: process.env.SNOWFLAKE_USERNAME,
			  password: process.env.SNOWFLAKE_PASSWORD,
			  database: process.env.SNOWFLAKE_DATABASE,
			  schema: process.env.SNOWFLAKE_SCHEMA,
			  warehouse: process.env.SNOWFLAKE_WAREHOUSE,
			  role: process.env.SNOWFLAKE_ROLE
			});
			snowflake.connect(function(err, conn) {
			  if (err) {
			  	reject(err);
			  } else {
			  	snowflake_connection=snowflake;
				resolve(snowflake);		    
			  }
			});
		});
	};

const snowflakeQueryPromise=function(sqlText,binds){
		return new Promise(function(resolve, reject) {
			snowflakeConnect().then(function(snowflake){
				snowflake.execute({
				  sqlText: sqlText,
				  binds: binds,
				  complete: function(err, stmt, rows) {
				    if (err) {
				      reject(err);
				    } else {
				      resolve(rows);
				    }
				  }
				});
			});
			
		});
	}


module.exports = { snowflakeConnect, snowflakeQueryPromise};