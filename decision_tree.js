const snowflakeFunctions = require('./snowflake-functions.js');
var snowflake;
const fs = require('fs');
var stream = fs.createWriteStream("main.log", {flags:'a'});
stream.write(new Date().toISOString()+"\r\n");



function main() {
	var runId=null;
    var snowflakeConnectPromise = snowflakeFunctions.snowflakeConnect();
    snowflakeConnectPromise.then(function(snowflakeConnection) {
        stream.write("Connected to snowflake\r\n");
	    snowflake=snowflakeConnection;
	    var snowflakeQuery = snowflakeFunctions.snowflakeQueryPromise('select ml_model_runs_sequence.nextval as RUNID',[]);
	    return snowflakeQuery.then(function(rows) {
	    	runId=rows[0].RUNID;
	    	stream.write("runId: "+runId+"\r\n");
	    	return {snowflake:snowflake,runId:runId}
	    }, function(err) {
	        stream.write(err);
	    });
		
    }, function(err) {
    	console.error(err);
    }).then(function(return_object){
    	var runId=return_object.runId;
    	var snowflake=return_object.snowflake;
    	
    	stream.write("inserting ml_model_runs entry\r\n");
    	var training_params={};
		var default_training_parameters={};
		default_training_parameters.cv_limit=10;
		default_training_parameters.total_count_limit=1;
		default_training_parameters.cv_decimal_places=5;
		default_training_parameters.average_decimal_places=2;
		default_training_parameters.maxDepth=15;
		default_training_parameters.maxFeatures=8;
		default_training_parameters.debugMessages=false;

		var training_parameters={...default_training_parameters,...training_params};
		
		var snowflakeQuery = snowflakeFunctions.snowflakeQueryPromise(
			"insert into ml_model_runs(run_id,table_name,algorithm,training_parameters,start_time) select :1,:2,:3,parse_json('"+JSON.stringify(training_parameters)+"'),current_timestamp::TIMESTAMP_NTZ",
			[runId, 'bikes_hours_eng','decision_tree']);
	    snowflakeQuery.then(function(rows) {
	    }, function(err) {
	        console.log(err);
	    });



		var inputObject={tableName:'bikes_hours_eng',
					whereClause:'1=1',
					whereClauseBindings:[],
					target:'CNT',
					remainingCols:'holiday,weekday,workingday,weathersit,temp,atemp,hum'.split(','),
					depth:0,
					trainingParameters:training_parameters,
					__dirname:__dirname};

		var finished=false;
		var model=leafCalc(inputObject,function(result){
			var snowflakeQuery = snowflakeFunctions.snowflakeQueryPromise(
				"update ml_model_runs set end_time=current_timestamp::TIMESTAMP_NTZ, model_object=parse_json('"+JSON.stringify(result)+"') where run_id=?",
				[runId]);
		    snowflakeQuery.then(function(rows) {
		    	stream.write("Finished!\r\n");
				stream.write(new Date().toISOString()+"\r\n");
				stream.end();
		    }, function(err) {
		        stream.write(err);
		        stream.end();
		    });

		});
    });
}


function leafCalc(input,done){

	const spawn = require('threads').spawn;
	const fs = require('fs');
	var tableName=input.tableName;
	var whereClause=input.whereClause;
	var whereClauseBindings=input.whereClauseBindings;
	var target=input.target;
	var remainingCols=input.remainingCols;
	var depth=input.depth;
	var trainingParameters=input.trainingParameters;

	var stream = fs.createWriteStream("main_"+depth+".log", {flags:'a'});

	const snowflakeFunctions = require(input.__dirname+'/snowflake-functions.js');

	var return_object={};
	if (trainingParameters.debugMessages){
	    return_object.cumulative_where_clause=whereClause;
	    return_object.cumulative_where_clause_bindings=whereClauseBindings;
	}

	var sqlText="select stddev("+target+") as target_stddev,"+
        "avg("+target+") as target_avg,"+
        "case when target_avg is not null and target_avg!=0 then target_stddev/target_avg*100 else 0 end as coef_of_variation,"+
        "count(*) as target_count "+
        "from "+tableName+" where "+whereClause;

	var snowflakeQuery = snowflakeFunctions.snowflakeQueryPromise(
		sqlText,
		whereClauseBindings);
    snowflakeQuery.then(function(rows) {
    	stream.write("Got first query",rows.length);
    	var results=rows[0];
    	var averageBelow=results.TARGET_AVG;


		if (averageBelow==null){
		  return done(null); // if there are no results below this value, return null so that this node can be removed
		}
		else{
		  averageBelow=averageBelow.toFixed(trainingParameters.average_decimal_places);
		}      
		if (depth >= trainingParameters.maxDepth){
		  return_object.prediction=averageBelow;
		  if (trainingParameters.debugMessages){
		    return_object.stopped_on="max_depth_reached (limit "+trainingParameters.maxDepth+", value "+depth+")";
		  }
		  return done(return_object);
		}
		if (remainingCols.length<1){
		  return_object.prediction=averageBelow;
		  if (trainingParameters.debugMessages){
		    return_object.stopped_on="last_attribute";
		  }
		  return done(return_object);
		}
		var target_count=results.TARGET_COUNT;
		if (target_count <= 1 || target_count <= trainingParameters.total_count_limit){
		  return_object.prediction=averageBelow;
		  if (trainingParameters.debugMessages){
		    return_object.stopped_on="below_child_record_count_limit (limit "+trainingParameters.total_count_limit+", value "+target_count+")";
		  }
		  return done(return_object);
		}
		var coefficientOfVariation=results.COEF_OF_VARIATION.toFixed(trainingParameters.cv_decimal_places);
		if (coefficientOfVariation < trainingParameters.cv_limit){
		  return_object.prediction=averageBelow;
		  if (trainingParameters.debugMessages){
		    return_object.stopped_on="below_cv_threshold (limit "+trainingParameters.cv_limit+", value "+coefficientOfVariation+")";
		  }
		  return done(return_object);
		}
		var stddevBeforeSplit = results.TARGET_STDDEV;
		if (target_count==0){
		    throw "The number of records during leaf node calculation was zero, this should not happen and means there's a bug in the stored proc";
		}
		if (stddevBeforeSplit==0){
		    throw "The standard deviation during leaf node calculation was zero, this should not happen and means there's a bug in the stored proc";
		}
		var columnQueries=[];
		for (var i=0;i<remainingCols.length && i < trainingParameters.maxFeatures;i++){
		    var col=remainingCols[i];
		    columnQueries.push("select '"+col+"' as col,"+
		        col+" as column_value,"+
		        "stddev("+target+") as sd_branch, "+
		        "count("+col+") as count_branch, "+
		        "count("+col+")/"+target_count+"*stddev("+target+") as p_times_s "+
		        "from "+tableName+" where "+whereClause+" group by "+col);
		}
		if (columnQueries.length==0){
		    throw "No subqueries were generated, this should not happen and means there's a bug in the stored proc";
		}
		var query="select col,"+stddevBeforeSplit+"-sum(p_times_s) as sdr from (";
		query=query+columnQueries.join(" union ");
		query=query+") group by col order by sdr desc";

		var snowflakeQuery = snowflakeFunctions.snowflakeQueryPromise(query, whereClauseBindings);
		var selectedCol,withSelectedColRemoved;
		snowflakeQuery.then(function(rows) {
			selectedCol=rows[0].COL;
			stream.write("Selected next column: "+selectedCol+"\r\n");
			withSelectedColRemoved=remainingCols.filter(function(value, index, arr){return value != selectedCol;});

			var query="select distinct("+selectedCol+") as VALUE from "+tableName;

			var snowflakeQuery = snowflakeFunctions.snowflakeQueryPromise(query,[]);

			return snowflakeQuery;
		}).then(function(rows){
			//stream.write("Counted instances for next column",selectedCol,rows);
			var thisNode={};
			if (trainingParameters.debugMessages){
			  thisNode.nextAttribute=selectedCol;
			  thisNode.coefficientOfVariation=coefficientOfVariation;
			}
			thisNode.children=[]
			
			var spawnList=[];
			var i=0;
			var results;
			for (var i=0;i<rows.length;i++){
				var whereClauseBindingsCopy=whereClauseBindings.slice();
				results=rows[i];
			    var child={};
			    child.columnValue=results.VALUE;
			    var childWhereClause=whereClause+" and "+selectedCol+"= :"+(whereClauseBindingsCopy.length+1);
			    whereClauseBindingsCopy.push(child.columnValue);
			    var inputObject={tableName:tableName,
								whereClause:childWhereClause,
								whereClauseBindings:whereClauseBindingsCopy,
								target:target,
								remainingCols:withSelectedColRemoved,
								depth:depth+1,
								trainingParameters:trainingParameters,
								__dirname: input.__dirname};
			    spawnList.push(inputObject);
			}
			var workersResponded=0;
			for (var i=0;i<spawnList.length;i++){
				const thread = spawn(leafCalc);
				thread
				  .send(spawnList[i])
				  // The handlers come here: (none of them is mandatory)
				  .on('message', function(response) {
				  	workersResponded++;
				  	stream.write('Response from worker:'+response+"\r\n");
				  	if (response!=null){
				      response.selectionCriteriaAttribute=selectedCol;
				      response.selectionCriteriaPredicate='=';
				      response.selectionCriteriaValue=child.columnValue;
				      thisNode.children.push(response);
				    }
			  		stream.write("at depth of "+depth+", "+workersResponded+" workers out of "+spawnList.length+" have responded"+"\r\n");
				  	
				  	if (workersResponded==spawnList.length){
				  		return done(thisNode);
				  		thread.kill();
				  	}
				  })
				  .on('error', function(error) {
				    console.error('Worker errored:',error.message);
				  })
				  .on('exit', function() {
				    stream.write('Worker has been terminated.'+"\r\n");
				    stream.end();
				  });
			}
		}, function(err) {
	        stream.write("Error on second query: "+err.message+"\r\n");
	    });
    }, function(err) {
        stream.write("Error on first query"+err.message+"\r\n");
    });


}


main();



