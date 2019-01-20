/* decision_tree_train - a stored procedure that trains a decision tree, and stores the model in the 'ml_model_runs' table
 *  Parameters:
 *    TABLE_NAME - the name of the table containing the training data
 *    TARGET - the name of the column containing the target variable (the one to predict)
 *    COLS - a comma separated list of the table columns to include as variables in the model
 *    TRAINING_PARAMS - an object containing training parameters, which can be:
         cv_limit (default 10) - Coefficient of Deviation limit, used to stop branching
         total_count_limit (default 1) - Total record count limit, used to stop branching
         cv_decimal_places (default 5) - The number of decimal places to round the Coefficient of Deviation calculation to
         average_decimal_places (default 2) - The number of decimal places to round the average calculation to (where multiple records exist at a leaf)
         maxDepth (default 15) - the maximum depth of the tree
         maxFeatures (default 8) - the maximum number of features to evaluate at a time
         debugMessages (default false) - set to true to include extra information in the output model about the state of each node
*/
create or replace procedure decision_tree_train(TABLE_NAME VARCHAR, TARGET VARCHAR, COLS VARCHAR,TRAINING_PARAMS VARIANT)
  returns string not null
  language javascript
  as
  $$  
  function leafCalc(tableName,whereClause,whereClauseBindings,target,remainingCols,depth,trainingParameters){
    var return_object={};
    if (training_parameters.debugMessages){
        return_object.cumulative_where_clause=whereClause;
        return_object.cumulative_where_clause_bindings=whereClauseBindings;
    }
    var results;
    results = snowflake.execute({
        sqlText: "select stddev("+target+") as target_stddev,"+
                        "avg("+target+") as target_avg,"+
                        "case when target_avg is not null and target_avg!=0 then target_stddev/target_avg*100 else 0 end as coef_of_variation,"+
                        "count(*) as target_count "+
                        "from "+tableName+" where "+whereClause,
        binds: whereClauseBindings
      });
    results.next();
    var averageBelow=results.getColumnValue(2);
    if (averageBelow==null){
      return null; // if there are no results below this value, return null so that this node can be removed
    }
    else{
      averageBelow=averageBelow.toFixed(trainingParameters.average_decimal_places);
    }      
    if (depth >= trainingParameters.maxDepth){
      return_object.prediction=averageBelow;
      if (training_parameters.debugMessages){
        return_object.stopped_on="max_depth_reached (limit "+trainingParameters.maxDepth+", value "+depth+")";
      }
      return return_object;
    }
    if (remainingCols.length<1){
      return_object.prediction=averageBelow;
      if (training_parameters.debugMessages){
        return_object.stopped_on="last_attribute";
      }
      return return_object;
    }
    var target_count=results.getColumnValue(4);
    if (target_count <= trainingParameters.total_count_limit){
      return_object.prediction=averageBelow;
      if (training_parameters.debugMessages){
        return_object.stopped_on="below_child_record_count_limit (limit "+trainingParameters.total_count_limit+", value "+target_count+")";
      }
      return return_object;
    }
    var coefficientOfVariation=results.getColumnValue(3).toFixed(trainingParameters.cv_decimal_places);
    if (coefficientOfVariation < trainingParameters.cv_limit){
      return_object.prediction=averageBelow;
      if (training_parameters.debugMessages){
        return_object.stopped_on="below_cv_threshold (limit "+trainingParameters.cv_limit+", value "+coefficientOfVariation+")";
      }
      return return_object;
    }
    var stddevBeforeSplit = results.getColumnValue(1);
    var countBeforeSplit = results.getColumnValue(4);
    if (countBeforeSplit==0){
        throw "The number of records during leaf node calculation was zero, this should not happen and means there's a bug in the stored proc";
    }
    if (stddevBeforeSplit==0){
        throw "The standard deviation during leaf node calculation was zero, this should not happen and means there's a bug in the stored proc";
    }
    var columnQueries=[];
    for (var i=0;i<remainingCols.length && i < training_parameters.maxFeatures;i++){
        var col=remainingCols[i];
        columnQueries.push("select '"+col+"' as col,"+
            col+" as column_value,"+
            "stddev("+target+") as sd_branch, "+
            "count("+col+") as count_branch, "+
            "count("+col+")/"+countBeforeSplit+"*stddev("+target+") as p_times_s "+
            "from "+tableName+" where "+whereClause+" group by "+col);
    }
    if (columnQueries.length==0){
        throw "No subqueries were generated, this should not happen and means there's a bug in the stored proc";
    }
    var query="select col,"+stddevBeforeSplit+"-sum(p_times_s) as sdr from (";
    query=query+columnQueries.join(" union ");
    query=query+") group by col order by sdr desc";
    results = snowflake.execute({
      sqlText: query,
      binds: whereClauseBindings
    });
    results.next();
    var selectedCol=results.getColumnValue(1);
    var withSelectedColRemoved=remainingCols.filter(function(value, index, arr){return value != selectedCol;});
    var results = snowflake.execute({
      sqlText: "select distinct("+selectedCol+") from "+TABLE_NAME
    });
    var thisNode={};
    if (training_parameters.debugMessages){
      thisNode.nextAttribute=selectedCol;
      thisNode.coefficientOfVariation=coefficientOfVariation;
    }
    thisNode.children=[]
    while(results.next()){
        var child={};
        child.columnValue=results.getColumnValue(1);
        var childWhereClause=whereClause+" and "+selectedCol+"= :"+(whereClauseBindings.length+1);
        whereClauseBindings.push(child.columnValue);
        var branchesBelow=leafCalc(tableName,childWhereClause,whereClauseBindings,target,withSelectedColRemoved,depth+1,trainingParameters);
        if (branchesBelow!=null){
          branchesBelow.selectionCriteriaAttribute=selectedCol;
          branchesBelow.selectionCriteriaPredicate='=';
          branchesBelow.selectionCriteriaValue=child.columnValue;
          thisNode.children.push(branchesBelow);
        }
    }
    return thisNode;
  }
  var columns=COLS.split(',');
  var results = snowflake.execute({
    sqlText: "select ml_model_runs_sequence.nextval"
  });
  results.next();
  var default_training_parameters={};
  default_training_parameters.cv_limit=10;
  default_training_parameters.total_count_limit=1;
  default_training_parameters.cv_decimal_places=5;
  default_training_parameters.average_decimal_places=2;
  default_training_parameters.maxDepth=15;
  default_training_parameters.maxFeatures=8;
  default_training_parameters.debugMessages=false;
  
  var training_parameters={...default_training_parameters,...TRAINING_PARAMS};
  
  var runId=results.getColumnValue(1);
  results = snowflake.execute({
    sqlText: "insert into ml_model_runs(run_id,table_name,algorithm,training_parameters,start_time) select :1,:2,:3,parse_json('"+JSON.stringify(training_parameters)+"'),current_timestamp::TIMESTAMP_NTZ",
    binds: [runId, TABLE_NAME,'decision_tree']
  });
  
  var model=leafCalc(TABLE_NAME,'1=1',[],TARGET,columns,0,training_parameters);
  results = snowflake.execute({
    sqlText: "update ml_model_runs set end_time=current_timestamp::TIMESTAMP_NTZ, model_object=parse_json('"+JSON.stringify(model)+"') where run_id=?",
    binds: [runId]
  });
  return runId;
  $$
  ;
