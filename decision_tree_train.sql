
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
  /*
   * The decision tree is contructed using some temporary training state. This function strips it out and just
   * leaves the decision tree path selectors and predictions.
   */
  function removeTrainingState(obj){
      for (var i=0;i<obj.children.length;i++){
          delete obj.children[i].whereClause;
          delete obj.children[i].whereClauseBindings;
          delete obj.children[i].remainingCols;
          if (obj.children[i].children){
            removeTrainingState(obj.children[i]);
          }
          else{
            if (typeof(obj.children[i].prediction)=='undefined'){
                obj.children.splice(i,1);
                i--;
            }
          }
      }
      delete obj.whereClause;
      delete obj.whereClauseBindings;
      delete obj.remainingCols;
  }
  /*
   * A function which calculates all decision tree splits at a particular horizontal "level" of the tree.
   * The function is called recursively, and leans on the pass-by-reference nature of javascript function calls
   * to keep the state tracking simple.
   *
   * This means that the treeNodes array parameter will contain just the node objects at the bottom of the tree, 
   * like so:
   *
   *              [          O          ]      <- First call, caller should keep a reference to this one
   *                       /   \
   *                      /     \
   *              [      O       O      ]      <- Second call (recursive)
   *                    /|\     / \
   *                   / | \   /   \
   *              [   O  O  O O     O   ]      <- Third call (recursive)
   *
   *
   * Parameters:
   * - tableName              : The name of the table/view containing the source data
   * - treeNodes              : An array of nodes at the current level. Each node object tracks its own cumulative
   *                             "where" clause, associated bindings as well as a list of remaining attributes 
   *                             from the original list
   * - target                 : The target attribute from the source table
   * - depth                  : The current depth of the tree
   * - trainingParameters     : Parameters that impact the training process
   * - allColumnDistinctValues: A precalculated map of all distinct values for each attribute
   */
  function levelCalc(tableName,treeNodes,target,depth,trainingParameters,allColumnDistinctValues){
    var results;
    
    var currentBranchQueries=[];
    var currentBranchQueryBindings=[];
    for (var i=0;i<treeNodes.length;i++){
        currentBranchQueries.push("select "+i+" as index,"+
                        "stddev("+target+") as target_stddev,"+
                        "avg("+target+") as target_avg,"+
                        "case when target_avg is not null and target_avg!=0 then target_stddev/target_avg*100 else 0 end as coef_of_variation,"+
                        "count(*) as target_count"+
                        " from "+tableName+" where "+treeNodes[i].whereClause);
        currentBranchQueryBindings=currentBranchQueryBindings.concat(treeNodes[i].whereClauseBindings);
    }
    results = snowflake.execute({
      sqlText: currentBranchQueries.join(' UNION '),
      binds: currentBranchQueryBindings
    });
    
    var columnQueries=[];
    var columnQueryBindings=[];
    while (results.next()){
        
        var index=results.getColumnValue(1);
        var stddevBeforeSplit = results.getColumnValue(2);
        var averageBelow=results.getColumnValue(3);
        var coefficientOfVariation=results.getColumnValue(4);
        var target_count=results.getColumnValue(5);
        var node=treeNodes[index];
        if (averageBelow==null){
          treeNodes[index]=null;
          /*if (training_parameters.debugMessages){
            node.stopped_on="no results below";
          }*/
          continue;
        }
        else{
          averageBelow=averageBelow.toFixed(trainingParameters.average_decimal_places);
        }
        if (training_parameters.debugMessages){
            node.averageBelow=averageBelow;
            node.stddevBeforeSplit=stddevBeforeSplit;
            node.coefficientOfVariation=coefficientOfVariation;
            node.target_count=target_count;
        }
        
        if (depth >= trainingParameters.maxDepth){
          node.prediction=averageBelow;
          if (training_parameters.debugMessages){
            node.stopped_on="max_depth_reached (limit "+trainingParameters.maxDepth+", value "+depth+")";
          }
          continue;
        }
        if (node.remainingCols.length<1){
          node.prediction=averageBelow;
          if (training_parameters.debugMessages){
            node.stopped_on="last_attribute";
          }
          continue;
        }
        if (target_count <= trainingParameters.total_count_limit){
          node.prediction=averageBelow;
          if (training_parameters.debugMessages){
            node.stopped_on="below_child_record_count_limit (limit "+trainingParameters.total_count_limit+", value "+target_count+")";
          }
          continue;
        }
        coefficientOfVariation=coefficientOfVariation.toFixed(trainingParameters.cv_decimal_places);
        if (coefficientOfVariation < trainingParameters.cv_limit){
          node.prediction=averageBelow;
          if (training_parameters.debugMessages){
            node.stopped_on="below_cv_threshold (limit "+trainingParameters.cv_limit+", value "+coefficientOfVariation+")";
          }
          continue;
        }
        if (depth > trainingParameters.maxDepth){
          node.prediction=averageBelow;
          if (training_parameters.debugMessages){
            node.stopped_on="depth "+trainingParameters.maxDepth+" exceeded by value "+depth+")";
          }
          continue;
        }
        if (target_count==0){
            throw "The number of records during leaf node calculation was zero, this should not happen and means there's a bug in the stored proc";
        }
        if (stddevBeforeSplit==0){
            throw "The standard deviation during leaf node calculation was zero, this should not happen and means there's a bug in the stored proc";
        }
        
        for (var i=0;i<node.remainingCols.length && i < training_parameters.maxFeatures;i++){
          var col=node.remainingCols[i];
          columnQueries.push("select '"+index+"' as index,"+
              "'"+col+"' as col,"+
              col+" as column_value,"+
              "stddev("+target+") as sd_branch, "+
              "count("+col+") as count_branch, "+
              "count("+col+")/"+target_count+"*stddev("+target+") as p_times_s, "+
              "median("+col+") as median_branch "+
              "from "+tableName+" where "+node.whereClause+" group by "+col);
          columnQueryBindings=columnQueryBindings.concat(node.whereClauseBindings);
        }
        
    }
    
    if (columnQueries.length==0){
        return;
    }
    var query="with childcalcs as (select index,col,"+stddevBeforeSplit+"-sum(p_times_s) as sdr, rank() over ( PARTITION BY index ORDER BY sdr DESC) as rank,median_branch from (";
    query=query+columnQueries.join(" union ");
    query=query+") group by INDEX, col,median_branch) select * from childcalcs where rank=1";
    results = snowflake.execute({
      sqlText: query,
      binds: columnQueryBindings
    });
    var nextLevelNodes=[];
    while (results.next()){
        // for each node at the current level of the tree, add two children that split either side of the median.
        // collect all the children into an array to pass back into this function to calculate the next level down.
        var index=results.getColumnValue(1);
        var col=results.getColumnValue(2);
        var medianValue=results.getColumnValue(5);
        var node=treeNodes[index];
        
        node.children=[];
        
        var newRemainingCols=node.remainingCols.filter(function(value, index, arr){return value != col;});

        var leftNodeBindings=node.whereClauseBindings.slice(0);
        leftNodeBindings.push(medianValue);
        var leftChildNode={}
        // training state
        leftChildNode.whereClause=node.whereClause+" and "+col+"<?";
        leftChildNode.whereClauseBindings=leftNodeBindings;
        leftChildNode.remainingCols=newRemainingCols;

        // tree navigation attributes
        leftChildNode.selectionCriteriaAttribute=col;
        leftChildNode.selectionCriteriaPredicate='<';
        leftChildNode.selectionCriteriaValue=medianValue;
        
        var rightNodeBindings=node.whereClauseBindings.slice(0);
        rightNodeBindings.push(medianValue);
        var rightChildNode={}
        // training state
        rightChildNode.whereClause=node.whereClause+" and "+col+">=?";
        rightChildNode.whereClauseBindings=rightNodeBindings;
        rightChildNode.remainingCols=newRemainingCols;

        // tree navigation attributes
        rightChildNode.selectionCriteriaAttribute=col;
        rightChildNode.selectionCriteriaPredicate='>=';
        rightChildNode.selectionCriteriaValue=medianValue;

        node.children.push(leftChildNode);
        nextLevelNodes.push(leftChildNode);
        node.children.push(rightChildNode);
        nextLevelNodes.push(rightChildNode);
        
    }
    if (nextLevelNodes.length > 0){
      levelCalc(tableName,nextLevelNodes,target,depth+1,trainingParameters,allColumnDistinctValues);
    }
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
  default_training_parameters.maxFeatures=4;
  default_training_parameters.debugMessages=false;
  
  var training_parameters={...default_training_parameters,...TRAINING_PARAMS};
  
  var runId=results.getColumnValue(1);
  results = snowflake.execute({
    sqlText: "insert into ml_model_runs(run_id,table_name,algorithm,training_parameters,start_time) select :1,:2,:3,parse_json('"+JSON.stringify(training_parameters)+"'),current_timestamp::TIMESTAMP_NTZ",
    binds: [runId, TABLE_NAME,'decision_tree']
  });
  var distinctValuesQueryComponents=[]
  for (var i=0;i<columns.length;i++){
    distinctValuesQueryComponents.push("select distinct "+columns[i]+",'"+columns[i]+"' as col from "+TABLE_NAME);
  }
  results = snowflake.execute({
    sqlText: distinctValuesQueryComponents.join(' union ')
  });
  var allColumnDistinctValues={};
  columns.map(function(c,i){allColumnDistinctValues[c]=[]});
  while (results.next()){
    var value=results.getColumnValue(1);
    var col=results.getColumnValue(2);
    allColumnDistinctValues[col].push(value);
  }
  
  var treeNodes=[{whereClause:"1=?",whereClauseBindings:[1],remainingCols:columns}]
  levelCalc(TABLE_NAME,treeNodes,TARGET,1,training_parameters,allColumnDistinctValues)
  var rootNode=treeNodes[0];
  // Clean up model object, remove all the state used during tree construction
  //removeKeys(rootNode,['whereClause','whereClauseBindings','remainingCols']);
  removeTrainingState(rootNode);
  results = snowflake.execute({
    sqlText: "update ml_model_runs set end_time=current_timestamp::TIMESTAMP_NTZ, model_object=parse_json('"+JSON.stringify(rootNode)+"') where run_id=?",
    binds: [runId]
  });
  return runId;
  $$
  ;
  

truncate table logging;

call decision_tree_train('bikes_hours_eng_2','CASUAL','hr_bucket,holiday,workingday,weathersit,temp,atemp,hum',null);



