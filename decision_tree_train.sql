
/* decision_tree_train - a stored procedure that trains a decision tree, and stores the model in the 'ml_model_runs' table
 *  Parameters:
 *    TABLE_NAME - the name of the table containing the training data
 *    TARGET - the name of the column containing the target variable (the one to predict)
 *    BINARY_COLS - a comma separated list of the table columns to include as binary variables (split by average value) in the model
 *    MULTIWAY_COLS - a comma separated list of the table columns to include as multi-way variables (split on all values present) in the model. Columns with high cardinality will have an adverse impact on performance.
 *    TRAINING_PARAMS - an object containing training parameters, which can be:
         cv_limit (default 10) - Coefficient of Deviation limit, used to stop branching
         total_count_limit (default 1) - Total record count limit, used to stop branching
         cv_decimal_places (default 5) - The number of decimal places to round the Coefficient of Deviation calculation to
         average_decimal_places (default 2) - The number of decimal places to round the average calculation to (where multiple records exist at a leaf)
         maxDepth (default 15) - the maximum depth of the tree
         maxFeatures (default 8) - the maximum number of features to evaluate at a time
         debugMessages (default false) - set to true to include extra information in the output model about the state of each node
*/
CREATE OR REPLACE PROCEDURE "DECISION_TREE_TRAIN"(MODEL_ID VARCHAR, TABLE_NAME VARCHAR, TARGET VARCHAR, BINARY_COLS VARCHAR, MULTIWAY_COLS VARCHAR, TRAINING_PARAMS VARIANT)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
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
                if (typeof(obj.children[i].prediction)==''undefined''){
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
       *                       /          *                      /            *              [      O       O      ]      <- Second call (recursive)
       *                    /|     /        *                   / |    /          *              [   O  O  O O     O   ]      <- Third call (recursive)
       *
       * and each node object looks like this: {whereClause:"1=? and x<? and y>?",whereClauseBindings:[1,4,2],remainingCols:[''a'',''b'',''c'']}
       *
       * Parameters:
       * - tableName              : The name of the table/view containing the source data
       * - treeNodes              : An array of nodes at the current level. Each node object tracks its own cumulative
       *                             "where" clause, associated bindings as well as a list of remaining attributes 
       *                             from the original list
       * - target                 : The target attribute from the source table
       * - depth                  : The current depth of the tree
       * - trainingParameters     : Parameters that impact the training process
       * - multiwayColumnDistinctValues: A precalculated map of all distinct values for each attribute selected as multi-way split
       */
      function levelCalc(tableName,treeNodes,target,depth,trainingParameters,multiwayColumnDistinctValues){
        var results;
        
        var currentBranchQueries=[];
        var currentBranchQueryBindings=[];
            
        // The first query collects some important information about each node:
        // 1) The standard deviation of all the target values from this node down, as we will pick the branch that reduces this value the most
        // 2) The average value of all the target values from this node down, as ultimately average is used as a predictor when we reach the leaf
        // 3) The coefficient of variation, can be used to stop building when it gets too small
        // 4) The number of target values from this node down, can be used to stop building when it gets too small
        // 5) For each potential branch below (from the list of remaining columns), the median value for columns using binary splits
        
        for (var i=0;i<treeNodes.length;i++){
            
            var remainingColumnMedians=[];
            for (var j=0;j<treeNodes[i].remainingBinaryCols.length;j++){
                remainingColumnMedians.push("''"+treeNodes[i].remainingBinaryCols[j]+"'',MEDIAN("+treeNodes[i].remainingBinaryCols[j]+")");
            }
            var remainingColumnMediansQuery=",null as medians";
            if (remainingColumnMedians.length>0){
                remainingColumnMediansQuery=",TO_JSON(OBJECT_CONSTRUCT("+remainingColumnMedians.join(",")+")) as medians";
            }
            currentBranchQueries.push("select "+i+" as index,"+
                            "stddev("+target+") as target_stddev,"+
                            "avg("+target+") as target_avg,"+
                            "case when target_avg is not null and target_avg!=0 then target_stddev/target_avg*100 else 0 end as coef_of_variation,"+
                            "count(*) as target_count"+
                            remainingColumnMediansQuery+
                            " from "+tableName+" where "+treeNodes[i].whereClause);
            currentBranchQueryBindings=currentBranchQueryBindings.concat(treeNodes[i].whereClauseBindings);
        }
        try{
          results = snowflake.execute({
            sqlText: currentBranchQueries.join('' UNION ''),
            binds: currentBranchQueryBindings
          });
        }
        catch(e){
          throw "Error executing first tree split query: "+e.message+". Bindings: "+currentBranchQueryBindings+", query: "+currentBranchQueries.join('' UNION '');
        }
        
        var columnQueries=[];
        var columnQueryBindings=[];
        var medianValues={};
        while (results.next()){
            
            var index=results.getColumnValue(1);
            var stddevBeforeSplit = results.getColumnValue(2);
            var averageBelow=results.getColumnValue(3);
            var coefficientOfVariation=results.getColumnValue(4);
            var target_count=results.getColumnValue(5);
            var node=treeNodes[index];
            var medianValuesRaw=results.getColumnValue(6);
            if (medianValuesRaw!=null){
              medianValues[index]=JSON.parse(medianValuesRaw.replace(new RegExp(''undefined'', ''g''), ''null''));
            }
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
            if (node.remainingBinaryCols.length + node.remainingMultiwayCols.length <1){
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
                throw "The number of records during leaf node calculation was zero, this should not happen and means there''s a bug in the stored proc";
            }
            if (stddevBeforeSplit==0){
                throw "The standard deviation during leaf node calculation was zero, this should not happen and means there''s a bug in the stored proc";
            }
            var featuresUsed=0;
            
            // for each binary split column, add a query for each side of the median
            
            for (var i=0;i<node.remainingBinaryCols.length && featuresUsed < training_parameters.maxFeatures;i++){
              featuresUsed=featuresUsed+1;
              var col=node.remainingBinaryCols[i];
              // don''t use this column if the median value came back empty
              if (typeof(medianValues[index][col])===''undefined''){
              throw "undefined median value "+col;
                continue;
              }
              var clause=col+"<"+medianValues[index][col];
              columnQueries.push("select ''"+index+"'' as index,"+
                  "''"+col+"'' as col,"+
                  "''"+clause+"'' as clause,"+
                  "stddev("+target+") as sd_branch, "+
                  "count("+col+") as count_branch, "+
                  "count("+col+")/"+target_count+"*stddev("+target+") as p_times_s, "+
                  stddevBeforeSplit+" as stddev_before_split "+
                  "from "+tableName+" where "+node.whereClause+" and "+clause);
              
              clause=col+"<"+medianValues[index][col];
              columnQueries.push("select ''"+index+"'' as index,"+
                  "''"+col+"'' as col,"+
                  "''"+clause+"'' as clause,"+
                  "stddev("+target+") as sd_branch, "+
                  "count("+col+") as count_branch, "+
                  "count("+col+")/"+target_count+"*stddev("+target+") as p_times_s, "+
                  stddevBeforeSplit+" as stddev_before_split "+
                  "from "+tableName+" where "+node.whereClause+" and "+clause);
              columnQueryBindings=columnQueryBindings.concat(node.whereClauseBindings).concat(node.whereClauseBindings);
            }
            
            // for each multiway split column, add a query for each value
            
            for (var i=0;i<node.remainingMultiwayCols.length && featuresUsed < training_parameters.maxFeatures;i++){
              featuresUsed=featuresUsed+1;
              var col=node.remainingMultiwayCols[i];
              var colDistinctValues=multiwayColumnDistinctValues[col];
              for (var j=0;j<colDistinctValues.length;j++){
                var clause=col+"=''"+colDistinctValues[j]+"''";
                if (colDistinctValues[j]==null){
                  clause=col+"=null";
                }
                columnQueries.push("select ''"+index+"'' as index,"+
                  "''"+col+"'' as col,"+
                  "''"+col+"=''''"+colDistinctValues[j]+"'''''' as clause,"+
                  "stddev("+target+") as sd_branch, "+
                  "count("+col+") as count_branch, "+
                  "count("+col+")/"+target_count+"*stddev("+target+") as p_times_s, "+
                  stddevBeforeSplit+" as stddev_before_split "+
                  "from "+tableName+" where "+node.whereClause+" and "+clause);
                columnQueryBindings=columnQueryBindings.concat(node.whereClauseBindings);
              }
            }
        }
        if (columnQueries.length==0){
            return;
        }
        var query="with childcalcs as (select index,col,clause,stddev_before_split-sum(p_times_s) as sdr, rank() over ( PARTITION BY index ORDER BY sdr DESC) as rank from (";
        query=query+columnQueries.join(" union ");
        query=query+") group by INDEX, col,stddev_before_split,clause) select * from childcalcs where rank=1";
        try{
          results = snowflake.execute({
            sqlText: query,
            binds: columnQueryBindings
          });
        }catch(e){
          throw "Error executing second tree split query: "+e.message;
        }
        var nextLevelNodes=[];
        // this is a loop going horizontally across the tree at the current level, having selected the best column for each node using the first query
        while (results.next()){
          // for each node at the current level of the tree, add child branches
          // collect all the children into an array to pass back into this function to calculate the next level down.
          var index=results.getColumnValue(1);
          var col=results.getColumnValue(2);
          var node=treeNodes[index];
          
          node.children=[];
    
          // if winning column is binary, add the left and right branches to the tree
          // if winning column is multiway, add a branch for every value
          if (typeof(multiwayColumnDistinctValues[col]) === "undefined"){
            var medianValue=medianValues[index][col];
            if (medianValue==null){
              throw "median is null, index "+index+" col "+col+" values "+JSON.stringify(medianValues);
            }
            if (typeof(medianValue)===''undefined''){
              throw "median is undefined, index "+index+" col "+col+" values "+JSON.stringify(medianValues);
            }
            var newRemainingBinaryCols=node.remainingBinaryCols.filter(function(value, index, arr){return value != col;});
            var leftNodeBindings=node.whereClauseBindings.slice(0);
            leftNodeBindings.push(medianValue);
            var leftChildNode={}
            // training state
            leftChildNode.whereClause=node.whereClause+" and "+col+"<?";
            leftChildNode.whereClauseBindings=leftNodeBindings;
            leftChildNode.remainingBinaryCols=newRemainingBinaryCols.slice(0);
            leftChildNode.remainingMultiwayCols=node.remainingMultiwayCols.slice(0);
    
            // tree navigation attributes
            leftChildNode.selectionCriteriaAttribute=col;
            leftChildNode.selectionCriteriaPredicate=''<'';
            leftChildNode.selectionCriteriaValue=medianValue;
    
            var rightNodeBindings=node.whereClauseBindings.slice(0);
            if (typeof(medianValue)===''undefined''){
              throw "median value was undefined for winning binary column "+col+", context: "+JSON.stringify(medianValues);
            }
            rightNodeBindings.push(medianValue);
            var rightChildNode={}
            // training state
            rightChildNode.whereClause=node.whereClause+" and "+col+">=?";
            rightChildNode.whereClauseBindings=rightNodeBindings;
            rightChildNode.remainingBinaryCols=newRemainingBinaryCols.slice(0);
            rightChildNode.remainingMultiwayCols=node.remainingMultiwayCols.slice(0);
    
            // tree navigation attributes
            rightChildNode.selectionCriteriaAttribute=col;
            rightChildNode.selectionCriteriaPredicate=''>='';
            rightChildNode.selectionCriteriaValue=medianValue;
    
            node.children.push(leftChildNode);
            nextLevelNodes.push(leftChildNode);
            node.children.push(rightChildNode);
            nextLevelNodes.push(rightChildNode);
          }
          else{
            var newRemainingMultiwayCols=node.remainingMultiwayCols.filter(function(value, index, arr){return value != col;});
            var colDistinctValues=multiwayColumnDistinctValues[col];
            for (var j=0;j<colDistinctValues.length;j++){
              var value=colDistinctValues[j];
              var clause=col+"=''"+value+"''";
    
              var nodeBindings=node.whereClauseBindings.slice(0);
              
              var childNode={}
              // training state
              if(value==null){
                // bindings don''t support null values
                childNode.whereClause=node.whereClause+" and "+col+"=null";            
              }else if(typeof(value)===''undefined''){
                // this should never happen
                throw "Undefined value encountered when building multiway split branches"
              }else{
                childNode.whereClause=node.whereClause+" and "+col+"=?";
                nodeBindings.push(value);
              }
              
              childNode.whereClauseBindings=nodeBindings;
              childNode.remainingBinaryCols=node.remainingBinaryCols.slice(0);
              childNode.remainingMultiwayCols=newRemainingMultiwayCols;
    
              // tree navigation attributes
              childNode.selectionCriteriaAttribute=col;
              childNode.selectionCriteriaPredicate=''='';
              childNode.selectionCriteriaValue=value;
    
    
              node.children.push(childNode);
              nextLevelNodes.push(childNode);
            }
          }
        }
        if (nextLevelNodes.length > 0){
          levelCalc(tableName,nextLevelNodes,target,depth+1,trainingParameters,multiwayColumnDistinctValues);
        }
      }
      var binaryColumns=BINARY_COLS.split('','').filter(function(value, index, arr){return value.length>0;});
      var multiwayColumns=MULTIWAY_COLS.split('','').filter(function(value, index, arr){return value.length>0;});
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
      default_training_parameters.maxFeatures=5;
      default_training_parameters.debugMessages=true;
      default_training_parameters.split_algorithm=''ID3'';
      
      var training_parameters={...default_training_parameters,...TRAINING_PARAMS};
      training_parameters.binaryColumns=binaryColumns;
      training_parameters.multiwayColumns=multiwayColumns;
      
      var runId=results.getColumnValue(1);
      results = snowflake.execute({
        sqlText: "insert into ml_model_runs(model_id,run_id,table_name,algorithm,training_parameters,start_time) select :1,:2,:3,:4,parse_json(''"+JSON.stringify(training_parameters)+"''),current_timestamp::TIMESTAMP_NTZ",
        binds: [MODEL_ID,runId, TABLE_NAME,''decision_tree'']
      });
      
      // For multi-way columns (non-binary), we pre-calculate all distinct values at the outset
      var multiwayColumnDistinctValues={};
      if (multiwayColumns.length>0){
        var distinctValuesQueryComponents=[];
        for (var i=0;i<multiwayColumns.length;i++){
          distinctValuesQueryComponents.push("select distinct "+multiwayColumns[i]+"::varchar,''"+multiwayColumns[i]+"'' as col from "+TABLE_NAME);
        }
        results = snowflake.execute({
          sqlText: distinctValuesQueryComponents.join('' union '')
        });
        multiwayColumns.map(function(c,i){multiwayColumnDistinctValues[c]=[]});
        while (results.next()){
          var value=results.getColumnValue(1);
          var col=results.getColumnValue(2);
          multiwayColumnDistinctValues[col].push(value);
        }
      }
      var treeNodes=[{whereClause:"1=?",whereClauseBindings:[1],remainingBinaryCols:binaryColumns,remainingMultiwayCols:multiwayColumns}]
      levelCalc(TABLE_NAME,treeNodes,TARGET,1,training_parameters,multiwayColumnDistinctValues)
      var rootNode=treeNodes[0];
      // Clean up model object, remove all the state used during tree construction
      removeTrainingState(rootNode);
      results = snowflake.execute({
        sqlText: "update ml_model_runs set end_time=current_timestamp::TIMESTAMP_NTZ, model_object=parse_json(''"+JSON.stringify(rootNode)+"'') where run_id=?",
        binds: [runId]
      });
      return runId;
      ';