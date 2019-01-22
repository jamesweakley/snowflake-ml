/**
* Score a decision tree using a case statement.
* WARNING, not secure: This needs to be refactored to use bindings instead of string interpolation.
*/
create or replace procedure score_decision_tree(TABLE_NAME VARCHAR, MODEL_OBJECT variant, SCORE_COLUMN VARCHAR)
  returns String not null
  language javascript
  as
  $$  
  
  function buildCaseStatement(modelNode){
    if (JSON.stringify(modelNode)=="{}"){
      throw "theres no attributes on the model object!!";
    }
    if (modelNode.prediction!=null){
      return modelNode.prediction;
    }
    var selectedChild=null;
    if (typeof(modelNode.children)==="undefined"){
      throw "No prediction value, but no children either";
    }
    var caseStatement="case "
    for (var i=0;i<modelNode.children.length;i++){
      var child=modelNode.children[i];
      caseStatement = caseStatement + "when "+child.selectionCriteriaAttribute+" "+child.selectionCriteriaPredicate+" "+child.selectionCriteriaValue+" then ";
      caseStatement = caseStatement + buildCaseStatement(child);
    }
    caseStatement = caseStatement + " end ";

    // none of the nodes matched, arbirarily choose the first one
    selectedChild=modelNode.children[0];
    return caseStatement;
  }
  
  function leafCalc(tableName,whereClause,target,remainingCols,depth,trainingParameters){
    var return_object={"cumulative_where_clause":whereClause};
    var results;
    results = snowflake.execute({
        sqlText: "select stddev("+target+") as target_stddev,"+
                        "avg("+target+") as target_avg,"+
                        "case when target_avg is not null then target_stddev/target_avg*100 else 0 end as coef_of_variation,"+
                        "count(*) as target_count "+
                        "from "+tableName+" where "+whereClause
      });
    results.next();
    var averageBelow=results.getColumnValue(2);
  }
  
  
  var caseStatement=buildCaseStatement(MODEL_OBJECT);
  
  var results = snowflake.execute({
    sqlText: "update "+TABLE_NAME+" set "+SCORE_COLUMN+"="+caseStatement
  });
  return 0;
  $$
  ;
