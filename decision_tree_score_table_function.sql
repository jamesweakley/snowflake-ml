CREATE OR REPLACE FUNCTION decision_tree_score(MODEL_OBJECT variant,ROW_DATA variant)
    RETURNS TABLE (SCORE FLOAT,MODEL variant)
    LANGUAGE JAVASCRIPT
    AS '{
    
    predicateFunction: function (predicate){
      if (predicate=="="){
         return function(a,b){return a == b};
      }
    },
    predict: function (modelNode,row){
      if (JSON.stringify(modelNode)=="{}"){
        throw "theres no attributes on the model object!!";
      }
      if (modelNode.prediction!=null){
        return modelNode.prediction;
      }
      var selectedChild=null;
      if (typeof(modelNode.children)==="undefined"){
      throw JSON.stringify(modelNode);
        throw "No prediction value, but no children either";
      }
      for (var i=0;i<modelNode.children.length;i++){
        var child=modelNode.children[i];
        // evaluate this node to see if it matches
        if (typeof(row[child.selectionCriteriaAttribute])==="undefined"){
          throw "model contains an attribute "+child.selectionCriteriaAttribute+", but the selected table does not contain this column: "+JSON.stringify(Object.getOwnPropertyNames(row));
        }
        var func = this.predicateFunction(child.selectionCriteriaPredicate);
        if (func(row[child.selectionCriteriaAttribute],child.selectionCriteriaValue)){
           selectedChild=child;
           break;
        }
      }
      if (selectedChild==null){
        if (typeof(modelNode.children)==="undefined"){
          return null;
        }
        // none of the nodes matched, arbirarily choose the first one
        selectedChild=modelNode.children[0];
      }
      return this.predict(selectedChild,row);
    },
    processRow: function (row, rowWriter, context) {
         
      this.ccount = this.ccount + 1;
      if (typeof(row.ROW_DATA)==="undefined" || typeof(row.MODEL_OBJECT)==="undefined"){
        return null;
      }
      rowWriter.writeRow({SCORE: this.predict(row.MODEL_OBJECT,row.ROW_DATA),MODEL:row.MODEL_OBJECT});
    },
    finalize: function (rowWriter, context) {
      rowWriter.writeRow({NUM: this.csum});
    },
    initialize: function(argumentInfo, context) {
      this.ccount = 0;
      this.csum = 0;
    }}';
