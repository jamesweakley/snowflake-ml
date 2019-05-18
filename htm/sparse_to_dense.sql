create or replace function SPARSE_TO_DENSE(SPARSE_ARRAY array)
  returns array
  language javascript
  immutable
as
$$
  return SPARSE_ARRAY.map(function(element, index){
    if (element==1){
        return index;
    }
  }).filter(function(element){return typeof(element)!=='undefined'});
$$
;
