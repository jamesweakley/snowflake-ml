/**
* K-Means algorithm.
* WARNING, not securable at the stored proc level due to its use of string interpolation in queries.
*/
create or replace procedure k_means(TABLE_NAME varchar, COLUMN_NAMES varchar, CLUSTER_INDEX_COLUMN_NAME varchar, CLUSTER_COUNT float, ITERATIONS float)
  returns String not null
  language javascript
  as
  $$  
  var columnNamesArray = COLUMN_NAMES.split(",");
  if (columnNamesArray.length != 2){
    throw "k_means currently only supports two dimensions";
  }
  // First, clear out any existing centroids for this table+column combo
  var results = snowflake.execute({
    sqlText: "delete from cluster_centroids where TABLE_NAME='"+TABLE_NAME+"' and COLUMNS='"+COLUMN_NAMES+"'"
  });
  
  // next, select random rows to use as initial centroid values
  results = snowflake.execute({
    sqlText: "select "+COLUMN_NAMES+" from "+TABLE_NAME+" sample ("+CLUSTER_COUNT+" rows)"
  });
  var clusterCentroids = {};
  var cluster_index = 0;
  while (results.next())  {
    clusterCentroids[cluster_index] = {};
    clusterCentroids[cluster_index]['x'] = results.getColumnValue(columnNamesArray[0]);
    clusterCentroids[cluster_index]['y'] = results.getColumnValue(columnNamesArray[1]);
    cluster_index++;
  }
  
  // Store the initial centroids in the tracking table
  snowflake.execute({
    sqlText: "insert into cluster_centroids (TABLE_NAME,COLUMNS,CENTROIDS) select '"+TABLE_NAME+"','"+COLUMN_NAMES+"',PARSE_JSON('"+JSON.stringify(clusterCentroids)+"')"
  });
  
  // Now iterate through and update the centroids
  for (var i=0;i<ITERATIONS;i++){
  
    // Assignment step: Assign each observation to the cluster whose mean has the least squared Euclidean distance, this is intuitively the "nearest" mean
    var assignmentQuery = ""+
      "  select MERGED_CLUSTERS"+
      "  FROM \""+TABLE_NAME+"\","+
      "  TABLE(UPDATE_CLUSTERS(\""+columnNamesArray[0]+"\", "+
      "                      \""+columnNamesArray[1]+"\", PARSE_JSON('"+JSON.stringify(clusterCentroids)+"')"+
      "                     )"+
      ") as update_clusters_result, "+
      "TABLE(MERGE_CLUSTERS(update_clusters_result.NEW_CLUSTER_TOTALS) over (partition by null)) as merge_clusters_result ";    
    results = snowflake.execute({
      sqlText: assignmentQuery
    });
    if (!results.next()){
      throw "No results returned from assignment query";
    }
    clusterCentroids = results.getColumnValue("MERGED_CLUSTERS");
    var updateCentroidsQuery = "update CLUSTER_CENTROIDS "+
                                    "set CENTROIDS = PARSE_JSON('"+JSON.stringify(clusterCentroids)+"') where TABLE_NAME='"+TABLE_NAME+"' and COLUMNS='"+COLUMN_NAMES+"'";
    snowflake.execute({
      sqlText: updateCentroidsQuery
    });

    var updateSourceTableQuery = "update \""+TABLE_NAME+"\" "+
                                    "set \""+CLUSTER_INDEX_COLUMN_NAME+"\"=WHICH_CLUSTER(\""+columnNamesArray[0]+"\",\""+columnNamesArray[1]+"\",PARSE_JSON('"+JSON.stringify(clusterCentroids)+"'));";
    snowflake.execute({
      sqlText: updateSourceTableQuery
    });


    //throw JSON.stringify(clusterCentroids);


    // Update step: Calculate the new means (centroids) of the observations in the new clusters.
    var updateCentroidsQuery =  "  with x as ( "+
                                "    select cluster_index,object_construct('x',avg(\""+columnNamesArray[0]+"\")::string,'y',"+
                                                                              "avg(\""+columnNamesArray[1]+"\")::string) as cluster_centroids"+
                                "  from \""+TABLE_NAME+"\" "+
                                "  group by cluster_index"+
                                "  )"+
                                "  select object_agg(cluster_index,cluster_centroids) as NEW_CLUSTERS from x";
    results = snowflake.execute({
      sqlText: updateCentroidsQuery
    });
    if (!results.next()){
      throw "No results returned from update query";
    }
    clusterCentroids = results.getColumnValue("NEW_CLUSTERS");
    
  }
  
  return "success";
  $$
  ;