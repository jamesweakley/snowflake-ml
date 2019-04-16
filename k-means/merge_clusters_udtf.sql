-- The MERGE_CLUSTERS function merges multiple cluster totals and counts into a single object containing cluster centroids
-- Where update_clusters acted like the Map part of a MapReduce, this function is the Reduce side and must be ran without 
-- partitioning so that it yields a single row.
-- Example input:
--+---------------------------------------------------------------------------------------------------------+
--|                                                                                          CLUSTER_TOTALS |
--|---------------------------------------------------------------------------------------------------------|
--|  {0:{x_total:5,y_total:6,count:1},1:{x_total:20,y_total:22,count:3},2:{x_total:45,y_total:60,count:50}} |
--|                                   {0:{x_total:21,y_total:18,count:12},1:{x_total:10,y_total:6,count:1}} |
--+---------------------------------------------------------------------------------------------------------+
-- Output:
-- cluster 0 centroid is x: (5+21)/13 = 2
--                       y: (6+18)/13 = 1.85
-- cluster 1 centroid is x: (20+10)/4 = 7.5
--                       y: (22+6)/13 = 2.15
-- cluster 2 centroid is x:     45/50 = 0.9
--                       y:     60/50 = 1.2
-- result looks like:
--+----------------------------------------------------+
--|                                    MERGED_CLUSTERS |
--|----------------------------------------------------|
--|  {0:{x:2,y:1.85},1:{x:7.5,y:2.15},2:{x:0.9,y:1.2}} |
--+----------------------------------------------------+
create or replace function MERGE_CLUSTERS(CLUSTER_TOTALS variant)
    returns table (MERGED_CLUSTERS variant)
    language javascript
    AS '{
    processRow: function (row, rowWriter, context) {
      for (var clusterId in row.CLUSTER_TOTALS){
        cluster=row.CLUSTER_TOTALS[clusterId];
        this.clusterXTotals[clusterId]=(this.clusterXTotals[clusterId] || 0) + cluster.x_total;
        this.clusterYTotals[clusterId]=(this.clusterYTotals[clusterId] || 0) + cluster.y_total
        this.clusterCounts[clusterId]=(this.clusterCounts[clusterId] || 0) + cluster.count;
      }
    },
    finalize: function (rowWriter, context) {
      var newClusters={}
      for (var clusterId in this.clusterCounts){
        newClusters[clusterId]={}
        if (this.clusterCounts[clusterId]>0){
          newClusters[clusterId].x=(this.clusterXTotals[clusterId]/this.clusterCounts[clusterId]).toPrecision(2);
          newClusters[clusterId].y=(this.clusterYTotals[clusterId]/this.clusterCounts[clusterId]).toPrecision(2);
        }
      }
      rowWriter.writeRow({"MERGED_CLUSTERS": newClusters});
    },
    initialize: function(argumentInfo, context) {
      // each of these variables will contain a map of cluster_no to running total
      this.clusterXTotals={};
      this.clusterYTotals={};
      this.clusterCounts={};
    }}';
    