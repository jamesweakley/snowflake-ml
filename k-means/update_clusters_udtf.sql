-- The purpose of UPDATE_CLUSTERS is to assign each input record to the closest cluster out of a list of clusters,
-- and gather information required to recalculate new centroids. This forms part of the k-means clustering algorithm
-- As a User Defined Table Function, it does this by traversing a table, and for each record:
-- 1) Determine which of the clusters in CLUSTER_CENTROIDS it belongs to (closest in 2D distance)
-- 2) Update a running total of values for that cluster, so that the new centroids can be calculated later
-- The reason we output totals in this manner rather than just new centroids, is so that the processing can be parallelized.
-- So UPDATE_CLUSTERS acts like the Map side of a MapReduce.

-- A worked example of a couple of iterations, where the rows looks like this:
--+------+------+----------------------------------------------------------+
--|    X |    Y |                                        CLUSTER_CENTROIDS |
--|------+------+----------------------------------------------------------|
--|    5 |    6 |    {0:{x:4.5,y:7.6}},{1:{x:1.0,y:-3.9},{2:{x:8.4,y:9.0}} |
--|  2.2 | -3.0 |    {0:{x:4.5,y:7.6}},{1:{x:1.0,y:-3.9},{2:{x:8.4,y:9.0}} |
--+------+------+----------------------------------------------------------+
-- Row 1
-- -----
-- Using Euclidian distance formula:
-- - Distance to cluster 0 is 1.6763054614240207 (initial winner)
-- - Distance to cluster 1 is 10.677546534667972 (not closer than current winner)
-- - Distance to cluster 2 is 4.534313619501853 (not closer than current winner)
-- Cluster 0 is the winner, so we add X value (5) and y value (6) to cluster 0's running totals
-- Row 2
-- -----
-- Using Euclidian distance formula:
-- - Distance to cluster 0 is 10.846658471621572 (initial winner)
-- - Distance to cluster 1 is 1.500000000000000 (closer than current winner, so this is the new winner)
-- - Distance to cluster 2 is 13.507035203922436 (not closer than current winner)
-- Cluster 1 is the winner, so we add X value (2.2) and y value (-3.0) to cluster 1's running totals
-- If this were the only row, the final output would be:
--+------------------------------------------------------------------------+
--|                                                     NEW_CLUSTER_TOTALS |
--|------------------------------------------------------------------------|
--| {0:{x_total:5,y_total:6,count:1},1:{x_total:2.2,y_total:-3.0,count:1}} |
--+------------------------------------------------------------------------+
-- Normally there will be many matches for each cluster.
--
-- Note: Cluster centroids are stored in an object rather than an array, so that we can maintain a
-- consistent identifier for each of them. It doesn't serve much purpose currently, but in future
-- we might handle 0-member clusters differently.
create or replace function UPDATE_CLUSTERS(X float, Y float, CLUSTER_CENTROIDS variant)
    returns table (NEW_CLUSTER_TOTALS variant)
    language javascript
    AS '{
    euclidianDistance: function(x1,x2,y1,y2){
      return Math.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2);
    },
    processRow: function (row, rowWriter, context) {
      var clusterIds=Object.keys(row.CLUSTER_CENTROIDS);
      var winningClusterIndex=clusterIds[0];
      let cluster=row.CLUSTER_CENTROIDS[winningClusterIndex];
      let distance;
      let clusterId;
      var winningClusterDistance=this.euclidianDistance(cluster.x,row.X,cluster.y,row.Y);
      // compare all clusters, starting from the second cluster id
      for (var clusterIdIndex=1; clusterIdIndex<clusterIds.length;clusterIdIndex++){
        clusterId=clusterIds[clusterIdIndex];
        cluster_centroid=row.CLUSTER_CENTROIDS[clusterId];
        distance=this.euclidianDistance(cluster_centroid.x,row.X,cluster_centroid.y,row.Y);
        if (distance<winningClusterDistance){
            winningClusterIndex=clusterId;
            winningClusterDistance=distance;
        }
      }
      this.clusterXTotals[winningClusterIndex]=(this.clusterXTotals[winningClusterIndex] || 0) + row.X;
      this.clusterYTotals[winningClusterIndex]=(this.clusterYTotals[winningClusterIndex] || 0) + row.Y
      this.clusterCounts[winningClusterIndex]=(this.clusterCounts[winningClusterIndex] || 0) + 1;
      this.initialClusterCentroids=row.CLUSTER_CENTROIDS;
    },
    finalize: function (rowWriter, context) {
      var newClusters={}
      for (var clusterId in Object.keys(this.initialClusterCentroids)){
        newClusters[clusterId]={}
        newClusters[clusterId].x_total=this.clusterXTotals[clusterId];
        newClusters[clusterId].y_total=this.clusterYTotals[clusterId];
        newClusters[clusterId].count=this.clusterCounts[clusterId];
      }
      rowWriter.writeRow({"NEW_CLUSTER_TOTALS": newClusters});
    },
    initialize: function(argumentInfo, context) {
      // each of these variables will contain a map of cluster_no to running total
      this.clusterXTotals={};
      this.clusterYTotals={};
      this.clusterCounts={};
    }}';
    
    