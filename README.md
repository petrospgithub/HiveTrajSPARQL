STIndexing source code github -> https://github.com/petrospgithub/STIndexing

mvn install:install-file -Dfile=<path>/STIndexing-jar-with-dependencies.jar -DgroupId=di.thesis -DartifactId=stindexing -Dversion=1.0 -Dpackaging=jar

Java 1.8
Scala 2.11.8 must be install in Hive (scala-library jar @ $HIVE_HOME/lib/)

Hive SpatioTemporal UDFs 

Define auxiliary jars directory or add jar @ $HIVE_HOME/lib/

Function List:

    -StartPoint(trajectory) //returns first point of trajectory
    -EndPoint(trajectory) //returns last point of trajectory
    -TrajLength(trajectory, 'Euclidean') //returns trajectory length based on distance function (Havershine, Euclidean, Manhattan)
    -Trajectory2Linestring(trajectory) //returns trajectory as linestring (spatial only) (for intergration with GIS tools for visualization)
    -TrajDuration(trajectory) //return trajectory duration in seconds
    -ST_Intersects3D (trajectory || STmbb, trajectory || STmbb, tolerance variables) // input trajectory and mbb or mbb and mbb, returns true if objects spatiotemporal intersects
                                                                        //tolerance variables used to extent mbb
    -MbbConstructor(trajectory || Spatiotemporal elements*) // returns Spatiotemporal MBB
    -TrajBoxDist(trajectory, STmbb) //returns spatial distance between trajectory and mbb based on MinDist function
    -DTW(trajectory, trajectory, w(algorithm parameter), distance_function, parameters for spatiotemporal tolerance) // distance between trajectories based on DTW algorithm
    -LCSS(trajectory, trajectory, parameters for spatiotemporal tolerance) // distance between trajectories based on LCSS algorithm
    -ST_IndexIntersects(STmbb, tree, tolerance variables) // returns id for trajectories that intersects with query=STmbb
    -IndexTrajKNN(trajectory, tree, mindist_threshold, temproal tolerance) // returns id for possible knn trajectories with query=trajectory
   