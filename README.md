STIndexing source code github -> https://github.com/petrospgithub/STIndexing

mvn install:install-file -Dfile=<path>/STIndexing-jar-with-dependencies.jar -DgroupId=di.thesis -DartifactId=stindexing -Dversion=1.0 -Dpackaging=jar

Java 1.8
Scala 2.11.8 must be install in Hive (scala-library jar @ $HIVE_HOME/lib/)

Hive SpatioTemporal UDFs 

Define auxiliary jars directory or add jar @ $HIVE_HOME/lib/

Function List:

    -Start Point (trajectory)
    -End Point (trajectory)
    -Trajectory2Linestring(trajectory) - (for intergration with GIS tools for visualization)
    -MBB, create trajectory mbb or give 6 input variables (min longitude, max longitude, min latitude, max latitude, min timestamp, max timestamp)
    -DTW (trajectory, trajectory, parameters for spatiotemporal flexibility)
    -LCSS (trajectory, trajectory, parameters for spatiotemporal flexibility)
    -SP_IndexIntersects(trajectory or box, tree) (spatial range query based on JTS R-tree index)
    -Duratiοn (trajectory)
    -Length (trajectory)
    -Spatiotemporal intersects (trajectory or box, box)
    -TODO add knn, intersects based on 3d index!