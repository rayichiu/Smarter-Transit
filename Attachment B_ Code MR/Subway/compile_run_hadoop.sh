javac -classpath `hadoop classpath` GenSubwayDatasetMapper.java
javac -classpath `hadoop classpath` GenSubwayDatasetReducer.java
javac -classpath `hadoop classpath`:./gson-2.9.0.jar:. GenSubwayDataset.java
jar cvf genSubwayDataset.jar *.class