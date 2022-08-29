hadoop fs -rm -r /user/pc3095/projectMR/output*
javac -classpath `hadoop classpath` YellowTaxi.java YellowTaxiMapper.java
jar cvf YellowTaxi.jar YellowTaxi*.class
hadoop jar YellowTaxi.jar YellowTaxi /user/pc3095/projectMR/*/ /user/pc3095/projectMR/output
