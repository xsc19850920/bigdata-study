./spark-submit --class com.genpact.housingloanwithspark.bigdata.HousingLoan  --executor-memory 2g --num-executors 3 file:////bigdata/spark/xsc/housingloanwithspark.jar

./spark-submit --class com.genpact.stock.bigdata.StockCalc  --executor-memory 2g --num-executors 3 file:////bigdata/spark/xsc/stockcalc.jar

./spark-submit --class com.genpact.stock.bigdata.StockSearch  --executor-memory 2g --num-executors 3 file:////bigdata/spark/xsc/stockcalc.jar 20180206