# Adaptive Watermarks: A Concept Drif-based Approach for Predicting Event-Time Progress in Data Streams


This repository contains the source code for the adaptive watermark generator.

This depends on the [ADWIN](https://github.com/abifet/adwin) library. As well as Apache Flink.



## How to run?

Import the maven project into your IDE.

You can unpack the data (csv files) in the data folder to folder on your computer.

You can check the example run in ~\src\main\java\ee\ut\cs\dsg\adaptivewatermark\flink\StreamingJob

You can pass a command line arguments as 
--input "C:\\Work\\Data\\DEBSGC2012TSOnlyMillisRecent.txt" --output "C:\\Work\\Data\\DEBSGC2012TSOnlyMillisRecentOutPeriodic.txt" --adaptive true --allowedLateness 100 --oooThreshold 1.1 --sensitivity 0.01 --sensitivityChangeRate 1.0 --windowWidth 1000 --period 10

 