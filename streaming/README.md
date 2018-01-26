## Streaming job
Spark Streaming job for parsing, denormalizing, and storing Kura protobuf metrics in Kudu.

## Sample Usage
### Build
`$ sbt clean assembly`
### Deploy
`$ spark-submit --master yarn --deploy-mode client --class com.cloudera.demo.iiot.IIoTDemoStreaming target/scala-2.10/iiot-demo-assembly-1.0.jar <kafka-broker-list e.g. broker-1:9092,broker-2:9092> <kudu-master e.g. kudu-master-1:7051,kudu-master-2:7051>`
