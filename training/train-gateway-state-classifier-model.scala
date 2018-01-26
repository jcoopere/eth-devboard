// Usage: spark-shell -i train-gateway-state-classifier-model.scala --jars "jpmml-sparkml-package/target/jpmml-sparkml-package-1.0-SNAPSHOT.jar" --packages "org.apache.kafka:kafka-clients:0.11.0.0,com.typesafe:config:1.2.1"

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer, RFormula}
import org.apache.spark.sql._

import java.io._
import java.util.Properties

case class LabeledTelemetry(label:Double, togglefour:Double, togglefive:Double, togglesix:Double)

val config = ConfigFactory.parseFile(new File("model-training.conf"))

// Fetch configs.
val sendKafka = config.getBoolean("publish_pmml_to_kafka")
val printPMML = config.getBoolean("print_pmml")
val kafkaBrokerList = config.getString("kafka_broker_list")

// Hardcoded params.
val trainingDataDir = "/training-data/"
val kafkaTopic = "model"

val sqc = new SQLContext(sc)

val data = sc.textFile(trainingDataDir)

// Parse strings into DF
val points = data.map(line => {
	val split = line.split(" ")

	new LabeledTelemetry(split(0).toDouble, split(1).split(":")(1).toDouble, split(2).split(":")(1).toDouble, split(3).split(":")(1).toDouble)
}).toDF

// Formula
val formula = new RFormula().setFormula("label ~ .").setLabelCol("label").setFeaturesCol("features")

// Label indexer
val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(points)

// Random Forest Classifier
val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("features").setNumTrees(10)

// Convert labels
val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

// Build Pipeline
val pipeline = new Pipeline().setStages(Array(formula, labelIndexer, rf, labelConverter))

// Split corpus into training and testing sets
val Array(trainingData, testData) = points.randomSplit(Array(0.7, 0.3))

// Train model
//val model = pipeline.fit(points)
val model = pipeline.fit(trainingData)

// Make predictions on testing set
val predictions = model.transform(testData)

// Evaluate efficacy
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("precision")
val accuracy = evaluator.evaluate(predictions)
println("Test Error = " + (1.0 - accuracy))

if (model != null) {
	// Convert to PMML
	val pmmlBytes = org.jpmml.sparkml.ConverterUtil.toPMMLByteArray(points.schema, model)

	if (printPMML) {
		println(new String(pmmlBytes, "UTF-8"))
	}

	// Send to Kafka
	if (sendKafka) {
		val props = new Properties
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerList)
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

		val producer = new KafkaProducer[String, String](props)

		val message = new ProducerRecord[String, String](kafkaTopic, null, new String(pmmlBytes, "UTF-8"))

		producer.send(message)
		producer.close()
	}
}

:quit
