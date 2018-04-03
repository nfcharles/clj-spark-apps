(defproject clj-spark-apps "0.1.0-SNAPSHOT"
  :description "TD-IDF Spark application"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [gorillalabs/sparkling "2.1.2"]
                 [org.apache.spark/spark-core_2.11 "2.1.2"]
                 [org.apache.spark/spark-sql_2.11 "2.1.1"]
                 [org.apache.hadoop/hadoop-aws "2.8.3"]
                 [com.fasterxml.jackson.core/jackson-core "2.6.5"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.6.5"]]
  :aot :all
  ;:aot [#".*" sparkling.serialization sparkling.destructuring]  
  :main clj-spark-apps.core
  :target-path "target/%s"
  ;:pom-plugins [[org.apache.maven.plugins/maven-shade-plugin 2.2]]  
  :profiles {:uberjar {:aot :all}
             :dev {:plugins [[lein-dotenv "RELEASE"]]}})
