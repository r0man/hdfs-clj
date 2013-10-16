(defproject hdfs-clj "0.1.9-SNAPSHOT"
  :description "A Clojure HDFS library."
  :url "https://github.com/r0man/hdfs-clj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :lein-release {:deploy-via :clojars}
  :dependencies [[org.clojure/clojure "1.5.1"]]
  :profiles {:provided {:dependencies [[org.apache.hadoop/hadoop-core "0.20.2-cdh3u6"]]}}
  :repositories {"cloudera" "https://repository.cloudera.com/artifactory/cloudera-repos"}  )
