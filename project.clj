(defproject hdfs-clj "0.1.7"
  :description "A Clojure HDFS library."
  :url "https://github.com/r0man/hdfs-clj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.0"]]
  :profiles {:dev {:dependencies [[org.apache.hadoop/hadoop-core "0.20.2-cdh3u5"]]}}
  :repositories {"cloudera" "https://repository.cloudera.com/artifactory/cloudera-repos"}  )
