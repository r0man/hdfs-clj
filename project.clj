(defproject hdfs-clj "0.1.15"
  :description "A Clojure HDFS library."
  :url "https://github.com/r0man/hdfs-clj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :lein-release {:deploy-via :clojars}
  :dependencies [[org.clojure/clojure "1.8.0"]]
  :profiles {:provided {:dependencies [[org.apache.hadoop/hadoop-common "2.7.2"]]}})
