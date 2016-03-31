(defproject hdfs-clj "0.1.17-SNAPSHOT"
  :description "A Clojure HDFS library."
  :url "https://github.com/r0man/hdfs-clj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :deploy-repositories [["releases" :clojars]]
  :dependencies [[org.clojure/clojure "1.8.0"]]
  :aliases {"lint" ["do" ["eastwood"]]
            "ci" ["do" ["difftest"] ["lint"]]}
  :eastwood {:exclude-linters [:deprecations]}
  :profiles {:dev {:plugins [[jonase/eastwood "0.2.3"]
                             [lein-difftest "2.0.0"]]}
             :provided {:dependencies [[org.apache.hadoop/hadoop-common "2.7.2"]]}})
