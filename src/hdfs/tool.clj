(ns hdfs.tool
  (:import org.apache.hadoop.conf.Configuration
           org.apache.hadoop.util.GenericOptionsParser
           org.apache.hadoop.util.ToolRunner))

(defmacro deftool [[tool args] & body]
  (let [clazz (symbol (namespace-munge (str *ns*)))]
    `(do
       (gen-class
        :extends "org.apache.hadoop.conf.Configured"
        :implements ["org.apache.hadoop.util.Tool"]
        :name ~clazz
        :state ~'state
        :init ~'init
        :constructors {[] []}
        :main true)
       (defn ~'-init []
         [[] (atom nil)])
       (defn ~'-run [~tool ~args]
         (let [~args (seq (.getRemainingArgs (GenericOptionsParser. ~args)))
               result# (do ~@body)]
           (if (number? result#)
             result# 0)))
       (defn ~'-main [& ~'args]
         (System/exit
          (ToolRunner/run
           (Configuration.)
           (clojure.lang.Reflector/invokeConstructor
            (resolve (symbol ~(str clazz))) (into-array []))
           (into-array String ~'args))))
       (defn ~'-getConf [~'tool]
         (deref (.state ~'tool)))
       (defn ~'-setConf [~'tool ~'conf]
         (if ~'conf (swap! (.state ~'tool) (constantly ~'conf)))))))

(defn run-tool [clazz & args]
  (.run (clojure.lang.Reflector/invokeConstructor
         (resolve (symbol (str clazz)))
         (into-array []))
        (into-array String args)))