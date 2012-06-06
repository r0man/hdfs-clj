(ns hdfs.core
  (:import [java.io BufferedReader BufferedWriter InputStreamReader OutputStreamWriter PrintWriter]
           [org.apache.hadoop.fs FileSystem FSDataInputStream FSDataOutputStream Path]
           [org.apache.hadoop.io.compress CompressionCodec CompressionCodecFactory]
           org.apache.hadoop.conf.Configuration
           org.apache.hadoop.io.SequenceFile$Reader
           org.apache.hadoop.util.ReflectionUtils)
  (:use [clojure.string :only (join split)]))

(defn ^Path make-path
  "Returns a org.apache.hadoop.fs.Path, passing each arg to
make-path. Multiple-arg versions treat the first argument as parent
and subsequent args as children relative to the parent."
  ([arg]
     (Path. (str arg)))
  ([parent child]
     (Path. ^Path (make-path parent) ^String (str child)))
  ([parent child & more]
     (reduce make-path (make-path parent child) more)))

(defn ^FileSystem filesystem
  "Returns the Hadoop filesystem from `path`."
  [path & configuration]
  (FileSystem/get (.toUri (make-path path)) (or configuration (Configuration.))))

(defn make-directory
  "Make the given path and all non-existent parents into directories."
  [path] (.mkdirs (filesystem path) (make-path path)))

(defn exists?
  "Returns true if `path` exists, otherwise false."
  [path] (.exists (filesystem path) (make-path path)))

(defn delete
  "Delete `path` recursively."
  [path] (.delete (filesystem path) (make-path path)))

(defn directory?
  "Returns true if `path` is a directory, otherwise false."
  [path] (.isDirectory (filesystem path) (make-path path)))

(defn file?
  "Returns true if `path` is a file, otherwise false."
  [path] (.isFile (filesystem path) (make-path path)))

(defn ^CompressionCodec compression-codec
  "Returns the compression codec for path."
  [path] (.getCodec (CompressionCodecFactory. (Configuration.)) (make-path path)))

(defn glob-status
  "Return all the files that match `pattern` and are not checksum files."
  [pattern]
  (let [path (make-path pattern)]
    (seq (.globStatus (filesystem path) path))))

(defn file-status
  "Returns a file status object that represents the path."
  [path]
  (let [path (make-path path)]
    (.getFileStatus (filesystem path) path)))

(defn file-size
  "Returns the file size of `path`."
  [path]
  (if-let [status (file-status path)]
    (.getLen status)))

(defn list-file-status
  "List the status of all files in directory."
  [path & [recursively]]
  (if recursively    
    (mapcat #(if (directory? (.getPath %1))
               (cons %1 (list-file-status (.getPath %1) true))
               [%1])
            (list-file-status path false))
    (seq (.listStatus (filesystem path) (make-path path)))))

(defn ^FSDataInputStream input-stream
  "Open `path` and return a FSDataInputStream."
  [path]
  (let [input-stream (.open (filesystem path) (make-path path))]
    (if-let [codec (compression-codec path)]
      (.createInputStream codec input-stream)
      input-stream)))

(defn ^FSDataOutputStream output-stream
  "Open `path` and return a FSDataOutputStream."
  [path]
  (let [output-stream (.create (filesystem path) (make-path path))]
    (if-let [codec (compression-codec path)]
      (.createOutputStream codec output-stream)
      output-stream)))

(defn ^BufferedReader buffered-reader
  "Open `path` and return a BufferedReader."
  [path] (BufferedReader. (InputStreamReader. (input-stream path))))

(defn ^BufferedWriter buffered-writer
  "Open `path` and return a BufferedWriter."
  [path] (BufferedWriter. (OutputStreamWriter. (output-stream path))))

(defn path?
  "Returns true if `arg` is a Path, otherwise false."
  [arg] (instance? Path arg))

(defn ^PrintWriter print-writer
  "Open `path` and return a BufferedWriter."
  [path] (PrintWriter. (output-stream path)))

(defn read-lines
  "Read lines from `input`."
  [input] (line-seq (buffered-reader input)))

(defn rename
  "Renames `source` to Path `destination`."
  [source destination & [overwrite]]
  (let [source (make-path source)
        destination (make-path destination)]
    (if (and overwrite (exists? destination))
      (delete destination))
    (.rename (filesystem source) source destination)))

(defn write-lines
  "Write `lines` to `output`."
  [output lines & [transform-fn]]
  (let [transform-fn (or transform-fn identity)]
    (with-open [writer (print-writer output)]
      (dorun (map #(.println writer (transform-fn %1)) lines)))))

(defn sequence-file-seq
  "Returns the content of a sequence file as a lazy seq of key value
  tuples."
  [path]
  (let [config (Configuration.)
        path (make-path path)
        reader (SequenceFile$Reader. (filesystem path) path config)]
    (letfn [(read []
              (lazy-seq
               (let [key (ReflectionUtils/newInstance (.getKeyClass reader) config)
                     value (ReflectionUtils/newInstance (.getValueClass reader) config)]
                 (if (.next reader key value)
                   (cons [key value] (read))
                   (do (.close reader) nil)))))]
      (read))))
