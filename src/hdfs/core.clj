(ns hdfs.core
  (:import [java.io BufferedReader BufferedWriter InputStreamReader OutputStreamWriter PrintWriter]
           [org.apache.hadoop.fs FileSystem FileUtil FSDataInputStream FSDataOutputStream LocalFileSystem Path]
           [org.apache.hadoop.io.compress CompressionCodec CompressionCodecFactory]
           org.apache.hadoop.conf.Configuration
           org.apache.hadoop.io.SequenceFile$Reader
           org.apache.hadoop.io.SequenceFile$Writer
           org.apache.hadoop.util.ReflectionUtils)
  (:refer-clojure :exclude [spit slurp])
  (:require [clojure.java.io :as io :refer [file delete-file]]
            [clojure.string :refer [join split]]))

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

(defn ^Configuration configuration
  "Returns the Hadoop configuration."
  [] (Configuration.))

(defn ^FileSystem filesystem
  "Returns the Hadoop filesystem from `path`."
  [path & [config]]
  (FileSystem/get (.toUri (make-path path)) (or config (configuration))))

(defn make-directory
  "Make the given path and all non-existent parents into directories."
  [path] (.mkdirs (filesystem path) (make-path path)))

(defn make-parents
  "Make the given path and all non-existent parents into directories."
  [path & [permissions]]
  (if-let [parent (.getParent (make-path path))]
    (if permissions
      (.mkdirs (filesystem parent) parent permissions)
      (.mkdirs (filesystem parent) parent))))

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
  [path] (.getCodec (CompressionCodecFactory. (configuration)) (make-path path)))

(defn delete-all-fs [fs paths]
  (dorun
   (for [t paths]
     (.delete fs (make-path t) true))))

(defmacro with-fs-tmp
  "Generates unique, temporary path names as subfolders of <root>/cascalog_reserved.
  <root> by default will be '/tmp', but you can configure it via the
  JobConf property `cascalog.io/tmp-dir-property`."
  [[fs-sym & tmp-syms] & body]
  (let [tmp-root (gensym "tmp-root")]
    `(let [config# (configuration)
           ~fs-sym (FileSystem/get config#)
           ~tmp-root (.get config# "hadoop.tmp.dir" "/tmp")
           ~@(mapcat (fn [t]
                       [t `(str ~tmp-root "/" (java.util.UUID/randomUUID))])
                     tmp-syms)]
       (.mkdirs ~fs-sym (make-path ~tmp-root))
       (try
         ~@body
         (finally
           (delete-all-fs ~fs-sym ~(vec tmp-syms)))))))

(defn copy-from-local-file
  "Copy the local file from `source` to `destination`."
  [source destination & {:keys [overwrite]}]
  (let [source (make-path source)
        destination (make-path destination)]
    (.copyFromLocalFile (filesystem destination) (or overwrite false) source destination)
    [source destination]))

(defn copy-to-local-file
  "Copy the file from `source` to `destination` on the local filesystem."
  [source destination]
  (let [source (make-path source)
        destination (make-path destination)]
    (.copyToLocalFile (filesystem source) source destination)
    [source destination]))

(defn crc-path
  "Returns the path to the CRC file of `filename`."
  [filename]
  (let [path (make-path filename)]
    (if-let [parent (.getParent path)]
      (make-path parent (str "." (.getName path) ".crc")))))

(defn crc-filename
  "Returns the filename to the CRC file of `filename`."
  [filename]
  (if-let [path (crc-path filename)]
    (str path)))

(defn glob-status
  "Return all the files that match `pattern` and are not checksum files."
  [pattern]
  (let [path (make-path pattern)]
    (seq (.globStatus (filesystem path) path))))

(defn glob-paths
  "Return a seq of Path objects that match `pattern` and are not checksum files."
  [pattern] (map #(.getPath %1) (glob-status pattern)))

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

(defn part-file-seq
  "Returns a seq of part files in `directory`"
  [directory & [recursively]]
  (for [status (list-file-status directory recursively)
        :let [path (.getPath status)]
        :when (and (not (directory? path))
                   (re-matches #".*/part-.*" (str path)))]
    path))

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

(defn copy-merge
  "Copy all files in `source-dir` to the output file `destination`."
  [source-dir destination & {:keys [header delete-source overwrite]}]
  (let [source-fs (filesystem source-dir)
        destination-fs (filesystem destination)]
    (when overwrite
      (delete destination))
    (with-fs-tmp [fs tmp-destination]
      (let [tmp-destination-fs (filesystem tmp-destination)]
        (FileUtil/copyMerge
         source-fs (make-path source-dir)
         tmp-destination-fs (make-path tmp-destination)
         (or delete-source false)
         (configuration)
         nil))
      (with-open [input (input-stream tmp-destination)
                  output (output-stream destination)]
        (if header (.writeBytes output header))
        (io/copy input output)))))

(defn copy-matching
  "Copy all files matching `pattern` to the output file `destination`."
  [pattern destination & {:keys [header delete-source overwrite]}]
  (when overwrite (delete destination))
  (with-open [output (output-stream destination)]
    (if header (.writeBytes output header))
    (doseq [status (glob-status pattern)
            :when (not (.isDir status))]
      (with-open [input (input-stream (.getPath status))]
        (io/copy input output))
      (if delete-source (delete (.getPath status))))))

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

(defn ^SequenceFile$Writer sequence-file-writer
  "Returns a sequence file writer for `key` and `val` classes."
  [path key val]
  (SequenceFile$Writer.
   (filesystem path)
   (configuration)
   (make-path path)
   key val))

(defn sequence-file-seq*
  "Returns the content of the sequence file at `path` as a lazy seq."
  [path]
  (let [config (configuration)
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

(defn sequence-file-seq
  "Returns the concatenation of the content of all sequence files that
  match `pattern` as a lazy seq."
  [path] (apply concat (map sequence-file-seq* (glob-paths path))))

(defn spit [f content & opts]
  (with-open [w (output-stream f)]
    (.writeBytes w content)))

(defn slurp [f & opts]
  (with-open [r (input-stream f)]
    (clojure.core/slurp r)))
