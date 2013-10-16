(ns hdfs.core-test
  (:import [java.io BufferedReader BufferedWriter InputStreamReader OutputStreamWriter PrintWriter]
           [org.apache.hadoop.fs LocalFileSystem FileStatus FileSystem FSDataInputStream FSDataOutputStream Path]
           [org.apache.hadoop.io.compress CompressionCodec CompressionCodecFactory BZip2Codec GzipCodec]
           org.apache.hadoop.conf.Configuration
           org.apache.hadoop.io.LongWritable
           org.apache.hadoop.io.SequenceFile$Writer)
  (:refer-clojure :exclude [spit slurp])
  (:use [clojure.string :only (join)]
        clojure.test
        hdfs.core))

(deftest test-buffered-reader
  (let [reader (buffered-reader "project.clj")]
    (is (instance? BufferedReader reader))))

(deftest test-buffered-writer
  (let [writer (buffered-writer "/tmp/buffered-writer")]
    (is (instance? BufferedWriter writer))))

(deftest test-configuration
  (is (instance? Configuration (configuration))))

(deftest test-print-writer
  (let [writer (print-writer "/tmp/print-writer")]
    (is (instance? PrintWriter writer))))

(deftest test-compression-codec
  (is (nil? (compression-codec "/tmp/seed.txt")))
  (is (instance? GzipCodec (compression-codec "/tmp/seed.gz")))
  (is (instance? BZip2Codec (compression-codec "/tmp/seed.bz2"))))

(deftest test-copy-from-local-file
  (let [source (make-path "project.clj")
        target (make-path "/tmp/test-copy-from-local-file")]
    (is (= [source target] (copy-from-local-file source target)))))

(deftest test-copy-to-local-file
  (let [source (make-path "project.clj")
        target (make-path "/tmp/test-copy-to-local-file")]
    (is (= [source target] (copy-to-local-file source target)))))

(deftest test-copy-merge
  (make-directory "/tmp/test-copy-merge/in")
  (spit "/tmp/test-copy-merge/in/part-00000" "1\n")
  (spit "/tmp/test-copy-merge/in/part-00001" "2\n")
  (copy-merge "/tmp/test-copy-merge/in" "/tmp/test-copy-merge/out" :overwrite true)
  (let [content (slurp "/tmp/test-copy-merge/out")]
    ;; TODO: Control order?
    (is (or (= "1\n2\n" content)
            (= "2\n1\n" content))))
  (copy-merge "/tmp/test-copy-merge/in" "/tmp/test-copy-merge/out" :overwrite true :header "a\n")
  (let [content (slurp "/tmp/test-copy-merge/out")]
    (is (or (= "a\n1\n2\n" content)
            (= "a\n2\n1\n" content)))))

(deftest test-crc-filename
  (is (= "/tmp/.0ac4d9d8-5dfe-4c37-980f-5bf4f5ced2e2.crc"
         (crc-filename "/tmp/0ac4d9d8-5dfe-4c37-980f-5bf4f5ced2e2"))))

(deftest test-input-stream
  (let [stream (input-stream "project.clj")]
    (is (instance? FSDataInputStream stream))))

(deftest test-output-stream
  (let [stream (output-stream "/tmp/output-stream")]
    (is (instance? FSDataOutputStream stream))))

(deftest test-exists?
  (let [path "/tmp/test-exists?"]
    (delete path)
    (is (not (exists? path)))
    (spit path "x")
    (is (exists? path))) )

(deftest test-delete
  (let [path "/tmp/test-delete"]
    (is (not (delete path)))
    (spit path "x")
    (is (delete path))))

(deftest test-directory?
  (let [path "/tmp/test-directory?"]
    (delete path)
    (is (not (directory? path)))
    (make-directory path)
    (is (directory? path))) )

(deftest test-file?
  (let [path "/tmp/test-file?"]
    (delete path)
    (is (not (file? path)))
    (spit path "x")
    (is (file? path))) )

(deftest test-glob-status
  (let [directory "/tmp/test-glob-status"]
    (make-directory directory)
    (write-lines (str directory "/part-00000") "12")
    (write-lines (str directory "/part-00001") "34")
    (let [status (glob-status (str directory "/*"))]
      (is (= 2 (count status)))
      (is (every? #(instance? FileStatus %1) status)))))

(deftest test-glob-paths
  (let [directory "/tmp/test-glob-paths"]
    (make-directory directory)
    (write-lines (str directory "/part-00000") "12")
    (write-lines (str directory "/part-00001") "34")
    (let [status (glob-paths (str directory "/*"))]
      (is (= 2 (count status)))
      (is (every? #(instance? Path %1) status)))))

(deftest test-file-status
  (is (instance? FileStatus (file-status "project.clj"))))

(deftest test-file-size
  (is (pos? (file-size "project.clj"))))

(deftest test-list-file-status
  (delete "/tmp/test-list-file-status")
  (make-directory "/tmp/test-list-file-status")
  (make-directory "/tmp/test-list-file-status/1")
  (make-directory "/tmp/test-list-file-status/2")
  (spit "/tmp/test-list-file-status/1/a.txt" "a")
  (spit "/tmp/test-list-file-status/2/b.txt" "b")
  (let [status (list-file-status "/tmp/test-list-file-status")]
    (is (= 2 (count status)))
    (is (every? #(instance? FileStatus %1) status))
    (is (= #{"file:/tmp/test-list-file-status/2"
             "file:/tmp/test-list-file-status/1"}
           (set (map  #(str (.getPath %1)) status)))))
  (let [status (list-file-status "/tmp/test-list-file-status" true)]
    (is (= 4 (count status)))
    (is (every? #(instance? FileStatus %1) status))
    (is (= #{"file:/tmp/test-list-file-status/2"
             "file:/tmp/test-list-file-status/2/b.txt"
             "file:/tmp/test-list-file-status/1"
             "file:/tmp/test-list-file-status/1/a.txt"}
           (set (map  #(str (.getPath %1)) status))))))

(deftest test-make-path
  (is (thrown? IllegalArgumentException (make-path nil)))
  (is (thrown? IllegalArgumentException (make-path "")))
  (let [path (make-path "/tmp")]
    (is (instance? Path path))
    (is (= "tmp" (.getName path))))
  (is (= (make-path "/tmp") (make-path (make-path "/tmp")))))

(deftest test-make-directory
  (is (make-directory "/tmp/make-directory"))
  (is (make-directory "/tmp/make-directory"))
  (is (make-directory "/tmp/make-directory/and-sub-directories")))

(deftest test-make-parents
  (delete "/tmp/test-make-parents")
  (is (not (make-parents "/")))
  (is (make-parents "/tmp/test-make-parents"))
  (is (not (exists? "/tmp/test-make-parents")))
  (is (make-parents "/tmp/test-make-parents/sub1/sub2"))
  (is (exists? "/tmp/test-make-parents"))
  (is (exists? "/tmp/test-make-parents/sub1")))

(deftest test-filesystem
  (let [filesystem (filesystem "/tmp")]
    (is (instance? LocalFileSystem filesystem)))
  (let [filesystem (filesystem "file://./tmp")]
    (is (instance? LocalFileSystem filesystem))))

(deftest test-path?
  (is (not (path? nil)))
  (is (not (path? "")))
  (is (not (path? "/tmp")))
  (is (path? (make-path "/tmp"))))

(deftest test-read-lines
  (let [path "/tmp/test-read-lines"]
    (spit path "1\n2\n")
    (is (= ["1" "2"] (read-lines path)))))

(deftest test-part-file-seq
  (make-directory "/tmp/test-part-file-seq/logs")
  (spit "/tmp/test-part-file-seq/part-00001" "1\n")
  (spit "/tmp/test-part-file-seq/part-00002" "1\n")
  (let [paths (map str (part-file-seq "/tmp/test-part-file-seq"))]
    (is (= 2 (count paths)))
    (is (contains? (set paths) "file:/tmp/test-part-file-seq/part-00001"))
    (is (contains? (set paths) "file:/tmp/test-part-file-seq/part-00002"))))

(deftest test-sequence-file-writer
  (let [path "/tmp/test-sequence-file-writer"]
    (is (instance? SequenceFile$Writer (sequence-file-writer path LongWritable LongWritable)))))

(deftest test-spit
  (let [file "/tmp/test-spit"]
    (spit file "x")
    (is (= "x" (clojure.core/slurp file)))))

(deftest test-slurp
  (let [file "/tmp/test-slurp"]
    (clojure.core/spit file "x")
    (is (= "x" (slurp file)))))

(deftest test-with-fs-tmp
  (let [files (atom {})]
    (with-fs-tmp [fs file-1 file-2]
      (is (string? file-1))
      (is (string? file-2))
      (spit file-1 "file-1")
      (spit file-2 "file-2")
      (swap! files assoc :file-1 file-1)
      (swap! files assoc :file-2 file-2))
    (let [{:keys [file-1 file-2]} @files]
      (is (not (exists? file-1)))
      (is (not (exists? file-2))))))
