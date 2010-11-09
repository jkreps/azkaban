(ns com.linkedin.mr-kluj.text-serialization
  (:require
   [com.linkedin.mr-kluj.job :as job]
   [com.linkedin.mr-kluj.utils :as utils])
    (:import
    [org.apache.hadoop.fs Path]
    [org.apache.hadoop.io Text LongWritable]
    [org.apache.hadoop.mapreduce Job]
    [org.apache.hadoop.mapreduce.lib.input FileInputFormat TextInputFormat]
    [org.apache.hadoop.mapreduce.lib.output FileOutputFormat TextOutputFormat]))

(defn de-textify
  "Pulls out the long and String from a text-input-file"
  []
  (job/map-mapper
     (fn [#^LongWritable key #^Text value _]
       [[(.get key) (.toString value)]])))

(defn text-file-input
  "Sets the input of the job to be to a text file using TextInputFormat"
  [path]
  (job/compose-wrappers
    (job/add-config
      (fn [#^Job job]
        (when (nil? path) (throw (RuntimeException. (format "Input on job[%s] cannot be null." (.getJobName job)))))
        (doto job
          (.setInputFormatClass TextInputFormat)
          (FileInputFormat/addInputPaths #^String path))))))

(defn text-file-output
  "Sets the output of the job to be to a text file using TextOutputFormat (which essentially just does a .toString() on
  the key and value"
  [path]
  (job/add-config
    (fn [#^Job job]
      (when (nil? path) (throw (RuntimeException. (format "Output on job[%s] cannot be null." (.getJobName job)))))
      (doto job
        (.setOutputFormatClass TextOutputFormat)
        (FileOutputFormat/setOutputPath (Path. path))))))

(defn cut
  "Applies com.linkedin.mr-kluj.utils/cut to the value"
  [#^String delimiter indexes]
  (job/map-mapper
   (let [cut-fn (utils/cut delimiter indexes)]
     (fn [key #^String value _]
       [[key (cut-fn value)]]))))

(defn deserialize
  "Applies deserialization functions to a vector of values

   deserialization-spec - [[index deser-fn] ...]"
  [deserialization-spec]
  (job/map-mapper
   (let [assoc-fn (apply comp (map (fn [[index deserialization-fn]]
				     (utils/assoc-fn deserialization-fn index))
				   deserialization-spec))]
     (fn [key value _]
       [[key (assoc-fn value)]]))))

(defn cut-and-assoc
  "Similar to cut except the output is a map instead of a vector.  The keys into the map are specified by the index-spec

   index-spec - a [[index name deserialization-fn] ...]"
  [#^String delimiter index-spec]
  (let [indexes (map first index-spec)
	names (map second index-spec)]
    (job/compose-wrappers
     (job/map-mapper
      (fn [key value _]
	[[key (zipmap names value)]]))
     (deserialize (map vector (map first index-spec) (map #(% 2) index-spec)))
     (cut delimiter indexes))))
  



