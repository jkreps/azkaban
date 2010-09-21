;Copyright 2010 LinkedIn, Inc
;
;Licensed under the Apache License, Version 2.0 (the "License"); you may not
;use this file except in compliance with the License. You may obtain a copy of
;the License at
;
;http://www.apache.org/licenses/LICENSE-2.0
;
;Unless required by applicable law or agreed to in writing, software
;distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
;WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
;License for the specific language governing permissions and limitations under
;the License.


(ns com.linkedin.mr-kluj.hadoop-utils
  (:import
   [org.apache.hadoop.fs FileSystem Path FileStatus]
   [org.apache.hadoop.io BytesWritable Text SequenceFile$Reader]
   [org.apache.hadoop.conf Configuration]
   [voldemort.serialization.json JsonTypeSerializer]))

(defn spider-path
  [#^FileSystem fs path]
  (let [path (Path. path)]
    (if (.isDir (.getFileStatus fs path))
      (mapcat (comp (partial spider-path fs) #(.toString #^Path %) #(.getPath #^FileStatus %)) (.listStatus fs path))
      [path])))

(defn hdfs-seq
  ([path]
     (hdfs-seq path {}))
  ([path
    {#^Configuration config :config
     key-writable :key-writable
     val-writable :val-writable
     key-getter :key-getter
     val-getter :val-getter
     :or {config (Configuration.)
	  key-writable (BytesWritable.)
	  val-writable (BytesWritable.)
	  key-getter (fn [#^BytesWritable in] (.getBytes in))
	  val-getter key-getter}}]
     (let [fs (FileSystem/get config)]
       ((fn sally [file-paths]
	  (if (empty? file-paths)
	    nil
	    (concat (let [hdfs-reader (org.apache.hadoop.io.SequenceFile$Reader. fs (first file-paths) config)]
		      ((fn billy []
			 (lazy-seq (if-let [has-next (.next hdfs-reader key-writable val-writable)]
				     (cons [(key-getter key-writable) (val-getter val-writable)]
					   (billy))
				     (do (.close hdfs-reader) nil))))))
		    (sally (rest file-paths)))))
	(spider-path fs path)))))

