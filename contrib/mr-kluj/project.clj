(defproject mr-kluj "1.0.0-SNAPSHOT"
  :description "A project to make map reduce jobs in clojure simple(r)"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
		 [log4j/log4j "1.2.15" :exclusions [javax.mail/mail
						    javax.jms/jms
						    com.sun.jdmk/jmxtools
						    com.sun.jmx/jmxri]]
		 [com.google.collections/google-collections "1.0-rc2"]
		 [joda-time/joda-time "1.6"]
		 [commons-lang/commons-lang "2.1"]
		 [commons-logging/commons-logging "1.0.4"]
		 [commons-httpclient/commons-httpclient "3.1"]
		 [azkaban/azkaban-common "0.4-SNAPSHOT"]
		 [org.apache.hadoop/hadoop "0.20.3-dev-core"]
		 [voldemort/voldemort "0.70.1"]]
  :dev-dependencies [[lein-javac "1.2.1-SNAPSHOT"]
		     [swank-clojure "1.2.1"]]
  :source-path "src/main/clj"
  :java-source-path [["src/main/java"]])
