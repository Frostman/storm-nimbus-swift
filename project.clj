(defproject storm-nimbus-swift "0.1.0-wip1"
  :source-path "src/clj"
  :test-path "test/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :dependencies [
                  [org.jclouds.api/swift "1.5.5"]
                  [org.jclouds.driver/jclouds-slf4j "1.5.5"]
                  ]
  :dev-dependencies [
                      [org.clojure/clojure "1.4.0"]
                      [org.clojars.frostman/storm "0.8.2-wip15-storage"]
                      ]
  :aot :all )
