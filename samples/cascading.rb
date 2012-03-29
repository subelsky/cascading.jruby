module Cascading
  # Constructs properties to be passed to Flow#complete or Cascade#complete
  # which will locate temporary Hadoop files in build/sample.  It is necessary
  # to pass these properties only because the sample apps are invoked using
  # JRuby's main method, which confuses the JobConf's attempt to find the
  # containing jar.
  def sample_properties
    dirs = {
      'test.build.data' => 'build/sample/build',
      'hadoop.tmp.dir' => 'build/sample/tmp',
      'hadoop.log.dir' => 'build/sample/log',
    }
    dirs.each{ |key, dir| `mkdir -p #{dir}` }

    job_conf = Java::OrgApacheHadoopMapred::JobConf.new
    job_conf.jar = dirs['test.build.data']
    dirs.each{ |key, dir| job_conf.set(key, dir) }

    job_conf.num_map_tasks = 1
    job_conf.num_reduce_tasks = 1

    properties = java.util.HashMap.new
    Java::CascadingFlowHadoop::HadoopPlanner.copy_job_conf(properties, job_conf)
    properties
  end
end
