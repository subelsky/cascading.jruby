OUTPUT_DIR = 'output'
BUILD_DIR = 'build/spec'

module ScopeTests
  def check_scope(params = {})
    name_params = [params[:source]].compact
    scope = scope(*name_params)
    values_fields = params[:values_fields]
    grouping_fields = params[:grouping_fields] || values_fields

    debug = params[:debug]
    debug_scope(*name_params) if debug

    scope.values_fields.to_a.should == values_fields
    scope.grouping_fields.to_a.should == grouping_fields
  end
end

module Cascading
  class Flow; include ScopeTests; end
  class Assembly; include ScopeTests; end
end

def test_flow(&block)
  cascade = cascade 'test_app' do
    flow 'test', &block
  end
  cascade.complete(cascading_properties)
end

def test_assembly(params = {}, &block)
  branches = params[:branches] || []

  test_flow do
    source 'input', tap('spec/resource/test_input.txt', :kind => :lfs, :scheme => text_line_scheme)

    # Default Fields defined by TextLineScheme
    check_scope :source => 'input', :values_fields => ['offset', 'line']

    assembly 'input', &block

    sink 'input', tap("#{OUTPUT_DIR}/out.txt", :kind => :lfs, :sink_mode => :replace)

    # Branches must be sunk so that they (and their assertions) will be run
    branches.each do |branch|
      sink branch, tap("#{OUTPUT_DIR}/#{branch}_out.txt", :kind => :lfs, :sink_mode => :replace)
    end
  end
end

def test_join_assembly(params = {}, &block)
  branches = params[:branches] || []

  test_flow do
    source 'left', tap('spec/resource/join_input.txt', :kind => :lfs, :scheme => text_line_scheme)
    source 'right', tap('spec/resource/join_input.txt', :kind => :lfs, :scheme => text_line_scheme)

    # Default Fields defined by TextLineScheme
    check_scope :source => 'left', :values_fields => ['offset', 'line']
    check_scope :source => 'right', :values_fields => ['offset', 'line']

    assembly 'left' do
      check_scope :values_fields => ['offset', 'line']
      split 'line', ['x', 'y', 'z'], :pattern => /,/
      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z']
    end

    assembly 'right' do
      check_scope :values_fields => ['offset', 'line']
      split 'line', ['x', 'y', 'z'], :pattern => /,/
      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z']
    end

    assembly 'join' do
      # Empty scope because there is no 'join' source or assembly
      check_scope :values_fields => []

      left_join 'left', 'right', :on => ['x']
      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_'],
        :grouping_fields => ['x']

      instance_eval(&block)
    end

    sink 'join', tap("#{OUTPUT_DIR}/join_out.txt", :kind => :lfs, :sink_mode => :replace)

    # Branches must be sunk so that they (and their assertions) will be run
    branches.each do |branch|
      sink branch, tap("#{OUTPUT_DIR}/#{branch}_out.txt", :kind => :lfs, :sink_mode => :replace)
    end
  end
end

def cascading_properties
  build_dir = "#{BUILD_DIR}/build"
  `mkdir -p #{build_dir}`
  tmp_dir = "#{BUILD_DIR}/tmp"
  `mkdir -p #{tmp_dir}`
  log_dir = "#{BUILD_DIR}/log"
  `mkdir -p #{log_dir}`

  # Local cluster settings
  #java.lang.System.set_property("test.build.data", build_dir)
  #java.lang.System.set_property("hadoop.tmp.dir", tmp_dir)
  #java.lang.System.set_property("hadoop.log.dir", log_dir)
  #conf = Java::OrgApacheHadoopConf::Configuration.new
  #dfs = Java::OrgApacheHadoopDfs::MiniDFSCluster.new(conf, 4, true, nil);
  #file_sys = dfs.file_system
  #mr = Java::OrgApacheHadoopMapred::MiniMRCluster.new(4, file_sys.uri.to_string, 1)
  #job_conf = mr.create_job_conf
  #job_conf.set("mapred.child.java.opts", "-Xmx512m")
  #job_conf.set("mapred.map.tasks.speculative.execution", "false")
  #job_conf.set("mapred.reduce.tasks.speculative.execution", "false")

  job_conf = Java::OrgApacheHadoopMapred::JobConf.new
  job_conf.jar = build_dir
  job_conf.set("test.build.data", build_dir)
  job_conf.set("hadoop.tmp.dir", tmp_dir)
  job_conf.set("hadoop.log.dir", log_dir)

  job_conf.num_map_tasks = 4
  job_conf.num_reduce_tasks = 1

  properties = java.util.HashMap.new({})
  Java::CascadingFlow::MultiMapReducePlanner.set_job_conf(properties, job_conf)
  properties
end
