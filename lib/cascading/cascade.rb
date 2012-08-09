require 'cascading/base'
require 'yaml'

module Cascading
  class Cascade < Cascading::Node
    extend Registerable

    attr_reader :mode

    # Builds a cascade given the specified name.  Optionally accepts a :mode
    # which will be used as the default mode for all child flows.  See
    # Cascading::Mode.parse for details.
    def initialize(name, params = {})
      @mode = params[:mode]
      super(name, nil) # A Cascade cannot have a parent
      self.class.add(name, self)
    end

    # Builds a child flow given a name and block.  Optionally accepts a :mode,
    # which will override the default mode stored in this cascade.
    def flow(name, params = {}, &block)
      raise "Could not build flow '#{name}'; block required" unless block_given?
      params[:mode] ||= mode
      flow = Flow.new(name, self, params)
      add_child(flow)
      flow.instance_eval(&block)
      flow
    end

    def describe(offset = '')
      "#{offset}#{name}:cascade\n#{child_names.map{ |child| children[child].describe("#{offset}  ") }.join("\n")}"
    end

    def draw(dir, properties = nil)
      @children.each do |name, flow|
        flow.connect(properties).writeDOT("#{dir}/#{name}.dot")
      end
    end

    def sink_metadata
      @children.inject({}) do |sink_fields, (name, flow)|
        sink_fields[name] = flow.sink_metadata
        sink_fields
      end
    end

    def write_sink_metadata(file_name)
      File.open(file_name, 'w') do |file|
        YAML.dump(sink_metadata, file)
      end
    end

    def complete(properties = nil)
      begin
        Java::CascadingCascade::CascadeConnector.new.connect(name, make_flows(@children, properties)).complete
      rescue NativeException => e
        raise CascadingException.new(e, 'Error completing cascade')
      end
    end

    private

    def make_flows(flows, properties)
      flow_instances = flows.map do |name, flow|
        cascading_flow = flow.connect(properties)
        flow.listeners.each { |l| cascading_flow.addListener(l) }
        cascading_flow
      end
      flow_instances.to_java(Java::CascadingFlow::Flow)
    end
  end
end
