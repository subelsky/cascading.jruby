# NativeException wrapper that prints the full nested stack trace of the Java
# exception and all of its causes wrapped by the NativeException.
# NativeException by default reveals only the first cause, which is
# insufficient for tracing cascading.jruby errors into JRuby code or revealing
# underlying Janino expression problems.
module Cascading
  class CascadingException < StandardError
    attr_accessor :ne, :message, :depth

    def initialize(native_exception, message)
      @ne = native_exception
      @message = message
      trace, @depth = trace_causes(@ne, 1)
      super("#{message}\n#{trace}")
    end

    # Fetch cause at depth.  If depth is not provided, root cause is returned.
    def cause(depth = @depth)
      if depth > @depth
        warn "WARNING: Depth (#{depth}) greater than depth of cause stack (#{@depth}) requested"
        nil
      else
        fetch_cause(@ne, depth)
      end
    end

    private

    def fetch_cause(ne, depth)
      return ne if depth <= 1
      fetch_cause(ne.cause, depth - 1)
    end

    def trace_causes(ne, depth)
      return ['', depth - 1] unless ne

      warn "ERROR: Exception does not respond to cause: #{ne}" unless ne.respond_to?(:cause)
      cause_trace, cause_depth = trace_causes(ne.respond_to?(:cause) ? ne.cause : nil, depth + 1)

      trace = "Cause #{depth}: #{ne.respond_to?(:java_class) ? ne.java_class : ne.class}: #{ne}\n"
      if ne.respond_to?(:stack_trace)
        trace += "#{ne.stack_trace.map{ |e| "  at #{e.class_name}.#{e.method_name}(#{e.file_name}:#{e.line_number})" }.join("\n")}\n"
      elsif ne.respond_to?(:backtrace)
        trace += "  #{ne.backtrace.join("\n  ")}\n"
      end
      trace += cause_trace

      [trace, cause_depth]
    end
  end
end
