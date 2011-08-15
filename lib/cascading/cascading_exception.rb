# NativeException wrapper that prints the full nested stack trace of the Java
# exception and all of its causes wrapped by the NativeException.
# NativeException by default reveals only the first cause, which is
# insufficient for tracing cascading.jruby errors into JRuby code or revealing
# underlying Janino expression problems.
class CascadingException < StandardError
  def initialize(native_exception, message)
    @ne = native_exception
    super("#{message}\n#{trace_causes(@ne, 1)}")
  end

  def cause(depth)
    fetch_cause(@ne, depth)
  end

  private

  def fetch_cause(ne, depth)
    return ne if depth <= 1
    fetch_cause(ne.cause, depth - 1)
  end

  def trace_causes(ne, depth)
    return unless ne
    trace = "Cause #{depth}: #{ne.respond_to?(:java_class) ? ne.java_class : ne.class}: #{ne}\n"
    if ne.respond_to?(:stack_trace)
      trace += "#{ne.stack_trace.map{ |e| "  at #{e.class_name}.#{e.method_name}(#{e.file_name}:#{e.line_number})" }.join("\n")}\n"
    elsif ne.respond_to?(:backtrace)
      trace += "  #{ne.backtrace.join("\n  ")}\n"
    end
    trace += "#{trace_causes(ne.cause, depth + 1)}"
  end
end
