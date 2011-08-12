class ExprStub
  attr_accessor :expression, :types

  def initialize(st)
    @expression = st.dup
    @types = {}

    # Simple regexp based parser for types

    JAVA_TYPE_MAP.each do |sym, klass|
      @expression.gsub!(/[A-Za-z0-9_]+:#{sym.to_s}/) do |match|
        name = match.split(/:/).first.gsub(/\s+/, "")
        @types[name] = klass
        match.gsub(/:#{sym.to_s}/, "")
      end
    end
  end

  # Scan, parse, and compile expression, then return this ExprStub upon
  # success.  Throws an ExprException upon failure.
  def compile
    evaluator
    self
  end

  # Evaluate this ExprStub given an array of actual arguments.  Throws an
  # ExprException upon failure. GOTCHA: requires values to be in order of
  # lexicographically sorted formal arguments.
  def evaluate(values)
    begin
      evaluator.evaluate(values.to_java)
    rescue java.lang.IllegalArgumentException => iae
      raise ExprArgException.new("Invalid arguments for expression '#{@expression}': #{values.inspect}\n#{iae}")
    rescue java.lang.reflect.InvocationTargetException => ite
      raise ExprArgException.new("Null arguments for expression '#{@expression}': #{values.inspect}\n#{iae}")
    end
  end

  # Evaluates this ExprStub with default values for each actual argument.
  # Throws an ExprException upon failure.
  def test_evaluate
    evaluate(test_values)
  end

  private

  # Building an evaluator ensures that the expression scans, parses, and
  # compiles
  def evaluator
    begin
      names, types = names_and_types
      Java::OrgCodehausJanino::ExpressionEvaluator.new(@expression, java.lang.Comparable.java_class, names, types)
    rescue Java::OrgCodehausJanino::CompileException => ce
      raise ExprCompileException.new("Failed to compile expression '#{@expression}':\n#{ce}")
    rescue Java::OrgCodehausJanino::Parser::ParseException => pe
      raise ExprParseException.new("Failed to parse expression '#{@expression}':\n#{pe}")
    rescue Java::OrgCodehausJanino::Scanner::ScanException => se
      raise ExprScanException.new("Failed to scan expression '#{@expression}':\n#{se}")
    end
  end

  # Extract Java names and types from @types hash
  def names_and_types
    names, types = @types.sort.inject([[], []]) do |(names, types), (name, type)|
      [names << name, types << type]
    end
    [names.to_java(java.lang.String), types.to_java(java.lang.Class)]
  end

  @@defaults = {
    java.lang.Integer.java_class => 0,
    java.lang.Boolean.java_class => false,
    java.lang.Double.java_class => 0.0,
    java.lang.Float.java_class => java.lang.Float.new(0.0),
    java.lang.Long.java_class => java.lang.Long.new(0),
    java.lang.String.java_class => '',
  }

  def test_values
    @types.sort.inject([]) do |test_values, (name, type)|
      test_values << @@defaults[type]
    end
  end
end

class ExprException < StandardError; end
class ExprCompileException < ExprException; end
class ExprParseException < ExprException; end
class ExprScanException < ExprException; end
class ExprArgException < ExprException; end
