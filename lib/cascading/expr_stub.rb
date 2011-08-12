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

  # Evaluates this ExprStub given a hash mapping argument names to argument
  # values.  Names may be strings or symbols. Throws an ExprException upon
  # failure.
  def eval(actual_args)
    actual_args = actual_args.inject({}) do |string_keys, (arg, value)|
      string_keys[arg.to_s] = specific_to_java(value, @types[arg.to_s])
      string_keys
    end
    args, values = split_hash(actual_args)
    unused = validate_fields(args)
    return self.eval(actual_args.reject{ |arg, value| unused.include?(arg) }) unless unused.empty?
    evaluate(values)
  end

  # Evaluates this ExprStub with default values for each actual argument.
  # Throws an ExprException upon failure.
  def test_evaluate
    evaluate(test_values)
  end

  def validate_scope(scope)
    validate_fields(scope.values_fields.to_a)
  end

  # Throws an exception if any arguments required by this ExprStub are missing
  # from fields.  Returns those fields which are unused.
  def validate_fields(fields)
    names = @types.keys.sort
    missing = names - fields
    raise ExprArgException.new("Expression '#{@expression}' is missing these fields: #{missing.inspect}\nRequires: #{names.inspect}, found: #{fields.inspect}") unless missing.empty?
    fields - names
  end

  private

  def split_hash(h)
    keys, values = h.sort.inject([[], []]) do |(keys, values), (key, value)|
      [keys << key, values << value]
    end
    [keys, values]
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
    names, types = split_hash(@types)
    [names.to_java(java.lang.String), types.to_java(java.lang.Class)]
  end

  # Makes best effort to convert Ruby numbers into Floats or Longs for a Janino
  # expression.  However, if the conversion fails, it returns the original
  # value so that the exception thrown will be from Janino, not this code.
  def specific_to_java(value, type)
    # GOTCHA: Java's Float and Long have constructors that take strings and
    # parse them.  If value is a string representation of a number, this code
    # could coerce it to a number whereas invocation of the Janino expression
    # would fail.  We therefore punt if value is a String.
    return value if value.kind_of?(::String)
    case type
      when java.lang.Float.java_class
        return value if value.kind_of?(::Integer)
        java.lang.Float.new(value) rescue value
      when java.lang.Long.java_class
        return value if value.kind_of?(::Float)
        java.lang.Long.new(value) rescue value
      else value
    end
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
