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

  def compile
    begin
      names, types = @types.sort.inject([[], []]) do |(names, types), (name, type)|
        names << name
        types << type
        [names, types]
      end
      evaluator = Java::OrgCodehausJanino::ExpressionEvaluator.new(@expression, java.lang.Comparable.java_class, names.to_java(java.lang.String), types.to_java(java.lang.Class))
    rescue Java::OrgCodehausJanino::CompileException => ce
      raise ExprCompileException.new("Failed to compile expression '#{@expression}':\n#{ce}")
    rescue Java::OrgCodehausJanino::Parser::ParseException => pe
      raise ExprParseException.new("Failed to parse expression '#{@expression}':\n#{pe}")
    rescue Java::OrgCodehausJanino::Scanner::ScanException => se
      raise ExprScanException.new("Failed to scan expression '#{@expression}':\n#{se}")
    end
    self
  end
end

class ExprException < StandardError; end
class ExprCompileException < ExprException; end
class ExprParseException < ExprException; end
class ExprScanException < ExprException; end
