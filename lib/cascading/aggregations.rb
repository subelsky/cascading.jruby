require 'cascading/operations'
require 'cascading/ext/array'

module Cascading
  # Rules enforced by Aggregations:
  #   Contains either 1 Buffer or >= 1 Aggregator (explicitly checked)
  #   No GroupBys, CoGroups, Joins, or Merges (methods for these pipes do not exist on Aggregations)
  #   No Eaches (Aggregations#each does not exist)
  #   Aggregations may not branch (Aggregations#branch does not exist)
  #
  # Externally enforced rules:
  #   May be empty (in which case, Aggregations is not instantiated)
  #   Must follow a GroupBy or CoGroup (not a Join or Merge)
  class Aggregations
    include Operations

    attr_reader :assembly_name, :tail_pipe, :scope

    def initialize(assembly)
      @assembly_name = assembly.name
      @tail_pipe = assembly.tail_pipe
      @scope = assembly.scope
    end

    def debug_scope
      puts "Current scope of aggregations for '#{assembly_name}':\n  #{scope}\n----------\n"
    end

    def make_pipe(type, parameters)
      pipe = type.new(*parameters)

      # Enforce 1 Buffer or >= 1 Aggregator rule
      if tail_pipe.kind_of?(Java::CascadingPipe::Every)
        raise 'Buffer must be sole aggregation' if tail_pipe.buffer? || (tail_pipe.aggregator? && pipe.buffer?)
      end

      @tail_pipe = pipe
      @scope = Scope.outgoing_scope(tail_pipe, [scope])
    end
    private :make_pipe

    # "Fix" out values fields after a sequence of Everies.  This is a field name
    # metadata fix which is why the Identity is not planned into the resulting
    # Cascading pipe.  Without it, all values fields would propagate through
    # non-empty aggregations, which doesn't match Cascading's planner's
    # behavior.
    def finalize
      discard_each = Java::CascadingPipe::Each.new(tail_pipe, all_fields, Java::CascadingOperation::Identity.new)
      @scope = Scope.outgoing_scope(discard_each, [scope])
    end

    # Builds an every pipe and adds it to the current list of aggregations.
    # Note that this list may be either exactly 1 Buffer or any number of
    # Aggregators.
    def every(*args)
      options = args.extract_options!

      in_fields = fields(args)
      out_fields = fields(options[:output])
      operation = options[:aggregator] || options[:buffer]

      parameters = [tail_pipe, in_fields, operation, out_fields].compact
      make_pipe(Java::CascadingPipe::Every, parameters)
    end

    def assert_group(*args)
      options = args.extract_options!

      assertion = args[0]
      assertion_level = options[:level] || Java::CascadingOperation::AssertionLevel::STRICT

      parameters = [tail_pipe, assertion_level, assertion]
      make_pipe(Java::CascadingPipe::Every, parameters)
    end

    def assert_group_size_equals(*args)
      options = args.extract_options!

      assertion = Java::CascadingOperationAssertion::AssertGroupSizeEquals.new(args[0])
      assert_group(assertion, options)
    end

    # Builds a series of every pipes for aggregation.
    #
    # Args can either be a list of fields to aggregate and an options hash or
    # a hash that maps input field name to output field name (similar to
    # insert) and an options hash.
    #
    # Options include:
    #   * <tt>:ignore</tt> a Java Array of Objects (for min and max) or Tuples
    #     (for first and last) of values for the aggregator to ignore
    #
    # <tt>function</tt> is a symbol that is the method to call to construct the Cascading Aggregator.
    def composite_aggregator(args, function)
      if !args.empty? && args.first.kind_of?(Hash)
        field_map = args.shift.sort
        options = args.extract_options!
      else
        options = args.extract_options!
        field_map = args.zip(args)
      end
      field_map.each do |in_field, out_field|
        agg = self.send(function, out_field, options)
        every(in_field, :aggregator => agg, :output => all_fields)
      end
      puts "WARNING: composite aggregator '#{function.to_s.gsub('_function', '')}' invoked on 0 fields; will be ignored" if field_map.empty?
    end

    def min(*args); composite_aggregator(args, :min_function); end
    def max(*args); composite_aggregator(args, :max_function); end
    def first(*args); composite_aggregator(args, :first_function); end
    def last(*args); composite_aggregator(args, :last_function); end
    def average(*args); composite_aggregator(args, :average_function); end

    # Counts elements of a group.  First unnamed parameter is the name of the
    # output count field (defaults to 'count' if it is not provided).
    def count(*args)
      options = args.extract_options!
      name = args[0] || 'count'
      every(last_grouping_fields, :aggregator => count_function(name, options), :output => all_fields)
    end

    # Fields to be summed may either be provided as an array, in which case
    # they will be aggregated into the same field in the given order, or as a
    # hash, in which case they will be aggregated from the field named by the
    # key into the field named by the value after being sorted.
    def sum(*args)
      options = args.extract_options!
      type = JAVA_TYPE_MAP[options[:type]]
      raise "No type specified for sum" unless type

      mapping = options[:mapping] ? options[:mapping].sort : args.zip(args)
      mapping.each do |in_field, out_field|
        every(in_field, :aggregator => sum_function(out_field, :type => type), :output => all_fields)
      end
    end
  end
end
