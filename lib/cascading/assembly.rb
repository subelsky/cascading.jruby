# -*- coding: utf-8 -*-
# Copyright 2009, Grégoire Marabout. All Rights Reserved.
#
# This is free software. Please see the LICENSE and COPYING files for details.

require 'cascading/base'
require 'cascading/operations'
require 'cascading/aggregations'
require 'cascading/ext/array'

module Cascading
  class Assembly < Cascading::Node
    include Operations

    attr_reader :head_pipe, :tail_pipe, :incoming_scopes, :outgoing_scopes

    def initialize(name, parent, outgoing_scopes = {})
      super(name, parent)

      @outgoing_scopes = outgoing_scopes
      if parent.kind_of?(Assembly)
        @head_pipe = Java::CascadingPipe::Pipe.new(name, parent.tail_pipe)
        # Copy to allow destructive update of name
        outgoing_scopes[name] = parent.scope.copy
        scope.scope.name = name
      else # Parent is a Flow
        @head_pipe = Java::CascadingPipe::Pipe.new(name)
        outgoing_scopes[name] ||= Scope.empty_scope(name)
      end
      @tail_pipe = head_pipe
      @incoming_scopes = [scope]
    end

    def describe(offset = '')
      incoming_scopes_desc = "#{incoming_scopes.map{ |incoming_scope| incoming_scope.values_fields.to_a.inspect }.join(', ')}"
      incoming_scopes_desc = "(#{incoming_scopes_desc})" unless incoming_scopes.size == 1
      description =  "#{offset}#{name}:assembly :: #{incoming_scopes_desc} -> #{scope.values_fields.to_a.inspect}"
      description += "\n#{child_names.map{ |child| children[child].describe("#{offset}  ") }.join("\n")}" unless children.empty?
      description
    end

    def parent_flow
      return parent if parent.kind_of?(Flow)
      parent.parent_flow
    end

    def scope
      outgoing_scopes[name]
    end

    def debug_scope
      puts "Current scope for '#{name}':\n  #{scope}\n----------\n"
    end

    def make_pipe(type, parameters)
      @tail_pipe = type.new(*parameters)
      outgoing_scopes[name] = Scope.outgoing_scope(tail_pipe, [scope])
    end
    private :make_pipe

    def apply_aggregations(group, incoming_scopes, &block)
      aggregations = Aggregations.new(self, group, incoming_scopes)
      if block_given?
        aggregations.instance_eval(&block)
        aggregations.finalize
      end

      @tail_pipe = aggregations.tail_pipe
      outgoing_scopes[name] = aggregations.scope
    end
    private :apply_aggregations

    def to_s
      "#{name} : head pipe : #{head_pipe} - tail pipe: #{tail_pipe}"
    end

    # Builds a join (CoGroup) pipe. Requires a list of assembly names to join
    # and :on to specify the group_fields.
    def join(*args, &block)
      options = args.extract_options!

      pipes, @incoming_scopes = [], []
      args.each do |assembly_name|
        assembly = parent_flow.find_child(assembly_name)
        raise "Could not find assembly '#{assembly_name}' in join" unless assembly

        pipes << assembly.tail_pipe
        incoming_scopes << outgoing_scopes[assembly.name]
      end

      group_fields_args = options[:on]
      raise 'join requires :on parameter' unless group_fields_args

      if group_fields_args.kind_of?(String)
        group_fields_args = [group_fields_args]
      end

      group_fields = []
      if group_fields_args.kind_of?(Array)
        pipes.size.times do
          group_fields << fields(group_fields_args)
        end
      elsif group_fields_args.kind_of?(Hash)
        pipes, @incoming_scopes = [], []
        keys = group_fields_args.keys.sort
        keys.each do |assembly_name|
          v = group_fields_args[assembly_name]
          assembly = parent_flow.find_child(assembly_name)
          raise "Could not find assembly '#{assembly_name}' in join" unless assembly

          pipes << assembly.tail_pipe
          incoming_scopes << outgoing_scopes[assembly.name]
          group_fields << fields(v)
        end
      else
        raise "Unsupported data type for :on in join: '#{group_fields_args.class}'"
      end

      raise 'join requires non-empty :on parameter' if group_fields_args.empty?
      group_fields = group_fields.to_java(Java::CascadingTuple::Fields)
      incoming_fields = incoming_scopes.map{ |s| s.values_fields }
      declared_fields = fields(options[:declared_fields] || dedup_fields(*incoming_fields))
      joiner = options[:joiner]

      case joiner
      when :inner, 'inner', nil
        joiner = Java::CascadingPipeJoiner::InnerJoin.new
      when :left,  'left'
        joiner = Java::CascadingPipeJoiner::LeftJoin.new
      when :right, 'right'
        joiner = Java::CascadingPipeJoiner::RightJoin.new
      when :outer, 'outer'
        joiner = Java::CascadingPipeJoiner::OuterJoin.new
      when Array
        joiner = joiner.map do |t|
          case t
          when true,  1, :inner then true
          when false, 0, :outer then false
          else fail "invalid mixed joiner entry: #{t}"
          end
        end
        joiner = Java::CascadingPipeJoiner::MixedJoin.new(joiner.to_java(:boolean))
      end
      result_group_fields = dedup_fields(*group_fields)
      parameters = [
        pipes.to_java(Java::CascadingPipe::Pipe),
        group_fields,
        declared_fields,
        result_group_fields,
        joiner
      ]
      apply_aggregations(Java::CascadingPipe::CoGroup.new(*parameters), incoming_scopes, &block)
    end
    alias co_group join

    def inner_join(*args, &block)
      options = args.extract_options!
      options[:joiner] = :inner
      args << options
      join(*args, &block)
    end

    def left_join(*args, &block)
      options = args.extract_options!
      options[:joiner] = :left
      args << options
      join(*args, &block)
    end

    def right_join(*args, &block)
      options = args.extract_options!
      options[:joiner] = :right
      args << options
      join(*args, &block)
    end

    def outer_join(*args, &block)
      options = args.extract_options!
      options[:joiner] = :outer
      args << options
      join(*args, &block)
    end

    # Builds a new branch.
    def branch(name, &block)
      raise "Could not build branch '#{name}'; block required" unless block_given?
      assembly = Assembly.new(name, self, outgoing_scopes)
      add_child(assembly)
      assembly.instance_eval(&block)
      assembly
    end

    # Builds a new GroupBy pipe that groups on the fields given in args.
    # Any block passed to this method should contain only Everies.
    def group_by(*args, &block)
      options = args.extract_options!
      group_fields = fields(args)
      sort_fields = fields(options[:sort_by])
      reverse = options[:reverse]

      parameters = [tail_pipe, group_fields, sort_fields, reverse].compact
      apply_aggregations(Java::CascadingPipe::GroupBy.new(*parameters), [scope], &block)
    end

    # Unifies multiple incoming pipes sharing the same field structure using a
    # GroupBy.  Accepts :on like join and :sort_by and :reverse like group_by,
    # as well as a block which may be used for a sequence of Every
    # aggregations.
    #
    # By default, groups only on the first field (see line 189 of GroupBy.java)
    def union(*args, &block)
      options = args.extract_options!
      group_fields = fields(options[:on])
      sort_fields = fields(options[:sort_by])
      reverse = options[:reverse]

      pipes, @incoming_scopes = [], []
      args.each do |assembly_name|
        assembly = parent_flow.find_child(assembly_name)
        raise "Could not find assembly '#{assembly_name}' in union" unless assembly

        pipes << assembly.tail_pipe
        incoming_scopes << outgoing_scopes[assembly.name]
      end

      # Must provide group_fields to ensure field name propagation
      group_fields = fields(incoming_scopes.first.values_fields.get(0)) unless group_fields

      # FIXME: GroupBy is missing a constructor for union in wip-255
      sort_fields = group_fields if !sort_fields && !reverse.nil?

      parameters = [pipes.to_java(Java::CascadingPipe::Pipe), group_fields, sort_fields, reverse].compact
      apply_aggregations(Java::CascadingPipe::GroupBy.new(*parameters), incoming_scopes, &block)
    end
    alias :union_pipes :union

    # Allows you to plugin Java SubAssemblies to a cascading.jruby Assembly.
    #
    # Assumptions:
    #   * You will use the tail_pipe of this Assembly, otherwise you'll leave
    #   it dangling as do join and union.
    #   * Your subassembly will have only 1 tail pipe; branching is not
    #   supported.  This allows you to continue operating upon the tail of the
    #   subassembly within this Assembly.
    #
    # This is a low-level tool, so be careful.
    def sub_assembly(sub_assembly)
      raise 'SubAssembly must call setTails in constructor' unless sub_assembly.tails
      raise 'SubAssembly must set exactly 1 tail in constructor' unless sub_assembly.tails.size == 1
      old_tail = tail_pipe
      @tail_pipe = sub_assembly.tails.first

      path = path(old_tail, tail_pipe)
      puts path.join(',')

      path.each do |pipe|
        outgoing_scopes[name] = Scope.outgoing_scope(pipe, [scope])
      end
    end

    def path(pipe, tail_pipes)
      unwound = Java::CascadingPipe::SubAssembly.unwind(tail_pipes).to_a
      # Join used because: http://jira.codehaus.org/browse/JRUBY-5136
      raise "path is only applicable to linear paths; found #{unwound.size} pipes: [#{unwound.join(',')}]" unless unwound.size == 1
      unwound = unwound.first
      pipe == unwound ? [] : (path(pipe, unwound.previous) + [unwound])
    end
    private :path

    # Builds a basic _each_ pipe, and adds it to the current assembly.
    # --
    # Example:
    #     each "line", :filter=>regex_splitter(["name", "val1", "val2", "id"],
    #                  :pattern => /[.,]*\s+/),
    #                  :output=>["id", "name", "val1", "val2"]
    def each(*args)
      options = args.extract_options!

      in_fields = fields(args)
      out_fields = fields(options[:output])
      operation = options[:filter] || options[:function]

      parameters = [tail_pipe, in_fields, operation, out_fields].compact
      make_pipe(Java::CascadingPipe::Each, parameters)
    end

    # Restricts the current assembly to the specified fields.
    # --
    # Example:
    #     project "field1", "field2"
    def project(*args)
      each fields(args), :function => Java::CascadingOperation::Identity.new
    end

    # Removes the specified fields from the current assembly.
    # --
    # Example:
    #     discard "field1", "field2"
    def discard(*args)
      discard_fields = fields(args)
      keep_fields = difference_fields(scope.values_fields, discard_fields)
      project(*keep_fields.to_a)
    end

    # Renames fields according to the mapping provided.
    # --
    # Example:
    #     rename "old_name" => "new_name"
    def rename(name_map)
      old_names = scope.values_fields.to_a
      new_names = old_names.map{ |name| name_map[name] || name }
      invalid = name_map.keys.sort - old_names
      raise "invalid names: #{invalid.inspect}" unless invalid.empty?

      each all_fields, :function => Java::CascadingOperation::Identity.new(fields(new_names))
    end

    def cast(type_map)
      names = type_map.keys.sort
      types = JAVA_TYPE_MAP.values_at(*type_map.values_at(*names))
      fields = fields(names)
      types = types.to_java(java.lang.Class)
      each fields, :function => Java::CascadingOperation::Identity.new(fields, types)
    end

    def copy(*args)
      options = args.extract_options!
      from = args[0] || all_fields
      into = args[1] || options[:into] || all_fields
      each fields(from), :function => Java::CascadingOperation::Identity.new(fields(into)), :output => all_fields
    end

    # A pipe that does nothing.
    def pass(*args)
      each all_fields, :function => Java::CascadingOperation::Identity.new
    end

    def assert(*args)
      options = args.extract_options!
      assertion = args[0]
      assertion_level = options[:level] || Java::CascadingOperation::AssertionLevel::STRICT

      parameters = [tail_pipe, assertion_level, assertion]
      make_pipe(Java::CascadingPipe::Each, parameters)
    end

    # Builds a debugging pipe.
    #
    # Without arguments, it generate a simple debug pipe, that prints all tuple to the standard
    # output.
    #
    # The other named options are:
    # * <tt>:print_fields</tt> a boolean. If is set to true, then it prints every 10 tuples.
    #
    def debug(*args)
      options = args.extract_options!
      print_fields = options[:print_fields] || true
      parameters = [print_fields].compact
      debug = Java::CascadingOperation::Debug.new(*parameters)
      debug.print_tuple_every = options[:tuple_interval] || 1
      debug.print_fields_every = options[:fields_interval] || 10
      each(all_fields, :filter => debug)
    end

    # Builds a pipe that assert the size of the tuple is the size specified in parameter.
    #
    # The method accept an unique uname argument : a number indicating the size expected.
    def assert_size_equals(*args)
      options = args.extract_options!
      assertion = Java::CascadingOperationAssertion::AssertSizeEquals.new(args[0])
      assert(assertion, options)
    end

    # Builds a pipe that assert the none of the fields in the tuple are null.
    def assert_not_null(*args)
      options = args.extract_options!
      assertion = Java::CascadingOperationAssertion::AssertNotNull.new
      assert(assertion, options)
    end

    # Builds a _parse_ pipe. This pipe will parse the fields specified in input (first unamed arguments),
    # using a specified regex pattern.
    #
    # If provided, the unamed arguments must be the fields to be parsed. If not provided, then all incoming
    # fields are used.
    #
    # The named options are:
    # * <tt>:pattern</tt> a string or regex. Specifies the regular expression used for parsing the argument fields.
    # * <tt>:output</tt> a string or array of strings. Specifies the outgoing fields (all fields will be output by default)
    def parse(*args)
        options = args.extract_options!
        fields = args || all_fields
        pattern = options[:pattern]
        output = options[:output] || all_fields
        each(fields, :filter => regex_parser(pattern, options), :output => output)
    end

    # Builds a pipe that splits a field into other fields, using a specified regular expression.
    #
    # The first unnamed argument is the field to be split.
    # The second unnamed argument is an array of strings indicating the fields receiving the result of the split.
    #
    # The named options are:
    # * <tt>:pattern</tt> a string or regex. Specifies the regular expression used for splitting the argument fields.
    # * <tt>:output</tt> a string or array of strings. Specifies the outgoing fields (all fields will be output by default)
    def split(*args)
      options = args.extract_options!
      fields = options[:into] || args[1]
      pattern = options[:pattern] || /[.,]*\s+/
      output = options[:output] || all_fields
      each(args[0], :function => regex_splitter(fields, :pattern => pattern), :output=>output)
    end

    # Builds a pipe that splits a field into new rows, using a specified regular expression.
    #
    # The first unnamed argument is the field to be split.
    # The second unnamed argument is the field receiving the result of the split.
    #
    # The named options are:
    # * <tt>:pattern</tt> a string or regex. Specifies the regular expression used for splitting the argument fields.
    # * <tt>:output</tt> a string or array of strings. Specifies the outgoing fields (all fields will be output by default)
    def split_rows(*args)
      options = args.extract_options!
      fields = options[:into] || args[1]
      pattern = options[:pattern] || /[.,]*\s+/
      output = options[:output] || all_fields
      each(args[0], :function => regex_split_generator(fields, :pattern => pattern), :output=>output)
    end

    # Builds a pipe that emits a new row for each regex group matched in a field, using a specified regular expression.
    #
    # The first unnamed argument is the field to be matched against.
    # The second unnamed argument is the field receiving the result of the match.
    #
    # The named options are:
    # * <tt>:pattern</tt> a string or regex. Specifies the regular expression used for matching the argument fields.
    # * <tt>:output</tt> a string or array of strings. Specifies the outgoing fields (all fields will be output by default)
    def match_rows(*args)
      options = args.extract_options!
      fields = options[:into] || args[1]
      pattern = options[:pattern] || /[\w]+/
      output = options[:output] || all_fields
      each(args[0], :function => regex_generator(fields, :pattern => pattern), :output=>output)
    end

    # Builds a pipe that parses the specified field as a date using hte provided format string.
    # The unamed argument specifies the field to format.
    #
    # The named options are:
    # * <tt>:into</tt> a string. It specifies the receiving field. By default, it will be named after
    # the input argument.
    # * <tt>:pattern</tt> a string. Specifies the date format.
    # * <tt>:output</tt> a string or array of strings. Specifies the outgoing fields (all fields will be output by default)
    def parse_date(*args)
      options = args.extract_options!
      field = options[:into] || "#{args[0]}_parsed"
      output = options[:output] || all_fields
      pattern = options[:pattern] || "yyyy/MM/dd"

      each args[0], :function => date_parser(field, pattern), :output => output
    end

    # Builds a pipe that format a date using a specified format pattern.
    #
    # The unamed argument specifies the field to format.
    #
    # The named options are:
    # * <tt>:into</tt> a string. It specifies the receiving field. By default, it will be named after
    # the input argument.
    # * <tt>:pattern</tt> a string. Specifies the date format.
    # * <tt>:timezone</tt> a string.  Specifies the timezone (defaults to UTC).
    # * <tt>:output</tt> a string or array of strings. Specifies the outgoing fields (all fields will be output by default)
    def format_date(*args)
      options = args.extract_options!
      field = options[:into] || "#{args[0]}_formatted"
      pattern = options[:pattern] || "yyyy/MM/dd"
      output = options[:output] || all_fields

      each args[0], :function => date_formatter(field, pattern, options[:timezone]), :output => output
    end

    # Builds a pipe that perform a query/replace based on a regular expression.
    #
    # The first unamed argument specifies the input field.
    #
    # The named options are:
    # * <tt>:pattern</tt> a string or regex. Specifies the pattern to look for in the input field. This non-optional argument
    # can also be specified as a second _unamed_ argument.
    # * <tt>:replacement</tt> a string. Specifies the replacement.
    # * <tt>:output</tt> a string or array of strings. Specifies the outgoing fields (all fields will be output by default)
    def replace(*args)
      options = args.extract_options!

      pattern = options[:pattern] || args[1]
      replacement = options[:replacement] || args[2]
      into = options[:into] || "#{args[0]}_replaced"
      output = options[:output] || all_fields

      each args[0], :function => regex_replace(into, pattern, replacement), :output => output
    end

    # Builds a pipe that inserts values into the current tuple.
    #
    # The method takes a hash as parameter. This hash contains as keys the names of the fields to insert
    # and as values, the values they must contain. For example:
    #
    #       insert {"who" => "Grégoire", "when" => Time.now.strftime("%Y-%m-%d") }
    #
    # will insert two new fields: a field _who_ containing the string "Grégoire", and a field _when_ containing
    # the formatted current date.
    # The methods outputs all fields.
    # The named options are:
    def insert(args)
      args.keys.sort.each do |field_name|
        value = args[field_name]

        if value.kind_of?(ExprStub)
          value.validate_scope(scope)
          each all_fields, :function => expression_function(field_name, :expression => value.expression, :parameters => value.types), :output => all_fields
        else
          each all_fields, :function => insert_function([field_name], :values => [value]), :output => all_fields
        end
      end
    end

    # Builds a pipe that filters the tuples based on an expression or a pattern (but not both !).
    #
    # The first unamed argument, if provided, is a filtering expression (using the Janino syntax).
    #
    # The named options are:
    # * <tt>:pattern</tt> a string. Specifies a regular expression pattern used to filter the tuples. If this
    # option is provided, then the filter is regular expression-based. This is incompatible with the _expression_ option.
    # * <tt>:expression</tt> a string. Specifies a Janino expression used to filter the tuples. This option has the
    # same effect than providing it as first unamed argument. If this option is provided, then the filter is Janino
    # expression-based. This is incompatible with the _pattern_ option.
    # * <tt>:validate</tt> a boolean.  Passed into Cascading#expr to enable or disable
    # expression validation.  Defaults to true.
    # * <tt>:validate_with</tt> a hash.  Actual arguments used by Cascading#expr for
    # expression validation.  Defaults to {}.
    def filter(*args)
      options = args.extract_options!
      from = options.delete(:from) || all_fields
      expression = options.delete(:expression) || args.shift
      regex = options.delete(:pattern)
      validate = options.has_key?(:validate) ? options.delete(:validate) : true
      validate_with = options.has_key?(:validate_with) ? options.delete(:validate_with) : {}

      if expression
        stub = expr(expression, { :validate => validate, :validate_with => validate_with })
        types, expression = stub.types, stub.expression

        stub.validate_scope(scope)
        each from, :filter => expression_filter(
          :parameters => types,
          :expression => expression
        )
      elsif regex
        each from, :filter => regex_filter(regex, options)
      end
    end

    def filter_null(*args)
      options = args.extract_options!
      each(args, :filter => Java::CascadingOperationFilter::FilterNull.new)
    end
    alias reject_null filter_null

    def filter_not_null(*args)
      options = args.extract_options!
      each(args, :filter => Java::CascadingOperationFilter::FilterNotNull.new)
    end
    alias where_null filter_not_null

    # Builds a pipe that rejects the tuples based on an expression.
    #
    # The first unamed argument, if provided, is a filtering expression (using the Janino syntax).
    #
    # The named options are:
    # * <tt>:expression</tt> a string. Specifies a Janino expression used to filter the tuples. This option has the
    # same effect than providing it as first unamed argument. If this option is provided, then the filter is Janino
    # expression-based.
    # * <tt>:validate</tt> a boolean.  Passed into Cascading#expr to enable or disable
    # expression validation.  Defaults to true.
    # * <tt>:validate_with</tt> a hash.  Actual arguments used by Cascading#expr for
    # expression validation.  Defaults to {}.
    def reject(*args)
      options = args.extract_options
      raise "Regex not allowed" if options && options[:pattern]

      filter(*args)
    end

    # Builds a pipe that includes just the tuples matching an expression.
    #
    # The first unamed argument, if provided, is a filtering expression (using the Janino syntax).
    #
    # The named options are:
    # * <tt>:expression</tt> a string. Specifies a Janino expression used to select the tuples. This option has the
    # same effect than providing it as first unamed argument. If this option is provided, then the filter is Janino
    # expression-based.
    # * <tt>:validate</tt> a boolean.  Passed into Cascading#expr to enable or disable
    # expression validation.  Defaults to true.
    # * <tt>:validate_with</tt> a hash.  Actual arguments used by Cascading#expr for
    # expression validation.  Defaults to {}.
    def where(*args)
      options = args.extract_options
      raise "Regex not allowed" if options && options[:pattern]

      if options[:expression]
        _, imports, expr = options[:expression].match(/^((?:\s*import.*;\s*)*)(.*)$/).to_a
        options[:expression] = "#{imports}!(#{expr})"
      elsif args[0]
        _, imports, expr = args[0].match(/^((?:\s*import.*;\s*)*)(.*)$/).to_a
        args[0] = "#{imports}!(#{expr})"
      end

      filter(*args)
    end

    # Builds a pipe that evaluates the specified Janino expression and insert it in a new field in the tuple.
    #
    # The named options are:
    # * <tt>:from</tt> a string or array of strings. Specifies the input fields.
    # * <tt>:express</tt> a string. The janino expression.
    # * <tt>:into</tt> a string. Specified the name of the field to insert with the result of the evaluation.
    # * <tt>:parameters</tt> a hash. Specifies the type mapping for the parameters. See Cascading::Operations.expression_function.
    def eval_expression(*args)
      options = args.extract_options!

      into = options.delete(:into)
      from = options.delete(:from) || all_fields
      output = options.delete(:output) || all_fields
      options[:expression] ||= args.shift
      options[:parameters] ||= args.shift

      each from, :function => expression_function(into, options), :output=>output
    end

    # Builds a pipe that returns distinct tuples based on the provided fields.
    #
    # The method accepts optional unamed argument specifying the fields to base the distinct on
    # (all fields, by default).
    def distinct(*args)
      raise "Distinct is badly broken"
      fields = args[0] || all_fields
      group_by *fields
      pass
    end

    def join_fields(*args)
      options = args.extract_options!
      output = options[:output] || all_fields

      each args, :function => field_joiner(options), :output => output
    end
  end
end
