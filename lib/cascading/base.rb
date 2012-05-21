# Copyright 2009, Grégoire Marabout. All Rights Reserved.
#
# This is free software. Please see the LICENSE and COPYING files for details.

module Cascading
  class Node
    attr_accessor :name, :parent, :children, :child_names, :last_child

    def initialize(name, parent)
      @name = name
      @parent = parent
      @children = {}
      @child_names = []
      @last_child = nil
    end

    # Children must be uniquely named within the scope of each Node.  This
    # ensures, for example, two assemblies are not created within the same flow
    # with the same name, causing joins, unions, and sinks on them to be
    # ambiguous.
    def add_child(node)
      raise AmbiguousNodeNameException.new("Attempted to add '#{node.qualified_name}', but node named '#{node.name}' already exists") if @children[node.name]

      @children[node.name] = node
      @child_names << node.name
      @last_child = node
      node
    end

    def qualified_name
      parent ? "#{parent.qualified_name}.#{name}" : name
    end

    def describe(offset = '')
      "#{offset}#{name}:node\n#{child_names.map{ |child| children[child].describe("#{offset}  ") }.join("\n")}"
    end
    alias desc describe

    # In order to find a child, we require it to be uniquely named within this
    # Node and its children.  This ensures, for example, branches in peer
    # assemblies or branches and assemblies do not conflict in joins, unions,
    # and sinks.
    def find_child(name)
      all_children_with_name = find_all_children_with_name(name)
      qualified_names = all_children_with_name.map{ |child| child.qualified_name }
      raise AmbiguousNodeNameException.new("Ambiguous lookup of child by name '#{name}'; found '#{qualified_names.join("', '")}'") if all_children_with_name.size > 1

      all_children_with_name.first
    end

    def root
      return self unless parent
      parent.root
    end

    protected

    def find_all_children_with_name(name)
      child_names.map do |child_name|
        children[child_name] if child_name == name
      end.compact + child_names.map do |child_name|
        children[child_name].find_all_children_with_name(name)
      end.flatten
    end
  end

  class AmbiguousNodeNameException < StandardError; end

  # A module to add auto-registration capability
  module Registerable
    def all
      @registered.nil? ? [] : @registered.values
    end

    def get(key)
      if key.is_a? self
        return key
      else
        @registered ||= {}
        return @registered[key]
      end
    end

    def reset
      @registered.clear if @registered
    end

    def add(name, instance)
      @registered ||= {}
      warn "WARNING: node named '#{name}' already registered in #{self}" if @registered[name]
      @registered[name] = instance
    end

    private

    def registered
      @registered ||= {}
      @registered
    end
  end
end
