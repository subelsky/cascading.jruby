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

    def add_child(node)
      child = root.find_child(node.name)
      warn "WARNING: adding '#{node.qualified_name}', but node named '#{node.name}' already exists at '#{child.qualified_name}'" if child

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

    def find_child(name)
      children.each do |child_name, child|
        return child if child_name == name
        result = child.find_child(name)
        return result if result
      end
      nil
    end

    def root
      return self unless parent
      parent.root
    end
  end

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
