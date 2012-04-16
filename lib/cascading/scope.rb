module Cascading
  class Scope
    attr_accessor :scope, :grouping_key_fields

    def initialize(scope, params = {})
      @scope = scope
      @grouping_key_fields = fields(params[:grouping_key_fields] || [])
    end

    def copy
      Scope.new(Java::CascadingFlow::Scope.new(@scope), :grouping_key_fields => @grouping_key_fields)
    end

    def self.empty_scope(name)
      Scope.new(Java::CascadingFlow::Scope.new(name))
    end

    def self.tap_scope(tap, name)
      java_scope = outgoing_scope_for(tap, java.util.HashSet.new)
      # Taps and Pipes don't name their outgoing scopes like other FlowElements
      java_scope.name = name
      Scope.new(java_scope)
    end

    def self.outgoing_scope(flow_element, incoming_scopes, grouping_key_fields)
      java_scopes = incoming_scopes.compact.map{ |s| s.scope }
      Scope.new(outgoing_scope_for(flow_element, java.util.HashSet.new(java_scopes)),
          :grouping_key_fields => grouping_key_fields
      )
    end

    def values_fields
      @scope.out_values_fields
    end

    def grouping_fields
      keys = @grouping_key_fields.to_a
      grouping_fields = @scope.out_grouping_fields.to_a
      # Overwrite key fields only
      fields(keys + grouping_fields[keys.size..-1])
    end

    def to_s
      kind = 'Unknown'
      kind = 'Tap'   if @scope.tap?
      kind = 'Group' if @scope.group?
      kind = 'Each'  if @scope.each?
      kind = 'Every' if @scope.every?
      <<-END
Scope name: #{@scope.name}
  Kind: #{kind}
  Key selectors:     #{@scope.key_selectors}
  Sorting selectors: #{@scope.sorting_selectors}
  Remainder fields:  #{@scope.remainder_fields}
  Declared fields:   #{@scope.declared_fields}
  Arguments
    selector:   #{@scope.arguments_selector}
    declarator: #{@scope.arguments_declarator}
  Out grouping
    selector:   #{@scope.out_grouping_selector}
    fields:     #{grouping_fields} (#{@scope.out_grouping_fields})
    key fields: #{@grouping_key_fields} (#{@scope.key_selectors})
  Out values
    selector: #{@scope.out_values_selector}
    fields:   #{values_fields} (#{@scope.out_values_fields})
END
    end

    private

    def self.outgoing_scope_for(flow_element, incoming_scopes)
      begin
        flow_element.outgoing_scope_for(incoming_scopes)
      rescue NativeException => e
        raise CascadingException.new(e, 'Exception computing outgoing scope')
      end
    end
  end
end
