require 'cascading/scope'

module Cascading
  # Allows you to plugin Java SubAssemblies to a cascading.jruby Assembly.
  #
  # Assumptions:
  #   * You will use the tail_pipe of this Assembly, otherwise you'll leave
  #   it dangling as do join and union.
  #   * Your subassembly will have only 1 tail pipe; branching is not
  #   supported.  This allows you to continue operating upon the tail of the
  #   subassembly within this Assembly.
  #   * Your subassembly will have only 1 input pipe; merging is not supported
  #   (yet; it will be for unions).
  #
  # This is a low-level tool, so be careful.
  class SubAssembly
    attr_reader :assembly, :sub_assembly, :tail_pipe, :scope

    def initialize(assembly, sub_assembly)
      @assembly = assembly
      @sub_assembly = sub_assembly
      @tail_pipe = assembly.tail_pipe
      @scope = assembly.scope

      # Enforces 1 tail pipe assumption
      raise 'SubAssembly must call setTails in constructor' unless sub_assembly.tails
      raise 'SubAssembly must set exactly 1 tail in constructor' unless sub_assembly.tails.size == 1
    end

    def finalize
      old_tail_pipe = tail_pipe
      @tail_pipe = sub_assembly.tails.first

      path = path(old_tail_pipe, tail_pipe)
      puts path.join(',')

      path.each do |pipe|
        @scope = Scope.outgoing_scope(pipe, [scope])
      end
    end

    private

    def path(pipe, tail_pipes)
      unwound = Java::CascadingPipe::SubAssembly.unwind(tail_pipes).to_a
      # Join used because: http://jira.codehaus.org/browse/JRUBY-5136
      raise "path is only applicable to linear paths; found #{unwound.size} pipes: [#{unwound.join(',')}]" unless unwound.size == 1
      unwound = unwound.first
      pipe == unwound ? [] : (path(pipe, unwound.previous) + [unwound])
    end
  end
end
