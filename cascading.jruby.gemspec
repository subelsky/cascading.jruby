# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = "cascading.jruby"
  s.version = "0.0.10"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Matt Walker", "Gr\303\251goire Marabout"]
  s.date = "2012-08-09"
  s.description = "cascading.jruby is a small DSL above Cascading, written in JRuby"
  s.email = "mwalker@etsy.com"
  s.extra_rdoc_files = ["History.txt", "LICENSE.txt"]
  s.files = ["lib/cascading.rb", "lib/cascading/aggregations.rb", "lib/cascading/assembly.rb", "lib/cascading/base.rb", "lib/cascading/cascade.rb", "lib/cascading/cascading.rb", "lib/cascading/cascading_exception.rb", "lib/cascading/expr_stub.rb", "lib/cascading/ext/array.rb", "lib/cascading/flow.rb", "lib/cascading/mode.rb", "lib/cascading/operations.rb", "lib/cascading/scope.rb", "lib/cascading/sub_assembly.rb", "lib/cascading/tap.rb"]
  s.homepage = "http://github.com/etsy/cascading.jruby"
  s.rdoc_options = ["--main", "README.md"]
  s.require_paths = ["lib"]
  s.rubyforge_project = "cascading.jruby"
  s.rubygems_version = "1.8.21"
  s.summary = "A JRuby DSL for Cascading"
  s.test_files = ["test/test_aggregations.rb", "test/test_assembly.rb", "test/test_cascade.rb", "test/test_cascading.rb", "test/test_exceptions.rb", "test/test_flow.rb", "test/test_local_execution.rb", "test/test_operations.rb"]

  if s.respond_to? :specification_version then
    s.specification_version = 3
  end
end
