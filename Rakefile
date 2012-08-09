$: << 'lib'

require 'bundler/setup'
require 'cascading'

load 'tasks/ant.rake'
load 'tasks/test.rake'
load 'tasks/spec.rake'
load 'tasks/samples.rake'
load 'tasks/git.rake'

task :default => 'test'

desc 'Remove gem and Java build files'
task :clean => ['ant:clean', 'samples:clean'] do
  puts 'Build files removed'
end
