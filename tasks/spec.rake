require 'spec/rake/spectask'

namespace :spec do
  desc 'Run all specs with basic output'
  Spec::Rake::SpecTask.new(:run) do |t|
    # Allow user to specify specs to run at command line
    _, spec_files = ARGV
    spec_files ||= FileList['spec/**/*_spec.rb']
    t.spec_files = spec_files
    t.verbose = true

    t.ruby_opts = ['-w']
    t.spec_opts = []
    t.libs += ['lib']
  end
end

desc 'spec:run with dependencies resolved'
task :spec => ['ant:retrieve', 'spec:run']
