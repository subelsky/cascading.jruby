namespace :git do
  desc 'Create a new tag in the Git repository'
  task :create_tag do |t|
    expected_version = Cascading::VERSION
    v = ENV['VERSION'] or abort 'Must supply VERSION=x.y.z'
    abort "Versions mismatch #{v} != #{expected_version}" if v != expected_version

    tag = "%s-%s" % ['cascading.jruby', v]
    msg = "Creating tag for cascading.jruby version #{v}"

    puts "Creating Git tag '#{tag}'"
    unless system("git tag -a -m '#{msg}' #{tag}")
      abort 'Tag creation failed'
    end

    if %x/git remote/ =~ %r/^origin\s*$/
      unless system "git push origin #{tag}"
        abort "Could not push tag to remote Git repository"
      end
    end
  end
end

task 'gem:release' => 'git:create_tag'
