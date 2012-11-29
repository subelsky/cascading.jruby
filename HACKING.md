# Hacking

Some hacking info on `cascading.jruby`:

For local development, install with (requires [bundler](http://gembundler.com/)):

    jruby -S bundle install

To run the tests (will download Cascading and Hadoop jars):

    jruby -S bundle exec rake

To create the gem:

    jruby -S gem build cascading.jruby.gemspec

To install it locally:

    jruby -S gem install cascading.jruby-xxx.gem

The `Cascading::Operations` module is mixed-in the `Cascading::Assembly` class to provide some shortcuts for common operations.

The file cascading/cascading.rb defines global helper methods for cascading like tap creation, fields creation, etc.
