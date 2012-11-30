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

The file cascading/cascading.rb defines global helper methods for cascading like
tap creation, fields creation, etc.

The `Cascading::Operations` module is deprecated.  The original idea from long
ago is that it would be useful to mixin operator wrappers to places other than
`Cascading::Assembly`, but this is not true.  Instead, put Eaches in
`Cascading::Assembly`, Everies in `Cascading::Aggregations`, and any more
generally useful utility code directly in the `Cascading` module
(cascading/cascading.rb).
