require 'test/unit'
require 'cascading'

class TC_Exceptions < Test::Unit::TestCase
  def test_cascading_exception
    ne3 = java.lang.IllegalArgumentException.new('Root cause')
    ne2 = Java::CascadingPipe::OperatorException.new(Java::CascadingPipe::Pipe.new('dummy'), 'Exception thrown by Cascading', ne3)
    ne1 = Java::Cascading::CascadingException.new('Exception Cascading hands us', ne2)
    e = CascadingException.new(ne1, 'cascading.jruby wrapper exception')

    assert_equal ne1, e.ne
    assert_match /^cascading\.jruby wrapper exception/, e.message
    assert_match /^Exception summary for: cascading\.jruby wrapper exception/, e.message
    assert_equal 3, e.depth

    assert_equal ne1, e.cause(1)
    assert_equal 'Exception Cascading hands us', e.cause(1).message

    assert_equal ne2, e.cause(2)
    # Cascading inserts Operator#to_s, here
    assert_match /Exception thrown by Cascading$/, e.cause(2).message

    assert_equal ne3, e.cause(3)
    assert_equal 'Root cause', e.cause(3).message

    # Shallower than depth 1 is the first cause
    (-5..0).each do |i|
      assert_equal ne1, e.cause(i)
      assert_equal 'Exception Cascading hands us', e.cause(i).message
    end

    # Deeper than the root cause is nil
    (4..10).each do |i|
      assert_nil e.cause(i)
    end

    # cause without depth returns root cause
    assert_equal ne3, e.cause
    assert_equal 'Root cause', e.cause.message
  end
end
