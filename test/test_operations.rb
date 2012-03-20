require 'test/unit'
require 'cascading'

class TC_Operations < Test::Unit::TestCase
  include Operations

  def test_aggregator_function_ignore_values
    min = min_function 'min_field', :ignore => [nil].to_java(:string)
    assert_not_nil min
  end

  def test_aggregator_function_ignore_tuples
    first = first_function 'first_field', :ignore => [Java::CascadingTuple::Tuple.new(-1)].to_java(Java::CascadingTuple::Tuple)
    assert_not_nil first
  end

  def test_aggregator_function_ignore_exception
    assert_raise RuntimeError do
      avg = average_function 'avg_field', :ignore => [nil].to_java(:string)
      assert_not_nil avg
    end
  end
end
