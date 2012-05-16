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

  def test_coerce_to_java_int
    result = coerce_to_java 1

    assert_equal result.class, Java::JavaLang::Integer
    assert_equal result.java_class, 1.to_java(java.lang.Integer).java_class
  end

  def test_coerce_to_java_string
    result = coerce_to_java "a"

    assert_equal result.class, Java::JavaLang::String
    assert_equal result.java_class, "1".to_java(java.lang.String).java_class
  end

  def test_coerce_to_java_double
    result = coerce_to_java 5.4

    assert_equal result.class, Java::JavaLang::Double
    assert_equal result.java_class, 5.5.to_java(java.lang.Double).java_class
  end

  def test_coerce_to_java_nil
    result = coerce_to_java nil

    assert_equal result.class, NilClass
  end

  def test_coerce_to_java_other
    result = coerce_to_java [1,2,3]

    assert_equal result.class, Java::JavaLang::String
    assert_equal result.java_class, "1".to_java(java.lang.String).java_class
  end

  def test_to_java_comparable_array
    results = to_java_comparable_array([1, "string", 1.5, nil])

    assert_equal results.map{|i| i.class}, [Fixnum, String, Float, NilClass]
  end

end
